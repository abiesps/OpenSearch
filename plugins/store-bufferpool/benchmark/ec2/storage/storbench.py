#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""Storage characterization driver for the storage branch (EBS gp3 vs EFS), run as root on the data node.

Per job: set the storage's kernel readahead (before the file is opened, because Linux copies it into the
file's f_ra at open), sync + drop the page cache, snapshot device counters, run fio (or wnread), snapshot the
counters again, and keep the raw tool JSON plus the device request-size histogram (ftrace:
block:block_rq_issue on EBS, nfs:nfs_initiate_read on EFS) and the totals from /sys/block/<dev>/stat or the
READ line of /proc/self/mountstats.

Regimes (see storage-model.md):
  poc-fio    psync buffered, POSIX_FADV_RANDOM on the file, read_ahead_kb=128: one device IO of exactly bs
             per pread (the device behaviour of a bufferpool window; probe in bp-iosize-review-readahead-probe.txt)
  poc-exact  wnread: read_ahead_kb=0 + posix_fadvise(WILLNEED, window) on a second fd + one pread (the
             bufferpool's exact call sequence, ec2-bench eb26156111a)
  s0-mmap    wnread mmap: whole file mapped, MADV_NORMAL, copy per op, mounted default readahead (stock
             MMapDirectory). fio's mmap engine is NOT used: with numjobs on one file it ignores offset_increment
             (all jobs read the same pages; device bytes << fio bytes, raw/invalid-fio-mmap.txt).
  s0-mmap-rnd wnread mmap-random: MADV_RANDOM on the mapping (stock mmap of RANDOM-advised files)
  s0-pread   psync buffered, no fadvise, mounted default readahead (stock NIOFS files / plain buffered read)
  direct     psync O_DIRECT (EBS only, reference; EFS refuses direct IO below 1 MiB)
"""
import argparse, json, os, random, re, subprocess, sys, threading, time

EBS_DEV = "nvme1n1"
EBS_FILE = "/data/fio/f64g"
EFS_FILE = "/mnt/efs/storage-branch/fio/f64g"
FILE_SIZE = 64 << 30
TRACE = "/sys/kernel/tracing"
WNREAD = "/opt/coldpath-storage/wnread"

PATTERNS = {"r8k": ("randread", 8192), "r32k": ("randread", 32768), "r128k": ("randread", 131072),
            "s128k": ("read", 131072), "r1m": ("randread", 1 << 20)}
REGIMES = {
    "poc-fio": ["r8k", "r32k", "r128k", "s128k", "r1m"],
    "poc-exact": ["r32k", "r128k", "s128k"],
    "s0-mmap": ["r32k", "r128k", "s128k"],
    "s0-mmap-rnd": ["r32k"],
    "s0-pread": ["r32k", "s128k"],
    "direct": ["r8k", "r32k", "r128k", "r1m"],
}
QDS = [1, 2, 4, 8, 16, 32, 64]


def sh(cmd):
    return subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True).stdout


def efs_bdi():
    return sh("mountpoint -d /mnt/efs").strip()


def ra_path(storage):
    return f"/sys/block/{EBS_DEV}/queue/read_ahead_kb" if storage == "ebs" else f"/sys/class/bdi/{efs_bdi()}/read_ahead_kb"


def default_ra(storage):
    return 128 if storage == "ebs" else 15360  # recorded at setup: EBS device default, EFS efs-utils/udev value


def set_ra(storage, kb):
    p = ra_path(storage)
    with open(p, "w") as f:
        f.write(str(kb))
    got = int(open(p).read())
    if got != kb:
        raise SystemExit(f"read_ahead_kb {p} = {got}, wanted {kb}")
    return got


def drop_caches():
    os.sync()
    with open("/proc/sys/vm/drop_caches", "w") as f:
        f.write("3")


def ebs_stat():
    v = open(f"/sys/block/{EBS_DEV}/stat").read().split()
    return {"reads": int(v[0]), "read_merges": int(v[1]), "read_sectors": int(v[2]), "read_ticks_ms": int(v[3])}


def efs_read_stat():
    txt = open("/proc/self/mountstats").read()
    sec = txt.split("mounted on /mnt/efs ")[1].split("\ndevice ")[0]
    m = re.search(r"^\s+READ: (.*)$", sec, re.M)
    v = [int(x) for x in m.group(1).split()]
    x = re.search(r"^\s+xprt:\s+(.*)$", sec, re.M).group(1).split()
    return {"ops": v[0], "trans": v[1], "timeouts": v[2], "bytes_sent": v[3], "bytes_recv": v[4],
            "queue_ms": v[5], "rtt_ms": v[6], "exec_ms": v[7], "errors": v[8] if len(v) > 8 else 0,
            "xprt": x}


def proxy_cpu():
    try:
        pid = sh("pgrep -f 'efs-proxy.*fs-060fb5da13f9a7e5b' | head -1").strip()
        v = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
        return {"pid": int(pid), "ticks": int(v[11]) + int(v[12])}
    except Exception as e:  # recorded as a gap, never as zero
        return {"error": str(e)}


def trace_start(storage):
    with open(f"{TRACE}/tracing_on", "w") as f: f.write("0")
    for ev in ("block/block_rq_issue", "nfs/nfs_initiate_read"):
        with open(f"{TRACE}/events/{ev}/enable", "w") as f: f.write("0")
    if open(f"{TRACE}/buffer_size_kb").read().split()[0] != "16384":
        with open(f"{TRACE}/buffer_size_kb", "w") as f: f.write("16384")
    with open(f"{TRACE}/trace", "w") as f: f.write("")
    ev = "block/block_rq_issue" if storage == "ebs" else "nfs/nfs_initiate_read"
    with open(f"{TRACE}/events/{ev}/enable", "w") as f: f.write("1")
    with open(f"{TRACE}/tracing_on", "w") as f: f.write("1")


def trace_stop(storage, raw=False):
    with open(f"{TRACE}/tracing_on", "w") as f: f.write("0")
    hist, overrun, reqs = {}, 0, []
    for cpu in os.listdir(f"{TRACE}/per_cpu"):
        st = open(f"{TRACE}/per_cpu/{cpu}/stats").read()
        overrun += int(re.search(r"overrun: (\d+)", st).group(1))
    dev = None
    with open(f"{TRACE}/trace") as f:
        for line in f:
            if storage == "ebs":
                # block_rq_issue: 259,1 R 131072 () 123456 + 256 [fio]
                m = re.search(r"block_rq_issue: (\d+),(\d+) (\S+) \d+ \(\S*\) (\d+) \+ (\d+)", line)
                if not m or "R" not in m.group(3) or m.group(3).startswith("F"):
                    continue
                key = f"{m.group(1)},{m.group(2)}"
                if dev is None:
                    dev = open(f"/sys/block/{EBS_DEV}/dev").read().strip().replace(":", ",")
                if key != dev:
                    continue
                size = int(m.group(5)) * 512
                if raw:
                    reqs.append((int(m.group(4)) * 512, size))
            else:
                m = re.search(r"nfs_initiate_read: fileid=[0-9a-f]+:[0-9a-f]+:(\d+) fhandle=\S+ offset=(\d+) count=(\d+)", line)
                if not m:
                    continue
                size = int(m.group(3))
                if raw:
                    reqs.append((int(m.group(1)), int(m.group(2)), size))
            hist[size] = hist.get(size, 0) + 1
    for ev in ("block/block_rq_issue", "nfs/nfs_initiate_read"):
        with open(f"{TRACE}/events/{ev}/enable", "w") as f: f.write("0")
    out = {"hist": {str(k): v for k, v in sorted(hist.items())}, "overrun": overrun}
    if raw:
        out["requests"] = reqs
    return out


def fio_cmd(storage, regime, pat, qd, runtime, ramp):
    rw, bs = PATTERNS[pat]
    region = FILE_SIZE // qd
    eng = "psync"
    fad = "random" if regime == "poc-fio" else "0"
    direct = 1 if regime == "direct" else 0
    f = EBS_FILE if storage == "ebs" else EFS_FILE
    return ["fio", "--name=j", f"--filename={f}", f"--rw={rw}", f"--bs={bs}", f"--ioengine={eng}",
            f"--direct={direct}", f"--numjobs={qd}", "--group_reporting=1", "--time_based=1",
            f"--runtime={runtime}", f"--ramp_time={ramp}", f"--size={region}", f"--offset_increment={region}",
            f"--fadvise_hint={fad}", "--norandommap=0", "--randrepeat=0", "--thread=1",
            "--percentile_list=50:90:99:99.9", "--output-format=json"]


def run_job(storage, regime, pat, qd, rep, runtime, ramp, outdir):
    ra = 128 if regime == "poc-fio" else 0 if regime == "poc-exact" else default_ra(storage)
    set_ra(storage, ra)
    drop_caches()
    time.sleep(0.5)
    before = ebs_stat() if storage == "ebs" else efs_read_stat()
    pc0 = proxy_cpu() if storage == "efs" else None
    trace_start(storage)
    # readahead must hold for the whole job (efs-utils watchdog rewrote the EFS bdi to 15360 about once a
    # second before optimize_readahead=false; common-rules.md DECISION ~22:00): sample it every 100 ms
    ra_start = int(open(ra_path(storage)).read())
    samples, stop = set(), threading.Event()
    def sampler():
        rp = ra_path(storage)
        while not stop.is_set():
            samples.add(int(open(rp).read()))
            stop.wait(0.1)
    th = threading.Thread(target=sampler, daemon=True); th.start()
    t0 = time.time()
    if regime in ("poc-exact", "s0-mmap", "s0-mmap-rnd"):
        _, bs = PATTERNS[pat]
        f = EBS_FILE if storage == "ebs" else EFS_FILE
        mode = {"poc-exact": "willneed", "s0-mmap": "mmap", "s0-mmap-rnd": "mmap-random"}[regime]
        cmd = [WNREAD, f, str(bs), "seq" if pat.startswith("s") else "rand", str(qd), str(runtime), str(ramp), mode]
    else:
        cmd = fio_cmd(storage, regime, pat, qd, runtime, ramp)
    p = subprocess.run(cmd, capture_output=True, text=True)
    t1 = time.time()
    stop.set(); th.join()
    tr = trace_stop(storage)
    after = ebs_stat() if storage == "ebs" else efs_read_stat()
    pc1 = proxy_cpu() if storage == "efs" else None
    ra_after = int(open(ra_path(storage)).read())
    if p.returncode != 0:
        raise SystemExit(f"job failed: {' '.join(cmd)}\n{p.stderr}")
    tool = json.loads(p.stdout[p.stdout.index("{"):])
    if storage == "ebs":
        dev = {k: after[k] - before[k] for k in before}
        dev["avg_read_bytes"] = dev["read_sectors"] * 512 / dev["reads"] if dev["reads"] else None
    else:
        dev = {k: after[k] - before[k] for k in before if k != "xprt"}
        dev["avg_recv_bytes_per_read"] = dev["bytes_recv"] / dev["ops"] if dev["ops"] else None
        dev["avg_rtt_ms"] = dev["rtt_ms"] / dev["ops"] if dev["ops"] else None
        dev["avg_exec_ms"] = dev["exec_ms"] / dev["ops"] if dev["ops"] else None
        dev["avg_queue_ms"] = dev["queue_ms"] / dev["ops"] if dev["ops"] else None
    name = f"{storage}_{regime}_{pat}_qd{qd}_rep{rep}"
    with open(os.path.join(outdir, name + ".json"), "w") as f:
        json.dump(tool, f)
    rec = {"name": name, "storage": storage, "regime": regime, "pattern": pat, "qd": qd, "rep": rep,
           "read_ahead_kb": ra, "read_ahead_kb_start": ra_start, "read_ahead_kb_after": ra_after,
           "read_ahead_kb_samples": sorted(samples), "ra_valid": ra_start == ra == ra_after and samples == {ra}, "wall_s": round(t1 - t0, 2), "cmd": cmd,
           "device": dev, "device_size_hist": tr["hist"], "trace_overrun": tr["overrun"],
           "summary": summarize(tool, regime)}
    # equal-work check: bytes the tool consumed in the measured phase vs bytes the device delivered (incl. ramp)
    rec["tool_bytes"] = tool["bytes"] if tool.get("tool") == "wnread" else tool["jobs"][0]["read"]["io_bytes"]
    rec["device_bytes"] = dev["read_sectors"] * 512 if storage == "ebs" else sum(int(k) * v for k, v in tr["hist"].items())
    rec["device_over_tool"] = round(rec["device_bytes"] / rec["tool_bytes"], 3) if rec["tool_bytes"] else None
    if pc0 and pc1 and "ticks" in pc0 and "ticks" in pc1:
        rec["efs_proxy_cpu_pct"] = round((pc1["ticks"] - pc0["ticks"]) / os.sysconf("SC_CLK_TCK") / (t1 - t0) * 100, 1)
    return rec


def summarize(tool, regime):
    if tool.get("tool") == "wnread":
        l = tool["lat_us"]
        return {"iops": tool["iops"], "MBps": tool["MBps"], "lat_us_mean": l["mean"], "p50_us": l["p50"],
                "p90_us": l["p90"], "p99_us": l["p99"], "p999_us": l["p999"], "ops": tool["ops"],
                "short_reads": tool["short_reads"], "errors": tool["errors"], "hint_errors": tool["hint_errors"]}
    r = tool["jobs"][0]["read"]
    pct = r["clat_ns"].get("percentile", {})
    g = lambda k: pct.get(k, 0) / 1000.0
    return {"iops": r["iops"], "MBps": r["bw_bytes"] / 1e6, "lat_us_mean": r["lat_ns"]["mean"] / 1000.0,
            "p50_us": g("50.000000"), "p90_us": g("90.000000"), "p99_us": g("99.000000"),
            "p999_us": g("99.900000"), "ops": r["total_ios"], "errors": tool["jobs"][0]["error"]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--rep-start", type=int, default=1)
    ap.add_argument("--runtime", type=int, default=8)
    ap.add_argument("--ramp", type=int, default=1)
    ap.add_argument("--storages", default="ebs,efs")
    ap.add_argument("--regimes", default=",".join(REGIMES))
    ap.add_argument("--qds", default=",".join(map(str, QDS)))
    ap.add_argument("--patterns", default="")
    ap.add_argument("--seed", type=int, default=20261004)
    a = ap.parse_args()
    os.makedirs(os.path.join(a.out, "raw"), exist_ok=True)
    pats = set(a.patterns.split(",")) if a.patterns else None
    jobs = []
    for st in a.storages.split(","):
        for rg in a.regimes.split(","):
            if rg == "direct" and st == "efs":
                continue
            for pat in REGIMES[rg]:
                if pats and pat not in pats:
                    continue
                for qd in map(int, a.qds.split(",")):
                    jobs.append((st, rg, pat, qd))
    log = open(os.path.join(a.out, "records.jsonl"), "a")
    done = set()
    try:
        for line in open(os.path.join(a.out, "records.jsonl")):
            done.add(json.loads(line)["name"])
    except FileNotFoundError:
        pass
    for rep in range(a.rep_start, a.rep_start + a.reps):
        order = jobs[:]
        random.Random(a.seed + rep).shuffle(order)  # randomized order per repetition, storages interleaved
        for i, (st, rg, pat, qd) in enumerate(order):
            name = f"{st}_{rg}_{pat}_qd{qd}_rep{rep}"
            if name in done:
                continue
            for attempt in range(3):
                rec = run_job(st, rg, pat, qd, rep, a.runtime, a.ramp, os.path.join(a.out, "raw"))
                if rec["ra_valid"]:
                    break
                with open(os.path.join(a.out, "invalid-ra.jsonl"), "a") as bad:
                    bad.write(json.dumps(rec) + "\n")
            else:
                raise SystemExit(f"{name}: read_ahead_kb did not hold in 3 attempts")
            rec["ts_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            log.write(json.dumps(rec) + "\n"); log.flush()
            s = rec["summary"]
            print(f"rep{rep} {i+1}/{len(order)} {name} iops={s['iops']:.0f} MBps={s['MBps']:.1f} "
                  f"p50={s['p50_us']:.0f}us p99={s['p99_us']:.0f}us dev={rec['device_size_hist']}", flush=True)
    # restore the mounted defaults when done
    set_ra("ebs", 128); set_ra("efs", 15360)


if __name__ == "__main__":
    main()
