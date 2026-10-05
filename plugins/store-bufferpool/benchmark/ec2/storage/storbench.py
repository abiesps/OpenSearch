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

EFS connection evidence per job: the mount's tcp xprt line at the start and the end (connect_count delta =
reconnects during the job) and a summary of the NFS 4.1 SEQUENCE replies (nfs4:nfs4_sequence_done: highest slot
used, server highest_slotid and target_highest_slotid). --xprt-log samples the xprt line every second for the
whole run; --ref runs a fixed reference job at the start and end of every repetition (a state indicator).
--reconnect remount forces a new EFS connection (umount + mount) every --reps-per-connection repetitions and
records it in connections.jsonl; every record then carries conn and conn_pos, and every EFS record the
efs-proxy backend TCP connections sampled once mid-job (proxy_backend_midjob). --pre-positions limits the --pre
job to some positions inside a connection group (e.g. 2: the second cycle of every connection).
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


# kernel tcp xprt line (net/sunrpc/xprtsock.c xs_tcp_print_stats): srcport bind_count connect_count
# connect_time idle_time sends recvs bad_xids req_u bklog_u max_slots sending_u pending_u
XPRT_FIELDS = ["srcport", "bind_count", "connect_count", "connect_time", "idle_time", "sends", "recvs",
               "bad_xids", "req_u", "bklog_u", "max_slots", "sending_u", "pending_u"]


def xprt_dict(x):
    if x and x[0] == "tcp":
        x = x[1:]
    return {k: int(v) for k, v in zip(XPRT_FIELDS, x)}


def efs_mount_port():
    # the local port efs-proxy listens on for the current mount (a remount picks a new port; the old proxy
    # lingers until the watchdog's unmount grace period ends, so the pid must be found by port)
    for line in open("/proc/mounts"):
        f = line.split()
        if len(f) > 3 and f[1] == "/mnt/efs":
            m = re.search(r"(?:^|,)port=(\d+)", f[3])
            return int(m.group(1)) if m else None
    return None


def proxy_pid():
    port = efs_mount_port()
    out = sh(f"pgrep -f 'efs-proxy .*fs-060fb5da13f9a7e5b\\.mnt\\.efs\\.{port}( |$)' || true").split()
    return int(out[0]) if out else None


def proxy_backend():
    """efs-proxy's TCP connections to the EFS mount target (port 2049): count and local/remote addresses."""
    pid = proxy_pid()
    conns = []
    for line in sh("ss -tnpH state established '( dport = :2049 )' || true").splitlines():
        if pid is not None and f"pid={pid}," not in line:
            continue
        f = line.split()
        conns.append({"local": f[2], "remote": f[3]})
    return {"proxy_pid": pid, "count": len(conns), "conns": conns}


def proxy_cpu():
    try:
        pid = proxy_pid()
        v = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
        return {"pid": int(pid), "ticks": int(v[11]) + int(v[12])}
    except Exception as e:  # recorded as a gap, never as zero
        return {"error": str(e)}


TRACE_EVENTS = ("block/block_rq_issue", "nfs/nfs_initiate_read", "nfs4/nfs4_sequence_done")


def trace_start(storage):
    with open(f"{TRACE}/tracing_on", "w") as f: f.write("0")
    for ev in TRACE_EVENTS:
        with open(f"{TRACE}/events/{ev}/enable", "w") as f: f.write("0")
    if open(f"{TRACE}/buffer_size_kb").read().split()[0] != "16384":
        with open(f"{TRACE}/buffer_size_kb", "w") as f: f.write("16384")
    with open(f"{TRACE}/trace", "w") as f: f.write("")
    # EFS: also the NFS 4.1 SEQUENCE replies, which carry the slot the client used and the server's
    # highest_slotid / target_highest_slotid (how many session slots the server grants)
    evs = ["block/block_rq_issue"] if storage == "ebs" else ["nfs/nfs_initiate_read", "nfs4/nfs4_sequence_done"]
    for ev in evs:
        with open(f"{TRACE}/events/{ev}/enable", "w") as f: f.write("1")
    with open(f"{TRACE}/tracing_on", "w") as f: f.write("1")


SEQ_RE = re.compile(r"nfs4_sequence_done: error=(-?\d+) .*?slot_nr=(\d+) seq_nr=\d+ highest_slotid=(\d+) "
                    r"target_highest_slotid=(\d+)")


def trace_stop(storage, raw=False):
    with open(f"{TRACE}/tracing_on", "w") as f: f.write("0")
    hist, overrun, reqs = {}, 0, []
    seq = {"events": 0, "errors": 0, "max_slot_nr": None, "max_highest_slotid": None,
           "min_target_highest_slotid": None, "max_target_highest_slotid": None}
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
                s = SEQ_RE.search(line)
                if s:
                    err, slot, hi, tgt = (int(x) for x in s.groups())
                    seq["events"] += 1
                    seq["errors"] += 1 if err != 0 else 0
                    seq["max_slot_nr"] = max(slot, seq["max_slot_nr"] or 0)
                    if err == 0:  # a failed SEQUENCE (reconnect) carries no slot grant from the server
                        seq["max_highest_slotid"] = max(hi, seq["max_highest_slotid"] or 0)
                        seq["min_target_highest_slotid"] = min(tgt, tgt if seq["min_target_highest_slotid"] is None
                                                               else seq["min_target_highest_slotid"])
                        seq["max_target_highest_slotid"] = max(tgt, seq["max_target_highest_slotid"] or 0)
                    continue
                m = re.search(r"nfs_initiate_read: fileid=[0-9a-f]+:[0-9a-f]+:(\d+) fhandle=\S+ offset=(\d+) count=(\d+)", line)
                if not m:
                    continue
                size = int(m.group(3))
                if raw:
                    reqs.append((int(m.group(1)), int(m.group(2)), size))
            hist[size] = hist.get(size, 0) + 1
    for ev in TRACE_EVENTS:
        with open(f"{TRACE}/events/{ev}/enable", "w") as f: f.write("0")
    out = {"hist": {str(k): v for k, v in sorted(hist.items())}, "overrun": overrun}
    if storage == "efs":
        out["nfs4_sequence"] = seq
    if raw:
        out["requests"] = reqs
    return out


def fio_cmd(storage, regime, pat, qd, runtime, ramp):
    rw, bs = PATTERNS[pat]
    # regions start on a 1 MiB boundary: with a QD that does not divide the file size into whole pages (96, 192),
    # unaligned regions turned every 8 KiB read into a 12 KiB NFS READ (3 pages; review-a fix iteration,
    # raw/fio-knee/invalid/). Powers of two are unchanged by this.
    region = FILE_SIZE // qd // (1 << 20) * (1 << 20)
    eng = "psync"
    fad = "random" if regime == "poc-fio" else "0"
    direct = 1 if regime == "direct" else 0
    f = EBS_FILE if storage == "ebs" else EFS_FILE
    return ["fio", "--name=j", f"--filename={f}", f"--rw={rw}", f"--bs={bs}", f"--ioengine={eng}",
            f"--direct={direct}", f"--numjobs={qd}", "--group_reporting=1", "--time_based=1",
            f"--runtime={runtime}", f"--ramp_time={ramp}", f"--size={region}", f"--offset_increment={region}",
            f"--fadvise_hint={fad}", "--norandommap=0", "--randrepeat=0", "--thread=1",
            "--percentile_list=50:90:99:99.9", "--output-format=json"]


def utc(t):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + f".{int(t % 1 * 1000):03d}Z"


def run_job(storage, regime, pat, qd, rep, runtime, ramp, outdir, name=None):
    ra = 128 if regime == "poc-fio" else 0 if regime == "poc-exact" else default_ra(storage)
    set_ra(storage, ra)
    drop_caches()
    time.sleep(0.5)
    before = ebs_stat() if storage == "ebs" else efs_read_stat()
    ts_start = time.time()
    pc0 = proxy_cpu() if storage == "efs" else None
    pb0 = proxy_backend()["count"] if storage == "efs" else None
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
    backend = {}
    if storage == "efs":
        # efs-proxy's backend TCP connections, sampled once in the measured phase (does the proxy open more
        # than one connection to the mount target under load?)
        def sample_backend():
            try:
                backend.update(proxy_backend())
            except Exception as e:  # recorded as a gap, never as zero
                backend["error"] = str(e)
        bt = threading.Timer(ramp + min(2.0, runtime / 2), sample_backend); bt.daemon = True; bt.start()
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
        # per-job connection evidence (review-a pass 2, finding 2): the xprt line at the start and at the end
        # of the job; connect_count_end - connect_count_start = reconnects during the job
        xs, xe = xprt_dict(before["xprt"]), xprt_dict(after["xprt"])
        dev["xprt_start"], dev["xprt_end"] = xs, xe
        dev["reconnects"] = xe["connect_count"] - xs["connect_count"]
    name = name or f"{storage}_{regime}_{pat}_qd{qd}_rep{rep}"
    with open(os.path.join(outdir, name + ".json"), "w") as f:
        json.dump(tool, f)
    rec = {"name": name, "storage": storage, "regime": regime, "pattern": pat, "qd": qd, "rep": rep,
           "ts_start_utc": utc(ts_start), "ts_end_utc": utc(time.time()), "runtime_s": runtime, "ramp_s": ramp,
           "read_ahead_kb": ra, "read_ahead_kb_start": ra_start, "read_ahead_kb_after": ra_after,
           "read_ahead_kb_samples": sorted(samples), "ra_valid": ra_start == ra == ra_after and samples == {ra}, "wall_s": round(t1 - t0, 2), "cmd": cmd,
           "device": dev, "device_size_hist": tr["hist"], "trace_overrun": tr["overrun"],
           "summary": summarize(tool, regime)}
    if "nfs4_sequence" in tr:
        rec["nfs4_sequence"] = tr["nfs4_sequence"]
    if storage == "efs":
        bt.cancel()
        rec["proxy_backend_midjob"] = backend or {"error": "not sampled (job shorter than the sample time)"}
        rec["proxy_backend_count_start_end"] = [pb0, proxy_backend()["count"]]
    # equal-work check: bytes the tool consumed in the measured phase vs bytes the device delivered (incl. ramp)
    rec["tool_bytes"] = tool["bytes"] if tool.get("tool") == "wnread" else tool["jobs"][0]["read"]["io_bytes"]
    rec["device_bytes"] = dev["read_sectors"] * 512 if storage == "ebs" else sum(int(k) * v for k, v in tr["hist"].items())
    rec["device_over_tool"] = round(rec["device_bytes"] / rec["tool_bytes"], 3) if rec["tool_bytes"] else None
    if pc0 and pc1 and "ticks" in pc0 and "ticks" in pc1:
        rec["efs_proxy_cpu_pct"] = round((pc1["ticks"] - pc0["ticks"]) / os.sysconf("SC_CLK_TCK") / (t1 - t0) * 100, 1)
    return rec


def wait_backend_stable(timeout_s=40.0, stable_s=3.0):
    """Poll efs-proxy's backend TCP connection count every 0.5 s until it has not changed for stable_s seconds
    (efs-proxy scales from 1 to max_multiplexed_connections = 5 after 1 s at >= 300 MiB/s; efs-utils 3.3.2
    src/proxy/src/controller.rs DEFAULT_SCALE_UP_CONFIG, should_scale_up). Returns the timeline."""
    t0, tl, last, since = time.time(), [], None, time.time()
    while time.time() - t0 < timeout_s:
        b = proxy_backend()
        tl.append([round(time.time() - t0, 1), b["count"]])
        if b["count"] != last:
            last, since = b["count"], time.time()
        elif time.time() - since >= stable_s:
            break
        time.sleep(0.5)
    return {"count": last, "stable": time.time() - since >= stable_s, "timeline": tl}


def reconnect_efs(conn, outdir, kill_old_proxy=False):
    """Force a new EFS connection: umount + mount (fstab: efs _netdev,tls). This gives a new efs-proxy process
    on a new local port, a new NFS client and session, and a new TLS/TCP connection from efs-proxy to the mount
    target (review-a pass 3, finding 1: the connection is the unit of replication). Records the evidence.
    kill_old_proxy: stop the unmounted mount's efs-proxy right away (the watchdog does it after its unmount grace
    period of about 1 minute); needed when a firewall rule allows only one backend connection at a time."""
    before = {"proxy_backend": proxy_backend(), "mount_port": efs_mount_port()}
    t0 = time.time()
    for attempt in range(3):
        drop_caches()
        old_pid = proxy_pid()
        u = subprocess.run("umount /mnt/efs", shell=True, capture_output=True, text=True)
        if kill_old_proxy and old_pid:
            subprocess.run(f"kill -TERM {old_pid}", shell=True, capture_output=True)
            for _ in range(100):
                if not os.path.exists(f"/proc/{old_pid}"):
                    break
                time.sleep(0.1)
        m = subprocess.run("mount /mnt/efs", shell=True, capture_output=True, text=True)
        if subprocess.run("mountpoint -q /mnt/efs", shell=True).returncode == 0 and os.path.exists(EFS_FILE):
            break
        time.sleep(5)
    else:
        raise SystemExit(f"remount failed: umount {u.returncode} {u.stderr.strip()} mount {m.returncode} {m.stderr.strip()}")
    t1 = time.time()
    os.stat(EFS_FILE)  # one metadata round trip, so the backend connection exists before it is recorded
    s = efs_read_stat()
    ev = {"conn": conn, "ts_utc": utc(t1), "remount_s": round(t1 - t0, 3), "attempts": attempt + 1,
          "before": before, "mount_port": efs_mount_port(), "bdi": efs_bdi(),
          "read_ahead_kb_after_mount": int(open(ra_path("efs")).read()), "proxy_backend": proxy_backend(),
          "xprt": xprt_dict(s["xprt"]),
          "proxies_running": sh("pgrep -af 'efs-proxy .*fs-060fb5da13f9a7e5b' || true").strip().splitlines()}
    with open(os.path.join(outdir, "connections.jsonl"), "a") as f:
        f.write(json.dumps(ev) + "\n")
    print(f"conn {conn}: remount {ev['remount_s']} s, port {ev['mount_port']}, backend {ev['proxy_backend']}", flush=True)
    return ev


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
    ap.add_argument("--plan", default="", help="JSON file: list of [storage, regime, pattern, [qd, ...]]; "
                    "replaces the storages x regimes x patterns x qds grid (per-curve queue depths)")
    ap.add_argument("--block", default="main", help="label stored in every record (separate measurement block)")
    ap.add_argument("--ref", default="", help="storage,regime,pattern,qd: a reference job run at the start and at "
                    "the end of every repetition, not shuffled (state indicator for the jobs in between)")
    ap.add_argument("--pre", default="", help="storage,regime,pattern,qd: a load job run first in every repetition, "
                    "before the --ref job (tests whether a heavy job changes the state of the jobs after it)")
    ap.add_argument("--reconnect", choices=("none", "remount"), default="none", help="remount: force a new EFS "
                    "connection (umount + mount) before the first repetition of every connection group")
    ap.add_argument("--reps-per-connection", type=int, default=1, help="repetitions (cycles) per forced connection")
    ap.add_argument("--warmup", default="", help="storage,regime,pattern,qd: a job run right after every forced "
                    "reconnect, followed by a wait until efs-proxy's backend connection count is stable (so the "
                    "cycles run on the scaled-up connection set); stored with role warmup")
    ap.add_argument("--warmup-runtime", type=int, default=3)
    ap.add_argument("--kill-old-proxy", action="store_true", help="stop the unmounted mount's efs-proxy at once "
                    "on every forced reconnect (needed with a one-backend-connection firewall rule)")
    ap.add_argument("--pre-positions", default="", help="comma list of 1-based positions inside a connection group "
                    "that get the --pre job (default: every repetition)")
    ap.add_argument("--xprt-log", action="store_true", help="sample the EFS mount's xprt line every second for "
                    "the whole run into xprt-timeline.jsonl (reconnect times independent of the jobs)")
    a = ap.parse_args()
    os.makedirs(os.path.join(a.out, "raw"), exist_ok=True)
    if a.xprt_log:
        xstop = threading.Event()
        def xlog():
            with open(os.path.join(a.out, "xprt-timeline.jsonl"), "a") as xf:
                while not xstop.is_set():
                    try:
                        s = efs_read_stat()
                        xf.write(json.dumps({"ts_utc": utc(time.time()), "read_ops": s["ops"], "read_errors": s["errors"],
                                             "mount_port": efs_mount_port(), **xprt_dict(s["xprt"])}) + "\n")
                    except Exception as e:  # the mount is absent for a moment during a forced remount
                        xf.write(json.dumps({"ts_utc": utc(time.time()), "error": str(e)}) + "\n")
                    xf.flush()
                    xstop.wait(1.0)
        threading.Thread(target=xlog, daemon=True).start()
    def spec(s):
        if not s:
            return None
        st_, rg_, pat_, qd_ = s.split(",")
        if pat_ not in REGIMES[rg_]:
            raise SystemExit(f"job spec not supported: {s}")
        return (st_, rg_, pat_, int(qd_))
    ref, pre, warm = spec(a.ref), spec(a.pre), spec(a.warmup)
    pats = set(a.patterns.split(",")) if a.patterns else None
    jobs = []
    if a.plan:
        for st, rg, pat, qds in json.load(open(a.plan)):
            if pat not in REGIMES[rg] or (rg == "direct" and st == "efs"):
                raise SystemExit(f"plan entry not supported: {st} {rg} {pat}")
            jobs.extend((st, rg, pat, int(qd)) for qd in qds)
    else:
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
    pre_pos = {int(x) for x in a.pre_positions.split(",")} if a.pre_positions else None
    rpc = max(1, a.reps_per_connection)
    conn = None
    for rep in range(a.rep_start, a.rep_start + a.reps):
        pos = (rep - a.rep_start) % rpc + 1  # 1-based position inside the connection group
        order = [(j, "measured") for j in jobs]
        random.Random(a.seed + rep).shuffle(order)  # randomized order per repetition, storages interleaved
        if ref:
            order = [(ref, "ref-start")] + order + [(ref, "ref-end")]
        if pre and (pre_pos is None or pos in pre_pos):
            order = [(pre, "load")] + order
        names = [f"{st}_{rg}_{pat}_qd{qd}_rep{rep}" + ("" if role == "measured" else f"_{role}")
                 for (st, rg, pat, qd), role in order]
        if a.reconnect == "remount" and (pos == 1 or conn is None) and any(n not in done for n in names):
            # a resumed group starts on a fresh connection too; records carry the connection id, so the
            # analysis groups by connection, never by repetition number
            cpath = os.path.join(a.out, "connections.jsonl")
            conn = (sum(1 for _ in open(cpath)) if os.path.exists(cpath) else 0) + 1  # unique per remount
            reconnect_efs(conn, a.out, kill_old_proxy=a.kill_old_proxy)
            if warm:
                st, rg, pat, qd = warm
                wrec = run_job(st, rg, pat, qd, rep, a.warmup_runtime, a.ramp, os.path.join(a.out, "raw"),
                               name=f"{st}_{rg}_{pat}_qd{qd}_rep{rep}_warmup_conn{conn}")
                wst = wait_backend_stable()
                wrec.update({"role": "warmup", "block": a.block, "conn": conn, "conn_pos": 0,
                             "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "backend_after_wait": wst})
                log.write(json.dumps(wrec) + "\n"); log.flush()
                print(f"conn {conn}: warmup {wrec['summary']['MBps']:.0f} MB/s, backend mid-job "
                      f"{wrec['proxy_backend_midjob'].get('count')}, after wait {wst['count']} (stable {wst['stable']})",
                      flush=True)
        for i, ((st, rg, pat, qd), role) in enumerate(order):
            name = f"{st}_{rg}_{pat}_qd{qd}_rep{rep}" + ("" if role == "measured" else f"_{role}")
            if name in done:
                continue
            for attempt in range(3):
                rec = run_job(st, rg, pat, qd, rep, a.runtime, a.ramp, os.path.join(a.out, "raw"), name=name)
                rec["role"] = role
                if rec["ra_valid"]:
                    break
                with open(os.path.join(a.out, "invalid-ra.jsonl"), "a") as bad:
                    bad.write(json.dumps(rec) + "\n")
            else:
                raise SystemExit(f"{name}: read_ahead_kb did not hold in 3 attempts")
            rec["ts_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            rec["block"] = a.block
            if a.reconnect != "none":
                rec["conn"], rec["conn_pos"] = conn, pos
            log.write(json.dumps(rec) + "\n"); log.flush()
            s = rec["summary"]
            print(f"rep{rep} {i+1}/{len(order)} {name} iops={s['iops']:.0f} MBps={s['MBps']:.1f} "
                  f"p50={s['p50_us']:.0f}us p99={s['p99_us']:.0f}us dev={rec['device_size_hist']}", flush=True)
    # restore the mounted defaults when done
    set_ra("ebs", 128); set_ra("efs", 15360)
    if a.xprt_log:
        xstop.set(); time.sleep(1.2)


if __name__ == "__main__":
    main()
