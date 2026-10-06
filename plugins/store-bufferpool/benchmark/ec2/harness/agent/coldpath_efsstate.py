#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Amazon EFS connection STATE probe: the storage model's state test (storage-model.md section 4.2.1; storage/state.py),
run on the measurement host between JVM runs, so every EFS run of a coldbench session can be classified as fast,
degraded or slow (common-rules.md "Precision target for phase A baselines": report the noise floor with and without
degraded samples).
The test is the storage branch's reference job, unchanged: fio, 8 KiB random reads, 64 jobs (queue depth 64),
psync, buffered, POSIX_FADV_RANDOM, read_ahead_kb 128 on the mount's bdi for the job (then the previous value is
written back), page cache dropped first, 1 s ramp + 8 s measured, on a 64 GiB file of real data on the same mount,
regions on 1 MiB boundaries (storage/storbench.py fio_cmd, regime poc-fio, pattern r8k). Level of the fio read IOPS:
fast >= 17,000, degraded 12,000-17,000, slow < 12,000 (storage/state.py FAST_MIN, SLOW_MAX). The state only changes
at a reconnect, so the probe also returns the NFS client's xprt connect_count and efs-proxy's pid and backend count.
The file is created once by the host setup (fio sequential O_DIRECT 1 MiB writes, as storage/prepare.sh), never
here; without it (or without fio) the probe returns a gap, never a level.
"""
import json
import os
import re
import subprocess
import time

FAST_MIN, SLOW_MAX = 17000.0, 12000.0
QD, BS, RUNTIME_S, RAMP_S, RA_KB = 64, 8192, 8, 1, 128
FILE_SIZE = 64 << 30


def level(iops):
    return "fast" if iops >= FAST_MIN else "slow" if iops < SLOW_MAX else "degraded"


def fio_cmd(path, size=FILE_SIZE):
    region = size // QD // (1 << 20) * (1 << 20)
    return ["fio", "--name=j", f"--filename={path}", "--rw=randread", f"--bs={BS}", "--ioengine=psync", "--direct=0",
            f"--numjobs={QD}", "--group_reporting=1", "--time_based=1", f"--runtime={RUNTIME_S}", f"--ramp_time={RAMP_S}",
            f"--size={region}", f"--offset_increment={region}", "--fadvise_hint=random", "--norandommap=0",
            "--randrepeat=0", "--thread=1", "--percentile_list=50:90:99:99.9", "--output-format=json"]


def _xprt(mountpoint):
    """The NFS client's xprt line of the mount (srcport bind_count connect_count ...) as a dict, or None."""
    with open("/proc/self/mountstats") as f:
        txt = f.read()
    sec = txt.split(f"mounted on {mountpoint} ")
    if len(sec) < 2:
        return None
    m = re.search(r"^\s+xprt:\s+tcp\s+(.*)$", sec[1].split("\ndevice ")[0], re.M)
    if not m:
        return None
    v = [int(x) for x in m.group(1).split()]
    return {"srcport": v[0], "bind_count": v[1], "connect_count": v[2]}


def probe(st, cfg, backend_connections):
    """st: the agent's storage dict of an EFS arm; cfg: agent config "efs_state_probe" {"file": PATH}."""
    if not st.get("nfs"):
        return {"gap": "not an NFS mount"}
    path = (cfg or {}).get("file")
    if not path:
        return {"gap": "efs_state_probe.file not configured"}
    try:
        size = os.stat(path).st_size
    except OSError as e:
        return {"gap": f"probe file {path}: {e}"}
    if size < FILE_SIZE:
        return {"gap": f"probe file {path} has {size} bytes, the state test needs {FILE_SIZE}"}
    mount = st["mount"]
    ra_path = f"/sys/class/bdi/{st['bdi']}/read_ahead_kb"
    with open(ra_path) as f:
        ra_before = int(f.read())
    x0, b0 = _xprt(mount["mountpoint"]), backend_connections(mount)
    out = {"file": path, "rule": {"fast_min": FAST_MIN, "slow_max": SLOW_MAX, "qd": QD, "bs": BS, "runtime_s": RUNTIME_S,
                                  "ramp_s": RAMP_S, "read_ahead_kb": RA_KB, "fadvise": "random", "engine": "psync"}}
    try:
        with open(ra_path, "w") as f:
            f.write(f"{RA_KB}\n")
        subprocess.run(["sync"], check=True)
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("3\n")
        time.sleep(0.5)
        t0 = time.time()
        p = subprocess.run(fio_cmd(path), capture_output=True, text=True, timeout=120)
        out["wall_s"] = round(time.time() - t0, 2)
    except FileNotFoundError:
        return {"gap": "fio is not installed"}
    finally:
        with open(ra_path, "w") as f:
            f.write(f"{ra_before}\n")
    with open(ra_path) as f:
        out["read_ahead_kb_restored"] = int(f.read()) == ra_before
    if p.returncode != 0:
        return {"gap": f"fio failed: {p.stderr[-500:]}"}
    r = json.loads(p.stdout[p.stdout.index("{"):])["jobs"][0]["read"]
    pct = r["clat_ns"].get("percentile", {})
    x1, b1 = _xprt(mount["mountpoint"]), backend_connections(mount)
    out.update({"iops": round(r["iops"]), "p50_us": pct.get("50.000000", 0) / 1000.0, "p99_us": pct.get("99.000000", 0) / 1000.0,
                "level": level(r["iops"]), "xprt_start": x0, "xprt_end": x1,
                "reconnects": (x1["connect_count"] - x0["connect_count"]) if x0 and x1 else None,
                "efs_connections_start": b0, "efs_connections_end": b1})
    return out
