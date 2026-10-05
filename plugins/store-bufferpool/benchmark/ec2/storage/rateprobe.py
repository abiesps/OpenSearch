#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""Equal-work probe (review-a finding 5): where do the device bytes outside fio's measured window come from?

Runs one storbench job (same regime, readahead, cache drop, counters and command line as storbench.py) with the
ftrace request log kept, then bins the device requests by their trace timestamp into 100 ms buckets relative to
the job start, so the device rate during fio startup, the ramp second, the 8 measured seconds and the teardown is
visible. Writes one JSON per job.

usage: rateprobe.py <out.json> <storage> <regime> <pattern> <qd>
"""
import json, re, subprocess, sys, time
sys.path.insert(0, "/opt/coldpath-storage")
import storbench as sb

out, storage, regime, pat, qd = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5])
ra = 128 if regime == "poc-fio" else 0 if regime == "poc-exact" else sb.default_ra(storage)
sb.set_ra(storage, ra)
sb.drop_caches()
time.sleep(0.5)
before = sb.ebs_stat() if storage == "ebs" else sb.efs_read_stat()
sb.trace_start(storage)
t_up = float(open("/proc/uptime").read().split()[0])  # ftrace default clock (local) ~ uptime seconds
cmd = sb.fio_cmd(storage, regime, pat, qd, 8, 1)
t0 = time.time()
p = subprocess.run(cmd, capture_output=True, text=True)
wall = time.time() - t0
with open(f"{sb.TRACE}/tracing_on", "w") as f:
    f.write("0")
after = sb.ebs_stat() if storage == "ebs" else sb.efs_read_stat()
tool = json.loads(p.stdout[p.stdout.index("{"):])
ev = "block_rq_issue" if storage == "ebs" else "nfs_initiate_read"
dev = open(f"/sys/block/{sb.EBS_DEV}/dev").read().strip().replace(":", ",")
bins = {}
first = last = None
n = 0
with open(f"{sb.TRACE}/trace") as f:
    for line in f:
        if ev not in line:
            continue
        m = re.search(r"\s(\d+\.\d+): ", line)
        if not m:
            continue
        ts = float(m.group(1))
        if storage == "ebs":
            mm = re.search(r"block_rq_issue: (\d+),(\d+) (\S+) \d+ \(\S*\) \d+ \+ (\d+)", line)
            if not mm or f"{mm.group(1)},{mm.group(2)}" != dev or "R" not in mm.group(3):
                continue
            size = int(mm.group(4)) * 512
        else:
            mm = re.search(r"count=(\d+)", line)
            size = int(mm.group(1))
        b = int((ts - t_up) * 10)
        bins[b] = bins.get(b, 0) + size
        first = ts if first is None else first
        last = ts
        n += 1
for e in ("block/block_rq_issue", "nfs/nfs_initiate_read"):
    with open(f"{sb.TRACE}/events/{e}/enable", "w") as f:
        f.write("0")
r = tool["jobs"][0]["read"]
res = {"job": [storage, regime, pat, qd], "cmd": cmd, "wall_s": round(wall, 3),
       "tool_bytes": r["io_bytes"], "tool_runtime_ms": r["runtime"], "tool_MBps": r["bw_bytes"] / 1e6,
       "device_requests": n, "device_bytes_trace": sum(bins.values()),
       "device_counter_delta": {k: after[k] - before[k] for k in before if k != "xprt"},
       "first_request_s": round(first - t_up, 3) if first else None, "last_request_s": round(last - t_up, 3) if last else None,
       "MBps_per_100ms": {str(b / 10): round(v / 0.1 / 1e6, 1) for b, v in sorted(bins.items())}}
json.dump(res, open(out, "w"), indent=1)
print(json.dumps({k: v for k, v in res.items() if k != "MBps_per_100ms"}))
