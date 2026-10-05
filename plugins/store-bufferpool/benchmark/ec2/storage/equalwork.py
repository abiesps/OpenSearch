#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""Equal-work and request-size acceptance check over storbench records (storage-model.md section 2).

usage: equalwork.py <records.jsonl>[,<records2.jsonl>...]

Rule (stated in review-a fix iteration; applied to every record of every block):
  1. size: >= 99 % of the device requests (block_rq_issue on EBS, nfs_initiate_read on EFS) have the expected
     size: the job's read size for poc-exact, poc-fio, direct and s0-pread random; 4 KiB for s0-mmap-rnd. Regimes
     whose request size is the kernel's choice (s0-mmap, s0-pread sequential) are not size-checked: the size is
     the result. One 16 KiB XFS metadata read per EBS job is allowed by the 1 % margin.
  2. equal work, for regimes where the reader asks for every byte it reads (the same set as 1):
     1.05 <= device bytes / tool bytes <= 1.30. A clean run is (ramp + runtime) / runtime = 9 / 8 = 1.125, because
     the device counters include the 1 s ramp and fio's start and stop, and the tool bytes do not. The upper bound
     allows what rateprobe.py measured: at the EBS IOPS or throughput cap the volume serves the first ~0.3 s after
     the idle gap at up to 6 x the provisioned rate (1.75 GB in the first second against 1.06 GB/s sustained),
     which alone gives 1.25 at the cap; 1.30 leaves 0.05 for startup and teardown (0.07-0.45 s measured).
  For amplifying regimes (kernel readahead or mmap read-around) the ratio is reported as the measured
  amplification and is not checked.
Prints violations and the ratio distribution per regime class.
"""
import json, os, sys
from collections import defaultdict

CHECKED = {"poc-exact", "poc-fio", "direct", "s0-mmap-rnd"}
SIZES = {"r8k": 8192, "r32k": 32768, "r128k": 131072, "s128k": 131072, "r1m": 1 << 20}


def checked(r):
    return r["regime"] in CHECKED or (r["regime"] == "s0-pread" and r["pattern"].startswith("r"))


def expected_size(r):
    if r["regime"] == "s0-mmap-rnd":
        return 4096
    s = SIZES[r["pattern"]]
    # EBS splits requests above max_sectors_kb (256 KiB) into 256 KiB requests
    return min(s, 262144) if r["storage"] == "ebs" else s


def ratio(r, path):
    if r.get("device_over_tool") is not None:
        return r["device_over_tool"]
    t = json.load(open(os.path.join(os.path.dirname(path), "raw", r["name"] + ".json")))
    tb = t["bytes"] if t.get("tool") == "wnread" else t["jobs"][0]["read"]["io_bytes"]
    db = r["device"]["read_sectors"] * 512 if r["storage"] == "ebs" else sum(int(k) * v for k, v in r["device_size_hist"].items())
    return round(db / tb, 3)


viol, dist = [], defaultdict(list)
n = 0
for path in sys.argv[1].split(","):
    for l in open(path):
        r = json.loads(l)
        n += 1
        x = ratio(r, path)
        cls = "checked" if checked(r) else "amplifying"
        dist[(cls, r["storage"])].append(x)
        if not checked(r):
            continue
        h = {int(k): v for k, v in r["device_size_hist"].items()}
        tot = sum(h.values())
        share = h.get(expected_size(r), 0) / tot if tot else 0
        if share < 0.99:
            viol.append((r["name"], r.get("block", "main"), "size share %.4f" % share))
        if not 1.05 <= x <= 1.30:
            viol.append((r["name"], r.get("block", "main"), "device/tool %.3f" % x))
print(f"records {n}, violations {len(viol)}")
for v in viol:
    print("  VIOLATION", *v)
for k, xs in sorted(dist.items()):
    xs.sort()
    print(f"{k[0]:10s} {k[1]}: n={len(xs)} min={xs[0]:.3f} median={xs[len(xs)//2]:.3f} max={xs[-1]:.3f} "
          f"above 1.125+0.01: {sum(1 for x in xs if x > 1.135)}")
