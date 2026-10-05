#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""Bufferpool load latency per window from bpload.jsonl.

Unit = one (op execution, file type) delta of /_bufferpool/stats; its mean load latency = load_time_micros /
(reads + prefetch_reads). Splits: storage x window size class (reads_by_size key: 32768 = (16 KiB, 32 KiB],
131072 = (64 KiB, 128 KiB], EOF-clipped windows fall in a lower class) x demand-only (prefetch_reads == 0) vs with
prefetch, CSS none vs all. Reports read-weighted mean, and median / p90 over units; plus the device check and the
NFS / block-device view per op (device reads, bytes, NFS RTT) for the same ops.
"""
import json, statistics as st, sys
from collections import defaultdict

recs = [json.loads(l) for l in open(sys.argv[1]) if '"type": "op"' in l]
out_json, out_md = sys.argv[2], sys.argv[3]
units = defaultdict(list)       # (storage, size, kind, css) -> [(mean_us, n_reads)]
for r in recs:
    if r["error"]:
        continue
    for ft, f in r["files"].items():
        n = f["reads"] + f["prefetch_reads"]
        if not n or len(f["reads_by_size"]) != 1:
            continue  # a unit must have one window size class so its mean belongs to that size
        size = next(iter(f["reads_by_size"]))
        kind = "demand" if f["prefetch_reads"] == 0 else "with_prefetch"
        mean = f["load_time_micros"] / n
        for c in (r["css"], "both"):
            units[(r["storage"], size, kind, c)].append((mean, n))
            units[(r["storage"], size, "all", c)].append((mean, n))


def pct(xs, q):
    xs = sorted(xs)
    return xs[int(q * (len(xs) - 1))]


rows = {}
for k, v in sorted(units.items()):
    ms = [m for m, _ in v]
    n = sum(c for _, c in v)
    rows["|".join(k)] = {"units": len(v), "reads": n, "weighted_mean_us": round(sum(m * c for m, c in v) / n, 1),
                         "median_us": round(st.median(ms), 1), "p90_us": round(pct(ms, 0.9), 1)}
# per-op storage view: NFS RTT per READ and total bufferpool reads; device check summary
chk = defaultdict(int)
nfs = defaultdict(list)
inflight = defaultdict(list)
for r in recs:
    chk[(r["storage"], r["device_check"]["valid"])] += 1
    d = r["device"]
    if r["storage"] == "efs" and d.get("ops"):
        nfs[r["css"]].append((d["rtt_ms"] / d["ops"], d["ops"]))
    inflight[(r["storage"], r["css"])].append(r.get("max_prefetch_reads_in_flight") or 0)
summary = {
    "device_check": {f"{s}|{'valid' if v else 'INVALID'}": n for (s, v), n in chk.items()},
    "ops": len(recs), "errors": sum(1 for r in recs if r["error"]),
    "efs_nfs_rtt_ms_weighted": {c: round(sum(a * b for a, b in v) / sum(b for _, b in v), 2) for c, v in nfs.items()},
    "max_prefetch_reads_in_flight_node_max": {f"{s}|{c}": max(v) for (s, c), v in inflight.items()},
    "resident_bytes_before_max": max(r.get("resident_bytes_before", 0) for r in recs),
    "drop_rounds_max": max(r.get("drop_rounds", 1) for r in recs),
}
json.dump({"rows": rows, "summary": summary}, open(out_json, "w"), indent=1)
with open(out_md, "w") as f:
    f.write("<table><tr><th>storage</th><th>window class (bytes)</th><th>reads</th><th>CSS</th><th>units</th>"
            "<th>reads</th><th>read-weighted mean us</th><th>median us</th><th>p90 us</th></tr>\n")
    for k, v in rows.items():
        s, size, kind, c = k.split("|")
        f.write(f"<tr><td>{s}</td><td>{size}</td><td>{kind}</td><td>{c}</td><td>{v['units']}</td><td>{v['reads']}</td>"
                f"<td>{v['weighted_mean_us']}</td><td>{v['median_us']}</td><td>{v['p90_us']}</td></tr>\n")
    f.write("</table>\n")
print(json.dumps(summary, indent=1))
for k, v in rows.items():
    if k.endswith("|both"):
        print(k, v)
