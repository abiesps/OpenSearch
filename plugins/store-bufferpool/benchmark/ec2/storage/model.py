#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""Build the storage model tables from records.jsonl (storbench.py) and bpload.jsonl (bpload.py).

Per (storage, regime, pattern, qd): median over repetitions of p50 / p90 / p99 latency, IOPS, MB/s, plus the range
across repetitions; device request-size mode and share; device-bytes / tool-bytes (amplification, the ramp
second is included in the device bytes, so a clean run is about (runtime + ramp) / runtime = 1.125).
Knee per curve = smallest QD that reaches >= 90 % of the curve's max throughput; latency at the knee vs QD 1.
Writes model.json and model-tables.md (markdown and HTML tables).
"""
import json, statistics as st, sys
from collections import defaultdict

recs = [json.loads(l) for l in open(sys.argv[1])]
out_json, out_md = sys.argv[2], sys.argv[3]
bp = [json.loads(l) for l in open(sys.argv[4])] if len(sys.argv) > 4 else []

g = defaultdict(list)
for r in recs:
    g[(r["storage"], r["regime"], r["pattern"], r["qd"])].append(r)


def med(xs):
    return st.median(xs) if xs else None


rows = {}
for k, rs in sorted(g.items()):
    s = [r["summary"] for r in rs]
    hist = defaultdict(int)
    for r in rs:
        for size, n in r["device_size_hist"].items():
            hist[int(size)] += n
    tot = sum(hist.values()) or 1
    mode = max(hist, key=hist.get) if hist else None
    dev = rs[0]["device"]
    row = {
        "reps": len(rs),
        "p50_us": med([x["p50_us"] for x in s]), "p90_us": med([x["p90_us"] for x in s]),
        "p99_us": med([x["p99_us"] for x in s]), "p999_us": med([x["p999_us"] for x in s]),
        "p50_us_range": [min(x["p50_us"] for x in s), max(x["p50_us"] for x in s)],
        "p99_us_range": [min(x["p99_us"] for x in s), max(x["p99_us"] for x in s)],
        "iops": med([x["iops"] for x in s]), "MBps": med([x["MBps"] for x in s]),
        "MBps_range": [min(x["MBps"] for x in s), max(x["MBps"] for x in s)],
        "device_size_mode": mode, "device_size_mode_share": round(hist[mode] / tot, 4) if mode else None,
        "device_reads": tot, "device_hist": {str(a): b for a, b in sorted(hist.items())},
        "device_over_tool": med([r.get("device_over_tool") for r in rs if r.get("device_over_tool")]),
        "read_ahead_kb": rs[0]["read_ahead_kb"],
        "trace_overrun": sum(r["trace_overrun"] for r in rs),
    }
    if k[0] == "efs":
        row["nfs_rtt_ms"] = med([r["device"]["avg_rtt_ms"] for r in rs if r["device"].get("avg_rtt_ms") is not None])
        row["nfs_exec_ms"] = med([r["device"]["avg_exec_ms"] for r in rs if r["device"].get("avg_exec_ms") is not None])
        row["efs_proxy_cpu_pct"] = med([r.get("efs_proxy_cpu_pct") for r in rs if r.get("efs_proxy_cpu_pct") is not None])
    else:
        ticks = [r["device"]["read_ticks_ms"] / r["device"]["reads"] for r in rs if r["device"].get("reads")]
        row["dev_await_ms"] = med(ticks)
    rows[k] = row

curves = defaultdict(dict)
for (stg, rg, pat, qd), row in rows.items():
    curves[(stg, rg, pat)][qd] = row
knees = {}
for k, c in curves.items():
    qds = sorted(c)
    mx = max(c[q]["MBps"] for q in qds)
    knee = next(q for q in qds if c[q]["MBps"] >= 0.9 * mx)
    q1 = c[qds[0]]
    knees[k] = {"max_MBps": mx, "max_iops": max(c[q]["iops"] for q in qds), "knee_qd": knee,
                "p50_us_qd1": q1["p50_us"], "p99_us_qd1": q1["p99_us"], "p50_us_knee": c[knee]["p50_us"],
                "p50_ratio_knee_vs_qd1": round(c[knee]["p50_us"] / q1["p50_us"], 2) if q1["p50_us"] else None,
                # Little's law: in-flight = throughput x latency (mean); compare with QD at each point
                "littles_law_inflight": {q: round(c[q]["iops"] * c[q]["p50_us"] / 1e6, 2) for q in qds}}

# bufferpool load latency (bpload.jsonl)
bpl = defaultdict(lambda: defaultdict(lambda: {"reads": 0, "prefetch_reads": 0, "us": 0, "ops": 0}))
bp_single = defaultdict(list)
for r in bp:
    if r.get("type") != "op" or r.get("error"):
        continue
    for ft, f in r["files"].items():
        n = f["reads"] + f["prefetch_reads"]
        if not n:
            continue
        sizes = f["reads_by_size"]
        size = max(sizes, key=sizes.get) if sizes else "?"
        key = (r["storage"], r["css"], ft)
        a = bpl[key][size]
        a["reads"] += f["reads"]; a["prefetch_reads"] += f["prefetch_reads"]; a["us"] += f["load_time_micros"]; a["ops"] += 1
        if len(sizes) == 1 and f["prefetch_reads"] == 0:
            bp_single[(r["storage"], size)].append(f["load_time_micros"] / n)
bp_rows = {}
for (stg, css, ft), bys in sorted(bpl.items()):
    for size, a in sorted(bys.items()):
        n = a["reads"] + a["prefetch_reads"]
        bp_rows[f"{stg}|{css}|{ft}|{size}"] = {"reads": a["reads"], "prefetch_reads": a["prefetch_reads"],
                                               "mean_load_us": round(a["us"] / n, 1), "ops": a["ops"]}
bp_demand = {f"{s}|{size}": {"n_op_filetypes": len(v), "median_us": round(st.median(v), 1),
                              "p90_us": round(sorted(v)[int(0.9 * (len(v) - 1))], 1)}
             for (s, size), v in sorted(bp_single.items())}

json.dump({"rows": {"|".join(map(str, k)): v for k, v in rows.items()},
           "curves": {"|".join(k): v for k, v in knees.items()}, "bufferpool_load": bp_rows,
           "bufferpool_demand_only": bp_demand}, open(out_json, "w"), indent=1)

with open(out_md, "w") as f:
    for (stg, rg, pat), c in sorted(curves.items()):
        kn = knees[(stg, rg, pat)]
        f.write(f"\n#### {stg.upper()} {rg} {pat} (read_ahead_kb {c[min(c)]['read_ahead_kb']}): knee QD {kn['knee_qd']}, "
                f"max {kn['max_MBps']:.0f} MB/s / {kn['max_iops']:.0f} IOPS\n\n")
        f.write("<table><tr><th>QD</th><th>p50 us</th><th>p90 us</th><th>p99 us</th><th>p50 range</th><th>IOPS</th>"
                "<th>MB/s</th><th>dev size mode (share)</th><th>dev/tool bytes</th><th>"
                + ("NFS RTT ms</th><th>proxy CPU %" if stg == "efs" else "dev await ms") + "</th></tr>\n")
        for q in sorted(c):
            r = c[q]
            extra = (f"<td>{r['nfs_rtt_ms']:.2f}</td><td>{r['efs_proxy_cpu_pct']}</td>" if stg == "efs" and r.get("nfs_rtt_ms") is not None
                     else f"<td>-</td><td>-</td>" if stg == "efs" else f"<td>{r['dev_await_ms']:.2f}</td>" if r.get("dev_await_ms") is not None else "<td>-</td>")
            f.write(f"<tr><td>{q}</td><td>{r['p50_us']:.0f}</td><td>{r['p90_us']:.0f}</td><td>{r['p99_us']:.0f}</td>"
                    f"<td>{r['p50_us_range'][0]:.0f}-{r['p50_us_range'][1]:.0f}</td><td>{r['iops']:.0f}</td>"
                    f"<td>{r['MBps']:.1f}</td><td>{r['device_size_mode']} ({r['device_size_mode_share']})</td>"
                    f"<td>{r['device_over_tool']}</td>{extra}</tr>\n")
        f.write("</table>\n")
    if bp_rows:
        f.write("\n#### Bufferpool load latency per window (load_time_micros / (reads + prefetch_reads))\n\n<table>"
                "<tr><th>storage</th><th>CSS</th><th>file type</th><th>window bytes</th><th>reads</th><th>prefetch reads</th>"
                "<th>mean us</th></tr>\n")
        for k, v in bp_rows.items():
            s, css, ft, size = k.split("|")
            f.write(f"<tr><td>{s}</td><td>{css}</td><td>{ft}</td><td>{size}</td><td>{v['reads']}</td>"
                    f"<td>{v['prefetch_reads']}</td><td>{v['mean_load_us']}</td></tr>\n")
        f.write("</table>\n")
