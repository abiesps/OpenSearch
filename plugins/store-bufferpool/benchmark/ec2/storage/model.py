#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""Build the storage model tables from records.jsonl (storbench.py) and bpload.jsonl (bpload.py).

usage: model.py <records.jsonl>[,<records2.jsonl>...] <model.json> <model-tables.md> [bpload.jsonl]

Records are grouped per measurement block (record field "block"; records without it are block "main"), because
EFS latency moves between sessions: every knee is computed against the QD 1 point of its own block.

Per (block, storage, regime, pattern, qd): median over repetitions of p50 / p90 / p99 latency, IOPS, MB/s, plus the
range across repetitions; device request-size mode and share; device-bytes / tool-bytes (the ramp second is in the
device counters, so a clean run is about (runtime + ramp) / runtime = 1.125).

Knee rule (the only one; storage-model.md section 4.2): the knee of a latency curve is the highest measured QD q
such that the median p50 at q and at every measured QD below q is <= KNEE_FACTOR x the median p50 at QD 1.
KNEE_FACTOR = 1.10: twice the 1-5 % repetition spread, so a crossing is a real latency increase, not noise.
The first measured QD above the knee is reported too, so the true knee lies in [knee_qd, next_qd). If no measured
QD crosses, the knee is "not located" (>= the largest QD measured).
Writes model.json and model-tables.md (HTML tables).
"""
import json, statistics as st, sys
from collections import defaultdict

KNEE_FACTOR = 1.10

import os


def load(path):
    """Records written before storbench.py recorded the equal-work fields (before 04:30 UTC) get them computed
    here from the raw tool JSON next to records.jsonl, with the same formula storbench.py uses."""
    out = []
    for l in open(path):
        r = json.loads(l)
        if r.get("device_over_tool") is None:
            t = json.load(open(os.path.join(os.path.dirname(path), "raw", r["name"] + ".json")))
            r["tool_bytes"] = t["bytes"] if t.get("tool") == "wnread" else t["jobs"][0]["read"]["io_bytes"]
            r["device_bytes"] = (r["device"]["read_sectors"] * 512 if r["storage"] == "ebs"
                                 else sum(int(k) * v for k, v in r["device_size_hist"].items()))
            r["device_over_tool"] = round(r["device_bytes"] / r["tool_bytes"], 3) if r["tool_bytes"] else None
            r["equal_work_backfilled"] = True
        out.append(r)
    return out


recs = [r for p in sys.argv[1].split(",") for r in load(p)]
out_json, out_md = sys.argv[2], sys.argv[3]
bp = [json.loads(l) for l in open(sys.argv[4])] if len(sys.argv) > 4 else []

g = defaultdict(list)
for r in recs:
    g[(r.get("block", "main"), r["storage"], r["regime"], r["pattern"], r["qd"])].append(r)


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
    row = {
        "reps": len(rs),
        "p50_us": med([x["p50_us"] for x in s]), "p90_us": med([x["p90_us"] for x in s]),
        "p99_us": med([x["p99_us"] for x in s]), "p999_us": med([x["p999_us"] for x in s]),
        "p50_us_reps": sorted(x["p50_us"] for x in s),
        "p50_us_range": [min(x["p50_us"] for x in s), max(x["p50_us"] for x in s)],
        "p99_us_range": [min(x["p99_us"] for x in s), max(x["p99_us"] for x in s)],
        "iops": med([x["iops"] for x in s]), "MBps": med([x["MBps"] for x in s]),
        "MBps_range": [min(x["MBps"] for x in s), max(x["MBps"] for x in s)],
        "device_size_mode": mode, "device_size_mode_share": round(hist[mode] / tot, 4) if mode else None,
        "device_reads": tot, "device_hist": {str(a): b for a, b in sorted(hist.items())},
        "device_over_tool": med([r.get("device_over_tool") for r in rs if r.get("device_over_tool")]),
        "read_ahead_kb": rs[0]["read_ahead_kb"],
        "ra_sampled": sum(1 for r in rs if "read_ahead_kb_samples" in r),
        "trace_overrun": sum(r["trace_overrun"] for r in rs),
    }
    if k[1] == "efs":
        row["nfs_rtt_ms"] = med([r["device"]["avg_rtt_ms"] for r in rs if r["device"].get("avg_rtt_ms") is not None])
        row["nfs_exec_ms"] = med([r["device"]["avg_exec_ms"] for r in rs if r["device"].get("avg_exec_ms") is not None])
        row["efs_proxy_cpu_pct"] = med([r.get("efs_proxy_cpu_pct") for r in rs if r.get("efs_proxy_cpu_pct") is not None])
    else:
        ticks = [r["device"]["read_ticks_ms"] / r["device"]["reads"] for r in rs if r["device"].get("reads")]
        row["dev_await_ms"] = med(ticks)
    rows[k] = row

curves = defaultdict(dict)
for (blk, stg, rg, pat, qd), row in rows.items():
    curves[(blk, stg, rg, pat)][qd] = row


def knee_of(c):
    qds = sorted(c)
    if qds[0] != 1:
        return None
    ref = c[1]["p50_us"]
    knee, nxt = 1, None
    for q in qds[1:]:
        if c[q]["p50_us"] <= KNEE_FACTOR * ref:
            knee = q
        else:
            nxt = q
            break
    return knee, nxt


knees = {}
for k, c in curves.items():
    qds = sorted(c)
    kn = knee_of(c)
    q1 = c[qds[0]]
    d = {"max_MBps": max(c[q]["MBps"] for q in qds), "max_MBps_qd": max(qds, key=lambda q: c[q]["MBps"]),
         "max_iops": max(c[q]["iops"] for q in qds), "qds": qds,
         "p50_us_qd1": q1["p50_us"] if qds[0] == 1 else None, "p99_us_qd1": q1["p99_us"] if qds[0] == 1 else None,
         "knee_rule": f"highest QD with p50 <= {KNEE_FACTOR} x p50 at QD 1 at it and every lower measured QD",
         # Little's law: in-flight = throughput x latency; compare with QD at each point
         "littles_law_inflight": {q: round(c[q]["iops"] * c[q]["p50_us"] / 1e6, 2) for q in qds}}
    if kn:
        knee, nxt = kn
        d.update({"knee_qd": knee, "knee_located": nxt is not None, "next_qd_above_knee": nxt,
                  "p50_us_knee": c[knee]["p50_us"], "p99_us_knee": c[knee]["p99_us"],
                  "MBps_knee": c[knee]["MBps"], "iops_knee": c[knee]["iops"],
                  "p50_ratio_knee_vs_qd1": round(c[knee]["p50_us"] / q1["p50_us"], 3),
                  "p99_ratio_knee_vs_qd1": round(c[knee]["p99_us"] / q1["p99_us"], 3)})
        if nxt is not None:
            d["p50_ratio_next_vs_qd1"] = round(c[nxt]["p50_us"] / q1["p50_us"], 3)
            d["p99_ratio_next_vs_qd1"] = round(c[nxt]["p99_us"] / q1["p99_us"], 3)
    knees[k] = d

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

json.dump({"knee_rule": knees and next(iter(knees.values()))["knee_rule"],
           "rows": {"|".join(map(str, k)): v for k, v in rows.items()},
           "curves": {"|".join(k): v for k, v in knees.items()}, "bufferpool_load": bp_rows,
           "bufferpool_demand_only": bp_demand}, open(out_json, "w"), indent=1)

with open(out_md, "w") as f:
    f.write(f"Knee rule: {knees and next(iter(knees.values()))['knee_rule']} (KNEE_FACTOR {KNEE_FACTOR}); "
            "computed per measurement block against that block's own QD 1.\n")
    for (blk, stg, rg, pat), c in sorted(curves.items()):
        kn = knees[(blk, stg, rg, pat)]
        if "knee_qd" in kn:
            ks = (f"knee QD {kn['knee_qd']} (next measured QD {kn['next_qd_above_knee']}: p50 x{kn['p50_ratio_next_vs_qd1']} of QD 1)"
                  if kn["knee_located"] else f"knee not located (>= QD {kn['knee_qd']}, the largest measured)")
        else:
            ks = "no QD 1 in this block: knee not computed"
        f.write(f"\n#### block {blk}: {stg.upper()} {rg} {pat} (read_ahead_kb {c[min(c)]['read_ahead_kb']}): {ks}, "
                f"max {kn['max_MBps']:.0f} MB/s at QD {kn['max_MBps_qd']} / {kn['max_iops']:.0f} IOPS\n\n")
        f.write("<table><tr><th>QD</th><th>p50 us</th><th>p90 us</th><th>p99 us</th><th>p50 per repetition</th><th>IOPS</th>"
                "<th>MB/s</th><th>dev size mode (share)</th><th>dev/tool bytes</th><th>"
                + ("NFS RTT ms</th><th>proxy CPU %" if stg == "efs" else "dev await ms") + "</th></tr>\n")
        for q in sorted(c):
            r = c[q]
            extra = (f"<td>{r['nfs_rtt_ms']:.2f}</td><td>{r['efs_proxy_cpu_pct']}</td>" if stg == "efs" and r.get("nfs_rtt_ms") is not None
                     else f"<td>-</td><td>-</td>" if stg == "efs" else f"<td>{r['dev_await_ms']:.2f}</td>" if r.get("dev_await_ms") is not None else "<td>-</td>")
            f.write(f"<tr><td>{q}</td><td>{r['p50_us']:.0f}</td><td>{r['p90_us']:.0f}</td><td>{r['p99_us']:.0f}</td>"
                    f"<td>{', '.join(f'{x:.0f}' for x in r['p50_us_reps'])}</td><td>{r['iops']:.0f}</td>"
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
