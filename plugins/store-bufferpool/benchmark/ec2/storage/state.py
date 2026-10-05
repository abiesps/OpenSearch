#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""EFS fast / slow state analysis (storage-model.md section 4.3; review-a pass 2, finding 1).

usage: state.py <state records.jsonl>[,...] <xprt-timeline.jsonl>[,...] <out.json> <out.md> [<older records.jsonl>,...]

The state block (storbench.py --block state --ref efs,poc-fio,r8k,64) runs, per cycle (= repetition):
a reference job (8 KiB random, queue depth 64), the measured jobs in a shuffled order, the reference job again.
The reference job is the state indicator: earlier blocks show it is bimodal (about 17,000-19,600 NFS READs per
second in the fast state, 9,400-10,300 in the slow state). It is independent of the jobs it classifies.

Cycle classification (three levels; the reference job showed three separate levels of NFS READs per second,
with empty gaps between them: 9,450-10,320 in the older blocks, 14,273-16,250 and 17,305-19,645 in the state
blocks):
  fast      both references >= FAST_MIN (17,000) READs/s and no reconnect in the cycle
  degraded  both references in [SLOW_MAX, FAST_MIN) = [12,000, 17,000), no reconnect
  slow      both references < SLOW_MAX (12,000), no reconnect
  mixed  the references are at different levels (state changed inside the cycle), no reconnect
The rule stated before the first state block had two levels at THRESHOLD = 14,000; it did not anticipate the
middle level. Its result is reported as well (field two_level_rule): under it every degraded cycle is "fast".
  reconnect  connect_count changed between the start of the first and the end of the last reference job;
         reported, not used for the per-state curves (the state may have changed at the reconnect)
Per state and per curve: for every cycle, the ratio p50(QD q) / p50(QD 1) of the SAME cycle (paired, so the
cycle's own QD 1 is the base); the median ratio over cycles with a bootstrap 95 % confidence interval
(10,000 resamples of cycles, seeded). Knee rule as in model.py: highest QD whose median ratio, and the median
ratio at every lower QD, is <= 1.10. The upper confidence bound is reported next to it.
Also: whether the state is constant within a connection epoch (the time between two reconnects), and an
RTT-based classification of the older blocks' jobs against the per-state cell RTTs measured here.
"""
import json, random, statistics as stt, sys
from collections import defaultdict

THRESHOLD = 14000.0  # two-level rule stated before the first state block
FAST_MIN, SLOW_MAX = 17000.0, 12000.0
KNEE = 1.10
recs = [json.loads(l) for p in sys.argv[1].split(",") for l in open(p)]
xts = [[json.loads(l) for l in open(p)] for p in sys.argv[2].split(",")]
out_json, out_md = sys.argv[3], sys.argv[4]
older = [json.loads(l) for p in (sys.argv[5].split(",") if len(sys.argv) > 5 else []) for l in open(p)]

cyc = defaultdict(dict)
for r in recs:
    cyc[(r["block"], r["rep"])][r.get("role", "measured") if r.get("role") != "measured" else (r["pattern"], r["qd"])] = r


def cc(r, end):
    return r["device"]["xprt_end" if end else "xprt_start"]["connect_count"]


cycles = []
for rep in sorted(cyc):
    c = cyc[rep]
    if "ref-start" not in c or "ref-end" not in c:
        continue
    a, b = c["ref-start"], c["ref-end"]
    ia, ib = a["summary"]["iops"], b["summary"]["iops"]
    rc = cc(b, True) - cc(a, False)
    lvl = lambda x: "fast" if x >= FAST_MIN else "slow" if x < SLOW_MAX else "degraded"
    two = lambda x: "fast" if x >= THRESHOLD else "slow"
    if rc:
        state = two_state = "reconnect"
    else:
        state = lvl(ia) if lvl(ia) == lvl(ib) else "mixed"
        two_state = two(ia) if two(ia) == two(ib) else "mixed"
    meas = {k: v for k, v in c.items() if isinstance(k, tuple)}
    load = c.get("load")
    cycles.append({"block": rep[0], "rep": rep[1], "state": state, "two_level_state": two_state, "ref_iops": [round(ia), round(ib)], "reconnects": rc,
                   "load": None if load is None else {"MBps": round(load["summary"]["MBps"]),
                                                       "reconnects": load["device"]["reconnects"],
                                                       "rtt_ms": round(load["device"]["avg_rtt_ms"], 2)},
                   "connect_count": [cc(a, False), cc(b, True)], "start": a["ts_start_utc"], "end": b["ts_end_utc"],
                   "ref_rtt_ms": [round(a["device"]["avg_rtt_ms"], 2), round(b["device"]["avg_rtt_ms"], 2)],
                   "jobs": {f"{p}|{q}": {"p50_us": v["summary"]["p50_us"], "p99_us": v["summary"]["p99_us"],
                                         "iops": round(v["summary"]["iops"]), "rtt_ms": round(v["device"]["avg_rtt_ms"], 3),
                                         "reconnects": v["device"]["reconnects"]} for (p, q), v in meas.items()}})

rng = random.Random(20261005)


def boot_median(xs, n=10000):
    if len(xs) < 2:
        return None
    ms = sorted(stt.median(rng.choices(xs, k=len(xs))) for _ in range(n))
    return [round(ms[int(0.025 * n)], 3), round(ms[int(0.975 * n) - 1], 3)]


curves = {}
STATES = ("fast", "degraded", "slow")
for state in STATES:
    cs = [c for c in cycles if c["state"] == state]
    if not cs:
        continue
    for pat in sorted({k.split("|")[0] for c in cs for k in c["jobs"]}):
        qds = sorted({int(k.split("|")[1]) for c in cs for k in c["jobs"] if k.startswith(pat + "|")})
        pts = {}
        for q in qds:
            ratios = [c["jobs"][f"{pat}|{q}"]["p50_us"] / c["jobs"][f"{pat}|1"]["p50_us"] for c in cs
                      if f"{pat}|{q}" in c["jobs"] and f"{pat}|1" in c["jobs"]]
            p50s = [c["jobs"][f"{pat}|{q}"]["p50_us"] for c in cs if f"{pat}|{q}" in c["jobs"]]
            pts[q] = {"cycles": len(ratios), "median_ratio_vs_qd1": round(stt.median(ratios), 3) if ratios else None,
                      "ratio_ci95": boot_median(ratios), "median_p50_us": round(stt.median(p50s)) if p50s else None,
                      "median_p99_us": round(stt.median([c["jobs"][f"{pat}|{q}"]["p99_us"] for c in cs if f"{pat}|{q}" in c["jobs"]])),
                      "median_iops": round(stt.median([c["jobs"][f"{pat}|{q}"]["iops"] for c in cs if f"{pat}|{q}" in c["jobs"]])),
                      "median_rtt_ms": round(stt.median([c["jobs"][f"{pat}|{q}"]["rtt_ms"] for c in cs if f"{pat}|{q}" in c["jobs"]]), 2),
                      "share_above_1.10": round(sum(1 for x in ratios if x > KNEE) / len(ratios), 3) if ratios else None}
        knee, nxt = 1, None
        for q in qds[1:]:
            if pts[q]["median_ratio_vs_qd1"] is not None and pts[q]["median_ratio_vs_qd1"] <= KNEE:
                knee = q
            else:
                nxt = q
                break
        curves[f"{state}|{pat}"] = {"cycles": len(cs), "points": pts, "knee_qd": knee, "next_qd_above_knee": nxt}

# state per connection epoch: epochs are delimited by connect_count values (cycles with a reconnect excluded)
epochs = defaultdict(list)
for c in cycles:
    if c["state"] != "reconnect":
        epochs[c["connect_count"][0]].append(c["state"])
epoch_summary = {str(k): {"cycles": len(v), "states": dict((s, v.count(s)) for s in set(v)),
                          "constant": len(set(x for x in v if x != "mixed")) <= 1} for k, v in sorted(epochs.items())}

# reconnect timeline from the 1 s xprt samples
recon = []
for p, q in ((p, q) for xt in xts for p, q in zip(xt, xt[1:])):
    if q["connect_count"] != p["connect_count"]:
        recon.append({"ts_utc": q["ts_utc"], "from": p["connect_count"], "to": q["connect_count"],
                      "read_ops_in_second": q["read_ops"] - p["read_ops"]})
idle_recon = sum(1 for x in recon if x["read_ops_in_second"] == 0)

# older blocks: classify each EFS POC-regime job of a cell that the fast cycles measured. The fast band of a cell
# is the range of its same-cycle ratio p50(q) / p50(QD 1) over all fast cycles (independent data); an older job is
# "not fast" if its ratio to its own block's QD 1 median is above max(1.10, the band's maximum), "fast" otherwise ("not fast" = slow or degraded, the cell alone cannot tell which), "reconnect" if the job saw a mid-job reconnect (NFS READ errors > 0: the in-flight READs fail and are retried).
# The 8 KiB queue-depth-64 cell (the reference job) is classified by READs per second against THRESHOLD instead.
fast_band = defaultdict(list)
for c in cycles:
    if c["state"] != "fast":
        continue
    for k, v in c["jobs"].items():
        p, q = k.split("|")
        if f"{p}|1" in c["jobs"]:
            fast_band[(p, int(q))].append(v["p50_us"] / c["jobs"][f"{p}|1"]["p50_us"])
fast_band = {k: [round(min(v), 3), round(max(v), 3)] for k, v in fast_band.items()}
oq1 = defaultdict(list)
for r in older:
    if r["storage"] == "efs" and r["qd"] == 1:
        oq1[(r.get("block", "main"), r["regime"], r["pattern"])].append(r["summary"]["p50_us"])
retro = []
for r in older:
    if r["storage"] != "efs" or r["regime"] not in ("poc-exact", "poc-fio"):
        continue
    blk = r.get("block", "main")
    key = (r["pattern"], r["qd"])
    is_ref = r["regime"] == "poc-fio" and key == ("r8k", 64)
    if not is_ref and (r["regime"] != "poc-exact" or key not in fast_band or r["qd"] == 1):
        continue
    base = oq1.get((blk, r["regime"], r["pattern"]))
    ratio = r["summary"]["p50_us"] / stt.median(base) if base else None
    if r["device"].get("errors"):
        state = "reconnect"
    elif is_ref:
        x = r["summary"]["iops"]
        state = "fast" if x >= FAST_MIN else "slow" if x < SLOW_MAX else "degraded"
    else:
        state = "not fast" if ratio is not None and ratio > max(KNEE, fast_band[key][1]) else "fast"
    retro.append({"name": r["name"], "block": blk, "ts_utc": r.get("ts_utc"), "pattern": r["pattern"], "qd": r["qd"],
                  "ratio_vs_block_qd1": None if ratio is None else round(ratio, 3),
                  "fast_band": None if is_ref else fast_band[key], "iops": round(r["summary"]["iops"]),
                  "rtt_ms": round(r["device"]["avg_rtt_ms"], 2), "nfs_errors": r["device"].get("errors"), "state": state})
retro_sum = defaultdict(lambda: defaultdict(int))
for x in retro:
    retro_sum[x["block"]][x["state"]] += 1
refrtt = {s: stt.median([x for c in cycles if c["state"] == s for x in c["ref_rtt_ms"]]) for s in STATES
          if any(c["state"] == s for c in cycles)}

res = {"threshold_reads_per_s": THRESHOLD, "knee_factor": KNEE,
       "level_rule": {"fast_min": FAST_MIN, "slow_max": SLOW_MAX},
       "cycle_states": {s: sum(1 for c in cycles if c["state"] == s) for s in STATES + ("mixed", "reconnect")},
       "two_level_rule": {s: sum(1 for c in cycles if c["two_level_state"] == s) for s in ("fast", "slow", "mixed", "reconnect")},
       "cycle_states_by_block": {b: {s: sum(1 for c in cycles if c["block"] == b and c["state"] == s)
                                     for s in STATES + ("mixed", "reconnect")} for b in sorted({c["block"] for c in cycles})},
       "cycles": cycles, "curves": curves, "epochs": epoch_summary, "reconnects": recon,
       "reconnects_while_idle": idle_recon, "ref_rtt_ms_by_state": refrtt,
       "fast_band_ratio_vs_qd1": {f"{p}|{q}": v for (p, q), v in sorted(fast_band.items())},
       "older_blocks_classification": {"summary": {b: dict(v) for b, v in retro_sum.items()}, "jobs": retro}}
json.dump(res, open(out_json, "w"), indent=1)

with open(out_md, "w") as f:
    f.write(f"Cycles: {res['cycle_states']} (levels: fast >= {FAST_MIN:.0f}, slow < {SLOW_MAX:.0f} READs/s on both "
            f"reference jobs); by block {res['cycle_states_by_block']}; two-level rule at {THRESHOLD:.0f}: {res['two_level_rule']}\n\n")
    for k, cv in curves.items():
        state, pat = k.split("|")
        f.write(f"#### {state} state, {pat} random (poc-exact), {cv['cycles']} cycles: knee QD {cv['knee_qd']}"
                f" (next measured QD {cv['next_qd_above_knee']})\n\n<table><tr><th>QD</th><th>cycles</th>"
                "<th>median p50 / same-cycle QD 1 p50</th><th>95 % confidence interval</th><th>share of cycles above 1.10</th>"
                "<th>median p50 us</th><th>median p99 us</th><th>median IOPS</th><th>median NFS RTT ms</th></tr>\n")
        for q, pt in sorted(cv["points"].items()):
            ci = pt["ratio_ci95"]
            f.write(f"<tr><td>{q}</td><td>{pt['cycles']}</td><td>{pt['median_ratio_vs_qd1']}</td>"
                    f"<td>{'-' if ci is None else f'{ci[0]}-{ci[1]}'}</td><td>{pt['share_above_1.10']}</td>"
                    f"<td>{pt['median_p50_us']}</td><td>{pt['median_p99_us']}</td><td>{pt['median_iops']}</td>"
                    f"<td>{pt['median_rtt_ms']}</td></tr>\n")
        f.write("</table>\n\n")
    f.write(f"Reconnects in the 1 s timeline: {len(recon)} ({idle_recon} in a second with no READ)\n\n")
    f.write("Epochs (connect_count: states): " + "; ".join(f"{k}: {v['states']}" for k, v in epoch_summary.items()) + "\n\n")
    f.write(f"Fast band (same-cycle ratio to QD 1, min-max over fast cycles): {res['fast_band_ratio_vs_qd1']}\n\n")
    f.write(f"Older blocks, classification: {res['older_blocks_classification']['summary']}\n\n")
    f.write("<table><tr><th>job</th><th>block</th><th>end time UTC</th><th>ratio to block QD 1</th><th>fast band</th>"
            "<th>READs/s</th><th>NFS RTT ms</th><th>NFS READ errors</th><th>state</th></tr>\n")
    for x in sorted(res["older_blocks_classification"]["jobs"], key=lambda x: x["ts_utc"] or ""):
        if x["state"] != "fast":
            f.write(f"<tr><td>{x['pattern']} QD {x['qd']}</td><td>{x['block']}</td><td>{x['ts_utc']}</td>"
                    f"<td>{x['ratio_vs_block_qd1']}</td><td>{x['fast_band']}</td><td>{x['iops']}</td><td>{x['rtt_ms']}</td>"
                    f"<td>{x['nfs_errors']}</td><td>{x['state']}</td></tr>\n")
    f.write("</table>\n")
print(json.dumps({"cycle_states": res["cycle_states"], "knees": {k: (v["knee_qd"], v["next_qd_above_knee"]) for k, v in curves.items()},
                  "reconnects": len(recon), "idle_reconnects": idle_recon}, indent=1))
