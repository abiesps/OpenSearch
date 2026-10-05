#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""EFS knee per connection (storage-model.md section 4.2.2; review-a pass 3, findings 1 and 4).

usage: conn.py <conn dir> <conn-pin dir> <efs-proxy logs dir> <state.json> <out.json> <out.md>

Analyses the blocks run by run_conn_blocks.sh, following preregistration-connections.md:
- every forced reconnect (umount + mount) is one connection set (records carry conn and conn_pos);
- a cycle is valid if all its jobs are valid (ra_valid, equalwork.py rule), connect_count did not change from
  the start of its first reference to the end of its last, efs-proxy's backend connection count was the same at
  the start, middle and end of every job, and the proxy log has no scale-up event (search, new connection,
  failed search, incarnation restart) between the start of its first and the end of its last job;
  conn-pin cycles must also be on exactly one backend connection;
- per connection: r_c(q) = mean over valid cycles of p50(q) / p50(queue depth 1) of the same cycle; knee_c = the
  highest grid queue depth with r_c <= 1.10 there and at every lower grid point;
- heavy-job check d_c(q) = ratio in cycle 2 (heavy 1 MiB job first) - ratio in cycle 1, q = 24 and 32;
- fixed-default rule: the minimum knee_c over the scaled-up connections of block conn, with the number of
  connections below each q and the Clopper-Pearson 95 % upper bound of that share;
- Spearman rank correlation of the reference level with r_c(32) and knee_c (bootstrap 95 % interval);
- partition: the id efs-proxy logs when it scales up (short sha1 of the id bytes).
"""
import glob, hashlib, json, math, os, random, re, statistics as stt, sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import equalwork  # noqa: E402  (same directory; the acceptance rule of section 2)

KNEE = 1.10
FAST_MIN, SLOW_MAX = 17000.0, 12000.0
GRID = [1, 4, 8, 12, 16, 20, 24, 28, 32]
PAT = "r128k"
rng = random.Random(20261007)

conn_dir, pin_dir, logs_dir, state_json, out_json, out_md = sys.argv[1:7]


def ts(s):
    # 2026-10-05T11:50:13.123Z -> seconds (UTC); only differences and order are used
    d, t = s.rstrip("Z").split("T")
    y, mo, da = (int(x) for x in d.split("-"))
    h, mi, se = t.split(":")
    days = (y - 1970) * 365 + (y - 1969) // 4 + sum([31, 29 if y % 4 == 0 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][:mo - 1]) + da - 1
    return days * 86400 + int(h) * 3600 + int(mi) * 60 + float(se)


# ---- efs-proxy logs: scale-up events per mount port
LINE = re.compile(r"^(\d{4}-\d\d-\d\dT[\d:.]+Z) (\d+) (\w+) (\S+) (.*)$")
events = defaultdict(list)  # port -> [(t, kind, detail)]
for path in sorted(glob.glob(os.path.join(logs_dir, "*.efs-proxy.log*"))):
    port = int(re.search(r"\.mnt\.efs\.(\d+)\.efs-proxy\.log", path).group(1))
    for line in open(path, errors="replace"):
        m = LINE.match(line)
        if not m:
            continue
        t, pid, lvl, mod, msg = m.groups()
        kind, detail = None, None
        if msg.startswith("Searching for a new connection"):
            kind = "search"
        elif msg.startswith("Established new TCP connection to"):
            ids = re.search(r"id: \[([\d, ]+)\]", msg)
            kind, detail = "established", hashlib.sha1(ids.group(1).encode()).hexdigest()[:8] if ids else None
        elif msg.startswith("Proxy incarnation restarted"):
            kind = "restart"
        elif msg.startswith("Attempt to scale up failed"):
            kind, detail = "scale_up_failed", msg.split(":", 1)[-1].strip()
        elif msg.startswith("Connection failed"):
            kind = "connection_failed"
        elif msg.startswith("Proxy performance"):
            nc = re.search(r"_num_connections: (\d+)", msg)
            wb = re.search(r"write_bytes: (\d+)", msg)
            td = re.search(r"time_delta: ([\d.]+)s", msg)
            kind = "perf"
            detail = {"num_connections": int(nc.group(1)) if nc else None,
                      "MBps_to_client": round(int(wb.group(1)) / float(td.group(1)) / 1e6, 1) if wb and td else None}
        if kind:
            events[port].append((ts(t), kind, detail, int(pid)))
for v in events.values():
    v.sort(key=lambda x: x[0])
SCALE_KINDS = {"search", "established", "restart", "scale_up_failed", "connection_failed"}


def load_block(d):
    recs = [json.loads(l) for l in open(os.path.join(d, "records.jsonl"))]
    conns = {c["conn"]: c for c in (json.loads(l) for l in open(os.path.join(d, "connections.jsonl")))}
    return recs, conns


def conn_events(c, conns):
    """Proxy events of connection c: its mount port, its proxy pid, from its mount until the next mount."""
    t0 = ts(conns[c]["ts_utc"]) - 5
    nxt = [ts(x["ts_utc"]) for k, x in conns.items() if k > c]
    t1 = min(nxt) if nxt else float("inf")
    pid = conns[c]["proxy_backend"]["proxy_pid"]
    return [e for e in events.get(conns[c]["mount_port"], []) if t0 <= e[0] < t1 and (pid is None or e[3] == pid)]


def lvl(x):
    return "fast" if x >= FAST_MIN else "slow" if x < SLOW_MAX else "degraded"


def analyse(d, block, pinned):
    recs, conns = load_block(d)
    path = os.path.join(d, "records.jsonl")
    cyc = defaultdict(dict)
    warm = {}
    for r in recs:
        if r.get("role") == "warmup":
            warm[r["conn"]] = r
            continue
        key = r["role"] if r["role"] != "measured" else r["qd"]
        cyc[(r["conn"], r["rep"])][key] = r
    cycles = []
    for (c, rep), jobs in sorted(cyc.items()):
        reasons = []
        need = ["ref-start", "ref-end"] + GRID
        missing = [k for k in need if k not in jobs]
        if missing:
            reasons.append(f"missing {missing}")
            cycles.append({"conn": c, "rep": rep, "valid": False, "reasons": reasons})
            continue
        js = list(jobs.values())
        for j in js:
            if not j["ra_valid"]:
                reasons.append(f"{j['name']} ra_valid false")
            v = equalwork.violations(j, path)
            if v:
                reasons.append(f"{j['name']} {v}")
        a, b = jobs["ref-start"], jobs["ref-end"]
        rc = b["device"]["xprt_end"]["connect_count"] - a["device"]["xprt_start"]["connect_count"]
        if rc:
            reasons.append(f"reconnect ({rc / 2:.0f} events)")
        counts = set()
        for j in js:
            se = j.get("proxy_backend_count_start_end") or [None, None]
            counts.update([se[0], se[1], j.get("proxy_backend_midjob", {}).get("count")])
        if len(counts) != 1 or None in counts:
            reasons.append(f"backend count changed {sorted(counts, key=str)}")
        backend = next(iter(counts)) if len(counts) == 1 else None
        if pinned and backend != 1:
            reasons.append("conn-pin cycle not on one backend connection")
        t_first = min(ts(j["ts_start_utc"]) for j in js)
        t_last = max(ts(j["ts_end_utc"]) for j in js)
        ev = [e for e in conn_events(c, conns) if e[1] in SCALE_KINDS and t_first <= e[0] <= t_last]
        if ev:
            reasons.append(f"proxy scale-up events in the cycle {[e[1] for e in ev]}")
        ia, ib = a["summary"]["iops"], b["summary"]["iops"]
        state = lvl(ia) if lvl(ia) == lvl(ib) else "mixed"
        ratios = {q: jobs[q]["summary"]["p50_us"] / jobs[1]["summary"]["p50_us"] for q in GRID}
        cycles.append({"conn": c, "rep": rep, "heavy": "load" in jobs, "valid": not reasons, "reasons": reasons,
                       "state": state, "ref_iops": [round(ia), round(ib)], "backend": backend,
                       "ratio": {q: round(x, 4) for q, x in ratios.items()},
                       "p50_us": {q: jobs[q]["summary"]["p50_us"] for q in GRID},
                       "iops": {q: round(jobs[q]["summary"]["iops"]) for q in GRID},
                       "rtt_ms": {q: round(jobs[q]["device"]["avg_rtt_ms"], 3) for q in GRID},
                       "load_MBps": round(jobs["load"]["summary"]["MBps"]) if "load" in jobs else None,
                       "start": a["ts_start_utc"], "end": b["ts_end_utc"]})
    per = []
    for c in sorted(conns):
        cs = [x for x in cycles if x["conn"] == c and x["valid"]]
        allc = [x for x in cycles if x["conn"] == c]
        evs = conn_events(c, conns)
        est = [e for e in evs if e[1] == "established"]
        w = warm.get(c)
        entry = {"conn": c, "ts_utc": conns[c]["ts_utc"], "mount_port": conns[c]["mount_port"],
                 "backend_local_at_mount": [x["local"] for x in conns[c]["proxy_backend"]["conns"]],
                 "cycles": len(allc), "valid_cycles": len(cs),
                 "invalid_reasons": [x["reasons"] for x in allc if not x["valid"]],
                 "scale_up": {"searches": sum(1 for e in evs if e[1] == "search"),
                              "established": len(est), "restarts": sum(1 for e in evs if e[1] == "restart"),
                              "failed": sum(1 for e in evs if e[1] == "scale_up_failed"),
                              "partition": est[-1][2] if est else None},
                 "warmup": None if w is None else {"MBps": round(w["summary"]["MBps"]),
                                                   "backend_after_wait": w["backend_after_wait"]["count"],
                                                   "stable": w["backend_after_wait"]["stable"]}}
        if cs:
            states = {x["state"] for x in cs}
            r_c = {q: stt.mean(x["ratio"][q] for x in cs) for q in GRID}
            knee, nxt = 1, None
            for q in GRID[1:]:
                if r_c[q] <= KNEE:
                    knee = q
                else:
                    nxt = q
                    break
            refs = [v for x in cs for v in x["ref_iops"]]
            entry.update({"state": states.pop() if len(states) == 1 else "mixed",
                          "backend": cs[0]["backend"] if len({x["backend"] for x in cs}) == 1 else "varies",
                          "ref_median": round(stt.median(refs)), "ref_range": [min(refs), max(refs)],
                          "r_c": {q: round(v, 4) for q, v in r_c.items()}, "knee": knee, "next_qd_above_knee": nxt,
                          "throughput_MBps": {q: round(stt.mean(x["iops"][q] for x in cs) * 131072 / 1e6) for q in GRID},
                          "p50_us_qd1": round(stt.mean(x["p50_us"][1] for x in cs))})
            ca = [x for x in cs if not x["heavy"]]
            cb = [x for x in cs if x["heavy"]]
            if ca and cb:
                entry["heavy_minus_none"] = {q: round(cb[0]["ratio"][q] - ca[0]["ratio"][q], 4) for q in (24, 32)}
            if ca:
                ra = ca[0]["ratio"]
                k = 1
                for q in GRID[1:]:
                    if ra[q] <= KNEE:
                        k = q
                    else:
                        break
                entry["knee_cycle_no_heavy"] = k
        per.append(entry)
    return {"cycles": cycles, "connections": per}


def boot(xs, f=stt.median, n=10000):
    if len(xs) < 2:
        return None
    ms = sorted(f(rng.choices(xs, k=len(xs))) for _ in range(n))
    return [round(ms[int(0.025 * n)], 4), round(ms[int(0.975 * n) - 1], 4)]


def binom_cdf(k, n, p):
    return sum(math.comb(n, i) * p ** i * (1 - p) ** (n - i) for i in range(k + 1))


def cp_upper(k, n, alpha=0.05):
    if k >= n:
        return 1.0
    lo, hi = 0.0, 1.0
    for _ in range(60):
        mid = (lo + hi) / 2
        if binom_cdf(k, n, mid) > alpha / 2:
            lo = mid
        else:
            hi = mid
    return round(hi, 4)


def ranks(xs):
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    r = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        for k in range(i, j + 1):
            r[order[k]] = (i + j) / 2 + 1
        i = j + 1
    return r


def spearman(xs, ys):
    if len(xs) < 3:
        return None
    rx, ry = ranks(xs), ranks(ys)
    mx, my = stt.mean(rx), stt.mean(ry)
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    den = math.sqrt(sum((a - mx) ** 2 for a in rx) * sum((b - my) ** 2 for b in ry))
    return num / den if den else None


def spearman_ci(pairs, n=10000):
    vals = []
    for _ in range(n):
        s = rng.choices(pairs, k=len(pairs))
        v = spearman([a for a, _ in s], [b for _, b in s])
        if v is not None:
            vals.append(v)
    vals.sort()
    return [round(vals[int(0.025 * len(vals))], 3), round(vals[int(0.975 * len(vals)) - 1], 3)] if vals else None


main = analyse(conn_dir, "conn", False)
pin = analyse(pin_dir, "conn-pin", True) if os.path.exists(os.path.join(pin_dir, "records.jsonl")) else None

used = [c for c in main["connections"] if c.get("knee") is not None]
scaled = [c for c in used if isinstance(c.get("backend"), int) and c["backend"] >= 2]
single = [c for c in used if c.get("backend") == 1]

# heavy-job check per state
heavy = {}
for st in ("fast", "degraded", "slow", "mixed"):
    cs = [c for c in scaled if c.get("state") == st and "heavy_minus_none" in c]
    if not cs:
        continue
    heavy[st] = {}
    for q in (24, 32):
        ds = [c["heavy_minus_none"][q] for c in cs]
        ci = boot(ds)
        heavy[st][q] = {"connections": len(ds), "median": round(stt.median(ds), 4), "ci95": ci,
                        "triggers_cycle_a_only": bool(len(ds) >= 5 and ci and (ci[0] > 0 or ci[1] < 0)
                                                      and abs(stt.median(ds)) > 0.03)}
cycle_a_only = any(v[q]["triggers_cycle_a_only"] for v in heavy.values() for q in v)
kkey = "knee_cycle_no_heavy" if cycle_a_only else "knee"

knees = [c[kkey] for c in scaled if c.get(kkey) is not None]
dist = {str(k): knees.count(k) for k in sorted(set(knees))}
below = {}
for q in (16, 20, 24, 28, 32):
    k = sum(1 for x in knees if x < q)
    below[q] = {"connections_below": k, "of": len(knees), "share": round(k / len(knees), 4) if knees else None,
                "clopper_pearson_upper95": cp_upper(k, len(knees)) if knees else None}
fixed_default = min(knees) if knees else None

# per-state pooled curves over connections (median of r_c, bootstrap over connections)
curves = {}
for st in ("fast", "degraded", "slow", "mixed"):
    cs = [c for c in scaled if c.get("state") == st]
    if cs:
        curves[st] = {"connections": len(cs), "points": {q: {"median_r_c": round(stt.median(c["r_c"][q] for c in cs), 4),
                                                             "ci95": boot([c["r_c"][q] for c in cs]),
                                                             "share_above_1.10": round(sum(1 for c in cs if c["r_c"][q] > KNEE) / len(cs), 3),
                                                             "median_MBps": round(stt.median(c["throughput_MBps"][q] for c in cs))}
                                                         for q in GRID},
                      "knees": {str(k): sum(1 for c in cs if c[kkey] == k) for k in sorted({c[kkey] for c in cs})}}

pairs32 = [(c["ref_median"], c["r_c"][32]) for c in scaled]
pairsk = [(c["ref_median"], c[kkey]) for c in scaled]
pred = {"spearman_ref_vs_r32": None if len(pairs32) < 3 else round(spearman(*zip(*pairs32)), 3),
        "ci95_ref_vs_r32": spearman_ci(pairs32) if len(pairs32) >= 3 else None,
        "spearman_ref_vs_knee": None if len(pairsk) < 3 or spearman(*zip(*pairsk)) is None else round(spearman(*zip(*pairsk)), 3),
        "ci95_ref_vs_knee": spearman_ci(pairsk) if len(pairsk) >= 3 else None}
parts = defaultdict(list)
for c in scaled:
    if c["scale_up"]["partition"]:
        parts[c["scale_up"]["partition"]].append({"conn": c["conn"], "ref_median": c["ref_median"], "state": c["state"],
                                                  "knee": c[kkey], "r_c32": c["r_c"][32]})

# the earlier state blocks' connection epochs, side by side (not pooled)
st_old = json.load(open(state_json))
ep = defaultdict(list)
for c in st_old["cycles"]:
    if c["state"] != "reconnect":
        ep[c["connect_count"][0]].append(c)
old_epochs = {}
for k, cs in sorted(ep.items()):
    r32 = [c["jobs"]["r128k|32"]["p50_us"] / c["jobs"]["r128k|1"]["p50_us"] for c in cs if "r128k|32" in c["jobs"] and "r128k|1" in c["jobs"]]
    old_epochs[str(k)] = {"cycles": len(cs), "ref_median": round(stt.median(x for c in cs for x in c["ref_iops"])),
                          "states": {s: sum(1 for c in cs if c["state"] == s) for s in {c["state"] for c in cs}},
                          "median_ratio_qd32": round(stt.median(r32), 3) if r32 else None}

res = {"rule": {"knee_factor": KNEE, "grid": GRID, "levels": {"fast_min": FAST_MIN, "slow_max": SLOW_MAX},
                "primary_knee": kkey},
       "conn": {"connections": len(main["connections"]), "used": len(used), "scaled_up": len(scaled),
                "single_backend": len(single),
                "cycles": len(main["cycles"]), "valid_cycles": sum(1 for c in main["cycles"] if c["valid"]),
                "states": {s: sum(1 for c in scaled if c.get("state") == s) for s in ("fast", "degraded", "slow", "mixed")},
                "backend_counts": {str(b): sum(1 for c in used if c.get("backend") == b) for b in sorted({c.get("backend") for c in used}, key=str)},
                "restarts_at_scale_up": sum(1 for c in used if c["scale_up"]["restarts"]),
                "knee_distribution": dist, "below": below, "fixed_default_min_knee": fixed_default,
                "heavy_job_check": heavy, "cycle_a_only": cycle_a_only, "curves": curves, "predictor": pred,
                "partitions": {k: v for k, v in parts.items()}, "per_connection": main["connections"],
                "cycle_records": main["cycles"]},
       "old_state_block_epochs": old_epochs}
if pin:
    pu = [c for c in pin["connections"] if c.get("knee") is not None]
    res["conn_pin"] = {"connections": len(pin["connections"]), "used": len(pu),
                       "cycles": len(pin["cycles"]), "valid_cycles": sum(1 for c in pin["cycles"] if c["valid"]),
                       "states": {s: sum(1 for c in pu if c.get("state") == s) for s in ("fast", "degraded", "slow", "mixed")},
                       "knee_distribution": {str(k): sum(1 for c in pu if c["knee"] == k) for k in sorted({c["knee"] for c in pu})},
                       "min_knee": min((c["knee"] for c in pu), default=None),
                       "points": {q: {"median_r_c": round(stt.median(c["r_c"][q] for c in pu), 4) if pu else None,
                                      "ci95": boot([c["r_c"][q] for c in pu]),
                                      "median_MBps": round(stt.median(c["throughput_MBps"][q] for c in pu)) if pu else None}
                                  for q in GRID},
                       "per_connection": pin["connections"], "cycle_records": pin["cycles"]}
json.dump(res, open(out_json, "w"), indent=1)

# ---- tables
with open(out_md, "w") as f:
    m = res["conn"]
    f.write(f"Block conn: {m['connections']} connections, {m['valid_cycles']} of {m['cycles']} cycles valid, "
            f"{m['used']} connections used, {m['scaled_up']} scaled up, {m['single_backend']} on one backend connection; "
            f"states {m['states']}; backend counts {m['backend_counts']}; scale-ups that restarted the incarnation "
            f"{m['restarts_at_scale_up']}; primary knee {kkey}.\n\n")
    f.write("<table><tr><th>connection</th><th>time UTC</th><th>valid cycles</th><th>backend connections</th>"
            "<th>partition</th><th>restart at scale-up</th><th>reference READs/s (median, range)</th><th>state</th>"
            + "".join(f"<th>queue depth {q}</th>" for q in GRID[1:]) +
            "<th>knee</th><th>knee, cycle without heavy job</th><th>heavy minus none at 24 / 32</th></tr>\n")
    for c in m["per_connection"]:
        if c.get("knee") is None:
            f.write(f"<tr><td>{c['conn']}</td><td>{c['ts_utc']}</td><td>0 of {c['cycles']}</td>"
                    f"<td colspan='{len(GRID) + 8}'>not used: {c['invalid_reasons']}</td></tr>\n")
            continue
        h = c.get("heavy_minus_none")
        f.write(f"<tr><td>{c['conn']}</td><td>{c['ts_utc'][11:19]}</td><td>{c['valid_cycles']} of {c['cycles']}</td>"
                f"<td>{c['backend']}</td><td>{c['scale_up']['partition']}</td><td>{c['scale_up']['restarts']}</td>"
                f"<td>{c['ref_median']:,} ({c['ref_range'][0]:,}-{c['ref_range'][1]:,})</td><td>{c['state']}</td>"
                + "".join(f"<td>{c['r_c'][q]:.3f}</td>" for q in GRID[1:]) +
                f"<td>{c['knee']}</td><td>{c.get('knee_cycle_no_heavy')}</td>"
                f"<td>{'-' if not h else f'{h[24]:+.3f} / {h[32]:+.3f}'}</td></tr>\n")
    f.write("</table>\n\n")
    f.write(f"Knee distribution (scaled-up connections): {dist}; minimum {fixed_default}\n\n<table><tr><th>q</th>"
            "<th>connections with knee below q</th><th>share</th><th>Clopper-Pearson 95 % upper bound</th></tr>\n")
    for q, v in below.items():
        f.write(f"<tr><td>{q}</td><td>{v['connections_below']} of {v['of']}</td><td>{v['share']}</td><td>{v['clopper_pearson_upper95']}</td></tr>\n")
    f.write("</table>\n\n")
    for st, cv in curves.items():
        f.write(f"#### {st}, {cv['connections']} connections, knees {cv['knees']}\n\n<table><tr><th>queue depth</th>"
                "<th>median r_c</th><th>95 % interval</th><th>share above 1.10</th><th>median MB/s</th></tr>\n")
        for q, p in cv["points"].items():
            f.write(f"<tr><td>{q}</td><td>{p['median_r_c']}</td><td>{p['ci95']}</td><td>{p['share_above_1.10']}</td><td>{p['median_MBps']}</td></tr>\n")
        f.write("</table>\n\n")
    f.write(f"Heavy-job check: {heavy}\n\nPredictor: {pred}\n\nPartitions: {dict(parts)}\n\n")
    f.write(f"Earlier state-block epochs: {old_epochs}\n\n")
    if pin:
        p = res["conn_pin"]
        f.write(f"Block conn-pin: {p['connections']} connections, {p['valid_cycles']} of {p['cycles']} cycles valid; "
                f"states {p['states']}; knees {p['knee_distribution']}; minimum {p['min_knee']}\n\n<table><tr><th>queue depth</th>"
                "<th>median r_c</th><th>95 % interval</th><th>median MB/s</th></tr>\n")
        for q, x in p["points"].items():
            f.write(f"<tr><td>{q}</td><td>{x['median_r_c']}</td><td>{x['ci95']}</td><td>{x['median_MBps']}</td></tr>\n")
        f.write("</table>\n\n<table><tr><th>connection</th><th>valid cycles</th><th>reference READs/s</th><th>state</th>"
                + "".join(f"<th>queue depth {q}</th>" for q in GRID[1:]) + "<th>knee</th><th>scale-up searches / failed</th></tr>\n")
        for c in p["per_connection"]:
            if c.get("knee") is None:
                f.write(f"<tr><td>{c['conn']}</td><td colspan='{len(GRID) + 4}'>not used: {c['invalid_reasons']}</td></tr>\n")
                continue
            f.write(f"<tr><td>{c['conn']}</td><td>{c['valid_cycles']} of {c['cycles']}</td><td>{c['ref_median']:,}</td><td>{c['state']}</td>"
                    + "".join(f"<td>{c['r_c'][q]:.3f}</td>" for q in GRID[1:]) +
                    f"<td>{c['knee']}</td><td>{c['scale_up']['searches']} / {c['scale_up']['failed']}</td></tr>\n")
        f.write("</table>\n")
print(json.dumps({"conn": {k: res["conn"][k] for k in ("connections", "used", "scaled_up", "single_backend", "valid_cycles",
                                                       "states", "knee_distribution", "fixed_default_min_knee", "cycle_a_only")},
                  "below": below, "heavy": heavy, "predictor": pred,
                  "pin": None if not pin else {k: res["conn_pin"][k] for k in ("used", "valid_cycles", "states", "knee_distribution", "min_knee")}},
                 indent=1, default=str))
