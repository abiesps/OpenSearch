#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Statistics for coldbench sessions (one or more session directories, merged by arm label). Stdlib only.

Unit = one JVM run. Every sample is first summarized per run (median of the run's iterations of that op), then:
  - change of the median per op, variant vs base: point = median(variant run medians) / median(base run medians) - 1,
    95% CI by bootstrap that resamples RUNS with replacement (--boot resamples, seeded);
  - p-value: exact two-sided Mann-Whitney on the run medians (Monte Carlo permutation of the median difference when
    the exact enumeration is too large); the permutation p of the median difference is reported too; Benjamini-
    Hochberg q across the ops of one comparison. The effect size is the bootstrap CI, the p-value only gates BH;
  - A/A floor (--aa LABEL1,LABEL2): per op and per family the |change| between two labels of the same arm; a change
    counts only when q < --alpha AND |change| > the A/A floor of its family (and > the op's own A/A CI half width);
  - Amdahl: cold median must not be below warm median (per arm and op), and a cold gain must not exceed the base's
    cold - warm budget;
  - warm steady state: per run, median of the second half vs the first half of the measured iterations;
  - reference-op drift: per run, last vs first execution of the reference op (cold and warm);
  - result equality across arms (total hits, hit ids / scores / sort values with tie groups, aggregations) and
    across runs of one arm; cold verification counts; IO per request (bufferpool loads per file type, demand vs
    prefetch, device bytes, r_await, aqu-sz).
Regressions (warm bar): the 95% CI excludes 0 on the slow side and the change exceeds the A/A floor, with no
multiple-comparison correction (a real slowdown must not be hidden by BH).
OUTCOME (non-inferiority, --ni-*; defaults from the arms file's "outcome"): per op, target (S2-CORE-EFS,
S2-CORE+PLANNER-EFS) / reference (S0-EBS; BASE-EBS in arms files of the baseline set, arms_baseline.py) for cold p50, cold p90 and warm p50 (cold p99 informational), each from a
two-level bootstrap (runs, then iterations in a run); PASS when the 95% CI upper bound <= 1 + delta with
delta = max(5 %, the reference's A/A MDE for that op and statistic) and the BH-adjusted one-sided p < alpha; WORSE
when the ratio is significantly above the margin (BH); else INCONCLUSIVE (add runs). Failing ops list their gap in ms
and the IO that remains (demand loads per file type, NFS READ ops, RTT, READs in flight).

  analyze.py SESSION_DIR [SESSION_DIR ...] --base S1 [--compare S2-A,S2-CORE] [--aa S1@a,S1@b] [--out DIR]
"""
import argparse
import collections
import itertools
import json
import math
import os
import random
import statistics
import sys

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
from common import read_jsonl  # noqa: E402


# ---------------------------------------------------------------- statistics primitives
def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return None
    k = (len(xs) - 1) * p / 100
    lo, hi = math.floor(k), math.ceil(k)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def boot_change(base_runs, var_runs, resamples, seed):
    """Point and 95% CI of median(var)/median(base) - 1, resampling run medians with replacement."""
    bm = statistics.median(base_runs)
    point = statistics.median(var_runs) / bm - 1 if bm else None
    rng = random.Random(seed)
    ch = []
    for _ in range(resamples):
        b = statistics.median(rng.choices(base_runs, k=len(base_runs)))
        v = statistics.median(rng.choices(var_runs, k=len(var_runs)))
        if b > 0:
            ch.append(v / b - 1)
    ch.sort()
    if not ch:
        return point, None, None
    return point, ch[int(0.025 * len(ch))], ch[max(0, int(math.ceil(0.975 * len(ch))) - 1)]


def perm_p(a, b, limit=20000, seed=11):
    """Two-sided permutation p of |median(b) - median(a)| over run medians."""
    obs = abs(statistics.median(b) - statistics.median(a))
    pooled = list(a) + list(b)
    n, total = len(a), len(pooled)
    extreme = count = 0
    if math.comb(total, n) <= limit:
        idx_all = range(total)
        for idx in itertools.combinations(idx_all, n):
            s = set(idx)
            x = [pooled[i] for i in idx]
            y = [pooled[i] for i in idx_all if i not in s]
            count += 1
            extreme += abs(statistics.median(y) - statistics.median(x)) >= obs - 1e-12
    else:
        rng = random.Random(seed)
        for _ in range(limit):
            rng.shuffle(pooled)
            count += 1
            extreme += abs(statistics.median(pooled[n:]) - statistics.median(pooled[:n])) >= obs - 1e-12
        extreme += 1
        count += 1
    return extreme / count


def mann_whitney_p(a, b, limit=20000):
    pooled = sorted(list(a) + list(b))

    def rank(v):
        lo = pooled.index(v)
        hi = len(pooled) - 1 - pooled[::-1].index(v)
        return (lo + hi) / 2 + 1
    ranks = [rank(v) for v in list(a) + list(b)]
    n = len(a)
    if math.comb(len(pooled), n) > limit:
        return None
    observed = sum(ranks[:n])
    mean = n * (len(pooled) + 1) / 2
    extreme = total = 0
    for idx in itertools.combinations(range(len(pooled)), n):
        total += 1
        extreme += abs(sum(ranks[i] for i in idx) - mean) >= abs(observed - mean) - 1e-9
    return extreme / total


def bh(ps):
    m = len(ps)
    order = sorted(range(m), key=lambda i: ps[i])
    q = [1.0] * m
    prev = 1.0
    for k_from_top, i in enumerate(reversed(order)):
        k = m - k_from_top
        prev = min(prev, ps[i] * m / k)
        q[i] = prev
    return q


# ---------------------------------------------------------------- loading
def cold_protocols(data, skip):
    """
    The cold protocol of every run (run record cold_protocol: "jit-warm" = every op ran once, unmeasured, before the
    cold block; absent = "jit-cold", sessions before that rule), with "/skip-iter<N" when --cold-skip-iters leaves out
    the first N cold iterations. One analysis (and so every comparison in it) must use one protocol: mixing is refused
    (common-rules DECISION "cold means data-cold on a JIT-warm JVM").
    """
    seen = {}
    # only runs that measured cold samples have a cold protocol (a warm-only session's runs have none)
    cold_runs = {k[2] for k in data.samples if k[0] == "cold"}
    for r in data.runs.values():
        if r.get("available", True) and (not cold_runs or r["run_id"] in cold_runs):
            p = r.get("cold_protocol", "jit-cold") + (f"/skip-iter{skip}" if skip else "")
            seen.setdefault(p, set()).add(r["label"])
    if len(seen) > 1:
        sys.exit(f"sessions with different cold protocols cannot be compared in one analysis: "
                 f"{ {p: sorted(ls) for p, ls in seen.items()} }")
    return next(iter(seen), "none")



def efs_level(n):
    """efs-proxy runs on 1 backend connection or multiplexes 5 (a 6th socket of the previous incarnation can linger):
    level "1", "5" (5 or more), or "N" for a partial count."""
    return "1" if n == 1 else "5" if n >= 5 else str(n)


def efs_state(sample):
    """EFS connection level of a sample: None (not EFS), "unknown" (EFS, not recorded), "1", "5", or "A->B"."""
    io = sample.get("io") or {}
    if io.get("nfs") is None:
        return None
    e = io.get("efs_connections")
    if e is None or e.get("start") is None or e.get("end") is None:
        return "unknown"
    a, b = efs_level(e["start"]), efs_level(e["end"])
    return a if a == b else f"{a}->{b}"


def in_incident(sample, incidents):
    """True when the sample's request [t - duration, t] overlaps a recorded NFS stall window (1 s margin)."""
    end = sample.get("t")
    if end is None:
        return False
    dur = (sample.get("wall_ms") or sample.get("took_ms") or 0) / 1000.0
    start = end - dur
    return any(start <= w_end + 1.0 and end >= w_start - 1.0 for w_start, w_end, _, _ in incidents)


class Data:
    def __init__(self, dirs, cold_metric, warm_metric, cold_skip_iters=0, keep_unknown_efs=False, efs_select=None,
                 efs_run_states=None):
        self.samples = collections.defaultdict(list)  # (mode, label, run_id, op) -> [sample]
        self.runs = {}
        self.results = collections.defaultdict(dict)  # (label, op) -> {run_id: canonical}
        self.sessions = []
        self.metric = {"cold": cold_metric, "warm": warm_metric, "ccold": cold_metric, "cwarm": warm_metric}
        self.excluded = collections.Counter()
        self.verify = collections.defaultdict(collections.Counter)
        self.batches = []
        self.skipped = collections.Counter()
        self.efs_counts = collections.Counter()  # (mode, label, efs state) -> samples
        self.efs_used = set()  # EFS states of the samples kept
        # kernel NFS stall windows recorded by coldbench (storage_incident): a sample that overlaps one measured the
        # stall of a hard mount, not the code; excluded from every verdict, counted as sensitivity
        self.incidents = []  # (start, end, server, run_id)
        for d in dirs:
            for r in read_jsonl(os.path.join(d, "samples.jsonl")):
                if r["type"] == "storage_incident":
                    for w in r.get("windows") or []:
                        self.incidents.append((w["start"], w["end"], w.get("server"), r.get("run_id")))
        self.incident_samples = collections.Counter()  # (mode, label) -> samples excluded as storage incident
        for d in dirs:
            for r in read_jsonl(os.path.join(d, "samples.jsonl")):
                t = r["type"]
                if t == "session":
                    self.sessions.append(r)
                elif t == "run":
                    self.runs[r["run_id"]] = r
                elif t == "sample":
                    if r["mode"] == "cold" and r.get("iter", 0) < cold_skip_iters:
                        self.skipped[r["label"]] += 1
                        continue
                    if r["mode"] in ("cold", "ccold") and r.get("cold_ok") is False:
                        self.excluded[(r["mode"], r["label"])] += 1
                        for k, v in r.get("checks", {}).items():
                            if v is False:
                                self.verify[r["label"]]["fail:" + k] += 1
                        continue
                    if r.get("error"):
                        self.excluded[(r["mode"], r["label"], "error")] += 1
                        continue
                    if self.incidents and in_incident(r, self.incidents):
                        self.excluded[(r["mode"], r["label"], "storage_incident")] += 1
                        self.incident_samples[(r["mode"], r["label"])] += 1
                        continue
                    # EFS samples: valid only at the session's backend connection count (common-rules.md "Amazon
                    # EFS connection count is a measured variable"); cold samples carry it in checks / cold_ok
                    efs = efs_state(r)
                    self.efs_counts[(r["mode"], r["label"], efs)] += 1
                    if ((efs == "unknown" and not keep_unknown_efs) or r.get("efs_connections_ok") is False
                            or (efs not in (None, "unknown") and efs_select is not None and efs != str(efs_select))):
                        self.excluded[(r["mode"], r["label"], "efs_connections")] += 1
                        continue
                    if efs is not None:
                        self.efs_used.add(efs)
                    if r["mode"] == "cold":
                        self.verify[r["label"]]["ok"] += 1
                        if r.get("checks", {}).get("io_size_ok") is False:
                            self.verify[r["label"]]["io_size_violations"] += 1
                        if r.get("checks", {}).get("no_io"):
                            self.verify[r["label"]]["no_io"] += 1
                    self.samples[(r["mode"], r["label"], r["run_id"], r["op"])].append(r)
                elif t == "result":
                    self.results[(r["label"], r["op"])][r["run_id"]] = r["canonical"]
                elif t == "batch":
                    self.batches.append(r)
        # runs discarded by coldbench (the node failed during the run; re-queued under a new run id): none of their
        # samples or results count
        self.discarded = set()
        for d in dirs:
            for r in read_jsonl(os.path.join(d, "samples.jsonl")):
                if r["type"] == "run_discarded":
                    self.discarded.add(r["run_id"])
        if self.discarded:
            for k in [k for k in self.samples if k[2] in self.discarded]:
                self.excluded[(k[0], k[1], "run_discarded")] += len(self.samples.pop(k))
            for k in self.results:
                for rid in self.discarded & set(self.results[k]):
                    del self.results[k][rid]
            for rid in self.discarded & set(self.runs):
                del self.runs[rid]
        # EFS runs by the storage model's state test at their start and end (coldbench run_end efs_state.run_state:
        # fast, degraded, slow, mixed, reconnect, unknown; absent = not probed); --efs-run-state keeps only the EFS
        # runs in the given states (EBS runs are not affected)
        self.efs_run_state = {}
        for d in dirs:
            for r in read_jsonl(os.path.join(d, "samples.jsonl")):
                if r["type"] == "run_end" and r.get("efs_state") is not None:
                    self.efs_run_state[r["run_id"]] = r["efs_state"].get("run_state", "unknown")
        self.efs_run_state_counts = collections.Counter()
        for rid, run in self.runs.items():
            if run.get("arm") and any(k[2] == rid and k[0] in ("cold", "warm") for k in self.samples):
                efs_run = any(s.get("io", {}).get("nfs") is not None for k, v in self.samples.items() if k[2] == rid for s in v[:1])
                if efs_run:
                    self.efs_run_state_counts[(run["label"], self.efs_run_state.get(rid, "not probed"))] += 1
        if efs_run_states:
            for k in [k for k in self.samples if self.samples[k] and self.samples[k][0].get("io", {}).get("nfs") is not None
                      and self.efs_run_state.get(k[2], "not probed") not in efs_run_states]:
                self.excluded[(k[0], k[1], "efs_run_state")] += len(self.samples.pop(k))
        self.ref = self.sessions[0]["reference_op"] if self.sessions else None
        self.families = {}
        for s in self.sessions:
            ops_file = s.get("ops_file")
            if ops_file and os.path.exists(ops_file):
                for o in json.load(open(ops_file))["ops"]:
                    self.families[o["name"]] = o["families"]

    def labels(self):
        return sorted({k[1] for k in self.samples})

    def ops(self, mode):
        return sorted({k[3] for k in self.samples if k[0] == mode})

    def values(self, mode, label, run_id, op):
        m = self.metric[mode]
        return [s[m] for s in self.samples.get((mode, label, run_id, op), []) if s.get(m) is not None]

    def run_medians(self, mode, label, op):
        out = {}
        for (md, lab, rid, o), ss in self.samples.items():
            if md == mode and lab == label and o == op:
                v = self.values(md, lab, rid, o)
                if v:
                    out[rid] = statistics.median(v)
        return out

    def pooled(self, mode, label, op):
        out = []
        for (md, lab, rid, o) in self.samples:
            if md == mode and lab == label and o == op:
                out += self.values(md, lab, rid, o)
        return out


def family_of(op, families):
    f = families.get(op)
    return "+".join(sorted(f)) if f else op.split(":", 1)[0]


# ---------------------------------------------------------------- analyses
def compare(data, mode, base, var, a, floors):
    rows = []
    for op in data.ops(mode):
        rb, rv = data.run_medians(mode, base, op), data.run_medians(mode, var, op)
        if len(rb) < 2 or len(rv) < 2:
            continue
        b, v = list(rb.values()), list(rv.values())
        point, lo, hi = boot_change(b, v, a.boot, a.seed)
        rows.append({"op": op, "family": family_of(op, data.families), "base_ms": statistics.median(b),
                     "var_ms": statistics.median(v), "d_ms": statistics.median(v) - statistics.median(b),
                     "change": point, "ci_low": lo, "ci_high": hi, "perm_p": perm_p(b, v), "mw_p": mann_whitney_p(b, v),
                     "n_base": len(b), "n_var": len(v)})
    for r in rows:
        # exact Mann-Whitney on run medians; the permutation p of the median difference is coarse at 5 runs (swapping
        # extreme runs leaves both medians unchanged), so it is reported, not used
        r["p"] = r["mw_p"] if r["mw_p"] is not None else r["perm_p"]
    qs = bh([r["p"] for r in rows]) if rows else []
    for r, q in zip(rows, qs):
        r["q"] = q
        fl = floors.get(mode, {})
        r["floor"] = max(fl.get("family", {}).get(r["family"], 0.0), fl.get("op", {}).get(r["op"], 0.0))
        outside = r["change"] is not None and abs(r["change"]) > r["floor"]
        r["significant"] = q < a.alpha and outside
        r["faster"] = r["significant"] and r["d_ms"] < 0
        r["slower"] = r["significant"] and r["d_ms"] > 0
        r["regression_bar"] = r["ci_low"] is not None and r["ci_low"] > 0 and outside
    return rows


def aa_floors(data, a, l1, l2):
    floors = {}
    for mode in ("cold", "warm"):
        per_op, per_fam = {}, {}
        for op in data.ops(mode):
            r1, r2 = data.run_medians(mode, l1, op), data.run_medians(mode, l2, op)
            if len(r1) < 2 or len(r2) < 2:
                continue
            point, lo, hi = boot_change(list(r1.values()), list(r2.values()), a.boot, a.seed)
            # op floor: the A/A |change| or the half width of its CI, whichever is larger; family floor: max |change|
            per_op[op] = max(abs(point), (abs(hi - lo) / 2) if lo is not None else 0.0)
            fam = family_of(op, data.families)
            per_fam[fam] = max(per_fam.get(fam, 0.0), abs(point))
        vals = sorted(per_op.values())
        floors[mode] = {"op": per_op, "family": per_fam, "p50": pct(vals, 50) if vals else None,
                        "p90": pct(vals, 90) if vals else None, "max": vals[-1] if vals else None}
    return floors


def amdahl(data, labels):
    out = []
    for lab in labels:
        for op in data.ops("cold"):
            c, w = data.run_medians("cold", lab, op), data.run_medians("warm", lab, op)
            if c and w:
                cm, wm = statistics.median(c.values()), statistics.median(w.values())
                if cm < wm:
                    out.append({"label": lab, "op": op, "cold_ms": cm, "warm_ms": wm})
    return out


def amdahl_budget(data, base, var):
    """Cold gain larger than the base's cold - warm budget is impossible: the change cannot remove more than the waits."""
    out = []
    for op in data.ops("cold"):
        cb, wb, cv = (data.run_medians("cold", base, op), data.run_medians("warm", base, op),
                      data.run_medians("cold", var, op))
        if cb and wb and cv:
            budget = statistics.median(cb.values()) - statistics.median(wb.values())
            gain = statistics.median(cb.values()) - statistics.median(cv.values())
            if gain > budget + 1e-9 and gain > 0:
                out.append({"op": op, "gain_ms": gain, "budget_ms": budget})
    return out


def steady_state(data, threshold):
    out = collections.defaultdict(lambda: {"runs": 0, "trend_runs": 0, "worst": None})
    for (mode, lab, rid, op), ss in data.samples.items():
        if mode != "warm":
            continue
        v = [s[data.metric["warm"]] for s in sorted(ss, key=lambda s: s["iter"])]
        if len(v) < 4:
            continue
        h = len(v) // 2
        m1, m2 = statistics.median(v[:h]), statistics.median(v[h:])
        trend = m2 / m1 - 1 if m1 else 0.0
        o = out[lab]
        o["runs"] += 1
        if abs(trend) > threshold:
            o["trend_runs"] += 1
        if o["worst"] is None or abs(trend) > abs(o["worst"][1]):
            o["worst"] = (f"{op} {rid}", trend)
    return dict(out)


def ref_drift(data):
    out = {}
    if not data.ref:
        return out
    for mode in ("cold", "warm"):
        per = collections.defaultdict(list)
        for (md, lab, rid, op), ss in data.samples.items():
            if md != mode or op != data.ref:
                continue
            by_pos = collections.defaultdict(list)
            for s in ss:
                by_pos[s.get("pos", 0)].append(s[data.metric[mode]])
            if len(by_pos) >= 2:
                first, last = min(by_pos), max(by_pos)
                f, l = statistics.median(by_pos[first]), statistics.median(by_pos[last])
                per[lab].append(l / f - 1 if f else 0.0)
        out[mode] = {lab: {"median_drift": statistics.median(v), "max_abs": max(abs(x) for x in v), "runs": len(v)}
                     for lab, v in per.items()}
    return out


def tie_groups(hits):
    groups = []
    for h in hits:
        key = json.dumps([h[1], h[2]])
        if groups and groups[-1][0] == key:
            groups[-1][1].append(h[0])
        else:
            groups.append((key, [h[0]]))
    return groups


def results_equal(x, y, compare_ids=True):
    """(equal, reason). Ties: every tie group but the last must hold the same ids; the last group the same size."""
    if x.get("digest") == y.get("digest"):
        return True, ""
    if x.get("total") != y.get("total"):
        return False, f"total {x.get('total')} vs {y.get('total')}"
    if x.get("aggs") != y.get("aggs"):
        return False, "aggregations differ"
    if "hits_digest" in x or "hits_digest" in y:
        if x.get("hits_count") != y.get("hits_count") or (compare_ids and x.get("hits_digest") != y.get("hits_digest")):
            return False, f"scroll hits {x.get('hits_count')} vs {y.get('hits_count')}"
        return True, ""
    gx, gy = tie_groups(x.get("hits", [])), tie_groups(y.get("hits", []))
    if [g[0] for g in gx] != [g[0] for g in gy]:
        return False, "hit scores / sort values differ"
    for i, (a, b) in enumerate(zip(gx, gy)):
        last = i == len(gx) - 1
        if len(a[1]) != len(b[1]) or (compare_ids and not last and sorted(a[1]) != sorted(b[1])):
            return False, f"tie group {i} differs"
    return True, "equal up to ties"


def equality(data, ref_label, compare_ids):
    out = {"within_arm": [], "across": []}
    by_label = collections.defaultdict(dict)
    for (lab, op), runs in data.results.items():
        canon = list(runs.values())
        for c in canon[1:]:
            ok, why = results_equal(canon[0], c, compare_ids)
            if not ok:
                out["within_arm"].append({"label": lab, "op": op, "reason": why})
                break
        by_label[lab][op] = canon[0]
    if ref_label in by_label:
        for lab, ops in by_label.items():
            if lab == ref_label:
                continue
            for op, c in ops.items():
                if op in by_label[ref_label]:
                    ok, why = results_equal(by_label[ref_label][op], c, compare_ids)
                    out["across"].append({"label": lab, "op": op, "equal": ok, "reason": why})
    return out


def io_table(data, labels, mode="cold"):
    out = {}
    for lab in labels:
        for op in data.ops(mode):
            ss = [s for (md, l, rid, o), v in data.samples.items() if md == mode and l == lab and o == op for s in v]
            if not ss:
                continue

            def med(f):
                xs = [f(s) for s in ss]
                xs = [x for x in xs if x is not None]
                return statistics.median(xs) if xs else None
            exts = collections.defaultdict(list)
            dem = collections.defaultdict(list)
            for s in ss:
                for ext, e in (s.get("io", {}).get("bp_files") or {}).items():
                    exts[ext].append(e.get("loads", 0) + e.get("prefetch_loads", 0))
                    dem[ext].append(e.get("loads", 0))
            out[(lab, op)] = {
                "demand_loads": med(lambda s: (s.get("io", {}).get("bp") or {}).get("loads")),
                "prefetch_loads": med(lambda s: (s.get("io", {}).get("bp") or {}).get("prefetch_loads")),
                "bp_bytes": med(lambda s: (s.get("io", {}).get("bp") or {}).get("bytes_loaded")),
                "jvm_read_bytes": med(lambda s: (s.get("io", {}).get("proc_io") or {}).get("read_bytes")),
                "dev_reads": med(lambda s: (s.get("io", {}).get("disk") or {}).get("reads")),
                "dev_read_bytes": med(lambda s: (s.get("io", {}).get("disk") or {}).get("read_bytes")),
                "r_await_ms": med(lambda s: (s.get("io", {}).get("disk") or {}).get("r_await_ms")),
                "aqu_sz": med(lambda s: (s.get("io", {}).get("disk") or {}).get("aqu_sz")),
                "loads_by_ext": {e: statistics.median(v) for e, v in sorted(exts.items())},
                "demand_by_ext": {e: statistics.median(v) for e, v in sorted(dem.items())},
                "nfs_read_ops": med(lambda s: ((s.get("io", {}).get("nfs") or {}).get("READ") or {}).get("ops")),
                "nfs_read_bytes": med(lambda s: ((s.get("io", {}).get("nfs") or {}).get("READ") or {}).get("bytes_recv")),
                "nfs_rtt_ms": med(lambda s: (s.get("io", {}).get("nfs") or {}).get("read_rtt_avg_ms")),
                "nfs_in_flight": med(lambda s: (s.get("io", {}).get("nfs") or {}).get("read_in_flight")),
            }
    return out


def storage_reads_check(data, pairs, tol):
    """
    All-off compared with baseline (USER DECISION 2026-10-05: the proof-of-concept build with every change off must equal
    the baseline): per cold op and pair (reference, target), the medians of the bufferpool's total storage reads and
    bytes per query, with the demand and prefetch split recorded separately (reads = demand + prefetch). An op whose
    total reads or bytes differ by more than tol (relative) is listed as different; it is evidence, never a verdict.
    """
    out = []
    for ref, tgt in pairs:
        for op in data.ops("cold"):
            row = {"reference": ref, "target": tgt, "op": op}
            for side, lab in (("ref", ref), ("tgt", tgt)):
                ss = [s for (md, l, rid, o), v in data.samples.items() if md == "cold" and l == lab and o == op for s in v]
                bps = [(s.get("io", {}).get("bp") or {}) for s in ss]
                bps = [b for b in bps if b]
                if not bps:
                    row[side] = None
                    continue

                def med(f):
                    return statistics.median(f(b) for b in bps)
                row[side] = {"n": len(bps), "reads": med(lambda b: b.get("reads", 0)),
                             "bytes_read": med(lambda b: b.get("bytes_read", 0)),
                             "demand_reads": med(lambda b: b.get("reads", 0) - b.get("prefetch_reads", 0)),
                             "prefetch_reads": med(lambda b: b.get("prefetch_reads", 0)),
                             "demand_loads": med(lambda b: b.get("loads", 0)),
                             "prefetch_loads": med(lambda b: b.get("prefetch_loads", 0))}
            if row["ref"] and row["tgt"]:
                def rel(k):
                    r, t = row["ref"][k], row["tgt"][k]
                    return (t / r) if r else (1.0 if t == 0 else None)
                row["reads_ratio"], row["bytes_ratio"] = rel("reads"), rel("bytes_read")
                row["different"] = any(x is None or abs(x - 1) > tol for x in (row["reads_ratio"], row["bytes_ratio"]))
            out.append(row)
    return out


def storage_pairs(outcome, labels, cli):
    """Pairs (reference, all-off) for the storage reads check: --io-check REF:TGT,..., else the arms file's
    outcome.storage_reads_pairs (arms_baseline.py: BASE-EBS:S1-EBS, BASE-EFS:S1-EFS, stock OpenSearch with the bufferpool
    against the all-off build), else each same-storage reference with its attribution reference. Pairs whose labels
    are not in the session are skipped."""
    if cli:
        return [tuple(x.split(":", 1)) for x in cli.split(",")]
    if (outcome or {}).get("storage_reads_pairs") is not None:
        return [(r, t) for r, t in outcome["storage_reads_pairs"] if r in labels and t in labels]
    att = (outcome or {}).get("attribution_reference") or {}
    pairs = []
    for ss in (outcome or {}).get("same_storage") or []:
        storage = ss["reference"].rsplit("-", 1)[-1]
        if att.get(storage):
            pairs.append((ss["reference"], att[storage]))
    return [(r, t) for r, t in pairs if r in labels and t in labels]


# ---------------------------------------------------------------- non-inferiority (the outcome)
NI_STATS = [("cold_p50", "cold", 50, True), ("cold_p90", "cold", 90, True), ("warm_p50", "warm", 50, True),
            ("cold_p99", "cold", 99, False)]


def run_samples(data, mode, labels, op):
    """[[values of one JVM run], ...] over every run of the given labels (A/A labels of one arm may be pooled)."""
    out = []
    for (md, lab, rid, o) in sorted(data.samples):
        if md == mode and lab in labels and o == op:
            v = data.values(md, lab, rid, o)
            if v:
                out.append(v)
    return out


def hboot_ratio(ref_runs, tgt_runs, q, resamples, seed):
    """
    stat(target) / stat(reference) for a quantile q of the pooled samples, with a two-level bootstrap: resample JVM
    runs with replacement, then iterations within each drawn run, pool, take the quantile. Returns point, the 2.5 and
    97.5 percentiles, and the sorted bootstrap ratios.
    """
    pool = lambda runs: [x for r in runs for x in r]  # noqa: E731
    rq = pct(pool(ref_runs), q)
    point = pct(pool(tgt_runs), q) / rq if rq else None
    rng = random.Random(seed)
    dist = []
    for _ in range(resamples):
        rr = [x for r in rng.choices(ref_runs, k=len(ref_runs)) for x in rng.choices(r, k=len(r))]
        tt = [x for r in rng.choices(tgt_runs, k=len(tgt_runs)) for x in rng.choices(r, k=len(r))]
        b = pct(rr, q)
        if b:
            dist.append(pct(tt, q) / b)
    dist.sort()
    if not dist:
        return point, None, None, dist
    return point, dist[int(0.025 * len(dist))], dist[max(0, int(math.ceil(0.975 * len(dist))) - 1)], dist


def noninferiority(data, targets, ref_labels, aa_labels, delta_min, a):
    """
    Per op and statistic: target / reference ratio with its 95% CI. Margin delta = max(delta_min, A/A MDE of the
    reference for that op and statistic; MDE = max(|A/A ratio - 1|, A/A CI half width)). One-sided bootstrap p-values:
    p_ni for H0 ratio >= 1 + delta, p_worse for H0 ratio <= 1 + delta; Benjamini-Hochberg over the ops of each
    (target, statistic). Verdict: PASS if the CI upper bound <= 1 + delta and q_ni < alpha; WORSE if q_worse < alpha;
    otherwise INCONCLUSIVE (add JVM runs; never reported as a pass). An op passes when every required statistic
    (cold p50, cold p90, warm p50) passes; cold p99 is informational.
    """
    out = {}
    for tgt in targets:
        rows = {}
        for name, mode, q, required in NI_STATS:
            cells = []
            for op in data.ops(mode):
                rr, tr = run_samples(data, mode, ref_labels, op), run_samples(data, mode, tgt.split("+"), op)
                if len(rr) < 2 or len(tr) < 2:
                    continue
                point, lo, hi, dist = hboot_ratio(rr, tr, q, a.ni_boot, a.seed)
                mde, aa = None, None
                if aa_labels:
                    r1, r2 = run_samples(data, mode, [aa_labels[0]], op), run_samples(data, mode, [aa_labels[1]], op)
                    if len(r1) >= 2 and len(r2) >= 2:
                        ap, alo, ahi, _ = hboot_ratio(r1, r2, q, a.ni_boot, a.seed + 1)
                        mde = max(abs(ap - 1), (ahi - alo) / 2 if alo is not None else 0.0)
                        aa = [ap, alo, ahi]
                delta = max(delta_min, mde or 0.0)
                margin = 1 + delta
                n = len(dist)
                p_ni = (sum(1 for x in dist if x >= margin) + 1) / (n + 1)
                p_worse = (sum(1 for x in dist if x <= margin) + 1) / (n + 1)
                ref_v = pct([x for r in rr for x in r], q)
                tgt_v = pct([x for r in tr for x in r], q)
                cells.append({"op": op, "stat": name, "required": required, "ratio": point, "ci_low": lo, "ci_high": hi,
                              "delta": delta, "aa_mde": mde, "aa": aa, "p_ni": p_ni, "p_worse": p_worse,
                              "ref_ms": ref_v, "tgt_ms": tgt_v, "gap_ms": tgt_v - ref_v, "runs": [len(rr), len(tr)]})
            q_ni = bh([c["p_ni"] for c in cells]) if cells else []
            q_w = bh([c["p_worse"] for c in cells]) if cells else []
            for c, x, y in zip(cells, q_ni, q_w):
                c["q_ni"], c["q_worse"] = x, y
                if c["ci_high"] is not None and c["ci_high"] <= 1 + c["delta"] and x < a.alpha:
                    c["verdict"] = "PASS"
                elif y < a.alpha:
                    c["verdict"] = "WORSE"
                else:
                    c["verdict"] = "INCONCLUSIVE"
                rows.setdefault(c["op"], {})[name] = c
        for op, st in rows.items():
            req = [st[n]["verdict"] for n, _, _, r in NI_STATS if r and n in st]
            missing = [n for n, _, _, r in NI_STATS if r and n not in st]
            st["_verdict"] = ("WORSE" if "WORSE" in req else "PASS" if req and all(v == "PASS" for v in req) and not missing
                              else "INCONCLUSIVE")
            st["_missing"] = missing
        out[tgt] = rows
    return out


def ni_report(md, ni, ref_labels, data, io):
    for tgt, rows in ni.items():
        verdicts = collections.Counter(st["_verdict"] for st in rows.values())
        md.append(f"\n## Outcome: {tgt} not worse than {'+'.join(ref_labels)} (per op; PASS {verdicts['PASS']}, "
                  f"WORSE {verdicts['WORSE']}, INCONCLUSIVE {verdicts['INCONCLUSIVE']} of {len(rows)})")
        md.append("Ratio target/reference [95% CI] vs margin 1+delta, delta = max(5 %, A/A MDE); BH over ops per statistic.")
        md.append("| op | cold p50 | cold p90 | warm p50 | cold p99 (info) | verdict |")
        md.append("|---|---|---|---|---|---|")

        def cell(c):
            if not c:
                return "-"
            ci = "" if c["ci_low"] is None else f" [{c['ci_low']:.2f}, {c['ci_high']:.2f}]"
            return f"{c['ratio']:.2f}{ci} <= {1 + c['delta']:.2f} {c['verdict']}"
        for op in sorted(rows):
            st = rows[op]
            md.append(f"| {op} | {cell(st.get('cold_p50'))} | {cell(st.get('cold_p90'))} | {cell(st.get('warm_p50'))} | "
                      f"{cell(st.get('cold_p99'))} | {st['_verdict']}{' (missing ' + ','.join(st['_missing']) + ')' if st['_missing'] else ''} |")
        bad = [(op, st) for op, st in sorted(rows.items()) if st["_verdict"] != "PASS"]
        if bad:
            md.append(f"\nOps that do not meet the outcome yet ({tgt}): gap in ms and the IO that remains (cold, medians per request)")
            for op, st in bad:
                gaps = ", ".join(f"{n} {st[n]['gap_ms']:+.1f} ms ({st[n]['verdict']})" for n, _, _, r in NI_STATS if r and n in st)
                x = io.get((tgt.split("+")[0], op)) or {}
                ext = sorted((x.get("demand_by_ext") or {}).items(), key=lambda kv: -kv[1])[:4]
                ioinfo = (f"demand loads {x.get('demand_loads')}, prefetch loads {x.get('prefetch_loads')}, top demand file types "
                          f"{', '.join(f'{e} {v:g}' for e, v in ext) or '-'}; NFS READ ops {x.get('nfs_read_ops')}, avg RTT "
                          f"{fmt_ms(x.get('nfs_rtt_ms'))} ms, READs in flight {x.get('nfs_in_flight')}")
                md.append(f"- {op}: {gaps}; {ioinfo}")


# ---------------------------------------------------------------- report
def fmt_ms(x):
    return "-" if x is None else (f"{x:.1f}" if abs(x) < 1000 else f"{x:.0f}")


def fmt_change(r):
    if r["change"] is None:
        return "-"
    ci = "" if r["ci_low"] is None else f" [{r['ci_low'] * 100:+.1f}, {r['ci_high'] * 100:+.1f}]"
    return f"{r['d_ms']:+.1f} ms ({r['change'] * 100:+.1f} %){ci}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("sessions", nargs="+")
    ap.add_argument("--base", required=True)
    ap.add_argument("--compare", help="comma list of labels (default: every other label)")
    ap.add_argument("--aa", help="LABEL1,LABEL2 of an A/A pair")
    ap.add_argument("--equality-ref", help="label every arm's results are compared to (default: S0 if present, else base)")
    ap.add_argument("--no-id-compare", action="store_true", help="compare results without doc ids (different indices)")
    ap.add_argument("--cold-metric", default="took_ms", choices=["took_ms", "wall_ms"])
    ap.add_argument("--warm-metric", default="wall_ms", choices=["took_ms", "wall_ms"])
    ap.add_argument("--boot", type=int, default=10000)
    ap.add_argument("--alpha", type=float, default=0.05)
    ap.add_argument("--steady-threshold", type=float, default=0.05)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--ni-target", help="comma list of target labels for the outcome verdict (default: arms outcome.targets); "
                    "LABEL1+LABEL2 pools the runs of several labels into one target, e.g. S1-EFS@a+S1-EFS@b")
    ap.add_argument("--ni-ref", help="reference label(s), comma list pooled, e.g. S0-EBS@a,S0-EBS@b (default: arms outcome)")
    ap.add_argument("--ni-aa", help="A/A pair of the reference for the margin (default: arms outcome.aa)")
    ap.add_argument("--ni-delta", type=float, default=0.05, help="minimum margin delta")
    ap.add_argument("--ni-boot", type=int, default=5000)
    ap.add_argument("--out", help="directory for report.md and analysis.json (default: first session)")
    ap.add_argument("--io-check", help="REF:TGT,... pairs for the storage reads check (default: from the arms file's "
                                       "outcome: each storage's baseline with its all-off attribution reference)")
    ap.add_argument("--io-check-tol", type=float, default=0.02, help="relative tolerance of the storage reads check")
    ap.add_argument("--cold-skip-iters", type=int, default=0,
                    help="leave out cold iterations < N of every op (sessions of the old jit-cold protocol: 1 leaves out "
                         "iteration 0, which also paid the JIT compilation of its query shape)")
    ap.add_argument("--efs-connections", type=int,
                    help="keep only EFS samples at this efs-proxy backend connection count (start = end); needed when "
                         "the kept EFS samples have more than one count")
    ap.add_argument("--efs-run-state", help="comma list: keep only EFS runs whose storage-model state test (coldbench "
                    "run_end efs_state) is one of these, e.g. fast or fast,degraded; 'not probed' names runs without it")
    ap.add_argument("--keep-unknown-efs-connections", action="store_true",
                    help="keep EFS samples recorded without the connection count (older sessions); the report labels "
                         "them 'connection state unknown', and they cannot carry an EFS verdict")
    a = ap.parse_args()
    data = Data(a.sessions, a.cold_metric, a.warm_metric, a.cold_skip_iters, a.keep_unknown_efs_connections,
                a.efs_connections, set(a.efs_run_state.split(",")) if a.efs_run_state else None)
    if len(data.efs_used - {"unknown"}) > 1:
        sys.exit(f"EFS samples at different backend connection counts {sorted(data.efs_used)} cannot be compared in one "
                 "analysis; select one with --efs-connections N")
    labels = data.labels()
    protocol = cold_protocols(data, a.cold_skip_iters)
    others = a.compare.split(",") if a.compare else [l for l in labels if l != a.base]
    floors = {}
    if a.aa:
        l1, l2 = a.aa.split(",")
        floors = aa_floors(data, a, l1, l2)
    out_dir = a.out or a.sessions[0]
    os.makedirs(out_dir, exist_ok=True)
    md = []
    res = {"labels": labels, "base": a.base, "floors": floors, "comparisons": {}, "excluded": {str(k): v for k, v in data.excluded.items()},
           "cold_protocol": protocol, "cold_skipped": dict(data.skipped),
           "efs_run_states": {f"{k[0]}|{k[1]}": v for k, v in data.efs_run_state_counts.items()}}
    md.append(f"# coldbench analysis: base {a.base}")
    md.append(f"Sessions: {', '.join(a.sessions)}. Unit = JVM run; per-run median, then median over runs. Cold metric "
              f"{a.cold_metric}, warm metric {a.warm_metric}. Bootstrap {a.boot} resamples of runs; exact Mann-Whitney p "
              f"on run medians; BH q < {a.alpha} across the ops of each comparison, and |change| above the A/A floor. "
              f"Cold protocol: {protocol}" + (f" ({sum(data.skipped.values())} cold samples of iterations < "
                                              f"{a.cold_skip_iters} left out)" if a.cold_skip_iters else "") + ".")
    if data.efs_counts:
        md.append("EFS backend connection count per kept or dropped sample (mode, label, count): "
                  + ", ".join(f"{m} {l} {e}: {n}" for (m, l, e), n in sorted(data.efs_counts.items(), key=str) if e is not None)
                  + (". Samples at 'unknown' are kept with connection state unknown and carry no EFS verdict."
                     if "unknown" in data.efs_used else "."))
    res["efs_connections"] = {"kept_states": sorted(data.efs_used),
                              "per_label": {f"{m}|{l}|{e}": n for (m, l, e), n in data.efs_counts.items() if e is not None}}
    res["storage_incidents"] = {"windows": [{"start": a0, "end": b0, "server": sv, "run_id": rid}
                                            for a0, b0, sv, rid in data.incidents],
                                "excluded_samples": {f"{m}|{l}": n for (m, l), n in data.incident_samples.items()}}
    if data.incidents:
        md.append(f"Storage incidents (kernel NFS stall windows, samples inside excluded from every verdict; "
                  f"sensitivity only): {len(data.incidents)} window(s), excluded samples "
                  + ", ".join(f"{m} {l}: {n}" for (m, l), n in sorted(data.incident_samples.items())) + ".")
    unavailable = [r for r in data.runs.values() if not r.get("available", True)]
    if unavailable:
        md.append("\n## Not available (gaps, not zero effects)")
        for r in unavailable:
            md.append(f"- {r['run_id']}: {r.get('reason')}")
    md.append("\n## Cold verification")
    md.append("| label | ok | no IO (vacuous) | excluded (failed checks) | reads above max IO size |")
    md.append("|---|---|---|---|---|")
    for lab in labels:
        v = data.verify[lab]
        fails = ", ".join(f"{k[5:]} {n}" for k, n in v.items() if k.startswith("fail:")) or "0"
        md.append(f"| {lab} | {v['ok']} | {v['no_io']} | {fails} | {v['io_size_violations']} |")
    if floors:
        md.append(f"\n## A/A noise floor ({a.aa})")
        for mode, f in floors.items():
            if f["p50"] is not None:
                md.append(f"- {mode}: |median change| p50 {f['p50'] * 100:.1f} %, p90 {f['p90'] * 100:.1f} %, max {f['max'] * 100:.1f} % "
                          f"over {len(f['op'])} ops")
    for var in others:
        for mode in ("cold", "warm"):
            rows = compare(data, mode, a.base, var, a, floors)
            if not rows:
                continue
            res["comparisons"][f"{var}:{mode}"] = rows
            faster = sum(r["faster"] for r in rows)
            slower = sum(r["slower"] for r in rows)
            bar = [r for r in rows if r["regression_bar"]]
            md.append(f"\n## {var} vs {a.base}, {mode}: significant faster {faster}, slower {slower} of {len(rows)} ops"
                      + (f"; warm regression bar FAILS on {len(bar)}: " + ", ".join(r['op'] for r in bar) if mode == "warm" and bar else
                         ("; warm regression bar PASS" if mode == "warm" else "")))
            md.append(f"| op | family | {a.base} ms | {var} ms | change [95% CI] | MW p | q | perm p | runs | sig |")
            md.append("|---|---|---|---|---|---|---|---|---|---|")
            for r in sorted(rows, key=lambda r: r["change"] or 0):
                mw = f"{r['perm_p']:.3f}"
                md.append(f"| {r['op']} | {r['family']} | {fmt_ms(r['base_ms'])} | {fmt_ms(r['var_ms'])} | {fmt_change(r)} | "
                          f"{r['p']:.3f} | {r['q']:.3f} | {mw} | "
                          f"{r['n_base']}/{r['n_var']} | {'faster' if r['faster'] else 'SLOWER' if r['slower'] else ''} |")
            if mode == "cold":
                bad = amdahl_budget(data, a.base, var)
                res["comparisons"][f"{var}:amdahl_budget"] = bad
                if bad:
                    md.append(f"\nAmdahl budget violated (cold gain > base cold - warm) on: "
                              + ", ".join(f"{b['op']} gain {b['gain_ms']:.1f} > budget {b['budget_ms']:.1f} ms" for b in bad))
    md.append("\n## Percentiles (pooled samples), ms: p50 / p90 / p99 / p100 (n)")
    for mode in ("cold", "warm", "ccold", "cwarm"):
        ops = data.ops(mode)
        if not ops:
            continue
        md.append(f"\n### {mode}")
        md.append("| op | " + " | ".join(labels) + " |")
        md.append("|---|" + "---|" * len(labels))
        for op in ops:
            cells = []
            for lab in labels:
                v = data.pooled(mode, lab, op)
                cells.append("-" if not v else f"{fmt_ms(pct(v, 50))} / {fmt_ms(pct(v, 90))} / {fmt_ms(pct(v, 99))} / {fmt_ms(max(v))} ({len(v)})")
            md.append(f"| {op} | " + " | ".join(cells) + " |")
    am = amdahl(data, labels)
    res["amdahl"] = am
    md.append("\n## Amdahl check (cold median must not be below warm median)")
    md.append("PASS" if not am else "\n".join(f"- {x['label']} {x['op']}: cold {x['cold_ms']:.1f} < warm {x['warm_ms']:.1f} ms" for x in am))
    st = steady_state(data, a.steady_threshold)
    res["steady_state"] = st
    md.append(f"\n## Warm steady state (|second-half / first-half median - 1| > {a.steady_threshold * 100:.0f} %)")
    for lab, s in sorted(st.items()):
        w = s["worst"]
        md.append(f"- {lab}: {s['trend_runs']} of {s['runs']} op-runs trend; worst {w[0]} {w[1] * 100:+.1f} %")
    dr = ref_drift(data)
    res["reference_drift"] = dr
    md.append(f"\n## Reference op drift ({data.ref}: last vs first in each run)")
    for mode, per in dr.items():
        for lab, d in sorted(per.items()):
            md.append(f"- {mode} {lab}: median {d['median_drift'] * 100:+.1f} %, max |{d['max_abs'] * 100:.1f}| % over {d['runs']} runs")
    eq_ref = a.equality_ref or ("S0" if "S0" in {k[0] for k in data.results} else a.base)
    eq = equality(data, eq_ref, not a.no_id_compare)
    res["equality"] = eq
    diff = [e for e in eq["across"] if not e["equal"]]
    md.append(f"\n## Result equality vs {eq_ref}: {len(eq['across']) - len(diff)} equal, {len(diff)} DIFFERENT; "
              f"within-arm differences {len(eq['within_arm'])}")
    for e in diff + eq["within_arm"]:
        md.append(f"- {e['label']} {e['op']}: {e['reason']}")
    io = io_table(data, labels)
    res["io_cold"] = {f"{k[0]}|{k[1]}": v for k, v in io.items()}
    md.append("\n## Cold IO per request (medians): demand loads / prefetch loads / storage MB (EBS device or NFS READ) / "
              "r_await or NFS RTT ms / aqu-sz or NFS READs in flight")
    md.append("| op | " + " | ".join(labels) + " |")
    md.append("|---|" + "---|" * len(labels))
    for op in data.ops("cold"):
        cells = []
        for lab in labels:
            x = io.get((lab, op))
            if not x:
                cells.append("-")
                continue
            nfs = x["nfs_read_ops"] is not None
            byts = x["nfs_read_bytes"] if nfs else x["dev_read_bytes"]
            dev = "-" if byts is None else f"{byts / 1e6:.1f}"
            lat = x["nfs_rtt_ms"] if nfs else x["r_await_ms"]
            q = x["nfs_in_flight"] if nfs else x["aqu_sz"]
            aq = "-" if q is None else f"{q:.2f}"
            cells.append(f"{fmt_ms(x['demand_loads'])} / {fmt_ms(x['prefetch_loads'])} / {dev}{' nfs' if nfs else ''} / "
                         f"{fmt_ms(lat)} / {aq}")
        md.append(f"| {op} | " + " | ".join(cells) + " |")
    outcome = (data.sessions[0].get("arms_file") or {}).get("outcome", {}) if data.sessions else {}
    pairs = storage_pairs(outcome, labels, a.io_check)
    if pairs:
        chk = storage_reads_check(data, pairs, a.io_check_tol)
        res["storage_reads_check"] = chk
        md.append(f"\n## Storage reads per cold query, all changes off compared with the baseline (medians; tolerance "
                  f"{a.io_check_tol:.0%}): total reads / bytes MB (demand reads + prefetch reads)")
        md.append("| op | pair | reference | target | reads ratio | bytes ratio | different |")
        md.append("|---|---|---|---|---|---|---|")
        for r in chk:
            def cell(x):
                return "-" if not x else (f"{x['reads']:.0f} / {x['bytes_read'] / 1e6:.2f} "
                                          f"({x['demand_reads']:.0f} + {x['prefetch_reads']:.0f})")
            rr = "-" if r.get("reads_ratio") is None else f"{r['reads_ratio']:.3f}"
            br = "-" if r.get("bytes_ratio") is None else f"{r['bytes_ratio']:.3f}"
            md.append(f"| {r['op']} | {r['reference']} : {r['target']} | {cell(r['ref'])} | {cell(r['tgt'])} | {rr} | {br} | "
                      f"{'YES' if r.get('different') else ''} |")
    targets = a.ni_target.split(",") if a.ni_target else [t for t in outcome.get("targets", []) if t in labels]
    unknown = [l for t in targets for l in t.split("+") if l not in labels]
    if unknown:
        sys.exit(f"--ni-target: unknown labels {unknown}; labels: {labels}")
    ref = a.ni_ref.split(",") if a.ni_ref else ([outcome["reference"]] if outcome.get("reference") else [])
    ref = [r for r in ref if r in labels] or [l for l in labels if l.split("@")[0] in ref]
    aa_ni = (a.ni_aa or outcome.get("aa") or "").split(",") if (a.ni_aa or outcome.get("aa")) else []
    aa_ni = aa_ni if len(aa_ni) == 2 and all(l in labels for l in aa_ni) else []
    if targets and ref:
        ni = noninferiority(data, targets, ref, aa_ni, a.ni_delta, a)
        res["noninferiority"] = {"reference": ref, "aa": aa_ni, "delta_min": a.ni_delta, "targets": ni}
        if not aa_ni:
            md.append(f"\nNOTE: no A/A pair of the reference for the non-inferiority margin (--ni-aa); delta = {a.ni_delta:.0%} only")
        ni_report(md, ni, ref, data, io)
    if data.batches:
        md.append("\n## Concurrent batches")
        for lab in labels:
            bs = [b for b in data.batches if b["label"] == lab]
            if bs:
                rej = [((b.get("prefetch_pool") or {}).get("rejected")) for b in bs]
                okc = sum(1 for b in bs if b.get("cold_ok", True))
                md.append(f"- {lab}: {len(bs)} batches, cold-verified {okc}, prefetch pool rejected (last) {rej[-1]}")
    with open(os.path.join(out_dir, "report.md"), "w") as f:
        f.write("\n".join(md) + "\n")
    with open(os.path.join(out_dir, "analysis.json"), "w") as f:
        json.dump(res, f, indent=1, default=str)
    print("\n".join(md))


if __name__ == "__main__":
    main()
