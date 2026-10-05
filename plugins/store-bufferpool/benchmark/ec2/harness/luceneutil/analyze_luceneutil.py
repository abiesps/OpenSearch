#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Turns luceneutil sessions (run_luceneutil.py manifest.jsonl + the per-JVM result logs) into a coldbench session
(samples.jsonl), so analyze.py computes the same statistics as for the OpenSearch arms: per-JVM-run medians, bootstrap
95% CI of the median change (resampling JVM runs), exact Mann-Whitney, Benjamini-Hochberg across tasks, the A/A floor,
Amdahl and steady-state checks, result equality across arms, and the non-inferiority verdict (--ni-*).
  - op = luceneutil task identity (its TASK line without results); families = [luceneutil category]
  - sample: one task instance, took_ms = wall_ms = luceneutil's msec; mode "warm" (warm session), "cold" (cold-strict:
    cold_ok from the agent's residency check before that task, IO deltas from the agent snapshots around it) or
    "cold" with checks {"luceneutil_cold": true} (cold-luceneutil: caches dropped once per JVM, not per task)
  - result: per task, its hit count and result lines (doc ids with scores or sort values, in order), the same thing
    luceneutil's verifyScores / verifyCounts compare
  - run: one per JVM run, with the switch read-backs parsed from its stdout (a NOT AVAILABLE line makes the arm a gap)

  analyze_luceneutil.py convert --session results/lu/warm-ebs-1 --out results/lu/warm-ebs-1/coldbench
  analyze_luceneutil.py analyze --session results/lu/warm-ebs-1 -- --base L0 --aa L0@a,L0@b --ni-ref L0 --ni-target L1
"""
import argparse
import collections
import hashlib
import json
import os
import re
import subprocess
import sys

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
sys.path.insert(0, os.path.dirname(here))
import lu_results  # noqa: E402
import switches as sw  # noqa: E402
from common import JsonlWriter, read_jsonl  # noqa: E402

SCHEMA = 1
HIT = re.compile(r"^doc=(\S+) (\S+?)=(.*)$")


def _cold_records(path):
    if not path or not os.path.exists(path):
        return []
    return list(read_jsonl(path))


def _io(pre, post, bp_pre=None, bp_post=None):
    """Device / NFS counter deltas of one task (same keys as coldbench.io_delta, from the agent snapshots), plus the
    bufferpool counter deltas when the task read through the bufferpool directory (same stats shape as the plugin)."""
    import coldbench  # the harness's own delta code

    return coldbench.io_delta({"agent": pre, "bp": bp_pre}, {"agent": post, "bp": bp_post})


def coldbench_efs_ok(io, target):
    """coldbench's rule: None if not an EFS sample or no target; else start == end == target on one efs-proxy process."""
    import coldbench

    return coldbench.efs_connections_ok(io, target)


def convert(session_dir, out_dir, reference=None):
    session = json.load(open(os.path.join(session_dir, "session.json")))
    manifest = list(read_jsonl(os.path.join(session_dir, "manifest.jsonl")))
    if not manifest:
        raise SystemExit(f"{session_dir}: empty manifest")
    mode = "warm" if session["mode"] == "warm" else "cold"
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "samples.jsonl")
    if os.path.exists(path):
        raise SystemExit(f"{path} exists: convert into a new directory")
    w = JsonlWriter(path)
    families, ops = {}, []
    parsed = []
    for m in manifest:
        res = lu_results.parse(open(m["log"], encoding="utf-8", errors="replace").read())
        parsed.append((m, res))
        for t in res["tasks"]:
            if t.key not in families:
                families[t.key] = [t.category]
                ops.append(t.key)
    ref = reference or sorted(ops, key=lambda k: (families[k][0], k))[0]
    ops_file = os.path.join(out_dir, "ops.json")
    with open(ops_file, "w") as f:
        json.dump({"corpus": "luceneutil-wikimediumall", "reference_op": ref,
                   "ops": [{"name": k, "families": families[k], "type": "luceneutil"} for k in ops]}, f, indent=1)
    w.write({"schema": SCHEMA, "type": "session", "ops": ops, "reference_op": ref, "ops_file": ops_file,
             "luceneutil_session": session, "arms_file": {"outcome": session.get("outcome", {})}})
    counts = collections.Counter()
    for m, res in parsed:
        label = m["label"]
        # the session id keeps run ids of a warm and a cold session (analysed together) apart
        run_id = f"{label}#{session.get('id', 's')}#r{m['iter']}"
        run = {"arm": m["arm"], "label": label, "round": m["iter"], "run_id": run_id}
        if session.get("cold_protocol"):
            run["cold_protocol"] = session["cold_protocol"]
        stdout = open(m["stdout"], errors="replace").read() if os.path.exists(m.get("stdout") or "") else ""
        try:
            readbacks = sw.parse_readbacks(stdout)
            available, reason = True, None
        except sw.SwitchError as e:
            readbacks, available, reason = {}, False, str(e)
        sent = {s["switch"]: s["value"] for s in m.get("switches", [])}
        bad = {k: v for k, v in sent.items() if readbacks.get(k, {}).get("readback") != v}
        if available and bad:
            available, reason = False, f"switch read-back missing or different: {bad}"
        w.write({"schema": SCHEMA, "type": "run", **run, "available": available, "reason": reason,
                 "switch_readbacks": readbacks, "manifest": m, "winddown_ms": res["winddown_ms"],
                 "elapsed_ms": res["elapsed_ms"], "avg_cpu_cores": res["avg_cpu_cores"]})
        if not available:
            continue
        cold = _cold_records(m.get("cold_log"))
        if session["mode"] == "cold-strict" and len(cold) != len(res["tasks"]):
            raise SystemExit(f"{m['log']}: {len(res['tasks'])} tasks but {len(cold)} cold records")
        # IO of the whole JVM run (agent snapshots before and after it): EFS backend connections at its start and end;
        # warm and luceneutil-cold samples carry it (strict-cold samples have their own per-task snapshots)
        jvm_io = None
        js = m.get("jvm_snapshots")
        if js:
            jvm_io = _io(js.get("pre"), js.get("post"))
            jvm_io["window"] = "jvm_run"
        efs_target = m.get("efs_connections_target")
        inc = m.get("storage_incidents") or {}
        if inc.get("windows"):
            # luceneutil logs task times relative to the JVM, not wall clock: the whole JVM run is treated as inside the
            # stall (conservative); every sample of the run gets t = run end, and the window covers the run
            w.write({"schema": SCHEMA, "type": "storage_incident", "run_id": run_id, "label": label,
                     "windows": [{"start": m["epoch_start"], "end": m["epoch_end"], "server": x.get("server"),
                                  "kernel_window": [x["start"], x["end"]]} for x in inc["windows"]]})
        last = {}
        for i, t in enumerate(res["tasks"]):
            rec = {"schema": SCHEMA, "type": "sample", "mode": mode, **run, "op": t.key, "iter": counts[(run_id, t.key)],
                   "took_ms": t.msec, "wall_ms": t.msec, "requests": 1, "digest": t.digest(), "thread": t.thread}
            if m.get("epoch_end") is not None:
                rec["t"] = m["epoch_end"]
            counts[(run_id, t.key)] += 1
            if session["mode"] == "cold-luceneutil" and rec["iter"] > 0:
                last[t.key] = t
                continue  # only the first instance of a task after the drop is cold; later ones find its pages cached
            if session["mode"] == "cold-strict":
                c = cold[i]
                # the cold record carries task.toString(), which is the TASK line of the result log (the categories
                # differ in spelling for PK and respell tasks: getCategory() PKLookup / Respell, log PK / respell)
                if c.get("task") != t.line:
                    raise SystemExit(f"{m['cold_log']}: record {i} is task {c.get('task')!r}, log task {t.line!r}")
                rec["cold_ok"] = bool(c["cold_ok"])
                rec["checks"] = {"page_cache_empty": c["resident_bytes"] == 0 or bool(c["cold_ok"]),
                                 "resident_bytes": c["resident_bytes"]}
                rec["io"] = _io(c.get("pre"), c.get("post"), c.get("bp_pre"), c.get("bp_post"))
                if c.get("read_sizes"):
                    rec["io"]["nfs_read_sizes" if c.get("trace") == "nfs" else "block_read_sizes"] = c["read_sizes"]
                if c.get("bp_pre") is not None:
                    # bufferpool directory: 0 cached blocks at the task start, and every device read is one window
                    import coldbench

                    rec["checks"]["bp_empty"] = c.get("bp_cached_blocks") == 0
                    dv = coldbench.device_vs_bufferpool(rec["io"])
                    rec["io"]["device_vs_bufferpool"] = dv
                    if dv["ok"] is not None:
                        rec["checks"]["device_reads_are_windows"] = dv["ok"]
                    rec["cold_ok"] = rec["cold_ok"] and rec["checks"]["bp_empty"] and dv["ok"] is not False
                eok = coldbench_efs_ok(rec["io"], efs_target)
                if eok is not None:
                    rec["checks"]["efs_connections_ok"] = eok
                    rec["cold_ok"] = rec["cold_ok"] and eok
                rec["clear"] = {"drop": {k: (c.get("drop") or {}).get(k) for k in ("pageout_ms", "sync_ms", "drop_ms")}}
            elif session["mode"] == "cold-luceneutil":
                drop = m.get("jvm_drop") or {}
                rec["cold_ok"] = bool(drop.get("cold_ok"))
                rec["checks"] = {"luceneutil_cold": True, "resident_bytes_at_jvm_start": drop.get("resident_bytes"),
                                 "gap": "caches dropped once per JVM (luceneutil cold=True) and tasks run concurrently, "
                                 "so a task may find pages another task read; first instance of each task only; the "
                                 "decision metric is cold-strict"}
            if session["mode"] != "cold-strict" and jvm_io is not None:
                rec["io"] = jvm_io
                eok = coldbench_efs_ok(jvm_io, efs_target)
                if eok is not None:
                    rec["efs_connections_ok"] = eok
                    if mode == "cold":
                        rec["checks"]["efs_connections_ok"] = eok
                        rec["cold_ok"] = rec["cold_ok"] and eok
            w.write(rec)
            last[t.key] = t
        for key, t in last.items():
            hits, other = [], []
            for line in t.results:
                mm = HIT.match(line)
                if mm:
                    doc, k, v = mm.groups()
                    hits.append([doc, v, None] if k == "score" else [doc, None, v])
                else:
                    other.append(line)  # facet results, groups, expanded terms: compared in order
            canon = {"total": [t.hits, "eq"], "hits": hits, "aggs": other or None, "aggs_size": len(other)}
            canon["digest"] = hashlib.sha256(json.dumps(canon, sort_keys=True).encode()).hexdigest()[:16]
            w.write({"schema": SCHEMA, "type": "result", **run, "op": key, "canonical": canon})
        w.write({"schema": SCHEMA, "type": "run_end", **run})
    w.close()
    return path


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("convert")
    c.add_argument("--session", required=True)
    c.add_argument("--out", required=True)
    c.add_argument("--reference", help="reference task key (default: first task of the first category)")
    an = sub.add_parser("analyze")
    an.add_argument("--session", required=True, nargs="+")
    an.add_argument("--out", required=True)
    an.add_argument("rest", nargs=argparse.REMAINDER, help="-- then analyze.py options (--base, --aa, --ni-*)")
    a = ap.parse_args()
    if a.cmd == "convert":
        print(convert(a.session, a.out, a.reference))
        return
    dirs = []
    for s in a.session:
        d = os.path.join(s, "coldbench")
        if not os.path.exists(os.path.join(d, "samples.jsonl")):
            convert(s, d)
        dirs.append(d)
    rest = [x for x in a.rest if x != "--"]
    cmd = [sys.executable, os.path.join(os.path.dirname(here), "analyze.py"), *dirs, "--out", a.out,
           "--cold-metric", "took_ms", "--warm-metric", "took_ms", *rest]
    sys.exit(subprocess.run(cmd).returncode)


if __name__ == "__main__":
    main()
