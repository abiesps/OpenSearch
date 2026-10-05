#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Converts an arms file to the configuration set of the USER DECISION 2026-10-05 (common-rules.md): the bufferpool is
the baseline, not a variable, and every node runs with jemalloc.

  baseline on Amazon EBS / Amazon EFS   BASE-EBS, BASE-EFS (and BASE-*-css): stock OpenSearch b44de786cef and stock
                                        Lucene WITH the bufferpool plugin (artifact baseline_bufferpool), agent arms
                                        BASE-EBS / BASE-EFS, the stock-format copy in store type bufferpoolfs, no
                                        switches. Each BASE arm copies the attribution reference S1 of its storage
                                        (same index key, same cluster settings) without the proof-of-concept-only
                                        indices and without switches.
  changes on Amazon EBS / Amazon EFS    the S2-* arms (the proof-of-concept artifact, changes on), unchanged
  attribution reference                 S1-* (the proof-of-concept artifact, all changes off), unchanged
  context only                          every stock memory-mapping arm (bufferpool false, the old S0-*) moves to
                                        "context_arms": it runs only when a session names it, and carries no verdict

Every bufferpool arm gets an explicit "build" (baseline or poc); the file gets
  "builds"     the artifact, tarball sha256, build_target and build_hash of each build (from artifacts.json), checked at
               every run start (GET /_bufferpool/stats build_target, GET / build_hash);
  "allocator"  jemalloc: LD_PRELOAD, MALLOC_CONF, no MALLOC_ARENA_MAX, the libjemalloc sha256 (node_allocator.sh pins
               the same values), checked at every node start from /proc/<pid>/maps and environ;
  "same_bufferpool_settings": true  every bufferpool configuration on one storage reports the same plugin settings;
  "outcome"    reference BASE-EBS for the targets on Amazon EFS (cold of the changes on EFS should match the baseline on
               EBS), plus "same_storage": each storage's changes against the baseline on the same storage (improvement
               and no warm regression), and "attribution_reference" S1 per storage.
All configurations use the bufferpool readahead rule (read_ahead_kb 0 plus the window hint): every arm in [arms] is a
bufferpool arm. A context arm keeps the as-mounted readahead of memory mapping.

  arms_baseline.py convert --in arms.example.json --out arms.example.json
  arms_baseline.py convert --in br-<branch>/arms.json --out br-<branch>/arms.baseline.json
"""
import argparse
import copy
import hashlib
import json
import os
import re
import sys

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
import runguards  # noqa: E402

JEMALLOC = {
    "name": "jemalloc",
    "ld_preload": "/usr/lib64/libjemalloc.so.2",
    "malloc_conf": "background_thread:true",
    "malloc_arena_max": None,
    "jemalloc_sha256": "99d2ff028c115cfba4d8e249bca417e03b36e650b257550e687b52ac40be65bf",
    "package": "jemalloc-5.2.1-7.amzn2023.x86_64",
    "_note": "written to every node unit by node_allocator.sh apply (the same drop-in 90-coldpath-allocator.conf); "
             "validated in baseline-bufferpool-2026-10-05/jemalloc-validation.md",
}
BUILDS = {
    "baseline": {"artifact": "baseline_bufferpool", "build_target": "stock", "build_hash": "b44de786cef"},
    "poc": {"artifact": "poc_iosize_conc2", "build_target": "poc", "build_hash": "d6daba062dc"},
}


def _artifacts(path):
    try:
        return json.load(open(path))
    except (OSError, ValueError):
        return {}


def builds_block(artifacts):
    out = copy.deepcopy(BUILDS)
    for b in out.values():
        rec = artifacts.get(b["artifact"]) or {}
        if rec.get("sha256"):
            b["sha256"] = rec["sha256"]
        if rec.get("s3_uri"):
            b["s3_uri"] = rec["s3_uri"]
    return out


def _storage_suffix(name, prefix):
    m = re.fullmatch(prefix + r"-(EBS|EFS)(-.+)?", name)
    return (m.group(1), m.group(2) or "") if m else (None, None)


def convert(cfg, artifacts):
    cfg = copy.deepcopy(cfg)
    arms = cfg["arms"]
    context = dict(cfg.get("context_arms") or {})
    changes = []
    poc_only = {k for k, v in cfg["indices"].items() if runguards.poc_only(v)}
    # 1. stock memory-mapping arms -> context only
    stock = [n for n, a in arms.items() if not a.get("not_applicable") and runguards.arm_build(a) == "stock"]
    for n in stock:
        a = arms.pop(n)
        a["build"] = "stock"
        a["note"] = ("context only (stock OpenSearch with memory mapping, no verdict; USER DECISION 2026-10-05): "
                     + a.get("note", "")).strip()
        context[n] = a
        changes.append(f"{n}: moved to context_arms")
    # 2. baseline arms, one per stock arm name (S0-X -> BASE-X), else one per S1 arm (S1-X -> BASE-X)
    names = [n for n in stock if n.startswith("S0-")] or [n for n in arms if n.startswith("S1-")]
    for n in names:
        storage, rest = _storage_suffix(n, n.split("-", 1)[0])
        if storage is None:
            continue
        base_name = f"BASE-{storage}{rest}"
        if base_name in arms:
            continue
        tmpl_name = next((t for t in (f"S1-{storage}{rest}", f"S1-{storage}") if t in arms), None)
        if tmpl_name is None:
            raise ValueError(f"{n}: no attribution reference S1-{storage} to copy the baseline arm {base_name} from")
        a = copy.deepcopy(arms[tmpl_name])
        src = context.get(n) or arms[n]
        a.update({"node": f"BASE-{storage}", "storage": storage, "bufferpool": True, "build": "baseline",
                  "switches": [], "atoms": []})
        a["cluster_settings"] = copy.deepcopy(src.get("cluster_settings", a.get("cluster_settings", {})))
        a["open"] = [k for k in a["open"] if k not in poc_only]
        if a["index"] in poc_only:
            raise ValueError(f"{tmpl_name}: its query index {a['index']} is proof-of-concept only")
        a["store_types"] = {k: "bufferpoolfs" for k in a["open"]}
        a.pop("not_applicable", None)
        a["note"] = (f"BASELINE on Amazon {storage}{' (' + rest[1:] + ' variant of ' + n + ')' if rest else ''}: stock "
                     "OpenSearch b44de786cef and "
                     "stock Lucene with the bufferpool plugin (baseline_bufferpool), same plugin settings and readahead "
                     f"rule as every configuration; copied from {tmpl_name} without switches")
        arms[base_name] = a
        changes.append(f"{base_name}: added (baseline build, copied from {tmpl_name}, cluster settings of {n})")
    # 3. explicit builds on every other bufferpool arm
    for n, a in arms.items():
        if a.get("not_applicable") or a.get("build"):
            continue
        a["build"] = "poc"
    # 4. outcome
    old = cfg.get("outcome") or {}
    targets = old.get("targets") or [t for t in ("S2-CORE-EFS", "S2-CORE+PLANNER-EFS") if t in arms]
    efs_t = [t for t in targets if t.endswith("-EFS") and t in arms]
    ebs_t = [t[:-4] + "-EBS" for t in efs_t if t[:-4] + "-EBS" in arms]
    outcome = {"reference": "BASE-EBS", "targets": efs_t, "aa": "BASE-EBS@a,BASE-EBS@b",
               "delta_min": old.get("delta_min", 0.05),
               "same_storage": [
                   {"reference": "BASE-EBS", "targets": ebs_t, "aa": "BASE-EBS@a,BASE-EBS@b"},
                   {"reference": "BASE-EFS", "targets": efs_t, "aa": "BASE-EFS@a,BASE-EFS@b"}],
               "attribution_reference": {"EBS": "S1-EBS", "EFS": "S1-EFS"},
               "rule": "USER DECISION 2026-10-05 (confirmed): for every query, cold latency of the changes on Amazon "
                       "EFS should match the baseline on Amazon EBS (reference/targets); improvements are measured on "
                       "both storages against the baseline on the same storage, with no warm regression against it "
                       "(same_storage: analyze.py --ni-ref <reference> --ni-target <targets> --ni-aa <aa>). Each "
                       "change alone is attributed against S1 of its storage. Context arms carry no verdict."}
    if old:
        outcome["_previous"] = old
    cfg["outcome"] = outcome
    changes.append(f"outcome: reference BASE-EBS, targets {efs_t}, same-storage EBS targets {ebs_t}")
    if context:
        cfg["context_arms"] = context
    cfg["builds"] = builds_block(artifacts)
    cfg["allocator"] = copy.deepcopy(JEMALLOC)
    cfg["same_bufferpool_settings"] = True
    return cfg, changes


DOC = ("Arms of one data node host, configuration set of the USER DECISION 2026-10-05 (arms_baseline.py): baseline "
       "BASE-EBS / BASE-EFS (stock OpenSearch and Lucene with the bufferpool plugin, agent arms BASE-*), changes S2-* "
       "and the all-off attribution reference S1-* (proof-of-concept build, agent arms POC-*), all with the bufferpool "
       "store, the same plugin settings, readahead 0 plus the window hint, and jemalloc on every node unit "
       "(\"allocator\", node_allocator.sh). Stock memory-mapping arms are in \"context_arms\": a session runs one only "
       "when its --arm-list names it. build: baseline arms get no base_switches. Earlier text: ")


def cmd_convert(a):
    raw = open(a.inp, "rb").read()
    cfg = json.loads(raw)
    if cfg.get("_baseline"):
        sys.exit(f"{a.inp} is already converted ({cfg['_baseline'].get('source')})")
    out, changes = convert(cfg, _artifacts(a.artifacts))
    out["_doc"] = DOC + str(cfg.get("_doc", ""))
    out["_baseline"] = {"source": os.path.basename(a.inp), "source_sha256": hashlib.sha256(raw).hexdigest(),
                        "changes": changes}
    with open(a.out, "w") as f:
        json.dump(out, f, indent=1)
        f.write("\n")
    print(f"{a.out}: {len(out['arms'])} arms, context arms {sorted(out.get('context_arms', {}))}")
    for c in changes:
        print("  " + c)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("convert")
    c.add_argument("--in", dest="inp", required=True)
    c.add_argument("--out", required=True)
    c.add_argument("--artifacts", default=os.path.join(here, "..", "artifacts.json"))
    a = ap.parse_args()
    {"convert": cmd_convert}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
