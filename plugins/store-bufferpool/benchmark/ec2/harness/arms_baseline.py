#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Converts an arms file to a configuration set with explicit builds, the jemalloc allocator for every node unit, and
plugin identity checks (common-rules.md).

Default set "memory-mapping" (USER CORRECTION 2026-10-06, supersedes the bufferpool-baseline decision):
  stock OpenSearch with memory mapping on Amazon EBS (outcome baseline) and Amazon EFS   S0-* stay first-class arms,
                                        "build": "stock", mounted default readahead, store type hybridfs
  the proof-of-concept build with the bufferpool, all changes off   S1-* (attribution reference), "build": "poc"
  the proof-of-concept build with the bufferpool, changes on        S2-*, "build": "poc", readahead 0 plus the hint
  optional diagnostic                   BASE-EBS / BASE-EFS in "context_arms": stock OpenSearch and stock Lucene WITH
                                        the bufferpool plugin (artifact baseline_bufferpool), copied from S1 of the
                                        storage without switches; it runs only when a session names it (separates the
                                        build from the store) and carries no verdict
  "outcome"    reference S0-EBS for the targets on Amazon EFS; "same_storage" lists each storage's warm no-regression
               comparison against stock memory mapping on the same storage; "attribution_reference" S1 per storage.
Set "bufferpool-baseline" (USER DECISION 2026-10-05, SUPERSEDED; kept for sessions analysed under it): BASE-* are the
baseline arms and the memory-mapping arms move to "context_arms".

Both sets add
  "builds"     the artifact, tarball sha256, build_target, build_hash, plugin source commit and installed plugin jar
               sha256 of each build (from artifacts.json), checked at every run start (GET /_bufferpool/stats
               build_target, GET / build_hash, agent GET /node/build plugin jar);
  "allocator"  jemalloc: LD_PRELOAD, MALLOC_CONF, no MALLOC_ARENA_MAX, the libjemalloc sha256 (node_allocator.sh pins
               the same values), checked at every node start from /proc/<pid>/maps and environ, stock nodes included;
  "same_bufferpool_settings": true  every bufferpool configuration on one storage reports the same plugin settings.

  arms_baseline.py convert --in arms.example.json --out arms.example.json
  arms_baseline.py convert --in br-<branch>/arms.json --out br-<branch>/arms.jemalloc.json
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
    "baseline": {"artifact": "baseline_bufferpool", "build_target": "stock", "build_hash": "b44de786cef",
                 "plugin_source_commit": "c83c646b873"},
    "poc": {"artifact": "poc_iosize_conc2", "build_target": "poc", "build_hash": "d6daba062dc",
            "plugin_source_commit": "c83c646b873"},
    "stock": {"artifact": "s0", "build_hash": "b44de786cef", "store": "memory mapping (hybridfs), no bufferpool plugin"},
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
        if rec.get("installed_plugin_jar_sha256"):
            b["plugin_jar_sha256"] = rec["installed_plugin_jar_sha256"]
        if rec.get("s3_uri"):
            b["s3_uri"] = rec["s3_uri"]
    return out


def _storage_suffix(name, prefix):
    m = re.fullmatch(prefix + r"-(EBS|EFS)(-.+)?", name)
    return (m.group(1), m.group(2) or "") if m else (None, None)


SETS = ("memory-mapping", "bufferpool-baseline")


def _base_arm(arms, context, n, poc_only):
    """BASE-<storage><suffix>: the baseline build (stock OpenSearch with the plugin), copied from S1 of the storage
    (same index key) without switches and proof-of-concept-only indices, with the cluster settings of stock arm n."""
    storage, rest = _storage_suffix(n, n.split("-", 1)[0])
    if storage is None:
        return None, None
    base_name = f"BASE-{storage}{rest}"
    tmpl_name = next((t for t in (f"S1-{storage}{rest}", f"S1-{storage}") if t in arms), None)
    if tmpl_name is None:
        raise ValueError(f"{n}: no attribution reference S1-{storage} to copy the arm {base_name} from")
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
    a["note"] = (f"stock OpenSearch b44de786cef and stock Lucene with the bufferpool plugin (baseline_bufferpool) on "
                 f"Amazon {storage}{' (' + rest[1:] + ' variant of ' + n + ')' if rest else ''}, same plugin settings "
                 f"and readahead rule as the proof-of-concept configurations; copied from {tmpl_name} without switches")
    return base_name, a


def convert(cfg, artifacts, config_set="memory-mapping"):
    """
    config_set "memory-mapping" (USER CORRECTION 2026-10-06, the default): the original set stays first-class
    (stock OpenSearch with memory mapping on both storages, S1-* all changes off, S2-* changes on); every arm gets an
    explicit build; stock OpenSearch with the bufferpool is added as an optional diagnostic in "context_arms"
    (BASE-*: runs only when named, no verdict); the outcome keeps stock memory mapping on Amazon EBS as its reference.
    config_set "bufferpool-baseline" (USER DECISION 2026-10-05, SUPERSEDED): BASE-* is the baseline, memory mapping
    moves to "context_arms". Both sets get "builds", the jemalloc "allocator" block and same_bufferpool_settings.
    """
    if config_set not in SETS:
        raise ValueError(f"configuration set {config_set!r}: one of {list(SETS)}")
    cfg = copy.deepcopy(cfg)
    arms = cfg["arms"]
    context = dict(cfg.get("context_arms") or {})
    changes = []
    poc_only = {k for k, v in cfg["indices"].items() if runguards.poc_only(v)}
    stock = [n for n, a in arms.items() if not a.get("not_applicable") and runguards.arm_build(a) == "stock"]
    for n in stock:
        arms[n]["build"] = "stock"
    if config_set == "bufferpool-baseline":
        for n in stock:
            a = arms.pop(n)
            a["note"] = ("context only (stock OpenSearch with memory mapping, no verdict; USER DECISION 2026-10-05): "
                         + a.get("note", "")).strip()
            context[n] = a
            changes.append(f"{n}: moved to context_arms")
    names = [n for n in stock if n.startswith("S0-")] or [n for n in arms if n.startswith("S1-")]
    for n in names:
        base_name, a = _base_arm(arms, context, n, poc_only)
        if base_name is None or base_name in arms or base_name in context:
            continue
        if config_set == "bufferpool-baseline":
            a["note"] = "BASELINE: " + a["note"]
            arms[base_name] = a
            changes.append(f"{base_name}: added (baseline build, copied from S1, cluster settings of {n})")
        else:
            a["note"] = "optional DIAGNOSTIC (separates the build from the store; no verdict): " + a["note"]
            context[base_name] = a
            changes.append(f"{base_name}: added to context_arms as an optional diagnostic (cluster settings of {n})")
    for n, a in arms.items():
        if a.get("not_applicable") or a.get("build"):
            continue
        a["build"] = "poc"
    old = cfg.get("outcome") or {}
    targets = old.get("targets") or [t for t in ("S2-CORE-EFS", "S2-CORE+PLANNER-EFS") if t in arms]
    efs_t = [t for t in targets if t.endswith("-EFS") and t in arms]
    ebs_t = [t[:-4] + "-EBS" for t in efs_t if t[:-4] + "-EBS" in arms]
    ref = "BASE" if config_set == "bufferpool-baseline" else "S0"
    outcome = {"reference": f"{ref}-EBS", "targets": efs_t, "aa": old.get("aa") if ref == "S0" and old.get("aa")
               else f"{ref}-EBS@a,{ref}-EBS@b", "delta_min": old.get("delta_min", 0.05),
               "same_storage": [
                   {"reference": f"{ref}-EBS", "targets": ebs_t, "aa": f"{ref}-EBS@a,{ref}-EBS@b"},
                   {"reference": f"{ref}-EFS", "targets": efs_t, "aa": f"{ref}-EFS@a,{ref}-EFS@b"}],
               "attribution_reference": {"EBS": "S1-EBS", "EFS": "S1-EFS"},
               # analyze.py storage reads check: stock OpenSearch with the bufferpool against the all-off build on the
               # same storage (the two builds read through the same plugin), when both are in the session
               "storage_reads_pairs": [["BASE-EBS", "S1-EBS"], ["BASE-EFS", "S1-EFS"]]}
    if ref == "S0":
        outcome["rule"] = ("USER CORRECTION 2026-10-06: for every query, cold median and cold 90th percentile of the "
                           "changes on Amazon EFS non-inferior to stock OpenSearch with memory mapping on Amazon EBS "
                           "(reference/targets); cold improves on each storage compared with the all-off reference S1 "
                           "of that storage (attribution_reference); no warm regression compared with stock OpenSearch "
                           "with memory mapping on the same storage (same_storage: analyze.py --ni-ref <reference> "
                           "--ni-target <targets> --ni-aa <aa>). Context arms (BASE-*) carry no verdict.")
    else:
        outcome["rule"] = ("USER DECISION 2026-10-05, SUPERSEDED by the USER CORRECTION 2026-10-06: the changes on Amazon "
                           "EFS against the baseline BASE-EBS; per storage against BASE of the same storage.")
    if old:
        outcome["_previous"] = old
    cfg["outcome"] = outcome
    changes.append(f"outcome: reference {ref}-EBS, targets {efs_t}, same-storage EBS targets {ebs_t}")
    if context:
        cfg["context_arms"] = context
    cfg["builds"] = builds_block(artifacts)
    cfg["allocator"] = copy.deepcopy(JEMALLOC)
    cfg["same_bufferpool_settings"] = True
    return cfg, changes


DOC = {
    "memory-mapping": (
        "Arms of one data node host, configuration set of the USER CORRECTION 2026-10-06 (arms_baseline.py): stock "
        "OpenSearch with memory mapping on Amazon EBS (the outcome baseline) and on Amazon EFS (S0-*, build stock, "
        "mounted default readahead), the proof-of-concept build with the bufferpool, all changes off (S1-*) and changes "
        "on (S2-*), build poc, readahead 0 plus the window hint; jemalloc on every node unit (\"allocator\", "
        "node_allocator.sh). Stock OpenSearch with the bufferpool (BASE-*, build baseline, no switches) is an optional "
        "diagnostic in \"context_arms\": a session runs one only when its --arm-list names it. Earlier text: "),
    "bufferpool-baseline": (
        "Arms of one data node host, configuration set of the USER DECISION 2026-10-05 (SUPERSEDED 2026-10-06; "
        "arms_baseline.py --set bufferpool-baseline): baseline BASE-EBS / BASE-EFS (stock OpenSearch with the bufferpool "
        "plugin), changes S2-* and the all-off reference S1-*, jemalloc on every node unit; stock memory-mapping arms in "
        "\"context_arms\". Earlier text: "),
}


def cmd_convert(a):
    raw = open(a.inp, "rb").read()
    cfg = json.loads(raw)
    if cfg.get("_baseline"):
        sys.exit(f"{a.inp} is already converted ({cfg['_baseline'].get('source')})")
    out, changes = convert(cfg, _artifacts(a.artifacts), a.set)
    out["_doc"] = DOC[a.set] + str(cfg.get("_doc", ""))
    out["_baseline"] = {"source": os.path.basename(a.inp), "source_sha256": hashlib.sha256(raw).hexdigest(),
                        "set": a.set, "changes": changes}
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
    c.add_argument("--set", choices=SETS, default="memory-mapping",
                   help="memory-mapping (default, USER CORRECTION 2026-10-06): stock memory mapping first-class and the "
                        "outcome reference, BASE-* an optional diagnostic; bufferpool-baseline: the superseded set")
    a = ap.parse_args()
    {"convert": cmd_convert}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
