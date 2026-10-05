#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Arms and indices files for the generic workloads, derived from the main workflow's arms file (arms.example.json, or
the branch's edited copy) so every workload runs the SAME arms, switch calls, verify maps, cluster settings and
outcome block. Only the [indices] section and what follows from the corpus are changed, each change recorded under
"_generic":
  - [indices]: stock_ebs / stock_efs (and split_ebs / split_efs where split BKD applies) with the workload's index
    names (geoshape: one key with `members`, the three osm* indices), the expected docs per index from the workload's
    document-count, the shard count and segments per shard of the layout;
    layout "multi" (the multi-shard index), "single" (<index>_1s: 1 shard, >= 30 GB, min_store_bytes set) or
    "single-1seg" (<index>_1s_1seg, 1 segment);
  - where split BKD does NOT apply (corpora/<w>.json split_bkd.applicable false, with the reason):
      an arm whose only change is B (S2-B-*)                       -> {"not_applicable": reason}
      an arm that without B equals another arm (S2-B+A = S2-A)       -> {"not_applicable": "... same as S2-A-EFS"}
      a combined arm (CORE, CORE+PLANNER, ALL, their -css variants)  -> runs on the stock-format index without B
    and split_* keys are removed from every arm's open / store_types.
coldbench reports a not-applicable arm as "Not available (gaps, not zero effects)".
Also (harness-ext review findings 4 and 7):
  - every S2-x-EFS arm of the base that has no S2-x-EBS gets one (common-rules "Both storages": every POC arm is
    first-class on EBS and EFS): the same atoms, switches, cluster settings and bufferpool store types, on node
    POC-EBS and the *_ebs index keys (the rule that turns the base's S2-CORE-EFS into its S2-CORE-EBS). Arms the base
    already has on EBS are kept as they are. -css variants are not copied.
  - "other_indices": "close" (unless the base sets it): on a grouped host the other workloads' open indices are
    closed before the arm's indices open (GENERIC-PLAN section 2).
  - the EBS copy of every POC -css arm (S1-EBS-css, S2-x-EBS-css): the concurrent segment search factor on both storages.
  - where split BKD applies: split_* indices on their own agent arms and data paths (--split-node-ebs/-efs, default
    POC-B-EBS / POC-B-EFS); arms on the stock data paths open no split index (common-rules "Data-path separation").

  arms_generic.py build --base arms.example.json --corpus geoshape --layout multi --shards 6 --segments 10 \
      --out arms.geoshape.json
  arms_generic.py build --base arms.example.json --corpus clickbench --layout single --segments 10 --out arms.clickbench.1s.json
  arms_generic.py indices --corpus clickbench --layout single-1seg --segments 1 --out indices.clickbench.1s1seg.json
"""
import argparse
import copy
import hashlib
import json
import os
import re
import sys

here = os.path.dirname(os.path.abspath(__file__))
GB = 1 << 30
SUFFIX = {"multi": "", "single": "_1s", "single-1seg": "_1s_1seg"}


def load_profile(corpus):
    return json.load(open(os.path.join(here, "corpora", corpus + ".json")))


def indices_for(profile, layout, shards, segments, min_store_gb=30, member_segments=None):
    if layout not in SUFFIX:
        raise ValueError(f"layout {layout}: one of {sorted(SUFFIX)}")
    idx = profile["indices"]
    if layout != "multi":
        if len(idx) > 1:
            # a single-shard index is one index: the candidate is named by the profile (geoshape: osmpolygons)
            cand = profile.get("single_shard", {}).get("index") or max(idx, key=lambda i: i["document_count"])["name"]
            idx = [i for i in idx if i["name"] == cand]
        shards = 1
    b = profile.get("split_bkd", {})
    formats = [("stock", "")] + ([("split", "_split")] if b.get("applicable") else [])
    out = {}
    for storage in ("EBS", "EFS"):
        for fmt, fsuf in formats:
            members = []
            for i in idx:
                # a small member that cannot reach the segment count gets its own (the same in every arm, recorded)
                seg = (member_segments or {}).get(i["name"], segments)
                m = {"name": f"{i['name']}{fsuf}{SUFFIX[layout]}", "docs": i["document_count"], "shards": shards,
                     "segments_per_shard": seg}
                if layout != "multi":
                    m["min_store_bytes"] = int(min_store_gb * GB)
                members.append(m)
            key = f"{fmt}_{storage.lower()}"
            e = {"storage": storage, "format": "stock" if fmt == "stock" else "split BKD (points_format meta)"}
            if len(members) == 1:
                e.update(members[0])
            else:
                e["members"] = members
                e["name"] = ",".join(m["name"] for m in members)
            out[key] = e
    return out


def _ebs_key(k):
    if not k.endswith("_efs"):
        raise ValueError(f"index key {k}: an EFS arm's keys end with _efs")
    return k[:-4] + "_ebs"


def add_ebs_arms(cfg):
    """Adds S2-x-EBS for every S2-x-EFS (not -css) the arms file lacks; returns the change list."""
    arms, changes = cfg["arms"], []
    for n, a in list(arms.items()):
        m = re.fullmatch(r"(S2-.+)-EFS", n)
        if not m or a.get("not_applicable") or m.group(1) + "-EBS" in arms:
            continue
        if a.get("node") != "POC-EFS" or a.get("storage") != "EFS" or not a.get("bufferpool"):
            raise ValueError(f"{n}: expected a POC-EFS bufferpool arm on EFS, got node {a.get('node')} storage "
                             f"{a.get('storage')}")
        e = copy.deepcopy(a)
        e["node"], e["storage"] = "POC-EBS", "EBS"
        e["index"] = _ebs_key(a["index"])
        e["open"] = [_ebs_key(k) for k in a["open"]]
        e["store_types"] = {_ebs_key(k): v for k, v in a.get("store_types", {}).items()}
        e["note"] = (a.get("note", "") + f" [generic: EBS copy of {n}, common-rules: POC arms on both storages]").strip()
        arms[m.group(1) + "-EBS"] = e
        changes.append(f"{m.group(1)}-EBS: added (EBS copy of {n})")
    return changes


def add_ebs_css_arms(cfg):
    """
    Adds the EBS copy of every -css arm on EFS (S1-EFS-css -> S1-EBS-css, S2-x-EFS-css -> S2-x-EBS-css) that the arms
    file lacks: common-rules "Both storages" makes the EBS baseline and POC arms first-class, and the concurrent segment
    search factor is measured on both storages. Same rule as add_ebs_arms (node POC-EBS, *_ebs keys).
    """
    arms, changes = cfg["arms"], []
    for n, a in list(arms.items()):
        m = re.fullmatch(r"(S[12].*)-EFS-css", n)
        if not m or a.get("not_applicable") or m.group(1) + "-EBS-css" in arms:
            continue
        if a.get("node") != "POC-EFS" or a.get("storage") != "EFS" or not a.get("bufferpool"):
            continue  # stock -css arms already exist on both storages (S0-EBS-css, S0-EFS-css)
        e = copy.deepcopy(a)
        e["node"], e["storage"] = "POC-EBS", "EBS"
        e["index"] = _ebs_key(a["index"])
        e["open"] = [_ebs_key(k) for k in a["open"]]
        e["store_types"] = {_ebs_key(k): v for k, v in a.get("store_types", {}).items()}
        e["note"] = (a.get("note", "") + f" [generic: EBS copy of {n}, common-rules: POC arms on both storages]").strip()
        arms[m.group(1) + "-EBS-css"] = e
        changes.append(f"{m.group(1)}-EBS-css: added (EBS copy of {n})")
    return changes


def isolate_split(cfg, nodes):
    """
    common-rules "Data-path separation for POC-only indices": the stock binary cannot start next to a closed split-BKD
    index, so split_* indices live on their own data paths with their own agent arms (nodes: {"EBS": "POC-B-EBS",
    "EFS": "POC-B-EFS"}). An arm that queries a split index runs on that node and opens only split indices of its
    storage; every other arm opens no split index. runguards.check_format_isolation refuses any other layout.
    """
    changes = []
    for n, a in cfg["arms"].items():
        if a.get("not_applicable"):
            continue
        if a["index"].startswith("split_"):
            node = nodes[a["storage"]]
            keep = [k for k in a["open"] if k.startswith("split_")]
            if a.get("node") != node or keep != a["open"]:
                changes.append(f"{n}: node {a.get('node')} -> {node}, open {a['open']} -> {keep} (split data path)")
            a["node"], a["open"] = node, keep
        else:
            keep = [k for k in a["open"] if not k.startswith("split_")]
            if keep != a["open"]:
                changes.append(f"{n}: open {a['open']} -> {keep} (no split index on a stock data path)")
            a["open"] = keep
        a["store_types"] = {k: v for k, v in a.get("store_types", {}).items() if k in a["open"]}
    return changes


def adapt_arms(base, profile, indices):
    b = profile.get("split_bkd", {})
    cfg = copy.deepcopy(base)
    changes = []
    cfg["indices"] = indices
    if b.get("applicable"):
        return cfg, changes
    reason = f"split BKD (B) is not applicable to {profile['corpus']}: {b.get('reason')}"
    arms = cfg["arms"]
    stock_atoms = {}
    for n, a in arms.items():
        if not a.get("index", "").startswith("split_"):
            stock_atoms.setdefault((a.get("node"), frozenset(a.get("atoms", [])), a.get("storage"),
                                    json.dumps(a.get("cluster_settings", {}), sort_keys=True)), n)
    for n, a in list(arms.items()):
        if a.get("not_applicable"):
            continue
        uses_split = a.get("index", "").startswith("split_")
        if uses_split:
            atoms = [x for x in a.get("atoms", []) if x != "B"]
            same = stock_atoms.get((a.get("node"), frozenset(atoms), a.get("storage"),
                                    json.dumps(a.get("cluster_settings", {}), sort_keys=True)))
            if not atoms:
                arms[n] = {"not_applicable": reason, "was": {"atoms": a.get("atoms"), "index": a["index"]}}
                changes.append(f"{n}: not applicable (B only)")
                continue
            if same and "CORE" not in n and "ALL" not in n:
                arms[n] = {"not_applicable": f"{reason}; without B it is the same arm as {same}",
                           "was": {"atoms": a.get("atoms"), "index": a["index"]}}
                changes.append(f"{n}: not applicable (without B = {same})")
                continue
            a["index"] = "stock_" + a["index"].split("_", 1)[1]
            a["atoms"] = atoms
            a["note"] = (a.get("note", "") + f" [generic: {reason}; this arm runs without B on the stock-format "
                         "index]").strip()
            changes.append(f"{n}: without B on {a['index']}")
        for key in [k for k in a.get("open", []) if k.startswith("split_")]:
            a["open"].remove(key)
        for key in [k for k in a.get("store_types", {}) if k.startswith("split_")]:
            del a["store_types"][key]
    return cfg, changes


def _member_segments(a):
    return {k: int(v) for k, v in (x.split("=", 1) for x in (a.member_segments or []))}


def cmd_build(a):
    base_raw = open(a.base, "rb").read()
    base = json.loads(base_raw)
    profile = load_profile(a.corpus)
    idx = indices_for(profile, a.layout, a.shards, a.segments, a.min_store_gb, _member_segments(a))
    with_ebs = copy.deepcopy(base)
    changes = add_ebs_arms(with_ebs)
    with_ebs.setdefault("other_indices", "close")
    changes.append(f"other_indices: {with_ebs['other_indices']}")
    changes += add_ebs_css_arms(with_ebs)
    cfg, more = adapt_arms(with_ebs, profile, idx)
    changes += more
    if profile.get("split_bkd", {}).get("applicable"):
        changes += isolate_split(cfg, {"EBS": a.split_node_ebs, "EFS": a.split_node_efs})
    cfg["_generic"] = {"corpus": a.corpus, "layout": a.layout, "base_arms_file": a.base,
                       "base_arms_sha256": hashlib.sha256(base_raw).hexdigest(), "split_bkd": profile.get("split_bkd"),
                       "changes": changes}
    with open(a.out, "w") as f:
        json.dump(cfg, f, indent=1)
    print(f"{a.out}: {len(cfg['arms'])} arms, {sum(1 for x in cfg['arms'].values() if x.get('not_applicable'))} not "
          f"applicable; indices {sorted(idx)}")


def cmd_indices(a):
    profile = load_profile(a.corpus)
    out = {"_doc": f"{a.corpus} {a.layout} copies (arms_generic.py); use with coldbench --indices",
           "indices": indices_for(profile, a.layout, a.shards, a.segments, a.min_store_gb, _member_segments(a))}
    with open(a.out, "w") as f:
        json.dump(out, f, indent=1)
    print(a.out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("build", "indices"):
        p = sub.add_parser(name)
        p.add_argument("--corpus", required=True)
        p.add_argument("--layout", choices=sorted(SUFFIX), default="multi")
        p.add_argument("--shards", type=int, default=6, help="primaries of the multi-shard layout (1 for single*)")
        p.add_argument("--segments", type=int, required=True, help="segments per shard after force-merge (verified)")
        p.add_argument("--min-store-gb", type=float, default=30)
        p.add_argument("--member-segments", action="append", help="INDEX=N for a workload index that cannot reach "
                       "--segments in every shard (indexprep.py forcemerge reports it); recorded in the file")
        p.add_argument("--out", required=True)
    sub.choices["build"].add_argument("--base", required=True, help="the main workflow's arms file")
    sub.choices["build"].add_argument("--split-node-ebs", default="POC-B-EBS", help="agent arm (own data path) of the "
                                      "split-BKD index on EBS")
    sub.choices["build"].add_argument("--split-node-efs", default="POC-B-EFS", help="agent arm (own data path) of the "
                                      "split-BKD index on EFS")
    a = ap.parse_args()
    {"build": cmd_build, "indices": cmd_indices}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
