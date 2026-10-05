#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""Part (g) of selftest_generic.py: ingest_osb.py rendering (H5) and arms_generic.py (H6)."""
import json
import os

here = os.path.dirname(os.path.abspath(__file__))


def part_g(osbw, tmp, check, generic):
    import arms_generic
    import ingest_osb

    for c in generic:
        p = json.load(open(os.path.join(here, "corpora", c + ".json")))
        b = p["split_bkd"]
        split = b.get("fields") if b.get("applicable") else []
        out = os.path.join(tmp, f"ingest-{c}")
        rename = {i["name"]: i["name"] + "_split" for i in p["indices"]} if split else {}
        rec = ingest_osb.derive(osbw, c, out, shards=6, replicas=0, renames=rename, split_fields=split)
        wl = json.load(open(os.path.join(out, "workload.json")))
        check(rec["expected_docs"] == {rename.get(i["name"], i["name"]): i["document_count"] for i in p["indices"]},
              f"(g) {c}: expected doc counts = the workload's document-count per index")
        check(all(cc.get("base-url") for cc in wl["corpora"]), f"(g) {c}: every corpus has a base-url")
        for ix in wl["indices"]:
            body = json.load(open(os.path.join(out, ix["body"])))
            st = body["settings"]
            check(st["index.number_of_shards"] == 6 and st["index.number_of_replicas"] == 0 and
                  "number_of_shards" not in st, f"(g) {c}/{ix['name']}: shard and replica override")
            for f in split:
                node = body["mappings"]
                for part in f.split("."):
                    node = node["properties"][part]
                check(node.get("meta") == {"points_format": "Lucene90Split"}, f"(g) {c}/{ix['name']}: split meta on {f}")
            check(bool(split) or "points_format" not in json.dumps(body), f"(g) {c}: no split meta where B is not applicable")
        sched = wl["test_procedures"][0]["schedule"]
        kinds = [s["operation"] if isinstance(s["operation"], str) else s["operation"]["operation-type"] for s in sched]
        check(kinds[:3] == ["delete-index", "create-index", "cluster-health"] and kinds[-1] == "refresh",
              f"(g) {c}: ingest schedule delete, create, health, bulk, refresh")
        bulk = [s for s in sched if isinstance(s["operation"], str)]
        check(len(bulk) == (3 if c == "geoshape" else 1) and all(s["clients"] == 8 for s in bulk),
              f"(g) {c}: every bulk op of the workload, 8 clients")
        check(all("bulk-size" in o for o in wl["operations"]), f"(g) {c}: the workload's own bulk-size kept")
    for c in ("geonames", "geopoint", "geopointshape"):
        rec = ingest_osb.derive(osbw, c, os.path.join(tmp, f"ingest-{c}-u"), procedure="update")
        wl = json.load(open(os.path.join(tmp, f"ingest-{c}-u", "workload.json")))
        check(rec["bulk_ops"] == ["index-update"] and wl["operations"][0].get("conflicts") == "random",
              f"(g) {c}: the update procedure runs the workload's index-update op with conflicts")
    try:
        ingest_osb.derive(osbw, "so", os.path.join(tmp, "ingest-bad"), split_fields=["title"])
        check(False, "(g) split meta refused on a text field")
    except ValueError:
        check(True, "(g) split meta refused on a text field")
    base = json.load(open(os.path.join(here, "arms.example.json")))
    for c in generic:
        p = arms_generic.load_profile(c)
        idx = arms_generic.indices_for(p, "multi", 6, 10)
        cfg, _ = arms_generic.adapt_arms(base, p, idx)
        b_ok = p["split_bkd"]["applicable"]
        check(set(cfg["arms"]) == set(base["arms"]), f"(g) {c}: the same arm names as the main arms file")
        check(cfg["base_switches"] == base["base_switches"] and cfg["outcome"] == base["outcome"],
              f"(g) {c}: base switches and outcome unchanged")
        for n, a in cfg["arms"].items():
            if a.get("not_applicable"):
                check(not b_ok and "B" in base["arms"][n].get("atoms", []), f"(g) {c}: {n} not applicable only for B arms")
                continue
            check(a["index"] in idx and all(k in idx for k in a["open"]), f"(g) {c}: {n} uses existing index keys")
            if b_ok:
                check(a == base["arms"][n], f"(g) {c}: {n} unchanged where B applies")
            else:
                check("B" not in a.get("atoms", []), f"(g) {c}: {n} runs without B")
        if not b_ok:
            check(cfg["arms"]["S2-B-EFS"].get("not_applicable") and cfg["arms"]["S2-CORE-EFS"]["index"] == "stock_efs",
                  f"(g) {c}: S2-B not applicable, S2-CORE on the stock-format index")
    gs = arms_generic.indices_for(arms_generic.load_profile("geoshape"), "multi", 6, 10,
                                  member_segments={"osmmultilinestrings": 4})
    check([m["name"] for m in gs["stock_efs"]["members"]] == ["osmlinestrings", "osmmultilinestrings", "osmpolygons"] and
          gs["stock_efs"]["members"][1]["segments_per_shard"] == 4, "(g) geoshape: members, per-member segment count")
    one = arms_generic.indices_for(arms_generic.load_profile("geoshape"), "single", 6, 10)
    check(one["stock_ebs"]["name"] == "osmpolygons_1s" and one["stock_ebs"]["shards"] == 1 and
          one["stock_ebs"]["min_store_bytes"] == 30 << 30, "(g) geoshape single shard: osmpolygons, 1 shard, >= 30 GB")
