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
    for c in ("nested", "percolator"):  # nested templates index.store.type, percolator has none
        out = os.path.join(tmp, f"ingest-{c}-bp")
        rec = ingest_osb.derive(osbw, c, out, shards=6, store_type="bufferpoolfs", params={"store_type": "hybridfs"})
        body = json.load(open(os.path.join(out, json.load(open(os.path.join(out, "workload.json")))["indices"][0]["body"])))
        check(body["settings"].get("index.store.type") == "bufferpoolfs" and
              any("index.store.type=bufferpoolfs" in o for o in rec["overrides"]),
              f"(g) {c}: --store-type bufferpoolfs replaces the workload's store type and is recorded")
        out2 = os.path.join(tmp, f"ingest-{c}-param")
        ingest_osb.derive(osbw, c, out2, shards=6, params={"store_type": "hybridfs"})
        body2 = json.load(open(os.path.join(out2, json.load(open(os.path.join(out2, "workload.json")))["indices"][0]["body"])))
        check(body2["settings"].get("index.store.type") == ("hybridfs" if c == "nested" else None),
              f"(g) {c}: without --store-type the workload's own store type (template parameter) is kept")
    # http_logs (main workflow): the profile names index-append, the only bulk op of the workload's index-only procedure
    p = json.load(open(os.path.join(here, "corpora", "http_logs.json")))
    out = os.path.join(tmp, "ingest-http_logs-split")
    rename = {i["name"]: i["name"].replace("logs", "logs_split", 1) for i in p["indices"]}
    rec = ingest_osb.derive(osbw, "http_logs", out, shards=6, renames=rename, split_fields=["@timestamp"],
                            store_type="bufferpoolfs")
    wl = json.load(open(os.path.join(out, "workload.json")))
    check(rec["bulk_ops"] == ["index-append"] and [c["name"] for c in wl["corpora"]] == ["http_logs"],
          "(g) http_logs: only index-append (no pipeline, unparsed or update variants)")
    check(rec["expected_docs"] == {rename[i["name"]]: i["document_count"] for i in p["indices"]},
          "(g) http_logs: expected doc counts per renamed index")
    body = json.load(open(os.path.join(out, "index-logs_split-241998.json")))
    check(body["mappings"]["properties"]["@timestamp"].get("meta") == {"points_format": "Lucene90Split"} and
          body["settings"]["index.store.type"] == "bufferpoolfs", "(g) http_logs: split meta and bufferpoolfs")
    # nyc_taxis (main workflow): only the index op (not update), the default procedure's create-index settings, and
    # the one document every build rejects (tip_amount outside half_float) declared, so OSB continues on that error
    out = os.path.join(tmp, "ingest-nyc_taxis-split")
    rec = ingest_osb.derive(osbw, "nyc_taxis", out, shards=6, renames={"nyc_taxis": "nyc_taxis_split"},
                            split_fields=["pickup_datetime", "dropoff_datetime"], store_type="bufferpoolfs")
    body = json.load(open(os.path.join(out, "index-nyc_taxis_split.json")))
    st = body["settings"]
    check(rec["bulk_ops"] == ["index"] and st["index.codec"] == "best_compression" and
          st["index.refresh_interval"] == "30s" and st["index.translog.flush_threshold_size"] == "4g" and
          st["index.store.type"] == "bufferpoolfs" and
          all(body["mappings"]["properties"][f].get("meta") == {"points_format": "Lucene90Split"}
              for f in ("pickup_datetime", "dropoff_datetime")),
          "(g) nyc_taxis: index op only, the default procedure's create-index settings, split meta, bufferpoolfs")
    check(rec["expected_docs"] == {"nyc_taxis_split": 165346692} and rec["known_rejected_docs"]["count"] == 1,
          "(g) nyc_taxis: expected docs = document-count, one known rejected document recorded")
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
    # EBS copies of every S2 POC arm (common-rules "Both storages") and other_indices=close (review findings 7, 4)
    import copy as _copy
    with_ebs = _copy.deepcopy(base)
    added = arms_generic.add_ebs_arms(with_ebs)
    efs = sorted(n for n, a in base["arms"].items() if n.startswith("S2-") and n.endswith("-EFS"))
    check(all(n[:-4] + "-EBS" in with_ebs["arms"] for n in efs), "(g) every S2-x-EFS arm has an S2-x-EBS arm")
    check(len(added) == sum(1 for n in efs if n[:-4] + "-EBS" not in base["arms"]) and
          with_ebs["arms"]["S2-CORE-EBS"] == base["arms"]["S2-CORE-EBS"], "(g) arms the base has on EBS are kept as they are")
    mirror = arms_generic.add_ebs_arms({"arms": {"S2-CORE-EFS": _copy.deepcopy(base["arms"]["S2-CORE-EFS"])}})
    check(mirror == ["S2-CORE-EBS: added (EBS copy of S2-CORE-EFS)"], "(g) the copy rule names its source")
    m = {"arms": {"S2-CORE-EFS": _copy.deepcopy(base["arms"]["S2-CORE-EFS"])}}
    arms_generic.add_ebs_arms(m)
    strip = lambda a: {k: v for k, v in a.items() if k != "note"}  # noqa: E731
    check(strip(m["arms"]["S2-CORE-EBS"]) == strip(base["arms"]["S2-CORE-EBS"]),
          "(g) the copy rule turns the base's S2-CORE-EFS into exactly its hand-written S2-CORE-EBS")
    for n in efs:
        a, e = with_ebs["arms"][n], with_ebs["arms"][n[:-4] + "-EBS"]
        check(e["node"] == "POC-EBS" and e["storage"] == "EBS" and e.get("switches") == a.get("switches") and
              e.get("atoms") == a.get("atoms") and e.get("cluster_settings") == a.get("cluster_settings") and
              all(k.endswith("_ebs") for k in e["open"]), f"(g) {n[:-4]}-EBS: same switches, atoms, settings on EBS")
    check(not any(n.endswith("-css") for n in set(with_ebs["arms"]) - set(base["arms"])), "(g) -css variants are not copied")
    css = _copy.deepcopy(with_ebs)
    added_css = arms_generic.add_ebs_css_arms(css)
    check("S1-EBS-css" in css["arms"] and css["arms"]["S1-EBS-css"]["node"] == "POC-EBS" and
          css["arms"]["S1-EBS-css"]["cluster_settings"] == base["arms"]["S1-EFS-css"]["cluster_settings"] and
          all(k.endswith("_ebs") for k in css["arms"]["S1-EBS-css"]["open"]) and
          all(n.endswith("-EBS-css") for n in set(css["arms"]) - set(with_ebs["arms"])) and
          len(added_css) == len(set(css["arms"]) - set(with_ebs["arms"])), "(g) EBS copies of the POC -css arms")
    p_n = arms_generic.load_profile("nested")
    cfg_n, _ = arms_generic.adapt_arms(css, p_n, arms_generic.indices_for(p_n, "multi", 6, 6))
    arms_generic.isolate_split(cfg_n, {"EBS": "POC-B-EBS", "EFS": "POC-B-EFS"})
    paths = {"S0-EBS": "/e", "S0-EFS": "/f", "POC-EBS": "/e", "POC-EFS": "/f", "POC-B-EBS": "/eb", "POC-B-EFS": "/fb"}
    import runguards as _rg
    iso = _rg.check_format_isolation(cfg_n, {k: {"data_path": v} for k, v in paths.items()})
    check(iso["ok"] and {n for x in iso["poc_only_indices"] for n in x["nodes"]} == {"POC-B-EBS", "POC-B-EFS"} and
          cfg_n["arms"]["S2-B-EFS"]["node"] == "POC-B-EFS" and cfg_n["arms"]["S2-B-EFS"]["open"] == ["split_efs"] and
          cfg_n["arms"]["S1-EFS"]["open"] == ["stock_efs"] and cfg_n["arms"]["S2-CORE-EBS"]["node"] == "POC-B-EBS",
          "(g) nested: split indices on their own nodes, the format-isolation guard accepts the arms")
    try:
        _rg.check_format_isolation(css | {"indices": cfg_n["indices"]}, {k: {"data_path": v} for k, v in paths.items()})
        check(False, "(g) the guard refuses split indices on the stock data paths")
    except RuntimeError:
        check(True, "(g) the guard refuses split indices on the stock data paths")
    import families_ext as _fe

    class _NullPct:
        def request(self, *a, **kw):
            return {"aggregations": {"p": {"values": {"5.0": None, "40.0": None, "50.0": None, "60.0": None, "95.0": None}}}}
    try:
        _fe._numeric_vals(_NullPct(), "x", ["answer_count"])
        check(False, "(g) a numeric field without values is refused at discovery")
    except RuntimeError:
        check(True, "(g) a numeric field without values is refused at discovery")
    import index_equality as _ie
    pg = [{"hits": {"hits": [{"_id": "x1", "_score": 1.0, "fields": {"qid": ["q7"]},
                              "inner_hits": {"answers": {"hits": {"hits": [{"_id": "x1", "_nested": {"field": "answers", "offset": 2}}]}}}},
                             {"_id": "x2", "_score": 1.0, "_source": {"a": 1}}]}}]
    miss = _ie.rekey(pg, "qid")
    h = pg[0]["hits"]["hits"]
    check(miss == 0 and h[0]["_id"] == "q7" and h[1]["_id"].startswith("src:") and
          h[0]["inner_hits"]["answers"]["hits"]["hits"][0]["_id"] == 'q7/{"field": "answers", "offset": 2}' and
          _ie.with_key_field({"size": 1}, "qid") == {"size": 1, "docvalue_fields": ["qid"]},
          "(g) index_equality: hits keyed by the document key (inner hits by parent key and nested offset)")
    ca = {"o": {"canonical": {"total": 1, "hits": [], "digest": "d1",
                              "aggs": {"a": {"doc_count_error_upper_bound": 1, "buckets": [{"key": "x", "doc_count": 2}]}}}}}
    cb = {"o": {"canonical": {"total": 1, "hits": [], "digest": "d2",
                              "aggs": {"a": {"doc_count_error_upper_bound": 2, "buckets": [{"key": "x", "doc_count": 3}]}}}}}
    cc = {"o": {"canonical": {"total": 1, "hits": [], "digest": "d3",
                              "aggs": {"a": {"doc_count_error_upper_bound": 2, "buckets": [{"key": "x", "doc_count": 2}]}}}}}
    ign = ["doc_count_error_upper_bound"]
    check(_ie.compare([{"name": "o"}], ca, cb, ign)[1] == 0 and _ie.compare([{"name": "o"}], ca, cc, ign)[1] == 1,
          "(g) index_equality: an ignored agg key is left out, every other difference still counts")
    orc = _ie.oracle_op({"name": "o", "body": {"aggs": {"t": {"terms": {"field": "tag"}}}}})
    check(orc["params"]["search_type"] == "dfs_query_then_fetch" and orc["body"]["profile"] is True and
          orc["body"]["aggs"]["t"]["terms"]["shard_size"] == _ie.ORACLE_SHARD_SIZE,
          "(g) index_equality: oracle form = dfs_query_then_fetch, profile, large terms shard_size")
    ex = json.load(open(os.path.join(here, "arms.generic.example.json")))
    check(ex["other_indices"] == "close" and "S2-CORE+PLANNER-EBS" in ex["arms"] and "S2-A-EBS" in ex["arms"],
          "(g) arms.generic.example.json: other_indices close, EBS POC arms present")
    check(ex["arms"]["S2-B-EBS"].get("not_applicable") and ex["arms"]["S2-CORE+PLANNER-EBS"]["index"] == "stock_ebs",
          "(g) geoshape: the EBS copies follow the split-BKD applicability like the EFS arms")
    # ingest_osb.refuse_existing: an index of the workload's own name is never deleted and rebuilt silently
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from common import JsonClient

    have = {"so", "so__ichk1"}

    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def do_GET(self):  # noqa: N802
            name = self.path.split("?")[0].split("/")[-1]
            body = json.dumps([{"index": name}] if name in have else {"error": "index_not_found_exception"}).encode()
            self.send_response(200 if name in have else 404)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    cl = JsonClient(f"http://127.0.0.1:{srv.server_address[1]}")
    try:
        ingest_osb.refuse_existing(cl, ["so"])
        check(False, "(g) ingest: an existing index of the workload's name is refused")
    except RuntimeError as e:
        check("--force-recreate" in str(e), "(g) ingest: an existing index of the workload's name is refused")
    check(ingest_osb.refuse_existing(cl, ["so"], force_recreate=True) == ["so"], "(g) ingest: --force-recreate rebuilds it")
    check(ingest_osb.refuse_existing(cl, ["so__ichk1", "so_split"]) == [],
          "(g) ingest: the check copies and absent indices are not refused")
    srv.shutdown()
