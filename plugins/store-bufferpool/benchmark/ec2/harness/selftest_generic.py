#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Self-test of the generic-workload extensions (no AWS, no OpenSearch). Needs a checkout of
opensearch-benchmark-workloads at the pinned commit and jinja2 (installed with opensearch-benchmark).
  (a) renders the ten generic workloads with --osb-only: op counts of GENERIC-PLAN section 4 (H1), every op is a JSON
      body that coldbench.execute sends to /<arm target>/_search (the workload's own index or path is not used), the
      param-source ops are deterministic (the committed queries/<w>.osb.json / geonames summary re-render identically)
  (b) big5, nyc_taxis, http_logs, pmc re-render byte-identical to the committed queries/<corpus>.osb.json
  (c) canonical() on response fixtures of every new shape: equal responses hash equal; a changed bucket, score,
      percolator slot, highlight fragment, inner hit or after_key hashes different; responses without the new keys
      hash exactly as the original canonical() did (big5 digests unchanged); terminated_early is reported
  (d) queries.py build --profile-values <fixture> --check-coverage for every new profile; not-applicable entries
      are validated; families_ext.discover keeps queries.discover's rules (same values on a mock client)
  (e) a mock-node coldbench session (selftest.py's mock) with a multi-index target (geoshape-like members), a
      param-source op and a not-applicable arm: per-member open / verify, the comma target queried, the gap reported
  (f) luceneutil: the switch table generated from the fork's source (gen_switches.parse on a Java fixture; and, with
      --lucene-fork, the committed switches.json equals a fresh generation), switch-spec validation and encoding,
      ColdpathSwitches.java itself (compiled from patch 0001 with javac when available: set, read back, exit 3 on an
      unknown class / setter / bad value), the luceneutil result-log parser on a log in luceneutil's format, the
      luceneutil -> coldbench conversion and analyze.py on it, and the generated driver script
  (g) ingest_osb.py: the derived ingest-only workload of every workload (shard / replica override, split-BKD meta only
      where B applies, base-url, expected doc counts, bulk ops with 8 clients, index-update procedure); arms_generic.py:
      the same arms as the main arms file, B arms not applicable where B does not apply, CORE without B, geoshape members
  selftest_generic.py --osb-workloads DIR [--lucene-fork DIR --fork-commit REF] [--keep DIR]
"""
import argparse
import copy
import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
sys.path.insert(0, os.path.join(here, "luceneutil"))
PY = sys.executable
GENERIC = ["clickbench", "eventdata", "geonames", "geopoint", "geopointshape", "geoshape", "nested", "noaa",
           "percolator", "so"]
MAIN = ["big5", "nyc_taxis", "http_logs", "pmc"]
OSB_COUNTS = {"clickbench": 46, "geonames": 40, "geopoint": 4, "geopointshape": 2, "geoshape": 2, "nested": 27,
              "noaa": 53, "percolator": 8, "eventdata": 0, "so": 0}
PIN = "29dd7e1f681df035c87a5fba0e31d0cc0ce46a4f"
RESULTS = []


def check(cond, what):
    RESULTS.append((bool(cond), what))
    if not cond:
        raise AssertionError(what)


def run(cmd, **kw):
    p = subprocess.run(cmd, capture_output=True, text=True, **kw)
    if p.returncode != 0:
        print(p.stdout[-4000:], p.stderr[-4000:])
        raise SystemExit(f"FAILED: {' '.join(cmd)}")
    return p.stdout


def build(corpus, osbw, out, extra=()):
    return run([PY, os.path.join(here, "queries.py"), "build", "--corpus", corpus, "--osb-workloads", osbw,
                "--osb-commit", PIN, "--out", out, *extra])


# ---------------------------------------------------------------- (a) and (b)
class RecordingClient:
    def __init__(self):
        self.calls = []
        self.last_wall_s = 0.001

    def request(self, method, path, body=None, timeout=None):
        self.calls.append((method, path, body))
        return {"took": 1, "timed_out": False, "_shards": {"failed": 0}, "hits": {"total": {"value": 1, "relation": "eq"},
                "hits": [{"_id": "x", "_score": 1.0, "sort": [1]}]}, "_scroll_id": "s"}

    def raw(self, method, path, body=None, timeout=None):
        self.calls.append((method, path, body))
        return 200, {}


def part_a_b(osbw, tmp):
    import coldbench
    import osb_import_ext

    for c in GENERIC:
        out = os.path.join(tmp, f"{c}.osb.json")
        build(c, osbw, out, ["--osb-only"])
        d = json.load(open(out))
        n = sum(o["name"].startswith("osb:") for o in d["ops"])
        check(n == OSB_COUNTS[c], f"(a) {c}: {n} OSB ops, plan says {OSB_COUNTS[c]}")
        target = "tgt_a,tgt_b" if c == "geoshape" else "tgt"
        for o in d["ops"]:
            check(isinstance(o["body"], dict) and json.loads(json.dumps(o["body"])) == o["body"], f"(a) {c} {o['name']}: JSON body")
            cl = RecordingClient()
            coldbench.execute(cl, target, o)
            m, path, body = cl.calls[0]
            check(path.startswith(f"/{target}/_search?") and "request_cache=false" in path,
                  f"(a) {c} {o['name']}: sent to the arm target with request_cache=false ({path})")
            check(body == o["body"] or (o["type"] == "search_after" and {k: v for k, v in body.items() if k != "search_after"} == o["body"]),
                  f"(a) {c} {o['name']}: body sent unchanged")
        if c == "clickbench":
            check(all("timeout" not in o["body"] for o in d["ops"]), "(a) clickbench: timeout removed from every DSL body")
            check(len(d["osb_import"]["body_edits"]) == 46, "(a) clickbench: the body edit is recorded on all 46 ops")
            ppl = [x for x in d["osb_import"]["not_measured"] if x.get("path") == "/_plugins/_ppl"]
            check(len(ppl) == 43, f"(a) clickbench: 43 PPL ops listed as not measured with the reason ({len(ppl)})")
        if c == "geonames":
            ps = [o for o in d["ops"] if o.get("param_source")]
            check(len(ps) == 15 and len({osb_import_ext.body_digest(o["body"]) for o in ps}) == 15,
                  "(a) geonames: 3 param sources x 5 distinct bodies")
            check(all(o["param_source"]["seed"] == 4 for o in ps), "(a) geonames: param-source seed 4 recorded")
            summ = json.load(open(os.path.join(here, "queries", "geonames.osb.summary.json")))
            check(osb_import_ext.summarize(d) == summ, "(a) geonames: re-render equals queries/geonames.osb.summary.json")
            cached = next(o for o in d["ops"] if o["name"] == "osb:country_agg_cached")
            check(cached.get("notes") and cached["params"] == {}, "(a) geonames: cache:true op noted, request_cache=false")
        else:
            committed = os.path.join(here, "queries", f"{c}.osb.json")
            check(open(out, "rb").read() == open(committed, "rb").read(), f"(a) {c}: re-render byte-identical to {committed}")
        if c == "nested":
            ps = [o for o in d["ops"] if o.get("param_source")]
            check(len(ps) == 25, "(a) nested: 5 param sources x 5 instances")
            ih = [o for o in ps if o["param_source"]["op_name"].endswith("big_size")]
            check(all(o["body"]["size"] == 100 and json.dumps(o["body"]).count('"inner_hits": {"size": 100}') == 1 for o in ih),
                  "(a) nested: op params size / inner_hits_size passed to the param source")
        if c == "geoshape":
            check(all(o.get("osb_index") == "osm*" for o in d["ops"]), "(a) geoshape: the workload's osm* target recorded")
    for c in MAIN:
        out = os.path.join(tmp, f"{c}.osb.json")
        build(c, osbw, out, ["--osb-only"])
        committed = os.path.join(here, "queries", f"{c}.osb.json")
        check(open(out, "rb").read() == open(committed, "rb").read(), f"(b) {c}: re-render byte-identical to the committed file")


# ---------------------------------------------------------------- (c)
def original_canonical(pages, typ):
    """coldbench.canonical() as it was before the generic extensions (commit b304c475834), for the digest check."""
    import coldbench

    first = pages[0]
    total = first.get("hits", {}).get("total")
    hits = [[h.get("_id"), coldbench._round(h.get("_score")), coldbench._round(h.get("sort"))] for p in pages
            for h in p.get("hits", {}).get("hits", [])]
    out = {"total": [total["value"], total["relation"]] if isinstance(total, dict) else total,
           "aggs": coldbench._round(first.get("aggregations")), "aggs_size": coldbench._agg_items(first.get("aggregations"))}
    if typ == "scroll":
        ids = sorted(str(h[0]) for h in hits)
        out["hits_count"] = len(ids)
        out["hits_digest"] = hashlib.sha256("\n".join(ids).encode()).hexdigest()
    else:
        out["hits"] = hits
    out["digest"] = hashlib.sha256(json.dumps(out, sort_keys=True).encode()).hexdigest()[:16]
    return out


def _resp(hits=None, aggs=None):
    r = {"took": 3, "timed_out": False, "_shards": {"failed": 0},
         "hits": {"total": {"value": 10, "relation": "eq"}, "hits": hits or []}}
    if aggs is not None:
        r["aggregations"] = aggs
    return r


def part_c():
    import coldbench

    plain = [_resp([{"_id": "a", "_score": 1.5, "sort": [3]}, {"_id": "b", "_score": 1.25}],
                   {"t": {"buckets": [{"key": "x", "doc_count": 3}]}})]
    for typ in ("search", "scroll"):
        check(coldbench.canonical(plain, typ) == original_canonical(plain, typ),
              f"(c) {typ}: a response without the new keys hashes exactly as before")
    fixtures = {
        "percolate": ([_resp([{"_id": "q1", "_score": 0.3, "fields": {"_percolator_document_slot": [0]}},
                              {"_id": "q2", "_score": 0.2, "fields": {"_percolator_document_slot": [0, 1]}}])],
                      lambda r: r[0]["hits"]["hits"][1]["fields"]["_percolator_document_slot"].pop()),
        "highlight": ([_resp([{"_id": "q1", "_score": 0.3, "highlight": {"body": ["prime <em>minister</em>"]}}])],
                      lambda r: r[0]["hits"]["hits"][0]["highlight"]["body"].__setitem__(0, "prime minister")),
        "inner_hits": ([_resp([{"_id": "d1", "_score": 2.0, "inner_hits": {"answers": {"hits": {
            "total": {"value": 2, "relation": "eq"}, "hits": [
                {"_id": "d1", "_nested": {"field": "answers", "offset": 0}, "_score": 1.0},
                {"_id": "d1", "_nested": {"field": "answers", "offset": 3}, "_score": 0.5}]}}}}])],
            lambda r: r[0]["hits"]["hits"][0]["inner_hits"]["answers"]["hits"]["hits"][1]["_nested"].__setitem__("offset", 4)),
        "score": ([_resp([{"_id": "a", "_score": 3.14159265358979}])],
                  lambda r: r[0]["hits"]["hits"][0].__setitem__("_score", 3.1416)),
        "top_hits": ([_resp(aggs={"t": {"buckets": [{"key": "x", "doc_count": 2, "top": {"hits": {"hits": [
            {"_id": "1", "_score": None, "sort": [5], "_source": {"a": 1}}]}}}]}})],
            lambda r: r[0]["aggregations"]["t"]["buckets"][0]["top"]["hits"]["hits"][0].__setitem__("_id", "2")),
        "top_metrics": ([_resp(aggs={"tm": {"top": [{"sort": [7.5], "metrics": {"TMAX": 30.1}}]}})],
                        lambda r: r[0]["aggregations"]["tm"]["top"][0]["metrics"].__setitem__("TMAX", 30.2)),
        "geohash_grid": ([_resp(aggs={"g": {"buckets": [{"key": "u0", "doc_count": 9}, {"key": "u1", "doc_count": 1}]}})],
                         lambda r: r[0]["aggregations"]["g"]["buckets"][1].__setitem__("doc_count", 2)),
        "geotile_grid": ([_resp(aggs={"g": {"buckets": [{"key": "4/8/5", "doc_count": 9}]}})],
                         lambda r: r[0]["aggregations"]["g"]["buckets"][0].__setitem__("key", "4/8/6")),
        "geo_bounds": ([_resp(aggs={"b": {"bounds": {"top_left": {"lat": 61.0, "lon": -10.0}, "bottom_right": {"lat": 35.0, "lon": 30.0}}}})],
                       lambda r: r[0]["aggregations"]["b"]["bounds"]["top_left"].__setitem__("lat", 61.5)),
        "geo_centroid": ([_resp(aggs={"c": {"location": {"lat": 50.123456789, "lon": 8.1}, "count": 5}})],
                         lambda r: r[0]["aggregations"]["c"]["location"].__setitem__("lat", 50.1234)),
        "significant_text": ([_resp(aggs={"s": {"doc_count": 10, "bg_count": 100, "buckets": [
            {"key": "w", "doc_count": 3, "score": 0.75, "bg_count": 9}]}})],
            lambda r: r[0]["aggregations"]["s"]["buckets"][0].__setitem__("score", 0.76)),
        "auto_date_histogram": ([_resp(aggs={"h": {"buckets": [{"key": 1, "doc_count": 2}], "interval": "1d"}})],
                                lambda r: r[0]["aggregations"]["h"].__setitem__("interval", "7d")),
        "multi_terms": ([_resp(aggs={"m": {"buckets": [{"key": ["a", 1], "key_as_string": "a|1", "doc_count": 2}]}})],
                        lambda r: r[0]["aggregations"]["m"]["buckets"][0]["key"].__setitem__(1, 2)),
        "composite": ([_resp(aggs={"c": {"after_key": {"k": "z"}, "buckets": [{"key": {"k": "z"}, "doc_count": 1}]}})],
                      lambda r: r[0]["aggregations"]["c"]["after_key"].__setitem__("k", "y")),
        "sampler_filters_nested": ([_resp(aggs={"s": {"doc_count": 5, "f": {"buckets": {"a": {"doc_count": 2}}},
                                                "n": {"doc_count": 7, "h": {"buckets": [{"key": 1, "doc_count": 7}]}}}})],
                                   lambda r: r[0]["aggregations"]["s"]["n"].__setitem__("doc_count", 8)),
    }
    for name, (pages, mutate) in fixtures.items():
        a = coldbench.canonical(copy.deepcopy(pages), "search")
        b = coldbench.canonical(copy.deepcopy(pages), "search")
        check(a["digest"] == b["digest"], f"(c) {name}: equal responses hash equal")
        changed = copy.deepcopy(pages)
        mutate(changed)
        check(coldbench.canonical(changed, "search")["digest"] != a["digest"], f"(c) {name}: a changed part hashes different")
    flags = coldbench.execute(type("C", (), {"last_wall_s": 0.0, "request": lambda self, *a, **k: {
        "took": 1, "timed_out": False, "terminated_early": True, "_shards": {"failed": 0}, "hits": {"hits": []}}})(),
        "i", {"name": "x", "type": "search", "body": {}})
    check(flags.get("flags") == {"terminated_early": [True]}, "(c) terminated_early recorded per execution")
    try:
        coldbench.execute(type("C", (), {"last_wall_s": 0.0, "request": lambda self, *a, **k: {
            "took": 1, "timed_out": True, "_shards": {"failed": 0}, "hits": {"hits": []}}})(), "i",
            {"name": "x", "type": "search", "body": {}})
        check(False, "(c) timed_out fails the sample")
    except RuntimeError:
        check(True, "(c) timed_out fails the sample")


# ---------------------------------------------------------------- (d)
T = {"min_ms": 1356998400000, "max_ms": 1388534400000}


def _kw(names):
    return {n: {"cardinality": 50 if i == 0 else 3 + i, "high": f"{n}-h", "mid": f"{n}-m", "low": f"{n}-l",
                "five": [f"{n}-{k}" for k in range(5)]} for i, n in enumerate(names)}


def _num(names):
    return {n: {"5.0": 1.0, "40.0": 10.0, "50.0": 12.0, "60.0": 14.0, "95.0": 90.0} for n in names}


def _txt(names):
    return {n: {"terms": ["alpha", "beta", "gamma"], "mid": "delta", "phrase": "alpha beta"} for n in names}


def _geo(fields):
    return {f: {"type": t, "top": 61.0, "left": -10.0, "bottom": 35.0, "right": 30.0, "centroid": {"lat": 50.0, "lon": 8.0},
                "centroid_rule": "fixture"} for f, t in fields}


def profile_values():
    nest = {"answers": {"date": {"answers.date": {"min_ms": T["min_ms"], "max_ms": T["max_ms"], "p": {
        "5.0": 1.36e12, "40.0": 1.37e12, "50.0": 1.372e12, "60.0": 1.374e12, "95.0": 1.385e12}}},
        "keyword": {"answers.user": {"cardinality": 1000, "high": "u1", "mid": "u2", "low": "u3"}}}}
    v = {}
    for c in GENERIC:
        p = json.load(open(os.path.join(here, "corpora", c + ".json")))
        x = {"rules": "fixture"}
        if p.get("time_field"):
            x["time"] = T
        x["keyword"] = _kw(p["keyword_fields"])
        x["numeric"] = _num(p["numeric_fields"])
        x["text"] = _txt(p["text_fields"])
        if p.get("geo_fields"):
            x["geo"] = _geo([(g["field"], g["type"]) for g in p["geo_fields"]])
        if p.get("nested"):
            x["nested"] = nest
        v[c] = x
    return v


class MockSearch:
    """Answers the discovery requests of queries.discover / families_ext.discover from fixed data."""

    def __init__(self):
        self.calls = []

    def request(self, method, path, body=None, timeout=None):
        self.calls.append(json.dumps(body, sort_keys=True))
        aggs = (body or {}).get("aggs", {})
        out = {"hits": {"hits": [{"_source": {"message": "alpha beta gamma delta alpha beta", "title": "alpha beta zeta"}}] * 3}}
        res = {}
        for name, a in aggs.items():
            if "min" in a:
                res[name] = {"value": T["min_ms"]}
            elif "max" in a:
                res[name] = {"value": T["max_ms"]}
            elif "terms" in a:
                res[name] = {"buckets": [{"key": f"k{i}", "doc_count": 100 - i} for i in range(40)]}
            elif "cardinality" in a:
                res[name] = {"value": 40}
            elif "percentiles" in a:
                res[name] = {"values": {"5.0": 1.0, "40.0": 4.0, "50.0": 5.0, "60.0": 6.0, "95.0": 9.5}}
        out["aggregations"] = res
        return out


def part_d(osbw, tmp):
    import families_ext
    import queries

    vals = profile_values()
    for c in GENERIC:
        vf = os.path.join(tmp, f"values-{c}.json")
        json.dump(vals[c], open(vf, "w"))
        out = os.path.join(tmp, f"{c}.ops.json")
        build(c, osbw, out, ["--profile-values", vf, "--check-coverage"])
        d = json.load(open(out))
        check(not d["missing_families"], f"(d) {c}: every required family covered or not applicable")
        check(all(str(r).strip() for r in d["families_not_applicable"].values()), f"(d) {c}: not-applicable reasons given")
        check(d["reference_op"] in {o["name"] for o in d["ops"]}, f"(d) {c}: reference op exists")
        names = [o["name"] for o in d["ops"]]
        check(len(names) == len(set(names)), f"(d) {c}: op names unique")
        p = json.load(open(os.path.join(here, "corpora", c + ".json")))
        if p.get("geo_fields"):
            check(all(f in d["families"] for f in families_ext.GEO_FAMILIES), f"(d) {c}: geo families generated")
        if p.get("nested"):
            check(all(f in d["families"] for f in families_ext.NESTED_FAMILIES), f"(d) {c}: nested families generated")
    p = json.load(open(os.path.join(here, "corpora", "so.json")))
    bad = dict(p, families_not_applicable={"agg:terms": "", "geo:nonsense": "x"})
    _, _, _, problems = families_ext.coverage([], bad)
    check(len(problems) == 2, f"(d) malformed not-applicable entries rejected: {problems}")
    # discovery: families_ext keeps queries.discover's rules (same requests, same values) on a timed profile
    prof = {"corpus": "t", "time_field": "@timestamp", "keyword_fields": ["a", "b"], "numeric_fields": ["n"],
            "text_fields": ["message"]}
    m1, m2 = MockSearch(), MockSearch()
    v1 = queries.discover(m1, "i", prof)
    v2 = families_ext.discover(m2, "i", prof)
    check(v1 == v2 and m1.calls == m2.calls, "(d) families_ext.discover == queries.discover on a timed profile")
    untimed = dict(prof)
    del untimed["time_field"]
    m3 = MockSearch()
    v3 = families_ext.discover(m3, "i", untimed)
    check({k: v3[k] for k in ("keyword", "numeric", "text")} == {k: v1[k] for k in ("keyword", "numeric", "text")},
          "(d) the untimed discovery uses the same keyword / numeric / text rules")
    # geo helpers: box area fraction and octagon
    v = {"top": 60.0, "left": 0.0, "bottom": 40.0, "right": 20.0, "centroid": {"lat": 50.0, "lon": 10.0}}
    t, l, b, r = families_ext.geo_box(v, 0.01)
    check(math.isclose((t - b) * (r - l), 0.01 * 400, rel_tol=1e-9), "(d) geo box covers 1 % of the extent area")
    ring = families_ext.octagon((t, l, b, r))
    check(len(ring) == 9 and ring[0] == ring[-1] and all(l <= x <= r and b <= y <= t for x, y in ring),
          "(d) octagon is closed and inside its box")


# ---------------------------------------------------------------- (e)
def part_e(osbw, tmp):
    import selftest as st

    class Indices(dict):
        """The mock's indices plus comma-joined targets (open when every member is open), like OpenSearch."""

        def __contains__(self, k):
            return dict.__contains__(self, k) or ("," in k and all(dict.__contains__(self, p) for p in k.split(",")))

        def __getitem__(self, k):
            if "," in k:
                ms = [dict.__getitem__(self, p) for p in k.split(",")]
                return {"status": "open" if all(m["status"] == "open" for m in ms) else "close", "uuid": "multi",
                        "store_type": ms[0]["store_type"]}
            return dict.__getitem__(self, k)

    m = st.Mock()
    m.indices = Indices({"osma": {"status": "open", "uuid": "u-a", "store_type": "bufferpoolfs"},
                         "osmb": {"status": "open", "uuid": "u-b", "store_type": "bufferpoolfs"}})
    _, os_url = st.serve(st.make_os_handler(m))
    _, agent_url = st.serve(st.make_agent_handler(m))
    token = os.path.join(tmp, "token")
    open(token, "w").write("selftest-token-0123456789\n")
    vals = profile_values()
    vf = os.path.join(tmp, "values-nested-e.json")
    json.dump(vals["nested"], open(vf, "w"))
    full = os.path.join(tmp, "nested-e.ops.json")
    build("nested", osbw, full, ["--profile-values", vf])
    d = json.load(open(full))
    keep = {d["reference_op"], "osb:randomized-nested-queries#1", "gen:nested_inner_hits_answers.date_p40_p60",
            "gen:nested_sort_answers.date_max_desc", "gen:scroll_match_all_doc"}
    d["ops"] = [o for o in d["ops"] if o["name"] in keep]
    check(len(d["ops"]) == len(keep), f"(e) ops present {sorted(o['name'] for o in d['ops'])}")
    ops = os.path.join(tmp, "ops-e.json")
    json.dump(d, open(ops, "w"))
    members = [{"name": "osma", "docs": 1000, "shards": 2, "segments_per_shard": 1},
               {"name": "osmb", "docs": 1000, "shards": 2, "segments_per_shard": 1}]
    arms = {"indices": {"stock_efs": {"storage": "EFS", "format": "stock", "members": members}},
            "base_switches": [], "outcome": {"reference": "S0-EBS", "targets": ["S1-EFS"]},
            "arms": {"S0-EBS": {"node": "S0-EBS", "bufferpool": False, "index": "stock_efs", "open": ["stock_efs"],
                                "store_types": {"stock_efs": "hybridfs"}},
                     "S1-EFS": {"node": "POC-EFS", "bufferpool": True, "index": "stock_efs", "open": ["stock_efs"],
                                "store_types": {"stock_efs": "bufferpoolfs"}},
                     "S2-B-EFS": {"not_applicable": "split BKD (B) is not applicable: fixture"}}}
    af = os.path.join(tmp, "arms-e.json")
    json.dump(arms, open(af, "w"))
    out = os.path.join(tmp, "session-e")
    run([PY, os.path.join(here, "coldbench.py"), "run", "--arms", af, "--ops", ops, "--url", os_url, "--agent", agent_url,
         "--token-file", token, "--arm-list", "S0-EBS,S1-EFS,S2-B-EFS", "--rounds", "2", "--cold-iters", "2",
         "--warm-warmup", "1", "--warm-iters", "3", "--out", out, "--strict"])
    recs = [json.loads(l) for l in open(os.path.join(out, "samples.jsonl"))]
    runs = [r for r in recs if r["type"] == "run"]
    na = [r for r in runs if r["arm"] == "S2-B-EFS"]
    check(len(na) == 2 and all(not r["available"] and "not applicable" in r["reason"] for r in na),
          "(e) not-applicable arm recorded as a gap in every round")
    ok = [r for r in runs if r.get("available")]
    check(all(r["query_index"] == "osma,osmb" for r in ok), "(e) the comma-joined members are the query target")
    check(all([x["name"] for x in r["indices"]["stock_efs"]["members"]] == ["osma", "osmb"] and
              r["indices"]["stock_efs"]["uuids"] == ["u-a", "u-b"] for r in ok), "(e) every member verified, uuids collected")
    cold = [r for r in recs if r["type"] == "sample" and r["mode"] == "cold"]
    check(cold and all(r["cold_ok"] for r in cold), "(e) cold samples verified")
    check(any(r["op"] == "osb:randomized-nested-queries#1" for r in cold), "(e) the param-source op ran")
    check(m.indices["osma"]["store_type"] == m.indices["osmb"]["store_type"], "(e) store type switched on every member")
    rep = run([PY, os.path.join(here, "analyze.py"), out, "--base", "S0-EBS", "--boot", "300", "--ni-boot", "200",
               "--warm-metric", "took_ms", "--out", os.path.join(tmp, "analysis-e")])
    check("Not available" in rep and "S2-B-EFS" in rep, "(e) analyze.py lists the not-applicable arm as a gap")
    res = json.load(open(os.path.join(tmp, "analysis-e", "analysis.json")))
    check(res["equality"]["across"] and all(e["equal"] for e in res["equality"]["across"]), "(e) results equal across arms")


# ---------------------------------------------------------------- (f)
JAVA_FIXTURE = """
package org.example.exp;
/** fixture */
public final class Exp {
  public enum Mode { OFF, ON, AUTO }
  private static volatile int n = 8;
  private static volatile boolean on;
  private static volatile Mode mode = Mode.OFF;
  // public static void setCommented(int x) { }
  public static void setN(int v) {
    if (v < 1 || v > 64) {
      throw new IllegalArgumentException("n");
    }
    n = v;
  }
  public static int getN() { return n; }
  public static void setOn(boolean v) { on = v; }
  public static boolean isOn() { return on; }
  public static void setMode(Mode m) { mode = m; }
  public static Mode getMode() { return mode; }
  public static void setOrphan(long x) { }
}
"""


def _patch_new_file(patch, path):
    lines, on = [], False
    for line in open(patch).read().splitlines():
        if line.startswith("+++ "):
            on = line[4:].strip() == "b/" + path
            continue
        if on:
            if line.startswith("diff --git"):
                break
            if line.startswith("+"):
                lines.append(line[1:])
    return "\n".join(lines) + "\n"


def part_f(tmp, fork, fork_commit):
    import analyze_luceneutil
    import gen_switches
    import lu_results
    import run_luceneutil
    import switches as sw

    fqcn, setters = gen_switches.parse(JAVA_FIXTURE)
    check(fqcn == "org.example.exp.Exp", "(f) gen_switches: class name")
    check(sorted(setters) == ["setMode", "setN", "setOn", "setOrphan"], f"(f) gen_switches: setters {sorted(setters)}")
    check(setters["setN"]["getter"] == "getN" and setters["setOn"]["getter"] == "isOn" and setters["setOrphan"]["getter"] is None,
          "(f) gen_switches: read-back getters")
    check(setters["setMode"]["enum"] == ["OFF", "ON", "AUTO"], "(f) gen_switches: enum values")
    check(setters["setN"]["checks"] == ["v < 1 || v > 64"], "(f) gen_switches: range check text")
    table = json.load(open(os.path.join(here, "luceneutil", "switches.json")))
    n = sum(len(c["setters"]) for c in table["classes"].values())
    check(n == 18 and len(table["classes"]) == 6, f"(f) switches.json: 6 classes, 18 setters ({n})")
    if fork:
        fresh = gen_switches.build(fork, fork_commit)
        check(fresh["classes"] == table["classes"], f"(f) switches.json equals a fresh generation from {fork_commit}")
    B = "org.apache.lucene.util.bkd.BKDExperiments."
    K = "org.apache.lucene.search.comparators.ComparatorExperiments."
    flag, res = sw.jvm_flag(table, {B + "setIntersectPrefetch": True, B + "setNodeBytes": "{io.sequential_bytes}",
                                    K + "setSkipperMode": "FIRST"}, {"io.sequential_bytes": 131072})
    check(flag == f"-Dcoldpath.switches={B}setIntersectPrefetch=true,{B}setNodeBytes=131072,{K}setSkipperMode=FIRST",
          f"(f) switch encoding {flag}")
    check(sw.jvm_flag(table, {})[0] == "", "(f) no switches -> no flag (stock)")
    for bad, why in (({"org.x.Nope.setA": 1}, "unknown class"), ({B + "setNope": 1}, "unknown setter"),
                     ({B + "setIntersectPrefetch": "yes"}, "bad boolean"), ({K + "setSkipperMode": "MAYBE"}, "enum"),
                     ({B + "setNodeBytes": "{missing}"}, "undefined param"), ({B + "setPrefetchChunks": 1.5}, "not an int")):
        try:
            sw.encode(table, bad, {})
            check(False, f"(f) switch spec rejected: {why}")
        except sw.SwitchError:
            check(True, f"(f) switch spec rejected: {why}")
    rb = sw.parse_readbacks(f"x\nCOLDPATH switch {B}setIntersectPrefetch=true readback=true\n")
    check(rb == {B + "setIntersectPrefetch": {"sent": "true", "readback": "true"}}, "(f) read-back lines parsed")
    try:
        sw.parse_readbacks("COLDPATH switches NOT AVAILABLE: class x is not in this Lucene build\n")
        check(False, "(f) NOT AVAILABLE detected")
    except sw.SwitchError:
        check(True, "(f) NOT AVAILABLE detected")
    # ColdpathSwitches.java itself, compiled from the patch, against the fixture class
    javac, java = shutil.which("javac"), shutil.which("java")
    if javac and java:
        jd = os.path.join(tmp, "java")
        os.makedirs(os.path.join(jd, "src", "org", "example", "exp"), exist_ok=True)
        os.makedirs(os.path.join(jd, "src", "perf"), exist_ok=True)
        open(os.path.join(jd, "src", "org", "example", "exp", "Exp.java"), "w").write(JAVA_FIXTURE)
        patch = os.path.join(here, "luceneutil", "patches", "0001-Set-Lucene-experiment-switches-from-Dcoldpath.switch.patch")
        src = _patch_new_file(patch, "src/main/perf/ColdpathSwitches.java")
        check("class ColdpathSwitches" in src, "(f) ColdpathSwitches.java extracted from patch 0001")
        open(os.path.join(jd, "src", "perf", "ColdpathSwitches.java"), "w").write(src)
        main = ("package perf; public class Main { public static void main(String[] a) { ColdpathSwitches.apply(); "
                "System.out.println(\"after \" + org.example.exp.Exp.getN() + \" \" + org.example.exp.Exp.isOn() + \" \" + "
                "org.example.exp.Exp.getMode()); } }")
        open(os.path.join(jd, "src", "perf", "Main.java"), "w").write(main)
        run([javac, "-d", os.path.join(jd, "classes"), *[os.path.join(r, f) for r, _, fs in os.walk(os.path.join(jd, "src")) for f in fs]])

        def jrun(spec):
            return subprocess.run([java, f"-Dcoldpath.switches={spec}", "-cp", os.path.join(jd, "classes"), "perf.Main"],
                                  capture_output=True, text=True)
        p = jrun("org.example.exp.Exp.setN=16,org.example.exp.Exp.setOn=true,org.example.exp.Exp.setMode=AUTO")
        check(p.returncode == 0 and "after 16 true AUTO" in p.stdout and
              "COLDPATH switch org.example.exp.Exp.setN=16 readback=16" in p.stdout, f"(f) Java: set and read back ({p.stdout})")
        check(len(sw.parse_readbacks(p.stdout)) == 3, "(f) Java read-back lines parse")
        for spec, why in (("org.example.nope.X.setN=1", "unknown class"), ("org.example.exp.Exp.setZ=1", "unknown setter"),
                          ("org.example.exp.Exp.setN=99", "setter range check"), ("org.example.exp.Exp.setOrphan=1", "no getter"),
                          ("org.example.exp.Exp.setMode=NOPE", "bad enum"), ("org.example.exp.Exp.setOn=1", "bad boolean")):
            p = jrun(spec)
            check(p.returncode == 3 and "NOT AVAILABLE" in p.stdout, f"(f) Java: exit 3 on {why}")
        p = subprocess.run([java, "-cp", os.path.join(jd, "classes"), "perf.Main"], capture_output=True, text=True)
        check(p.returncode == 0 and "none (stock behaviour)" in p.stdout and "after 8 false OFF" in p.stdout,
              "(f) Java: no property -> nothing set")
    else:
        RESULTS.append((True, "(f) Java ColdpathSwitches run SKIPPED: no javac/java on PATH"))
    # luceneutil result log parser
    log = os.path.join(here, "luceneutil", "testdata", "wikimedium-sample.log")
    r = lu_results.parse(open(log).read())
    check(len(r["tasks"]) == 6 and r["winddown_ms"] == 31234.5 and r["avg_cpu_cores"] == 1.87, "(f) parser: run metrics")
    keys = [t.key for t in r["tasks"]]
    check(keys[0] == keys[4] == "cat=HighTerm q=body:list s=null facets=[]" and r["tasks"][0].digest() == r["tasks"][4].digest(),
          "(f) parser: same task, same key and results digest")
    check(r["tasks"][3].key.endswith("facets=[taxonomy:Month]") and r["tasks"][3].results[-1] == "Feb (2723344)",
          "(f) parser: facet request in the key, facet results kept")
    check(all("getFacetResults time" not in x for t in r["tasks"] for x in t.results), "(f) parser: timing lines not in results")
    check(r["tasks"][2].results[0] == "doc=901 lastModNDV=1335862382000", "(f) parser: sort values kept")
    # luceneutil -> coldbench conversion and analyze.py (two arms, 4 JVM runs each)
    sess = os.path.join(tmp, "lu-session")
    os.makedirs(sess)
    cfg = json.load(open(os.path.join(here, "luceneutil", "arms.luceneutil.example.json")))
    driver, s = run_luceneutil.plan(cfg, table, "warm", "wikimedium.10M.nostopwords.tasks", "EBS", ["L0", "L2-A"], sess)
    compile(open(driver).read(), driver, "exec")
    check("-Dcoldpath.switches=" + B + "setIntersectPrefetch=true" in s["arms"][1]["java_command"], "(f) driver: arm flags")
    check(s["arms"][0]["java_command"] == cfg["java_command"], "(f) driver: stock arm has the base java command only")
    try:
        run_luceneutil.plan(dict(cfg, arms={"X": {"checkout": "stock", "index": "stock", "switches": {B + "setIntersectPrefetch": True}}}),
                            table, "warm", "t", "EBS", ["X"], os.path.join(tmp, "lu-bad"))
        check(False, "(f) switches on stock Lucene rejected")
    except ValueError:
        check(True, "(f) switches on stock Lucene rejected")
    text = open(log).read()
    manifest = []
    for it in range(4):
        for label, scale in (("L0", 1.0), ("L2-A", 0.8)):
            lf = os.path.join(sess, f"x.{label}.{it}")
            out_lines = []
            for line in text.splitlines():
                mm = lu_results.LATENCY.match(line.strip())
                out_lines.append(f"  {float(mm.group(1)) * scale * (1 + 0.01 * it):.4f} msec @ {mm.group(2)} msec" if mm else line)
            open(lf, "w").write("\n".join(out_lines) + "\n")
            sws = [] if label == "L0" else [{"switch": B + "setIntersectPrefetch", "value": "true"}]
            open(lf + ".stdout", "w").write("COLDPATH switches: none (stock behaviour)\n" if label == "L0" else
                                            f"COLDPATH switch {B}setIntersectPrefetch=true readback=true\n")
            manifest.append({"iter": it, "label": label, "arm": label, "log": lf, "stdout": lf + ".stdout", "cold_log": None,
                             "switches": sws})
    with open(os.path.join(sess, "manifest.jsonl"), "w") as f:
        for m in manifest:
            f.write(json.dumps(m) + "\n")
    cb = os.path.join(sess, "coldbench")
    analyze_luceneutil.convert(sess, cb)
    run([PY, os.path.join(here, "analyze.py"), cb, "--base", "L0", "--boot", "500", "--ni-boot", "300",
               "--warm-metric", "took_ms", "--ni-ref", "L0", "--ni-target", "L2-A", "--out", os.path.join(sess, "analysis")])
    res = json.load(open(os.path.join(sess, "analysis", "analysis.json")))
    rows = res["comparisons"]["L2-A:warm"]
    check(len(rows) == 5 and all(-0.25 < x["change"] < -0.15 for x in rows), f"(f) analyze.py on luceneutil runs: "
          f"{[(x['op'], round(x['change'], 3)) for x in rows]}")
    check(res["equality"]["across"] and all(e["equal"] for e in res["equality"]["across"]), "(f) luceneutil results equal")
    # a run whose read-back is missing is a gap, not a measurement
    bad = os.path.join(tmp, "lu-session-bad")
    shutil.copytree(sess, bad, ignore=shutil.ignore_patterns("coldbench", "analysis"))
    open(os.path.join(bad, "x.L2-A.0.stdout"), "w").write("COLDPATH switches: none (stock behaviour)\n")
    with open(os.path.join(bad, "manifest.jsonl"), "w") as f:
        for m in manifest:
            m = dict(m, log=m["log"].replace(sess, bad), stdout=m["stdout"].replace(sess, bad))
            f.write(json.dumps(m) + "\n")
    analyze_luceneutil.convert(bad, os.path.join(bad, "coldbench"))
    runs = [json.loads(l) for l in open(os.path.join(bad, "coldbench", "samples.jsonl")) if '"type": "run"' in l]
    gap = [r for r in runs if r["run_id"] == "L2-A#r0"]
    check(gap and gap[0]["available"] is False, "(f) missing switch read-back makes the JVM run a gap")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--osb-workloads", required=True, help=f"opensearch-benchmark-workloads checkout at {PIN}")
    ap.add_argument("--lucene-fork", help="Lucene fork checkout: also regenerate switches.json and compare")
    ap.add_argument("--fork-commit", default="origin/bkd-split")
    ap.add_argument("--keep")
    a = ap.parse_args()
    head = subprocess.run(["git", "-C", a.osb_workloads, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    if head != PIN:
        sys.exit(f"{a.osb_workloads} is at {head}, the pin is {PIN}")
    tmp = a.keep or tempfile.mkdtemp(prefix="selftest-generic-")
    os.makedirs(tmp, exist_ok=True)
    for name, fn in (("a,b", lambda: part_a_b(a.osb_workloads, tmp)), ("c", part_c),
                     ("d", lambda: part_d(a.osb_workloads, tmp)), ("e", lambda: part_e(a.osb_workloads, tmp)),
                     ("f", lambda: part_f(tmp, a.lucene_fork, a.fork_commit)),
                     ("g", lambda: __import__("selftest_generic_ingest").part_g(a.osb_workloads, tmp, check, GENERIC))):
        n0 = len(RESULTS)
        fn()
        print(f"part ({name}): {len(RESULTS) - n0} checks passed", flush=True)
    for ok, what in RESULTS:
        if "SKIPPED" in what:
            print(what)
    print(f"\nSELFTEST-GENERIC PASS: {len(RESULTS)} checks ({tmp})")
    if not a.keep:
        shutil.rmtree(tmp)


if __name__ == "__main__":
    main()
