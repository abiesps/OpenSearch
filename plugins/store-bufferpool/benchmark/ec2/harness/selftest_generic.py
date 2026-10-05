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
import re
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

    def __init__(self, count=None):
        self.calls = []
        self.counts = []  # _count requests (families_ext's sanity checks), kept apart from the discovery requests
        self.count = count or (lambda body: 7)

    def request(self, method, path, body=None, timeout=None):
        if path.endswith("/_count"):
            self.counts.append(body)
            return {"count": self.count(body)}
        self.calls.append(json.dumps(body, sort_keys=True))
        if path.endswith("/_analyze"):
            toks = re.findall(r"[a-z0-9]+", body["text"].lower())
            return {"tokens": [{"token": t, "position": i} for i, t in enumerate(toks)]}
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

    # fallbacks of generated ops that would match nothing (pmc): empty 1h window at the middle, disjoint top terms
    class AnchorMock:
        def __init__(self, count):
            self.count, self.calls = count, []

        def request(self, method, path, body=None, timeout=None):
            self.calls.append((path, body))
            if path.endswith("/_count"):
                return {"count": self.count}
            gte = "gte" in json.dumps(body)
            return {"hits": {"hits": [{"sort": [5000.0 if gte else 4000.0]}]}}
    check(queries.time_anchor(AnchorMock(3), "i", "ts", 0, 8000) is None, "(d) time anchor: populated 1h window kept")
    a = queries.time_anchor(AnchorMock(0), "i", "ts", 0, 8000)
    check(a and a["anchor_ms"] == 5000.0 and a["anchor_rule"], "(d) time anchor: empty 1h window -> first doc at or after the middle")
    ga = queries.generate({"corpus": "x", "time_field": "ts", "keyword_fields": ["k"], "numeric_fields": [], "text_fields": []},
                          {"time": {"min_ms": 0, "max_ms": 8000, "anchor_ms": 5000.0},
                           "keyword": {"k": {"cardinality": 5, "high": "a", "mid": "b", "low": "c", "five": ["a"]}},
                           "numeric": {}, "text": {}})
    r1h = [o for o in ga if o["name"] == "gen:range_ts_1h"][0]["body"]["query"]["range"]["ts"]
    check(r1h["gte"] == 5000 - 1800000 and r1h["lt"] == 5000 + 1800000, "(d) time anchor: windows centred on the anchor")
    long_text = " ".join(f"w{i}" for i in range(9000))
    pcs = queries._pieces(long_text)
    check("".join(pcs) == long_text and len(pcs) > 1 and all(len(x) <= queries.ANALYZE_CHARS for x in pcs)
          and queries._pieces("short text") == ["short text"], "(d) _analyze pieces: lossless, bounded, short text unchanged")
    check(queries.and2_pair(["a", "b", "c"], [{"a", "b"}, {"c"}]) is None, "(d) and2: co-occurring top pair kept")
    check(queries.and2_pair(["bmc", "plos", "one", "x"], [{"bmc"}, {"plos", "one"}, {"plos", "one", "x"}, {"bmc", "x"}])
          == ["plos", "one"], "(d) and2: disjoint top pair -> the most co-occurring pair of the top 10")
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
        # a bool op never excludes a term it also requires or scores (an always-empty or dead clause)
        def terms(clauses):
            return {json.dumps(x["term"], sort_keys=True) for x in clauses or [] if "term" in x}
        dead = [o["name"] for o in d["ops"] for b in [o["body"].get("query", {}).get("bool")] if b
                and terms(b.get("must_not")) & (terms(b.get("filter")) | terms(b.get("must")) | terms(b.get("should")))]
        check(not dead, f"(d) {c}: no bool op whose must_not repeats one of its own terms: {dead}")
        p = json.load(open(os.path.join(here, "corpora", c + ".json")))
        if p.get("geo_fields"):
            check(all(f in d["families"] for f in families_ext.GEO_FAMILIES), f"(d) {c}: geo families generated")
            # OpenSearch accepts only relation intersects in a geo_shape query on a geo_point field
            points = {g["field"] for g in p["geo_fields"] if g["type"] == "geo_point"}
            bad_rel = [o["name"] for o in d["ops"] for gs in [o["body"].get("query", {}).get("geo_shape", {})]
                       for f, q in gs.items() if f in points and q.get("relation") not in (None, "intersects")]
            check(not bad_rel, f"(d) {c}: no within/disjoint geo_shape query on a geo_point field: {bad_rel}")
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
    # m2.counts also holds queries.time_anchor's 1h-window count (no bool query); count only the must_not checks
    check(len([c for c in m2.counts if "bool" in c.get("query", {})]) == 1 and "must_not_field" not in v2,
          "(d) must_not field kept when it leaves docs")
    # a must_not term that excludes every doc of the filter: the next keyword field by cardinality is used
    prof3 = dict(prof, keyword_fields=["a", "b", "c"])
    m4 = MockSearch(lambda body: 0 if "bool" in body["query"] and "b" in json.dumps(body["query"]["bool"]["must_not"]) else 3)
    v4 = families_ext.discover(m4, "i", prof3)
    check(v4.get("must_not_field", {}).get("field") == "c", f"(d) must_not falls back to the next field: {v4.get('must_not_field')}")
    ops4 = queries.generate(prof3, v4)
    b4 = next(o for o in ops4 if o["name"] == "gen:bool_filter_must_not")["body"]["query"]["bool"]["must_not"][0]["term"]
    check(list(b4) == ["c"], f"(d) generated must_not uses the fallback field: {b4}")
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
    # geo_shape centre: the densest geotile cell, small enough to fit in the smallest box
    world = {"top": 84.0, "left": -180.0, "bottom": -87.0, "right": 180.0}
    zw = families_ext.densest_tile_zoom(world)
    check(zw == 6 and 360.0 / (1 << zw) <= math.sqrt(0.001) * 360.0 / 2 < 360.0 / (1 << (zw - 1)),
          f"(d) densest-tile zoom for a world extent is the smallest that fits half the 0.1 % box: {zw}")
    c = families_ext.geotile_centre("6/33/21")
    check(math.isclose(c["lon"], 33.5 / 64 * 360 - 180) and 49.0 < c["lat"] < 52.0,
          f"(d) geotile 6/33/21 centre is in central Europe: {c}")
    c0 = families_ext.geotile_centre("0/0/0")
    check(abs(c0["lat"]) < 1e-9 and abs(c0["lon"]) < 1e-9, "(d) geotile 0/0/0 centre is (0, 0)")

    class TileMock:
        def __init__(self):
            self.calls = []

        def request(self, method, path, body=None, timeout=None):
            self.calls.append(body)
            aggs = body["aggs"]
            if "b" in aggs:
                return {"aggregations": {"b": {"bounds": {"top_left": {"lat": 84.0, "lon": -180.0},
                                                         "bottom_right": {"lat": -87.0, "lon": 180.0}}}}}
            return {"aggregations": {"t": {"buckets": [{"key": "6/32/22", "doc_count": 5}, {"key": "6/33/21", "doc_count": 7},
                                                       {"key": "6/31/22", "doc_count": 7}]}}}
    tm = TileMock()
    gv = families_ext._geo_vals(tm, "i", [{"field": "shape", "type": "geo_shape"}])["shape"]
    check(gv["centroid"] == families_ext.geotile_centre("6/31/22") and "geotile_grid" in gv["centroid_rule"]
          and tm.calls[1]["aggs"]["t"]["geotile_grid"]["precision"] == 6,
          f"(d) geo_shape centre = densest tile (ties: smallest key), zoom 6: {gv['centroid']}")
    class PointMock(TileMock):
        def __init__(self, docs):
            super().__init__()
            self.docs = docs
        def request(self, method, path, body=None, timeout=None):
            if path.endswith("/_count"):
                self.calls.append(body)
                return {"count": self.docs}
            r = super().request(method, path, body, timeout)
            if "c" in body["aggs"]:
                r["aggregations"]["c"] = {"location": {"lat": 37.85, "lon": 0.70}}
            return r
    gp = families_ext._geo_vals(PointMock(12), "i", [{"field": "loc", "type": "geo_point"}])["loc"]
    check(gp["centroid"] == {"lat": 37.85, "lon": 0.70} and gp["centroid_rule"] == "geo_centroid"
          and gp["centroid_check"]["docs"] == 12, f"(d) geo_point keeps a populated centroid: {gp}")
    gp0 = families_ext._geo_vals(PointMock(0), "i", [{"field": "loc", "type": "geo_point"}])["loc"]
    check(gp0["centroid"] == families_ext.geotile_centre("6/31/22") and gp0["centroid_rule"].startswith("geo_centroid")
          and "geotile_grid" in gp0["centroid_rule"], f"(d) an empty geo_point centroid moves to the densest tile: {gp0}")
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


def _part_f_discovery(tmp, gen_switches):
    """gen_switches finds the switch classes from git: a fixture repo with a base tag, then fork commits."""
    repo = os.path.join(tmp, "fork-fixture")
    src = os.path.join(repo, "lucene", "core", "src", "java", "org", "example")
    os.makedirs(src)

    def git(*args):
        subprocess.run(["git", "-C", repo, *args], check=True, capture_output=True, text=True)

    def commit(msg):
        git("add", "-A")
        git("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", msg)
    git("init", "-q")
    stock = ("package org.example;\npublic final class Stock {\n  public static void setLimit(int v) { }\n"
             "  public static int getLimit() { return 0; }\n  public static void setPair(int a, int b) { }\n}\n")
    open(os.path.join(src, "Stock.java"), "w").write(stock)
    open(os.path.join(src, "Plain.java"), "w").write("package org.example;\npublic final class Plain { }\n")
    commit("base")
    git("tag", "base")
    open(os.path.join(src, "Exp.java"), "w").write(JAVA_FIXTURE.replace("org.example.exp", "org.example"))
    # a stock class the fork changes (adds a switch), and one it changes without adding a setter
    open(os.path.join(src, "Stock.java"), "w").write(stock.replace("}\n}", "}\n  public static void setFast(boolean v) { }\n"
                                                                    "  public static boolean isFast() { return true; }\n}"))
    open(os.path.join(src, "Plain.java"), "w").write("package org.example;\npublic final class Plain { int x; }\n")
    commit("fork 1")
    t1 = gen_switches.build(repo, "HEAD", "base")
    check(sorted(t1["classes"]) == ["org.example.Exp", "org.example.Stock"],
          f"(f) discovery: the added class and the changed stock class with a new setter ({sorted(t1['classes'])})")
    check(sorted(t1["classes"]["org.example.Stock"]["setters"]) == ["setFast"],
          "(f) discovery: only the fork's added setters of a stock class (not setLimit / setPair)")
    check(sorted(t1["classes"]["org.example.Exp"]["setters"]) == ["setMode", "setN", "setOn", "setOrphan"],
          "(f) discovery: every setter of a new class")
    # a 7th class in a later POC commit: the old table no longer matches
    open(os.path.join(src, "More.java"), "w").write("package org.example;\npublic final class More {\n"
                                                    "  public static void setDepth(int v) { }\n"
                                                    "  public static int getDepth() { return 1; }\n}\n")
    commit("fork 2")
    t2 = gen_switches.build(repo, "HEAD", "base")
    diff = gen_switches.compare(t1, t2)
    check(len(diff) == 1 and "org.example.More" in diff[0] and "not in the table" in diff[0],
          f"(f) --check fails on a switch class missing from switches.json ({diff})")
    back = gen_switches.compare(t2, t1)
    check(back and "adds no switch" in back[0], "(f) --check fails on a table class the fork does not have")
    # an added setter the parser cannot read stops the generation
    open(os.path.join(src, "More.java"), "w").write("package org.example;\npublic final class More {\n"
                                                    "  public static void setDepth(int v, int w) { }\n}\n")
    commit("fork 3")
    try:
        gen_switches.build(repo, "HEAD", "base")
        check(False, "(f) discovery: unreadable added setter stops the generation")
    except SystemExit as e:
        check("does not read" in str(e), f"(f) discovery: unreadable added setter stops the generation ({e})")


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
        check(gen_switches.compare(table, fresh) == [], "(f) --check: no difference")
    _part_f_discovery(tmp, gen_switches)
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
    # one session interleaving arms on both storages (label ARM:STORAGE@REPEAT)
    d2, s2 = run_luceneutil.plan(cfg, table, "cold-strict", "wikimedium.10M.nostopwords.tasks", "EBS",
                                 ["L0:EBS@a", "L0:EFS@a", "L1:EFS", "L1"], os.path.join(tmp, "lu-mixed"))
    compile(open(d2).read(), d2, "exec")
    check([x["storage"] for x in s2["arms"]] == ["EBS", "EFS", "EFS", "EBS"] and s2["storage"] == "EBS,EFS"
          and s2["arms"][1]["storage_spec"]["index_dir_base"] == cfg["storages"]["EFS"]["index_dir_base"],
          "(f) driver: per-arm storage from the label, default --storage")
    check(s2["cold_protocol"] == "jit-warm" and s["cold_protocol"] is None, "(f) driver: strict cold is jit-warm, warm has none")
    check("cold_jvm_count" in open(d2).read() and "/cache/drop?pageout=0" in open(d2).read(),
          "(f) driver: cold JVM count read; cold-luceneutil drops through the agent")
    # luceneutil's comparison reads the JVM runs from the manifest (a driver that kept them only in memory lost them)
    txt = open(d2).read()
    check('results = {l: [m["log"] for m in runs if m["label"] == l] for l in labels}' in txt and "expected {jvm_count} or more per label" in txt,
          "(f) driver: the result comparison uses every manifest run")
    d3, s3 = run_luceneutil.plan(cfg, table, "cold-strict", "wikimedium.10M.nostopwords.tasks", "EBS", ["L0:EBS@a", "L1:EFS@a"],
                                 os.path.join(tmp, "lu-cont"), iter_offset=8)
    compile(open(d3).read(), d3, "exec")
    check(s3["iter_offset"] == 8 and "range(iter_offset, iter_offset + comp.jvmCount)" in open(d3).read(),
          "(f) driver: a continuation session continues the JVM iteration sequence")
    check("def run_one(label, it, seed, remeasure=False):" in txt and "efs_invalid_samples" in txt and "stopping" not in
          txt.split("def run_one")[1].split("efs_invalid = 0")[1].split("if strict:")[0],
          "(f) driver: EFS samples off the connection target are re-measured at the end, not a stop")
    rd = run_luceneutil.report_driver(os.path.join(tmp, "lu-mixed"))
    compile(open(rd).read(), rd, "exec")
    check("results = {l: [m" in open(rd).read(), "(f) report driver for a finished session compiles")
    for bad_labels in (["L0:EBS", "L0:EBS"], ["L0:XFS"]):
        try:
            run_luceneutil.plan(cfg, table, "warm", "t", "EBS", bad_labels, os.path.join(tmp, "lu-bad2"))
            check(False, f"(f) driver: labels {bad_labels} rejected")
        except ValueError:
            check(True, f"(f) driver: labels {bad_labels} rejected")
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
    # a pooled outcome target (LABEL1+LABEL2): the runs of both labels form one target
    run([PY, os.path.join(here, "analyze.py"), cb, "--base", "L0", "--boot", "200", "--ni-boot", "200", "--warm-metric", "took_ms",
         "--ni-ref", "L0", "--ni-target", "L2-A+L0", "--out", os.path.join(sess, "analysis-pooled")])
    pooled = json.load(open(os.path.join(sess, "analysis-pooled", "analysis.json")))["noninferiority"]["targets"]["L2-A+L0"]
    check(pooled and all(st["warm_p50"]["runs"] == [4, 8] for st in pooled.values() if "warm_p50" in st),
          "(f) analyze.py pools LABEL1+LABEL2 targets (4 reference runs, 8 target runs)")
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
    gap = [r for r in runs if r["label"] == "L2-A" and r["run_id"].endswith("#r0")]
    check(gap and gap[0]["available"] is False, "(f) missing switch read-back makes the JVM run a gap")
    # EFS backend connections: a warm JVM run on EFS carries its start / end count; only 5 -> 5 on one proxy is valid
    efs = os.path.join(tmp, "lu-session-efs")
    shutil.copytree(sess, efs, ignore=shutil.ignore_patterns("coldbench", "analysis*"))
    snap = lambda n, pid, rb: {"t_mono": 1.0 + rb, "pid": None, "proc_io": None, "disk": None,  # noqa: E731
                               "nfs": {"mountpoint": "/mnt/efs", "normal_read_bytes": rb, "direct_read_bytes": 0,
                                       "server_read_bytes": rb, "read_pages": 0, "ops": {}},
                               "efs_connections": {"count": n, "proxy_pid": pid}}
    with open(os.path.join(efs, "manifest.jsonl"), "w") as f:
        for m in manifest:
            m = dict(m, log=m["log"].replace(sess, efs), stdout=m["stdout"].replace(sess, efs), efs_connections_target=5,
                     jvm_snapshots={"pre": snap(1 if (m["label"] == "L2-A" and m["iter"] == 1) else 5, 77, 0),
                                    "post": snap(5, 77, 1000)})
            f.write(json.dumps(m) + "\n")
    analyze_luceneutil.convert(efs, os.path.join(efs, "coldbench"))
    smp = [json.loads(l) for l in open(os.path.join(efs, "coldbench", "samples.jsonl")) if '"type": "sample"' in l]
    bad_run = [x for x in smp if x["label"] == "L2-A" and x["run_id"].endswith("#r1")]
    good = [x for x in smp if not (x["label"] == "L2-A" and x["run_id"].endswith("#r1"))]
    check(bad_run and all(x["efs_connections_ok"] is False for x in bad_run) and all(x["efs_connections_ok"] for x in good)
          and all(x["io"]["efs_connections"]["start"] in (1, 5) for x in smp),
          "(f) EFS warm samples carry the JVM run's connection count; 1 -> 5 is not valid")


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
