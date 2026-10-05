#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Builds the operation set of one corpus: every search operation of the corpus' OpenSearch Benchmark workload (rendered
from its operations/*.json with jinja2), plus generated operations that cover every query family the cold-path work
targets, from a corpus profile (corpora/<corpus>.json) that names fields by ROLE only. Values (terms at fixed
frequency ranks, time windows, numeric percentiles, text terms and phrases) are DISCOVERED from the data with fixed
rules, so nothing is tuned to one benchmark's constants:
  keyword  term (high, mid, low frequency), terms (5 values)
  text     match (one term; OR of 3; AND of 2), match_phrase, query_string, multi_match
  bool     must+filter, should (minimum_should_match 1), filter+must_not, must+should+filter+must_not
  bkd      date range 1h/1d/7d, numeric range narrow (p40-p60) and wide (p5-p95), range + sort on the same field
  sort     @timestamp asc/desc, numeric asc/desc, keyword asc/desc, search_after (3 pages)
  agg      date_histogram (full span and in a 1d range), terms (high and low cardinality), composite, range,
           cardinality, significant_terms, stats in a range
  scroll   match_all and range-filtered, sorted by _doc
Every op carries "families" tags; `build --check-coverage` fails if a required family has no op.

  queries.py build --corpus big5 --osb-workloads DIR --url http://DATA:9200 --index big5 --out ops/big5.json
  queries.py build --corpus big5 --osb-workloads DIR --profile-values values.json --out ...   (no cluster)
  queries.py build --corpus big5 --osb-workloads DIR --osb-only --out queries/big5.osb.json
  queries.py check --ops ops/big5.json --url http://DATA:9200 --index big5   (zero-hit / error report per op)
"""
import argparse
import collections
import datetime
import glob
import json
import os
import re
import sys

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
from common import JsonClient  # noqa: E402

REQUIRED_FAMILIES = [
    "keyword:term", "keyword:terms", "text:match", "text:phrase", "text:query_string", "text:multi_match",
    "bool:must", "bool:should", "bool:filter", "bool:must_not", "bkd:range_date", "bkd:range_numeric",
    "sort:date_asc", "sort:date_desc", "sort:numeric_asc", "sort:numeric_desc", "sort:keyword", "sort:search_after",
    "agg:date_histogram", "agg:terms", "agg:composite", "agg:range", "agg:cardinality", "agg:significant_terms",
    "scroll",
]
# fixed date_histogram ladder: the first interval giving at most MAX_BUCKETS buckets over the span is used
INTERVALS = [("1m", 60), ("1h", 3600), ("1d", 86400), ("7d", 7 * 86400), ("30d", 30 * 86400)]
MAX_BUCKETS = 2000
TOKEN = re.compile(r"[a-z]{3,}")


# ---------------------------------------------------------------- OSB workload operations
def render_osb_ops(workload_dir, ops_files, params, profile=None, distribution_version="3.0.0", now_epoch=1759600000):
    """The search operations of an OSB workload, rendered with jinja2 like OSB does (fixed `now` for determinism)."""
    import jinja2  # installed with opensearch-benchmark

    env = jinja2.Environment(loader=jinja2.FileSystemLoader(workload_dir), undefined=jinja2.ChainableUndefined)

    def days_ago(start_date, end_date, date_format="%d-%m-%Y"):
        start = datetime.datetime.strptime(start_date, date_format)
        end = datetime.datetime.fromtimestamp(end_date) if isinstance(end_date, (int, float)) else end_date
        return (end - start).days

    env.filters["days_ago"] = days_ago
    out = []
    for rel in ops_files:
        text = env.get_template(rel).render(distribution_version=distribution_version, now=now_epoch, **params)
        text = text.strip().rstrip(",")
        items = json.loads("[" + text + "]")
        for op in items:
            if op.get("operation-type") != "search":
                continue
            if "body" not in op:
                print(f"skip {op['name']}: no body (param source)", file=sys.stderr)
                continue
            o = {"name": "osb:" + op["name"], "source": f"osb:{os.path.basename(workload_dir)}/{rel}", "type": "search",
                 "body": op["body"], "params": {k: str(v).lower() if isinstance(v, bool) else str(v)
                                                for k, v in (op.get("request-params") or {}).items()}}
            if "pages" in op:
                o["type"] = "scroll"
                o["pages"] = int(op["pages"])
                o["page_size"] = int(op.get("results-per-page", 1000))
            o["families"] = classify(o, profile)
            out.append(o)
    return out


def classify(op, profile=None):
    """Family tags of an op from its body (used for OSB ops and as a cross-check of generated ones)."""
    body = op["body"]
    text = json.dumps(body)
    fam = set()
    if op["type"] == "scroll":
        fam.add("scroll")
    for k, tag in (('"term"', "keyword:term"), ('"terms"', "keyword:terms"), ('"match"', "text:match"),
                   ('"match_phrase"', "text:phrase"), ('"query_string"', "text:query_string"),
                   ('"multi_match"', "text:multi_match"), ('"must"', "bool:must"), ('"should"', "bool:should"),
                   ('"filter"', "bool:filter"), ('"must_not"', "bool:must_not"), ('"date_histogram"', "agg:date_histogram"),
                   ('"auto_date_histogram"', "agg:date_histogram"), ('"composite"', "agg:composite"),
                   ('"cardinality"', "agg:cardinality"), ('"significant_terms"', "agg:significant_terms"),
                   ('"search_after"', "sort:search_after"), ('"collapse"', "collapse"), ('"multi_terms"', "agg:terms")):
        if k + ":" in text:
            fam.add(tag)
    aggs = body.get("aggs") or body.get("aggregations") or {}
    if '"range": {"field"' in json.dumps(aggs) or '"ranges"' in json.dumps(aggs):
        fam.add("agg:range")
    if re.search(r'"terms": \{"field"', json.dumps(aggs)):
        fam.add("agg:terms")
    if '"stats"' in json.dumps(aggs) or '"avg"' in json.dumps(aggs):
        fam.add("agg:stats")
    for field in _walk_key(body.get("query", {}), "range"):
        fam.add(_field_role(field, profile, "bkd:range_date", "bkd:range_numeric", "bkd:range_other"))
    sort = body.get("sort")
    for entry in (sort if isinstance(sort, list) else [sort] if sort else []):
        if isinstance(entry, str):
            field, order = entry, "asc" if entry != "_score" else "desc"
        else:
            field = next(iter(entry))
            spec = entry[field]
            order = spec.get("order", "asc") if isinstance(spec, dict) else spec
        if field in ("_doc", "_score", "_shard_doc"):
            continue
        role = _field_role(field, profile, "date", "numeric", "keyword")
        fam.add(f"sort:{role}_{order}" if role != "keyword" else "sort:keyword")
    return sorted(fam)


def _walk_key(node, key):
    """Field names under every {key: {field: ...}} in a query tree."""
    if isinstance(node, dict):
        for k, v in node.items():
            if k == key and isinstance(v, dict):
                yield from (f for f in v if f not in ("boost",))
            else:
                yield from _walk_key(v, key)
    elif isinstance(node, list):
        for v in node:
            yield from _walk_key(v, key)


def _field_role(field, profile, date, numeric, other):
    if profile is None:
        return other
    if field == profile.get("time_field") or field in profile.get("date_fields", []):
        return date
    if field in profile.get("numeric_fields", []):
        return numeric
    return other


# ---------------------------------------------------------------- discovery
def _get(src, dotted):
    v = src
    for p in dotted.split("."):
        if isinstance(v, dict) and p in v:
            v = v[p]
        elif isinstance(v, dict) and dotted in v:
            return v[dotted]
        else:
            return None
    return v


def phrase_bigrams(client, index, field, text):
    """
    Phrase candidates of one document: two TOKEN tokens at ADJACENT positions of the field's own analyzer (_analyze).
    TOKEN-filtered tokens are not adjacent in the indexed text when a number or a short word sits between them (big5:
    "mar cron" from "Mar 22 ... cron"); such a phrase matches nothing, and on a match_only_text field the phrase query
    then confirms every candidate from _source (161 s on big5 100 GB).
    """
    return analyze(client, index, field, text)[0]


def analyze(client, index, field, text):
    """(phrase bigrams, indexed tokens) of one document by the field's own analyzer (_analyze)."""
    an = client.request("POST", f"/{index}/_analyze", {"field": field, "text": str(text or "")})
    pos = {tk["position"]: tk["token"] for tk in an.get("tokens", [])}
    bigrams = {(pos[p], pos[p + 1]) for p in pos if p + 1 in pos and TOKEN.fullmatch(pos[p]) and TOKEN.fullmatch(pos[p + 1])}
    return bigrams, set(pos.values())


def indexed_ranked(docfreq, indexed):
    """
    Candidate terms by document frequency, keeping only tokens the analyzer indexes. A TOKEN match inside a longer
    token is not a term of the field (http_logs: "anime" from "/anime_1.gif", which the standard tokenizer keeps
    as one token) and its match query has 0 hits; where every TOKEN match is a token (whitespace text), nothing is
    dropped and the ranking is the same as before.
    """
    return [w for w, _ in sorted(docfreq.items(), key=lambda kv: (-kv[1], kv[0])) if w in indexed]


ANCHOR_RULE = ("time windows are centred on the middle of [min,max]; when the 1h window there holds no document "
               "(timestamps with day or coarser resolution, or a gap in the data), they are centred on the first "
               "document timestamp at or after the middle (the last one before it if there is none)")
AND2_RULE = ("match AND of the two most frequent terms; when no sampled document holds both (terms of disjoint "
             "values, such as two journal names), the pair of the 10 most frequent terms that the most sampled "
             "documents hold together, ties by term rank")


def time_anchor(client, index, tf, tmin, tmax):
    """
    {"anchor_ms": t, "anchor_rule": ...} when the 1h window around the middle of the time span holds no document,
    else None (the windows stay centred on the middle). pmc: publication dates at day resolution, so a 1h window
    centred between two days matched nothing.
    """
    mid = (tmin + tmax) / 2
    q = {"range": {tf: {"gte": int(mid - 1800e3), "lt": int(mid + 1800e3), "format": "epoch_millis"}}}
    if client.request("POST", f"/{index}/_count", {"query": q})["count"] > 0:
        return None
    for cmp, order in (("gte", "asc"), ("lte", "desc")):
        r = client.request("POST", f"/{index}/_search?request_cache=false",
                           {"size": 1, "_source": False, "query": {"range": {tf: {cmp: int(mid), "format": "epoch_millis"}}},
                            "sort": [{tf: {"order": order, "format": "epoch_millis"}}]})
        hits = r["hits"]["hits"]
        if hits:
            return {"anchor_ms": float(hits[0]["sort"][0]), "anchor_rule": ANCHOR_RULE}
    return None


def and2_pair(ranked, docsets):
    """The AND pair for match_and2 when the two most frequent terms never occur together in the sample, else None."""
    def together(a, b):
        return sum(1 for s in docsets if a in s and b in s)
    if together(ranked[0], ranked[1]) > 0:
        return None
    top = ranked[:10]
    best = max(((together(a, b), -i, -j, a, b) for i, a in enumerate(top) for j, b in enumerate(top) if i < j),
               default=None)
    if best is None or best[0] == 0:
        return None
    return [best[3], best[4]]


def discover(client, index, profile):
    """Values for the generated ops, from the data, with fixed rank/percentile rules (recorded in the output)."""
    vals = {"rules": "time: middle of [min,max]; keyword: terms by count, ranks 0 / len//10 / last of top 1000; "
                     "numeric: p5/p40/p50/p60/p95; text: sample of 200 docs (random_score seed 42), token doc "
                     "frequency ranks 0/1/2 and len//4, most frequent bigram of adjacent analyzer positions (_analyze)"}
    tf = profile["time_field"]
    r = client.request("POST", f"/{index}/_search?request_cache=false",
                       {"size": 0, "aggs": {"min": {"min": {"field": tf}}, "max": {"max": {"field": tf}}}})
    tmin, tmax = r["aggregations"]["min"]["value"], r["aggregations"]["max"]["value"]
    vals["time"] = {"min_ms": tmin, "max_ms": tmax}
    anchor = time_anchor(client, index, tf, tmin, tmax)
    if anchor is not None:
        vals["time"].update(anchor)
    vals["keyword"] = {}
    for f in profile["keyword_fields"]:
        r = client.request("POST", f"/{index}/_search?request_cache=false",
                           {"size": 0, "aggs": {"t": {"terms": {"field": f, "size": 1000}}, "c": {"cardinality": {"field": f}}}})
        b = r["aggregations"]["t"]["buckets"]
        if not b:
            continue
        vals["keyword"][f] = {"cardinality": r["aggregations"]["c"]["value"], "high": b[0]["key"],
                              "mid": b[len(b) // 10]["key"], "low": b[-1]["key"],
                              "five": [b[i]["key"] for i in sorted({0, len(b) // 20, len(b) // 10, len(b) // 2, len(b) - 1})]}
    vals["numeric"] = {}
    for f in profile["numeric_fields"]:
        r = client.request("POST", f"/{index}/_search?request_cache=false",
                           {"size": 0, "aggs": {"p": {"percentiles": {"field": f, "percents": [5, 40, 50, 60, 95]}}}})
        p = r["aggregations"]["p"]["values"]
        if all(v is None for v in p.values()):
            # a mapped field without any value would give range bounds of null (0 hits) and sorts on missing values
            raise RuntimeError(f"numeric field {f} has no value in {index}: remove it from the profile's "
                               "numeric_fields and list its families as not applicable with that reason")
        vals["numeric"][f] = {k: p[k] for k in p}
    vals["text"] = {}
    for f in profile["text_fields"]:
        r = client.request("POST", f"/{index}/_search?request_cache=false",
                           {"size": 200, "_source": [f], "query": {"function_score": {"query": {"exists": {"field": f}},
                            "random_score": {"seed": 42, "field": "_seq_no"}}}})
        docfreq, bigrams, indexed, docsets = collections.Counter(), collections.Counter(), set(), []
        for h in r["hits"]["hits"]:
            t = _get(h.get("_source", {}), f)
            if isinstance(t, list):
                t = " ".join(map(str, t))
            toks = TOKEN.findall(str(t or "").lower())
            docfreq.update(set(toks))
            # _analyze needs one concrete index: the hit's own (http_logs discovers over logs-*)
            bg, tk = analyze(client, h.get("_index") or index, f, t)
            bigrams.update(bg)
            indexed |= tk
            docsets.append(tk)
        ranked = indexed_ranked(docfreq, indexed)
        if len(ranked) < 4:
            continue
        bg = sorted(bigrams.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
        vals["text"][f] = {"terms": [ranked[0], ranked[1], ranked[2]], "mid": ranked[len(ranked) // 4],
                           "phrase": " ".join(bg)}
        and2 = and2_pair(ranked, docsets)
        if and2 is not None:
            vals["text"][f]["and2"], vals["text"][f]["and2_rule"] = and2, AND2_RULE
    return vals


# ---------------------------------------------------------------- generated ops
def generate(profile, vals):
    tf = profile["time_field"]
    tmin, tmax = vals["time"]["min_ms"], vals["time"]["max_ms"]
    # time_anchor (discover) moves the centre only where the 1h window at the middle is empty
    mid = vals["time"].get("anchor_ms", (tmin + tmax) / 2)
    win = {"1h": 3600e3, "1d": 86400e3, "7d": 7 * 86400e3}
    # epoch_millis works whatever date format the mapping declares
    rng = {k: {"gte": int(mid - w / 2), "lt": int(mid + w / 2), "format": "epoch_millis"} for k, w in win.items()}
    span_s = max(1, (tmax - tmin) / 1000)
    interval = next((name for name, s in INTERVALS if span_s / s <= MAX_BUCKETS), INTERVALS[-1][0])
    kw = vals["keyword"]
    kws = [f for f in profile["keyword_fields"] if f in kw]
    hi_card = max(kws, key=lambda f: kw[f]["cardinality"])
    lo_card = min((f for f in kws if kw[f]["cardinality"] >= 2), key=lambda f: kw[f]["cardinality"])
    nums = [f for f in profile["numeric_fields"] if f in vals["numeric"]]
    texts = [f for f in profile["text_fields"] if f in vals["text"]]
    ops = []

    def add(name, families, body, typ="search", **extra):
        o = {"name": "gen:" + name, "source": f"generated:{profile['corpus']}", "type": typ, "body": body,
             "params": {}, **extra}
        o["families"] = sorted(set(families) | set(classify(o, profile)))
        ops.append(o)

    # keyword
    for f in kws[:2]:
        for level in ("high", "mid", "low"):
            add(f"term_{f}_{level}", {"keyword:term"}, {"query": {"term": {f: kw[f][level]}}})
        add(f"terms5_{f}", {"keyword:terms"}, {"query": {"terms": {f: kw[f]["five"]}}})
    # text
    for f in texts:
        t = vals["text"][f]
        add(f"match_{f}_high", {"text:match"}, {"query": {"match": {f: t["terms"][0]}}})
        add(f"match_{f}_mid", {"text:match"}, {"query": {"match": {f: t["mid"]}}})
        add(f"match_or3_{f}", {"text:match"}, {"query": {"match": {f: " ".join(t["terms"])}}})
        and2 = t.get("and2", t["terms"][:2])  # and2_pair (discover) only where the top two never co-occur
        add(f"match_and2_{f}", {"text:match"}, {"query": {"match": {f: {"query": " ".join(and2), "operator": "and"}}}})
        add(f"phrase_{f}", {"text:phrase"}, {"query": {"match_phrase": {f: t["phrase"]}}})
        add(f"query_string_{f}", {"text:query_string"},
            {"query": {"query_string": {"query": f"{f}:({t['terms'][0]} AND {t['mid']}) OR {f}:{t['terms'][1]}"}}})
    if texts:
        f0 = texts[0]
        # a profile may name the fields (http_logs: its one text field and its keyword sub-field; the highest
        # cardinality keyword-like field there is an ip field, which rejects a text query)
        mm_fields = profile.get("multi_match_fields") or (texts if len(texts) > 1 else texts + [hi_card])
        add("multi_match", {"text:multi_match"},
            {"query": {"multi_match": {"query": " ".join(vals["text"][f0]["terms"][:2]), "fields": mm_fields}}})
    # bool
    k0 = kws[0]
    # must_not field: the highest-cardinality keyword field other than k0 when k0 is itself the highest-cardinality
    # one (eventdata: agent), so a filter and a must_not never name the same term (an always-empty bool); profiles
    # whose k0 is not the highest-cardinality field keep hi_card (unchanged op sets)
    neg = hi_card if hi_card != k0 else max((f for f in kws if f != k0), key=lambda f: kw[f]["cardinality"], default=k0)
    text_clause = {"match": {texts[0]: vals["text"][texts[0]]["terms"][0]}} if texts else {"term": {k0: kw[k0]["high"]}}
    add("bool_must_filter", {"bool:must", "bool:filter", "bkd:range_date"},
        {"query": {"bool": {"must": [text_clause], "filter": [{"range": {tf: rng["1d"]}}]}}})
    add("bool_should", {"bool:should", "keyword:term"},
        {"query": {"bool": {"should": [{"term": {k0: kw[k0]["mid"]}}, {"term": {hi_card: kw[hi_card]["low"]}}],
                            "minimum_should_match": 1}}})
    # families_ext.discover may name another field when this one excludes every doc of the filter (no key: unchanged)
    mn = (vals.get("must_not_field") or {}).get("field", neg)
    add("bool_filter_must_not", {"bool:filter", "bool:must_not", "keyword:term"},
        {"query": {"bool": {"filter": [{"term": {k0: kw[k0]["high"]}}], "must_not": [{"term": {mn: kw[mn]["high"]}}]}}})
    add("bool_all", {"bool:must", "bool:should", "bool:filter", "bool:must_not", "bkd:range_date"},
        {"query": {"bool": {"must": [text_clause], "should": [{"term": {k0: kw[k0]["mid"]}}],
                            "filter": [{"range": {tf: rng["7d"]}}], "must_not": [{"term": {neg: kw[neg]["mid"]}}]}}})
    # bkd ranges and sorts
    for w in ("1h", "1d", "7d"):
        add(f"range_{tf}_{w}", {"bkd:range_date"}, {"query": {"range": {tf: rng[w]}}})
        add(f"range_{tf}_{w}_count", {"bkd:range_date"}, {"size": 0, "track_total_hits": True, "query": {"range": {tf: rng[w]}}})
    for f in nums:
        p = vals["numeric"][f]
        add(f"range_{f}_narrow", {"bkd:range_numeric"}, {"query": {"range": {f: {"gte": p["40.0"], "lte": p["60.0"]}}}})
        add(f"range_{f}_wide", {"bkd:range_numeric"}, {"query": {"range": {f: {"gte": p["5.0"], "lte": p["95.0"]}}}})
        add(f"range_{f}_sort_desc", {"bkd:range_numeric", "sort:numeric_desc"},
            {"query": {"range": {f: {"gte": p["5.0"], "lte": p["95.0"]}}}, "sort": [{f: "desc"}]})
        for d in ("asc", "desc"):
            add(f"sort_{f}_{d}", {f"sort:numeric_{d}"}, {"query": {"match_all": {}}, "sort": [{f: d}]})
    for d in ("asc", "desc"):
        add(f"sort_{tf}_{d}", {f"sort:date_{d}"}, {"query": {"match_all": {}}, "sort": [{tf: d}]})
        add(f"range_1d_sort_{tf}_{d}", {f"sort:date_{d}", "bkd:range_date"},
            {"query": {"range": {tf: rng["1d"]}}, "sort": [{tf: d}]})
        add(f"sort_{hi_card}_{d}", {"sort:keyword"}, {"query": {"match_all": {}}, "sort": [{hi_card: d}]})
    add(f"search_after_{tf}_desc", {"sort:search_after", "sort:date_desc"},
        {"size": 100, "query": {"match_all": {}}, "sort": [{tf: "desc"}, {hi_card: "asc"}]}, typ="search_after", pages=3)
    # aggregations
    add("agg_date_histogram", {"agg:date_histogram"},
        {"size": 0, "aggs": {"a": {"date_histogram": {"field": tf, "fixed_interval": interval}}}})
    add("agg_date_histogram_1d_1m", {"agg:date_histogram", "bkd:range_date"},
        {"size": 0, "query": {"range": {tf: rng["1d"]}}, "aggs": {"a": {"date_histogram": {"field": tf, "fixed_interval": "1m"}}}})
    add(f"agg_terms_{hi_card}", {"agg:terms"}, {"size": 0, "aggs": {"a": {"terms": {"field": hi_card, "size": 10}}}})
    add(f"agg_terms_{lo_card}", {"agg:terms"}, {"size": 0, "aggs": {"a": {"terms": {"field": lo_card, "size": 10}}}})
    add("agg_composite", {"agg:composite"},
        {"size": 0, "aggs": {"a": {"composite": {"size": 1000, "sources": [
            {"k": {"terms": {"field": lo_card}}}, {"t": {"date_histogram": {"field": tf, "fixed_interval": interval}}}]}}}})
    if nums:
        p = vals["numeric"][nums[0]]
        add(f"agg_range_{nums[0]}", {"agg:range"},
            {"size": 0, "aggs": {"a": {"range": {"field": nums[0], "ranges": [
                {"to": p["5.0"]}, {"from": p["5.0"], "to": p["40.0"]}, {"from": p["40.0"], "to": p["60.0"]},
                {"from": p["60.0"], "to": p["95.0"]}, {"from": p["95.0"]}]}}}})
        add(f"agg_stats_{nums[0]}_1d", {"agg:stats", "bkd:range_date"},
            {"size": 0, "query": {"range": {tf: rng["1d"]}}, "aggs": {"a": {"stats": {"field": nums[0]}}}})
    add(f"agg_cardinality_{hi_card}", {"agg:cardinality"}, {"size": 0, "aggs": {"a": {"cardinality": {"field": hi_card}}}})
    fg = next((f for f in kws if f != lo_card), hi_card)
    add(f"agg_significant_terms_{lo_card}", {"agg:significant_terms", "keyword:term"},
        {"size": 0, "query": {"term": {fg: kw[fg]["mid"]}}, "aggs": {"a": {"significant_terms": {"field": lo_card}}}})
    # scroll
    add("scroll_match_all_doc", {"scroll"}, {"query": {"match_all": {}}, "sort": ["_doc"]}, typ="scroll", pages=10, page_size=1000)
    add(f"scroll_range_7d_doc", {"scroll", "bkd:range_date"}, {"query": {"range": {tf: rng["7d"]}}, "sort": ["_doc"]},
        typ="scroll", pages=10, page_size=1000)
    return ops


def coverage(ops):
    have = set()
    for o in ops:
        have.update(o["families"])
    missing = [f for f in REQUIRED_FAMILIES if f not in have]
    return have, missing


def cmd_build(a):
    profile = json.load(open(os.path.join(here, "corpora", a.corpus + ".json")))
    # generic workloads (clickbench, geo*, nested, percolator, ...): osb_import_ext / families_ext; others unchanged
    ext = profile.get("osb_import") == "ext"
    if ext:
        import families_ext
        import osb_import_ext
    ops, osb_report = [], None
    if a.osb_workloads:
        wd = os.path.join(a.osb_workloads, profile["osb"]["workload"])
        if ext:
            ext_ops, osb_report = osb_import_ext.render(wd, profile, classify, families_ext.classify_extra)
            ops += ext_ops
        else:
            ops += render_osb_ops(wd, profile["osb"]["ops_files"], profile["osb"].get("params", {}), profile)
        drop = set(profile["osb"].get("exclude", []))
        ops = [o for o in ops if o["name"][len("osb:"):] not in drop]
    if a.osb_only:
        vals = None
    elif a.profile_values:
        vals = json.load(open(a.profile_values))
    else:
        if not a.url:
            sys.exit("need --url (discovery), --profile-values or --osb-only")
        vals = (families_ext.discover if ext else discover)(JsonClient(a.url), a.index, profile)
    if vals is not None:
        ops += families_ext.generate(profile, vals) if ext else generate(profile, vals)
    names = [o["name"] for o in ops]
    dup = [n for n, c in collections.Counter(names).items() if c > 1]
    if dup:
        sys.exit(f"duplicate op names {dup}")
    ref = profile.get("reference_op")
    if vals is not None and ref not in names:
        sys.exit(f"reference_op [{ref}] of the profile is not an op; ops: {names}")
    if ext:
        have, missing, not_applicable, problems = families_ext.coverage(ops, profile)
        if problems:
            sys.exit(f"profile {a.corpus}: {problems}")
    else:
        have, missing = coverage(ops)
    out = {"corpus": a.corpus, "profile": profile, "values": vals, "reference_op": ref,
           "osb_workloads_commit": a.osb_commit, "ops": ops, "families": sorted(have), "missing_families": missing}
    if ext:
        out["families_not_applicable"] = not_applicable
        out["required_families"] = families_ext.required_families(profile)
        out["osb_import"] = osb_report
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    with open(a.out, "w") as f:
        json.dump(out, f, indent=1, sort_keys=True)
    print(f"{a.out}: {len(ops)} ops ({sum(o['name'].startswith('osb:') for o in ops)} from OSB), "
          f"families {len(have)}; missing {missing or 'none'}")
    if a.check_coverage and missing:
        sys.exit(1)


def cmd_check(a):
    import coldbench  # noqa: E402 - only for the executors

    ops = json.load(open(a.ops))["ops"]
    client = JsonClient(a.url)
    bad = 0
    for op in ops:
        try:
            res = coldbench.execute(client, a.index, op)
            total = res["canonical"].get("total")
            # track_total_hits=false responses carry no total: count the returned hits instead
            hits = total[0] if total else len(res["canonical"].get("hits") or [])
            nb = res["canonical"].get("aggs_size")
            flag = "" if (hits or nb) else "  ZERO"
            bad += bool(flag)
            print(f"{op['name']:<58} took {res['took_ms']:7.1f} ms hits {hits} agg_items {nb}{flag}")
        except Exception as e:  # noqa: BLE001
            bad += 1
            print(f"{op['name']:<58} ERROR {e}")
    print(f"{bad} ops with zero results or errors of {len(ops)}")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--corpus", required=True)
    b.add_argument("--osb-workloads", help="checkout of opensearch-benchmark-workloads")
    b.add_argument("--osb-commit", help="commit of that checkout, recorded in the output")
    b.add_argument("--url")
    b.add_argument("--index")
    b.add_argument("--profile-values", help="JSON of discovered values (skip discovery)")
    b.add_argument("--out", required=True)
    b.add_argument("--osb-only", action="store_true", help="only the rendered OSB ops (no cluster needed)")
    b.add_argument("--check-coverage", action="store_true")
    c = sub.add_parser("check")
    c.add_argument("--ops", required=True)
    c.add_argument("--url", required=True)
    c.add_argument("--index", required=True)
    a = ap.parse_args()
    {"build": cmd_build, "check": cmd_check}[a.cmd](a)


if __name__ == "__main__":
    main()
