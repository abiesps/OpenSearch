#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Generated query families for the generic workloads (profiles with "osb_import": "ext"), on top of queries.py:
  standard families  the profile's roles decide: a profile with a time field and keyword fields uses
                     queries.generate unchanged (same rules as big5); a profile without a time field (geonames) gets
                     the same families from the same rules minus the date ones (_generate_untimed); a profile
                     without numeric fields gets agg:range from a date_range over the time field
  geo (geo_point and geo_shape fields)
                     extent from geo_bounds (wrap_longitude false), centre = geo_centroid (geo_point) or the centre of
                     the bounds (geo_shape has no geo_centroid); boxes centred there covering 0.1 %, 1 %, 10 % of the
                     extent area -> geo_bounding_box and geo_shape envelope (intersects; within and disjoint at 1 %);
                     geo_distance with radius 0.1 %, 1 %, 10 % of the extent diagonal; an octagon inscribed in the
                     1 % box -> geo_polygon (geo_point) and geo_shape polygon intersects; aggregations geohash_grid
                     (precision 3, 5) and geotile_grid (zoom 4, 8) in the 10 % box, geo_bounds, geo_centroid
                     (geo_point); _geo_distance sort in the 1 % box (geo_point)
  nested (profile "nested": [{path, date_fields, keyword_fields}])
                     nested range on the nested date at its p40-p60 and p5-p95, nested term at high/mid/low
                     frequency rank, nested query with inner_hits (size 3), nested sort (max of the nested date,
                     asc and desc), nested + date_histogram and nested + terms aggregations
  percolate          the workload's own ops (no generated op: the index holds stored queries)
  scroll             match_all sorted by _doc for every profile (the scroll path does not depend on field roles)
Values are discovered from the data with fixed rules (recorded in the values file); nothing comes from a workload's
constants. coverage() checks the required families of the profile; a family the profile lists in
families_not_applicable (with a reason) is not required.
"""
import collections
import math

import queries

GEO_FAMILIES = ["geo:bbox", "geo:distance", "geo:polygon", "geo:shape", "geo:agg"]
NESTED_FAMILIES = ["nested:query", "nested:inner_hits", "nested:sort", "nested:agg"]
PERCOLATE_FAMILIES = ["percolate"]
AREA_FRACTIONS = [("0.1pct", 0.001), ("1pct", 0.01), ("10pct", 0.1)]
EARTH_KM = 6371.0088
NUMERIC_COMPOSITE_BUCKETS = 20


# ---------------------------------------------------------------- classification of the extra families
def _walk(node, key):
    """Every value under key `key` anywhere in a JSON tree."""
    if isinstance(node, dict):
        for k, v in node.items():
            if k == key:
                yield v
            yield from _walk(v, key)
    elif isinstance(node, list):
        for v in node:
            yield from _walk(v, key)


def classify_extra(op):
    """Family tags the base classify() does not know: geo, nested, percolate, highlight, top_hits, top_metrics."""
    body = op["body"]
    query = body.get("query", {})
    aggs = body.get("aggs") or body.get("aggregations") or {}
    fam = set()
    if any(True for _ in _walk(query, "geo_bounding_box")):
        fam.add("geo:bbox")
    if any(True for _ in _walk(query, "geo_distance")):
        fam.add("geo:distance")
    if any(True for _ in _walk(query, "geo_polygon")):
        fam.add("geo:polygon")
    for gs in _walk(query, "geo_shape"):
        fam.add("geo:shape")
        for spec in (gs.values() if isinstance(gs, dict) else []):
            shape = spec.get("shape") if isinstance(spec, dict) else None
            if isinstance(shape, dict) and str(shape.get("type", "")).lower() in ("polygon", "multipolygon"):
                fam.add("geo:polygon")
    for k in ("geohash_grid", "geotile_grid", "geo_bounds", "geo_centroid", "geo_distance"):
        if any(True for _ in _walk(aggs, k)):
            fam.add("geo:agg")
    sort = body.get("sort")
    for entry in (sort if isinstance(sort, list) else [sort] if sort else []):
        if isinstance(entry, dict):
            if "_geo_distance" in entry:
                fam.add("geo:sort")
            for spec in entry.values():
                if isinstance(spec, dict) and "nested" in spec:
                    fam.add("nested:sort")
    if any(True for _ in _walk(query, "nested")):
        fam.add("nested:query")
    if any(True for _ in _walk(query, "inner_hits")):
        fam.add("nested:inner_hits")
    if any(isinstance(v, dict) and "path" in v for v in _walk(aggs, "nested")):
        fam.add("nested:agg")
    if any(True for _ in _walk(query, "percolate")):
        fam.add("percolate")
    if "highlight" in body:
        fam.add("highlight")
    for k, tag in (("top_hits", "agg:top_hits"), ("top_metrics", "agg:top_metrics"), ("date_range", "agg:range"),
                   ("histogram", "agg:histogram"), ("percentiles", "agg:percentiles"),
                   ("value_count", "agg:stats"), ("sum", "agg:stats"), ("max", "agg:stats"), ("min", "agg:stats")):
        if any(True for _ in _walk(aggs, k)):
            fam.add(tag)
    return sorted(fam)


def required_families(profile):
    req = list(queries.REQUIRED_FAMILIES)
    if profile.get("geo_fields"):
        req += GEO_FAMILIES
    if profile.get("nested"):
        req += NESTED_FAMILIES
    if profile.get("percolate"):
        req += PERCOLATE_FAMILIES
    return req


def coverage(ops, profile):
    """(have, missing, not_applicable, problems): problems are malformed not-applicable entries."""
    have = set()
    for o in ops:
        have.update(o["families"])
    req = required_families(profile)
    na = profile.get("families_not_applicable", {})
    problems = [f"families_not_applicable[{f}] has no reason" for f, r in na.items() if not str(r or "").strip()]
    problems += [f"families_not_applicable[{f}] is not a required family" for f in na if f not in req]
    missing = [f for f in req if f not in have and f not in na]
    return have, missing, {f: na[f] for f in req if f in na}, problems


# ---------------------------------------------------------------- discovery
def _keyword_vals(client, index, fields):
    """Same rule as queries.discover: terms by count (top 1000), ranks 0 / len//10 / last, five values."""
    out = {}
    for f in fields:
        r = client.request("POST", f"/{index}/_search?request_cache=false",
                           {"size": 0, "aggs": {"t": {"terms": {"field": f, "size": 1000}}, "c": {"cardinality": {"field": f}}}})
        b = r["aggregations"]["t"]["buckets"]
        if not b:
            continue
        out[f] = {"cardinality": r["aggregations"]["c"]["value"], "high": b[0]["key"], "mid": b[len(b) // 10]["key"],
                  "low": b[-1]["key"],
                  "five": [b[i]["key"] for i in sorted({0, len(b) // 20, len(b) // 10, len(b) // 2, len(b) - 1})]}
    return out


def _numeric_vals(client, index, fields):
    out = {}
    for f in fields:
        r = client.request("POST", f"/{index}/_search?request_cache=false",
                           {"size": 0, "aggs": {"p": {"percentiles": {"field": f, "percents": [5, 40, 50, 60, 95]}}}})
        p = r["aggregations"]["p"]["values"]
        out[f] = {k: p[k] for k in p}
    return out


def _text_vals(client, index, fields):
    out = {}
    for f in fields:
        r = client.request("POST", f"/{index}/_search?request_cache=false",
                           {"size": 200, "_source": [f], "query": {"function_score": {"query": {"exists": {"field": f}},
                            "random_score": {"seed": 42, "field": "_seq_no"}}}})
        docfreq, bigrams = collections.Counter(), collections.Counter()
        for h in r["hits"]["hits"]:
            t = queries._get(h.get("_source", {}), f)
            if isinstance(t, list):
                t = " ".join(map(str, t))
            toks = queries.TOKEN.findall(str(t or "").lower())
            docfreq.update(set(toks))
            bigrams.update(set(zip(toks, toks[1:])))
        ranked = [w for w, _ in sorted(docfreq.items(), key=lambda kv: (-kv[1], kv[0]))]
        if len(ranked) < 4:
            continue
        bg = sorted(bigrams.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
        out[f] = {"terms": [ranked[0], ranked[1], ranked[2]], "mid": ranked[len(ranked) // 4], "phrase": " ".join(bg)}
    return out


def _geo_vals(client, index, geo):
    out = {}
    for g in geo:
        f, typ = g["field"], g["type"]
        aggs = {"b": {"geo_bounds": {"field": f, "wrap_longitude": False}}}
        if typ == "geo_point":
            aggs["c"] = {"geo_centroid": {"field": f}}
        r = client.request("POST", f"/{index}/_search?request_cache=false", {"size": 0, "aggs": aggs})
        b = r["aggregations"]["b"].get("bounds")
        if not b:
            continue
        v = {"type": typ, "top": b["top_left"]["lat"], "left": b["top_left"]["lon"], "bottom": b["bottom_right"]["lat"],
             "right": b["bottom_right"]["lon"]}
        if typ == "geo_point" and r["aggregations"].get("c", {}).get("location"):
            v["centroid"] = dict(r["aggregations"]["c"]["location"])
            v["centroid_rule"] = "geo_centroid"
        else:
            v["centroid"] = {"lat": (v["top"] + v["bottom"]) / 2, "lon": (v["left"] + v["right"]) / 2}
            v["centroid_rule"] = "centre of geo_bounds (geo_shape has no geo_centroid)"
        out[f] = v
    return out


def _nested_vals(client, index, nested):
    out = {}
    for n in nested:
        p = n["path"]
        nv = {"date": {}, "keyword": {}}
        for d in n.get("date_fields", []):
            r = client.request("POST", f"/{index}/_search?request_cache=false", {"size": 0, "aggs": {"n": {
                "nested": {"path": p}, "aggs": {"min": {"min": {"field": d}}, "max": {"max": {"field": d}},
                                                "p": {"percentiles": {"field": d, "percents": [5, 40, 50, 60, 95]}}}}}})
            a = r["aggregations"]["n"]
            if a["min"]["value"] is None:
                continue
            nv["date"][d] = {"min_ms": a["min"]["value"], "max_ms": a["max"]["value"], "p": dict(a["p"]["values"])}
        for k in n.get("keyword_fields", []):
            r = client.request("POST", f"/{index}/_search?request_cache=false", {"size": 0, "aggs": {"n": {
                "nested": {"path": p}, "aggs": {"t": {"terms": {"field": k, "size": 1000}}, "c": {"cardinality": {"field": k}}}}}})
            b = r["aggregations"]["n"]["t"]["buckets"]
            if not b:
                continue
            nv["keyword"][k] = {"cardinality": r["aggregations"]["n"]["c"]["value"], "high": b[0]["key"],
                                "mid": b[len(b) // 10]["key"], "low": b[-1]["key"]}
        out[p] = nv
    return out


UNTIMED_RULES = ("keyword: terms by count, ranks 0 / len//10 / last of top 1000; numeric: p5/p40/p50/p60/p95; text: "
                 "sample of 200 docs (random_score seed 42), token doc frequency ranks 0/1/2 and len//4, most frequent "
                 "adjacent bigram (the rules of queries.discover, without a time field)")


def discover(client, index, profile):
    """queries.discover's values (same rules) plus geo and nested values; works without a time field."""
    if profile.get("time_field"):
        vals = queries.discover(client, index, profile)
    else:
        vals = {"rules": UNTIMED_RULES, "keyword": _keyword_vals(client, index, profile.get("keyword_fields", [])),
                "numeric": _numeric_vals(client, index, profile.get("numeric_fields", [])),
                "text": _text_vals(client, index, profile.get("text_fields", []))}
    if profile.get("geo_fields"):
        vals["geo"] = _geo_vals(client, index, profile["geo_fields"])
        vals["rules"] += ("; geo: geo_bounds (wrap_longitude false) and geo_centroid (geo_point) or the centre of "
                          "the bounds (geo_shape)")
    if profile.get("nested"):
        vals["nested"] = _nested_vals(client, index, profile["nested"])
        vals["rules"] += ("; nested: date min/max/p5/p40/p50/p60/p95 and keyword ranks 0 / len//10 / last of the top "
                          "1000, all inside a nested aggregation")
    return vals


# ---------------------------------------------------------------- generation
class _Ops:
    def __init__(self, profile):
        self.profile, self.ops = profile, []

    def add(self, name, families, body, typ="search", **extra):
        o = {"name": "gen:" + name, "source": f"generated:{self.profile['corpus']}", "type": typ, "body": body,
             "params": {}, **extra}
        o["families"] = sorted(set(families) | set(queries.classify(o, self.profile)) | set(classify_extra(o)))
        self.ops.append(o)


def _generate_untimed(profile, vals):
    """queries.generate's keyword, text, bool, numeric, sort, agg and scroll families for a profile without dates."""
    g = _Ops(profile)
    kw = vals["keyword"]
    kws = [f for f in profile.get("keyword_fields", []) if f in kw]
    if not kws:
        return g.ops
    hi_card = max(kws, key=lambda f: kw[f]["cardinality"])
    lo_card = min((f for f in kws if kw[f]["cardinality"] >= 2), key=lambda f: kw[f]["cardinality"])
    nums = [f for f in profile.get("numeric_fields", []) if f in vals["numeric"]]
    texts = [f for f in profile.get("text_fields", []) if f in vals["text"]]
    for f in kws[:2]:
        for level in ("high", "mid", "low"):
            g.add(f"term_{f}_{level}", {"keyword:term"}, {"query": {"term": {f: kw[f][level]}}})
        g.add(f"terms5_{f}", {"keyword:terms"}, {"query": {"terms": {f: kw[f]["five"]}}})
    for f in texts:
        t = vals["text"][f]
        g.add(f"match_{f}_high", {"text:match"}, {"query": {"match": {f: t["terms"][0]}}})
        g.add(f"match_{f}_mid", {"text:match"}, {"query": {"match": {f: t["mid"]}}})
        g.add(f"match_or3_{f}", {"text:match"}, {"query": {"match": {f: " ".join(t["terms"])}}})
        g.add(f"match_and2_{f}", {"text:match"}, {"query": {"match": {f: {"query": " ".join(t["terms"][:2]), "operator": "and"}}}})
        g.add(f"phrase_{f}", {"text:phrase"}, {"query": {"match_phrase": {f: t["phrase"]}}})
        g.add(f"query_string_{f}", {"text:query_string"},
              {"query": {"query_string": {"query": f"{f}:({t['terms'][0]} AND {t['mid']}) OR {f}:{t['terms'][1]}"}}})
    if texts:
        mm_fields = texts if len(texts) > 1 else texts + [hi_card]
        g.add("multi_match", {"text:multi_match"},
              {"query": {"multi_match": {"query": " ".join(vals["text"][texts[0]]["terms"][:2]), "fields": mm_fields}}})
    k0 = kws[0]
    text_clause = {"match": {texts[0]: vals["text"][texts[0]]["terms"][0]}} if texts else {"term": {k0: kw[k0]["high"]}}
    # the filter range is the first numeric field's p5-p95 (the timed profiles use a 1d / 7d date window)
    wide = None
    if nums:
        p = vals["numeric"][nums[0]]
        wide = {"range": {nums[0]: {"gte": p["5.0"], "lte": p["95.0"]}}}
    g.add("bool_must_filter", {"bool:must", "bool:filter"},
          {"query": {"bool": {"must": [text_clause], "filter": [wide or {"term": {k0: kw[k0]["mid"]}}]}}})
    g.add("bool_should", {"bool:should", "keyword:term"},
          {"query": {"bool": {"should": [{"term": {k0: kw[k0]["mid"]}}, {"term": {hi_card: kw[hi_card]["low"]}}],
                              "minimum_should_match": 1}}})
    g.add("bool_filter_must_not", {"bool:filter", "bool:must_not", "keyword:term"},
          {"query": {"bool": {"filter": [{"term": {k0: kw[k0]["high"]}}], "must_not": [{"term": {hi_card: kw[hi_card]["high"]}}]}}})
    g.add("bool_all", {"bool:must", "bool:should", "bool:filter", "bool:must_not"},
          {"query": {"bool": {"must": [text_clause], "should": [{"term": {k0: kw[k0]["mid"]}}],
                              "filter": [wide or {"term": {k0: kw[k0]["high"]}}],
                              "must_not": [{"term": {hi_card: kw[hi_card]["mid"]}}]}}})
    for f in nums:
        p = vals["numeric"][f]
        g.add(f"range_{f}_narrow", {"bkd:range_numeric"}, {"query": {"range": {f: {"gte": p["40.0"], "lte": p["60.0"]}}}})
        g.add(f"range_{f}_narrow_count", {"bkd:range_numeric"},
              {"size": 0, "track_total_hits": True, "query": {"range": {f: {"gte": p["40.0"], "lte": p["60.0"]}}}})
        g.add(f"range_{f}_wide", {"bkd:range_numeric"}, {"query": {"range": {f: {"gte": p["5.0"], "lte": p["95.0"]}}}})
        g.add(f"range_{f}_sort_desc", {"bkd:range_numeric", "sort:numeric_desc"},
              {"query": {"range": {f: {"gte": p["5.0"], "lte": p["95.0"]}}}, "sort": [{f: "desc"}]})
        for d in ("asc", "desc"):
            g.add(f"sort_{f}_{d}", {f"sort:numeric_{d}"}, {"query": {"match_all": {}}, "sort": [{f: d}]})
    for d in ("asc", "desc"):
        g.add(f"sort_{hi_card}_{d}", {"sort:keyword"}, {"query": {"match_all": {}}, "sort": [{hi_card: d}]})
    if nums:
        g.add(f"search_after_{nums[0]}_desc", {"sort:search_after", "sort:numeric_desc"},
              {"size": 100, "query": {"match_all": {}}, "sort": [{nums[0]: "desc"}, {hi_card: "asc"}]}, typ="search_after",
              pages=3)
    else:
        g.add(f"search_after_{hi_card}_asc", {"sort:search_after", "sort:keyword"},
              {"size": 100, "query": {"match_all": {}}, "sort": [{hi_card: "asc"}]}, typ="search_after", pages=3)
    g.add(f"agg_terms_{hi_card}", {"agg:terms"}, {"size": 0, "aggs": {"a": {"terms": {"field": hi_card, "size": 10}}}})
    g.add(f"agg_terms_{lo_card}", {"agg:terms"}, {"size": 0, "aggs": {"a": {"terms": {"field": lo_card, "size": 10}}}})
    sources = [{"k": {"terms": {"field": lo_card}}}]
    if nums:
        p = vals["numeric"][nums[0]]
        # fixed rule: NUMERIC_COMPOSITE_BUCKETS buckets over p5-p95 (at least 1)
        interval = max(1.0, (p["95.0"] - p["5.0"]) / NUMERIC_COMPOSITE_BUCKETS)
        sources.append({"h": {"histogram": {"field": nums[0], "interval": interval}}})
    g.add("agg_composite", {"agg:composite"}, {"size": 0, "aggs": {"a": {"composite": {"size": 1000, "sources": sources}}}})
    if nums:
        p = vals["numeric"][nums[0]]
        g.add(f"agg_range_{nums[0]}", {"agg:range"},
              {"size": 0, "aggs": {"a": {"range": {"field": nums[0], "ranges": [
                  {"to": p["5.0"]}, {"from": p["5.0"], "to": p["40.0"]}, {"from": p["40.0"], "to": p["60.0"]},
                  {"from": p["60.0"], "to": p["95.0"]}, {"from": p["95.0"]}]}}}})
        g.add(f"agg_stats_{nums[0]}_wide", {"agg:stats", "bkd:range_numeric"},
              {"size": 0, "query": wide, "aggs": {"a": {"stats": {"field": nums[0]}}}})
    g.add(f"agg_cardinality_{hi_card}", {"agg:cardinality"}, {"size": 0, "aggs": {"a": {"cardinality": {"field": hi_card}}}})
    fg = next((f for f in kws if f != lo_card), hi_card)
    g.add(f"agg_significant_terms_{lo_card}", {"agg:significant_terms", "keyword:term"},
          {"size": 0, "query": {"term": {fg: kw[fg]["mid"]}}, "aggs": {"a": {"significant_terms": {"field": lo_card}}}})
    g.add("scroll_match_all_doc", {"scroll"}, {"query": {"match_all": {}}, "sort": ["_doc"]}, typ="scroll", pages=10,
          page_size=1000)
    if wide:
        g.add(f"scroll_range_{nums[0]}_wide_doc", {"scroll", "bkd:range_numeric"}, {"query": wide, "sort": ["_doc"]},
              typ="scroll", pages=10, page_size=1000)
    return g.ops


def _haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_KM * math.asin(min(1.0, math.sqrt(a)))


def _clamp(v, lo, hi):
    return max(lo, min(hi, v))


def geo_box(v, frac):
    """Box centred on the centre value covering `frac` of the extent area: (top, left, bottom, right)."""
    hw = math.sqrt(frac) * (v["right"] - v["left"]) / 2
    hh = math.sqrt(frac) * (v["top"] - v["bottom"]) / 2
    c = v["centroid"]
    return (_clamp(c["lat"] + hh, -90, 90), _clamp(c["lon"] - hw, -180, 180),
            _clamp(c["lat"] - hh, -90, 90), _clamp(c["lon"] + hw, -180, 180))


def octagon(box):
    """Closed ring of an octagon inscribed in the box, counter-clockwise, as [lon, lat] pairs."""
    top, left, bottom, right = box
    clat, clon = (top + bottom) / 2, (left + right) / 2
    hw, hh = (right - left) / 2, (top - bottom) / 2
    ring = []
    for k in range(8):
        ang = math.radians(22.5 + 45 * k)
        ring.append([round(clon + hw * math.cos(ang), 7), round(clat + hh * math.sin(ang), 7)])
    return ring + [ring[0]]


def _envelope(box):
    top, left, bottom, right = box
    return {"type": "envelope", "coordinates": [[left, top], [right, bottom]]}


def _bbox_filter(f, box):
    top, left, bottom, right = box
    return {"geo_bounding_box": {f: {"top_left": {"lat": top, "lon": left}, "bottom_right": {"lat": bottom, "lon": right}}}}


def _generate_geo(g, profile, vals):
    for gf in profile.get("geo_fields", []):
        f, typ = gf["field"], gf["type"]
        v = (vals.get("geo") or {}).get(f)
        if not v:
            continue
        c = v["centroid"]
        diag = _haversine_km(v["top"], v["left"], v["bottom"], v["right"])
        for tag, frac in AREA_FRACTIONS:
            box = geo_box(v, frac)
            g.add(f"geo_bbox_{f}_{tag}", {"geo:bbox"}, {"query": _bbox_filter(f, box)})
            g.add(f"geo_shape_envelope_intersects_{f}_{tag}", {"geo:shape"},
                  {"query": {"geo_shape": {f: {"shape": _envelope(box), "relation": "intersects"}}}})
            g.add(f"geo_distance_{f}_{tag}", {"geo:distance"},
                  {"query": {"geo_distance": {"distance": f"{max(0.001, frac * diag):.3f}km", f: {"lat": c["lat"], "lon": c["lon"]}}}})
        box1 = geo_box(v, 0.01)
        for rel in ("within", "disjoint"):
            g.add(f"geo_shape_envelope_{rel}_{f}_1pct", {"geo:shape"},
                  {"query": {"geo_shape": {f: {"shape": _envelope(box1), "relation": rel}}}})
        ring = octagon(box1)
        g.add(f"geo_shape_polygon_intersects_{f}_1pct", {"geo:shape", "geo:polygon"},
              {"query": {"geo_shape": {f: {"shape": {"type": "polygon", "coordinates": [ring]}, "relation": "intersects"}}}})
        if typ == "geo_point":
            g.add(f"geo_polygon_{f}_1pct", {"geo:polygon"},
                  {"query": {"geo_polygon": {f: {"points": [{"lat": lat, "lon": lon} for lon, lat in ring]}}}})
        box10 = geo_box(v, 0.1)
        for prec in (3, 5):
            g.add(f"geo_agg_geohash_grid_{f}_p{prec}", {"geo:agg"},
                  {"size": 0, "query": _bbox_filter(f, box10), "aggs": {"a": {"geohash_grid": {"field": f, "precision": prec}}}})
        for zoom in (4, 8):
            g.add(f"geo_agg_geotile_grid_{f}_z{zoom}", {"geo:agg"},
                  {"size": 0, "query": _bbox_filter(f, box10), "aggs": {"a": {"geotile_grid": {"field": f, "precision": zoom}}}})
        g.add(f"geo_agg_bounds_{f}", {"geo:agg"}, {"size": 0, "aggs": {"a": {"geo_bounds": {"field": f, "wrap_longitude": False}}}})
        if typ == "geo_point":
            g.add(f"geo_agg_centroid_{f}", {"geo:agg"}, {"size": 0, "aggs": {"a": {"geo_centroid": {"field": f}}}})
            g.add(f"geo_sort_distance_{f}_1pct", {"geo:sort"},
                  {"query": _bbox_filter(f, box1), "sort": [{"_geo_distance": {f: {"lat": c["lat"], "lon": c["lon"]},
                                                                                 "order": "asc", "unit": "km"}}]})


def _generate_nested(g, profile, vals):
    for n in profile.get("nested", []):
        p = n["path"]
        nv = (vals.get("nested") or {}).get(p)
        if not nv:
            continue
        for d, dv in nv["date"].items():
            q = dv["p"]
            for tag, lo, hi in (("p40_p60", "40.0", "60.0"), ("p5_p95", "5.0", "95.0")):
                rng = {"gte": int(q[lo]), "lte": int(q[hi]), "format": "epoch_millis"}
                g.add(f"nested_range_{d}_{tag}", {"nested:query", "bkd:range_date"},
                      {"query": {"nested": {"path": p, "query": {"range": {d: rng}}}}})
            rng = {"gte": int(q["40.0"]), "lte": int(q["60.0"]), "format": "epoch_millis"}
            g.add(f"nested_inner_hits_{d}_p40_p60", {"nested:query", "nested:inner_hits"},
                  {"query": {"nested": {"path": p, "query": {"range": {d: rng}}, "inner_hits": {"size": 3}}}})
            for o in ("asc", "desc"):
                g.add(f"nested_sort_{d}_max_{o}", {"nested:sort"},
                      {"query": {"match_all": {}}, "sort": [{d: {"order": o, "mode": "max", "nested": {"path": p}}}]})
            span_s = max(1, (dv["max_ms"] - dv["min_ms"]) / 1000)
            interval = next((nm for nm, s in queries.INTERVALS if span_s / s <= queries.MAX_BUCKETS), queries.INTERVALS[-1][0])
            g.add(f"nested_agg_date_histogram_{d}", {"nested:agg", "agg:date_histogram"},
                  {"size": 0, "aggs": {"n": {"nested": {"path": p}, "aggs": {"h": {"date_histogram": {
                      "field": d, "fixed_interval": interval}}}}}})
        for k, kv in nv["keyword"].items():
            for level in ("high", "mid", "low"):
                g.add(f"nested_term_{k}_{level}", {"nested:query", "keyword:term"},
                      {"query": {"nested": {"path": p, "query": {"term": {k: kv[level]}}}}})
            g.add(f"nested_agg_terms_{k}", {"nested:agg", "agg:terms"},
                  {"size": 0, "aggs": {"n": {"nested": {"path": p}, "aggs": {"t": {"terms": {"field": k, "size": 10}}}}}})


def generate(profile, vals):
    """Generated ops of a generic profile (names gen:..., same schema as queries.generate)."""
    g = _Ops(profile)
    if profile.get("time_field") and profile.get("keyword_fields") and vals.get("keyword"):
        g.ops += queries.generate(profile, vals)
        if not [f for f in profile.get("numeric_fields", []) if f in vals.get("numeric", {})]:
            tf, t0, t1 = profile["time_field"], vals["time"]["min_ms"], vals["time"]["max_ms"]
            step = (t1 - t0) / 5
            edges = [int(t0 + step * i) for i in range(1, 5)]
            g.add(f"agg_date_range_{tf}", {"agg:range"},
                  {"size": 0, "aggs": {"a": {"date_range": {"field": tf, "format": "epoch_millis", "ranges": [
                      {"to": str(edges[0])}, {"from": str(edges[0]), "to": str(edges[1])},
                      {"from": str(edges[1]), "to": str(edges[2])}, {"from": str(edges[2]), "to": str(edges[3])},
                      {"from": str(edges[3])}]}}}})
    elif profile.get("keyword_fields") and vals.get("keyword"):
        g.ops += _generate_untimed(profile, vals)
    _generate_geo(g, profile, vals)
    _generate_nested(g, profile, vals)
    if not any("scroll" in o["families"] for o in g.ops):
        g.add("scroll_match_all_doc", {"scroll"}, {"query": {"match_all": {}}, "sort": ["_doc"]}, typ="scroll", pages=10,
              page_size=1000)
    for o in g.ops:
        # every generated op of a generic profile also carries the extra tags (queries.generate does not know them)
        o["families"] = sorted(set(o["families"]) | set(classify_extra(o)))
    return g.ops


def describe(have, missing, na):
    return {"have": sorted(have), "missing": missing, "not_applicable": na}


if __name__ == "__main__":
    raise SystemExit("families_ext.py is used by queries.py build (profiles with \"osb_import\": \"ext\")")
