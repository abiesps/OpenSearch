#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Result-equality parts of hits that coldbench.canonical() does not cover, for the generic workloads:
  - percolate: fields._percolator_document_slot (and _percolator_document_slot_<n> of multi-document percolation)
  - highlight: the fragments per field, in order
  - nested inner_hits: per hit and per inner-hits name, total, then each inner hit's _id, _nested (field, offset and
    the nested child chain) and _score (rounded like canonical())
  - matched_queries
canonical() already hashes total hits, hit ids, scores (rounded to FLOAT_DIGITS) and sort values in order, and the
whole aggregations object (every aggregation type: top_hits, top_metrics, geo grids, geo_bounds, geo_centroid,
significant_*, auto_date_histogram interval, multi_terms keys, composite after_key, sampler, filters, nested, ...).
hit_extras() returns None when no hit carries any of the keys above, so canonical() adds nothing and the digests of
every existing op (big5, nyc_taxis, http_logs, pmc) stay what they were. Response checks: timed_out and
_shards.failed already fail the sample in coldbench.execute(); terminated_early is recorded per sample by
response_flags().
"""


def _nested_chain(n):
    out = []
    while isinstance(n, dict):
        out.append([n.get("field"), n.get("offset")])
        n = n.get("_nested")
    return out


def _inner_hits(ih, rnd):
    out = {}
    for name in sorted(ih):
        h = (ih[name] or {}).get("hits", {})
        total = h.get("total")
        out[name] = {"total": [total.get("value"), total.get("relation")] if isinstance(total, dict) else total,
                     "hits": [[x.get("_id"), _nested_chain(x.get("_nested")), rnd(x.get("_score"))] for x in h.get("hits", [])]}
    return out


def hit_extra(h, rnd):
    e = {}
    slots = {k: v for k, v in (h.get("fields") or {}).items() if k.startswith("_percolator_document_slot")}
    if slots:
        e["percolator_slots"] = {k: slots[k] for k in sorted(slots)}
    if h.get("highlight"):
        e["highlight"] = {k: list(v) for k, v in sorted(h["highlight"].items())}
    if h.get("inner_hits"):
        e["inner_hits"] = _inner_hits(h["inner_hits"], rnd)
    if h.get("matched_queries"):
        mq = h["matched_queries"]
        e["matched_queries"] = sorted(mq) if isinstance(mq, list) else {k: rnd(v) for k, v in sorted(mq.items())}
    if h.get("_nested"):
        e["nested"] = _nested_chain(h["_nested"])
    return e


def hit_extras(pages, rnd):
    """Per hit (same order as canonical()'s hits) the extra parts, or None if no hit has any."""
    extras = [hit_extra(h, rnd) for p in pages for h in p.get("hits", {}).get("hits", [])]
    return extras if any(extras) else None


def response_flags(pages):
    """terminated_early of the pages that report it (None when no page does)."""
    te = [p["terminated_early"] for p in pages if "terminated_early" in p]
    return {"terminated_early": te} if te else None
