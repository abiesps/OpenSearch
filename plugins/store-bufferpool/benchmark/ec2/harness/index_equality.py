#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Per-operation result equality of two indices that hold the same documents but were ingested separately (the
stock-format index and the split-BKD "B" index): auto-generated _id values and the doc order inside a segment
differ, so the cross-arm check of analyze.py (ids within tie groups) cannot be used as is. Every op of an op set runs
once on each index (on the same node or on two nodes) and is compared with analyze.results_equal on canonical
results whose hit ids are replaced by a document key:
  --key-field F   the value of a keyword field that identifies a document (nested: qid), read through
                  docvalue_fields (added to the request; it does not change hits, scores, sort values or aggs);
  default         sha256 of the hit's _source (when _source is returned).
Nested inner hits are keyed by (parent key, _nested path and offset). Totals, aggregations, scores and sort values
must be identical; tie groups (equal score and sort values) must hold the same keys, except the last group, which
must have the same size (the cut inside a tie depends on doc order). scroll ops compare the hit count and the
order-free digest of keys.

  index_equality.py run --ops ops/nested.json --url http://DATA:9200 --index sonested --key-field qid --out a.json
  (restart the node with the other arm)
  index_equality.py run --ops ops/nested.json --url http://DATA:9200 --index sonested_split --key-field qid --out b.json
  index_equality.py compare --ops ops/nested.json a.json b.json --out split-equality.json
"""
import argparse
import copy
import hashlib
import json
import os
import sys

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
import analyze  # noqa: E402
import coldbench  # noqa: E402
from common import JsonClient  # noqa: E402


def _key(hit, key_field):
    if key_field:
        v = (hit.get("fields") or {}).get(key_field)
        if v:
            return str(v[0])
    src = hit.get("_source")
    if src is not None:
        return "src:" + hashlib.sha256(json.dumps(src, sort_keys=True).encode()).hexdigest()[:16]
    return None


def rekey(pages, key_field):
    """Replaces every hit _id (and inner hit _id) by the document key; returns the number of hits without a key."""
    missing = 0
    for p in pages:
        for h in p.get("hits", {}).get("hits", []):
            k = _key(h, key_field)
            if k is None:
                missing += 1
                k = "nokey:" + str(h.get("_id"))
            h["_id"] = k
            for name, ih in (h.get("inner_hits") or {}).items():
                for c in ih.get("hits", {}).get("hits", []):
                    c["_id"] = f"{k}/{json.dumps(c.get('_nested'), sort_keys=True)}"
    return missing


def with_key_field(body, key_field):
    if not key_field:
        return body
    b = copy.deepcopy(body)
    dv = b.get("docvalue_fields") or []
    if key_field not in dv:
        b["docvalue_fields"] = dv + [key_field]
    return b


class _Rekeying:
    """Client wrapper: every search response is re-keyed before coldbench.execute builds the canonical result."""

    def __init__(self, client, key_field):
        self.c, self.key_field, self.missing = client, key_field, 0
        self.last_wall_s = 0.0

    def request(self, method, path, body=None, **kw):
        r = self.c.request(method, path, body, **kw)
        self.last_wall_s = self.c.last_wall_s
        if isinstance(r, dict) and "hits" in r:
            self.missing += rekey([r], self.key_field)
        return r

    def raw(self, *a, **kw):
        return self.c.raw(*a, **kw)


def oracle_op(op):
    """
    Oracle form of an op (verification only, never measured): search_type=dfs_query_then_fetch makes scores use
    index-wide term statistics, so BM25 scores do not depend on which shard a document was routed to (the two indices
    route auto-generated ids differently); profile=true runs every query through the profiler's plain scorer path,
    which g-nested-percolator found to return the true top hits of nested max sorts where the default path of stock
    OpenSearch 3.10 / Lucene 10.5.1 does not (phaseA.md, finding "nested sort"); terms-like aggregations get a large
    shard_size (_exact_terms) so their buckets do not depend on the shard distribution either.
    """
    o = dict(op)
    o["params"] = {**op.get("params", {}), "search_type": "dfs_query_then_fetch"}
    o["body"] = {**_exact_terms(o["body"]), "profile": True}
    return o


ORACLE_SHARD_SIZE = 100000


def _exact_terms(v):
    """terms / significant_terms: shard_size raised to ORACLE_SHARD_SIZE (unless larger), so the top buckets and their
    counts do not depend on which shard holds which documents (doc_count_error_upper_bound shows what remains)."""
    if isinstance(v, list):
        return [_exact_terms(x) for x in v]
    if not isinstance(v, dict):
        return v
    out = {}
    for k, x in v.items():
        if k == "composite":
            out[k] = x  # composite "terms" sources are exact (paged) and take no shard_size
            continue
        if k in ("terms", "significant_terms") and isinstance(x, dict) and "field" in x:
            x = {**x, "shard_size": max(int(x.get("shard_size", 0)), ORACLE_SHARD_SIZE)}
        out[k] = _exact_terms(x)
    return out


def run(ops, url, index, key_field, oracle=False):
    client = _Rekeying(JsonClient(url, timeout=3600.0), key_field)
    out = {}
    for op in ops:
        o = oracle_op(op) if oracle else dict(op)
        o["body"] = with_key_field(o["body"], key_field)
        try:
            r = coldbench.execute(client, index, o)
            out[op["name"]] = {"canonical": r["canonical"], "took_ms": r["took_ms"]}
        except Exception as e:  # noqa: BLE001 - recorded per op, never skipped silently
            out[op["name"]] = {"error": f"{type(e).__name__}: {e}"[:1000]}
    return out, client.missing


def _drop_keys(v, keys):
    if isinstance(v, dict):
        return {k: _drop_keys(x, keys) for k, x in v.items() if k not in keys}
    if isinstance(v, list):
        return [_drop_keys(x, keys) for x in v]
    return v


def compare(ops, ra, rb, ignore_agg_keys=()):
    """ignore_agg_keys: aggregation keys left out of the comparison (recorded), e.g. doc_count_error_upper_bound, an
    error bound of terms aggregations that depends on how documents are distributed over shards."""
    rows, n_eq = [], 0
    for op in ops:
        x, y = ra.get(op["name"], {"error": "not run"}), rb.get(op["name"], {"error": "not run"})
        if "error" in x or "error" in y:
            ok, why = False, f"error: a={x.get('error')} b={y.get('error')}"
        else:
            cx, cy = x["canonical"], y["canonical"]
            if ignore_agg_keys:
                cx = {**cx, "aggs": _drop_keys(cx.get("aggs"), set(ignore_agg_keys))}
                cy = {**cy, "aggs": _drop_keys(cy.get("aggs"), set(ignore_agg_keys))}
                # the digests cover the dropped keys: recompute both, never compare stale or missing digests
                for c in (cx, cy):
                    c["digest"] = hashlib.sha256(json.dumps({k: v for k, v in c.items() if k != "digest"},
                                                            sort_keys=True).encode()).hexdigest()[:16]
            ok, why = analyze.results_equal(cx, cy, True)
        n_eq += ok
        rows.append({"op": op["name"], "equal": ok, "reason": why,
                     "digest_a": x.get("canonical", {}).get("digest"), "digest_b": y.get("canonical", {}).get("digest"),
                     "total_a": x.get("canonical", {}).get("total"), "total_b": y.get("canonical", {}).get("total"),
                     "aggs_items": x.get("canonical", {}).get("aggs_size")})
    return rows, n_eq


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="one side: every op on one index of the running node")
    r.add_argument("--ops", required=True)
    r.add_argument("--url", required=True)
    r.add_argument("--index", required=True)
    r.add_argument("--key-field")
    r.add_argument("--oracle", action="store_true", help="verification form: dfs_query_then_fetch + profile (oracle_op)")
    r.add_argument("--out", required=True)
    c = sub.add_parser("compare", help="two sides written by run (the two indices may live on different nodes)")
    c.add_argument("--ops", required=True)
    c.add_argument("a")
    c.add_argument("b")
    c.add_argument("--ignore-agg-key", action="append", default=[], help="aggregation key left out (recorded)")
    c.add_argument("--out", required=True)
    a = ap.parse_args()
    ops = json.load(open(a.ops))["ops"]
    if a.cmd == "run":
        res, miss = run(ops, a.url, a.index, a.key_field, a.oracle)
        side = {"url": a.url, "index": a.index, "key_field": a.key_field, "oracle": a.oracle, "hits_without_key": miss,
                "node": JsonClient(a.url).request("GET", "/"), "results": res}
        os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
        json.dump(side, open(a.out, "w"), indent=1)
        print(json.dumps({"ops": len(ops), "errors": sum(1 for v in res.values() if "error" in v), "hits_without_key": miss}))
        return
    sa, sb = json.load(open(a.a)), json.load(open(a.b))
    if sa["key_field"] != sb["key_field"] or sa.get("oracle") != sb.get("oracle"):
        raise SystemExit("the two sides use different key fields or request forms")
    rows, n_eq = compare(ops, sa["results"], sb["results"], a.ignore_agg_key)
    res = {"ops": len(ops), "equal": n_eq, "different": len(ops) - n_eq, "key_field": sa["key_field"],
           "oracle": sa.get("oracle", False), "ignored_agg_keys": a.ignore_agg_key,
           "hits_without_key": {"a": sa["hits_without_key"], "b": sb["hits_without_key"]},
           "a": {k: sa[k] for k in ("url", "index", "node")}, "b": {k: sb[k] for k in ("url", "index", "node")},
           "rows": rows}
    json.dump(res, open(a.out, "w"), indent=1)
    print(json.dumps({k: res[k] for k in ("ops", "equal", "different", "hits_without_key")}))
    for row in rows:
        if not row["equal"]:
            print("DIFF", row["op"], row["reason"])
    sys.exit(0 if n_eq == len(ops) else 1)


if __name__ == "__main__":
    main()
