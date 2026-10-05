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

  index_equality.py --ops ops/nested.json --url-a http://DATA:9200 --index-a sonested \
      --url-b http://DATA:9200 --index-b sonested_split --key-field qid --out results/nested/split-equality.json
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


def run(ops, url, index, key_field):
    client = _Rekeying(JsonClient(url, timeout=3600.0), key_field)
    out = {}
    for op in ops:
        o = dict(op)
        o["body"] = with_key_field(op["body"], key_field)
        try:
            r = coldbench.execute(client, index, o)
            out[op["name"]] = {"canonical": r["canonical"], "took_ms": r["took_ms"]}
        except Exception as e:  # noqa: BLE001 - recorded per op, never skipped silently
            out[op["name"]] = {"error": f"{type(e).__name__}: {e}"[:1000]}
    return out, client.missing


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ops", required=True)
    ap.add_argument("--url-a", required=True)
    ap.add_argument("--index-a", required=True)
    ap.add_argument("--url-b")
    ap.add_argument("--index-b", required=True)
    ap.add_argument("--key-field")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    ops = json.load(open(a.ops))["ops"]
    ra, ma = run(ops, a.url_a, a.index_a, a.key_field)
    rb, mb = run(ops, a.url_b or a.url_a, a.index_b, a.key_field)
    rows, n_eq = [], 0
    for op in ops:
        x, y = ra[op["name"]], rb[op["name"]]
        if "error" in x or "error" in y:
            ok, why = False, f"error: a={x.get('error')} b={y.get('error')}"
        else:
            ok, why = analyze.results_equal(x["canonical"], y["canonical"], True)
        n_eq += ok
        rows.append({"op": op["name"], "equal": ok, "reason": why,
                     "digest_a": x.get("canonical", {}).get("digest"), "digest_b": y.get("canonical", {}).get("digest"),
                     "total_a": x.get("canonical", {}).get("total"), "total_b": y.get("canonical", {}).get("total"),
                     "aggs_items": x.get("canonical", {}).get("aggs_size")})
    res = {"ops": len(ops), "equal": n_eq, "different": len(ops) - n_eq, "key_field": a.key_field,
           "hits_without_key": {"a": ma, "b": mb}, "a": {"url": a.url_a, "index": a.index_a},
           "b": {"url": a.url_b or a.url_a, "index": a.index_b}, "rows": rows, "results_a": ra, "results_b": rb}
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    json.dump(res, open(a.out, "w"), indent=1)
    print(json.dumps({k: res[k] for k in ("ops", "equal", "different", "hits_without_key")}))
    for r in rows:
        if not r["equal"]:
            print("DIFF", r["op"], r["reason"])
    sys.exit(0 if n_eq == len(ops) else 1)


if __name__ == "__main__":
    main()
