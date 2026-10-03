#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Stage 4: per-leaf attribution of the @timestamp points reads of bench_aggs.py sort queries.

Joins three files:
  --layout  validate/BkdLayout.java JSON (every @timestamp leaf: file pointer, doc ID bytes, value bytes)
  --lucene  validate/SortTrace.java JSON lines (per query: getPointTree calls with callers, every leaf each
            intersection read, as doc IDs only (inside) or doc IDs + values (crosses), the leaves of the hits)
  --trace   validate/dv_trace.py JSON of the same queries on the node, cold (the blocks actually loaded)
and prints per query: intersections by caller, the comparator's estimates, leaves read (distinct; inside / crosses),
leaves holding a returned hit, the .kdd blocks those leaf reads cover (all bytes of a crossing leaf, the doc ID bytes
of an inside leaf) against the .kdd blocks the node loaded, and the blocks a split doc IDs / values layout would read
(inside leaves: doc ID bytes only, packed contiguously; crossing leaves: their doc IDs and their values).

Usage: validate/sort_attr.py --layout BKD.json --lucene SORTTRACE.jsonl --trace TRACE.json [--out OUT.json]
"""

import argparse
import collections
import json

BLOCK = 131072


def blocks_of(start, end):
    return set(range(start // BLOCK, (end - 1) // BLOCK + 1)) if end > start else set()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--layout", required=True)
    parser.add_argument("--lucene", required=True)
    parser.add_argument("--trace", required=True)
    parser.add_argument("--field", default="@timestamp")
    parser.add_argument("--out")
    args = parser.parse_args()
    layout = json.load(open(args.layout))
    field = next(f for f in layout["fields"] if f["field"] == args.field)
    leaves = field["leaves"]  # fp, count, doc_bytes, value_bytes, ...
    lo_block, hi_block = field["leaves_start"] // BLOCK, (field["leaves_end"] - 1) // BLOCK
    # a split layout: doc IDs of all leaves packed in one stream, values in another (same leaf order)
    doc_off, val_off = [0], [0]
    for l in leaves:
        doc_off.append(doc_off[-1] + l[2])
        val_off.append(val_off[-1] + l[3])
    traces = {r["query"]: r for r in json.load(open(args.trace))["results"]}
    out = []
    print("query | intersections (comparator / range) | comparator estimates | leaves read (inside / crosses) | "
          "hit leaves | .kdd blocks: by Lucene leaf reads, loaded on node, both | split layout: doc-ID blocks + value blocks")
    for line in open(args.lucene):
        r = json.loads(line)
        q = r["query"]
        calls = r["calls"]
        inter = collections.Counter()
        for c in calls:
            if c.startswith("intersect<"):
                inter["comparator" if "NumericComparator" in c else "range" if "PointRangeQuery" in c else c] += 1
        est = sum(n for c, n in zip(calls, r["estimates"]) if c.startswith("NumericComparator"))
        by_kind = {0: set(), 1: set()}
        reads = collections.Counter()
        per_caller_leaves = collections.defaultdict(set)
        for call, leaf, kind in r["visits"]:
            by_kind[kind].add(leaf)
            reads[kind] += 1
            who = "comparator" if "NumericComparator" in calls[call] else "range"
            per_caller_leaves[who].add(leaf)
        inside_only = by_kind[0] - by_kind[1]
        crosses = by_kind[1]
        hit_leaves = {h[2] for h in r["hits"]}
        lucene_blocks = set()
        for leaf in by_kind[0]:
            fp, _, db, vb = leaves[leaf][:4]
            lucene_blocks |= blocks_of(fp, fp + db)
        for leaf in crosses:
            fp, _, db, vb = leaves[leaf][:4]
            lucene_blocks |= blocks_of(fp, fp + db + vb)
        node_blocks = set()
        t = traces.get(q)
        if t:
            # entries are [file, block, prefetch] or, since dv_trace adds the region, [file, block, prefetch, region]
            node_blocks = {x[1] for x in t["aggregation"]["blocks"] if x[0].endswith(".kdd") and lo_block <= x[1] <= hi_block}
        split_doc = set()
        split_val = set()
        for leaf in by_kind[0] | crosses:
            split_doc |= blocks_of(doc_off[leaf], doc_off[leaf + 1])
        for leaf in crosses:
            split_val |= blocks_of(val_off[leaf], val_off[leaf + 1])
        row = {
            "query": q, "collected": r["collected"], "intersections": dict(inter), "comparator_estimates": est,
            "leaf_reads": {"inside": reads[0], "crosses": reads[1]},
            "distinct_leaves": {"inside_only": len(inside_only), "crosses": len(crosses)},
            "leaves_by_caller": {k: len(v) for k, v in per_caller_leaves.items()},
            "hit_leaves": len(hit_leaves), "hit_leaves_read": len(hit_leaves & (by_kind[0] | crosses)),
            "kdd_blocks_lucene": len(lucene_blocks), "kdd_blocks_node": len(node_blocks) if t else None,
            "kdd_blocks_both": len(lucene_blocks & node_blocks) if t else None,
            "split_doc_blocks": len(split_doc), "split_value_blocks": len(split_val),
            "doc_bytes_read": sum(leaves[x][2] for x in by_kind[0] | crosses), "value_bytes_read": sum(leaves[x][3] for x in crosses),
        }
        out.append(row)
        print(f"{q:<26} | {inter.get('comparator', 0)} / {inter.get('range', 0)} | {est} | "
              f"{len(inside_only)} / {len(crosses)} (reads {reads[0]} / {reads[1]}) | {len(hit_leaves)} | "
              f"{len(lucene_blocks)}, {row['kdd_blocks_node']}, {row['kdd_blocks_both']} | {len(split_doc)} + {len(split_val)}"
              f" | collected {r['collected']:,}")
    if args.out:
        json.dump(out, open(args.out, "w"), indent=1)


if __name__ == "__main__":
    main()
