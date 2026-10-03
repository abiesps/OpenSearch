#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
C3a: attributes the cold block loads of bench_aggs.py queries to files, doc-values fields and regions, and code.

For each query, cold (cache cleared), with the bufferpool trace on:
  - the aggregation query as bench_aggs.py sends it,
  - the same query without aggregations (track_total_hits true, so every match is collected): what the query alone
    loads; the difference is what the aggregation loads.
Each load is mapped to a region of validate/DocValuesLayout.java's output (field + values / value-jump-table /
ords-values / skipper / ...) by the byte range of its block; other files are reported by extension. Also reported:
the codec and search frames that asked for the load, whether a field's loads come in increasing offset order (one
stream), and how many loads were waited on.

Sort queries (bench_aggs.py sort_*; --queries sort for all of them) have no aggregation, so only the request itself is
traced. With --bkd-layout (validate/BkdLayout.java output) .kdi/.kdd loads are attributed to points fields too, and
every load is kept in trace order with its callers ("load_list") for per-leaf attribution (validate/sort_attr.py).

Usage: validate/dv_trace.py --layout LAYOUT.json [--bkd-layout BKD.json] [--queries dh:s50:7d,...|sort] [--mode MODE]
       [--latency-ms 4]
"""

import argparse
import collections
import json
import os
import sys

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(here))
import bench_aggs as ba  # noqa: E402
import bench_postings as bp  # noqa: E402


def load_regions(path, *more):
    regions = json.load(open(path))["regions"]
    for m in more:
        if m:
            regions = regions + json.load(open(m))["regions"]
    # most specific (smallest) region first, so a jump table inside its values region wins
    return sorted(regions, key=lambda r: r["end"] - r["start"])


def attribute(event, regions, block_size):
    file = event["file"]
    ext = file.rsplit(".", 1)[-1]
    start, end = event["offset"], event["offset"] + event["size"]
    hits = [r for r in regions if r["file"] == file and r["start"] < end and r["end"] > start]
    if not hits:
        return ext, ext, "-"
    # the block may overlap several regions (a region ends inside it); name the most specific, mark it shared
    r = hits[0]
    shared = len({(h["field"], h["region"]) for h in hits}) > 1
    return ext, f"{r['field']}:{r['region']}", "shared" if shared else "-"


def traced_search(client, index, body, mode):
    client.request("POST", f"/_bufferpool/agg_batch?mode={mode}")
    client.request("POST", "/_bufferpool/cache/_clear")
    client.request("POST", "/_bufferpool/stats/_reset")
    client.request("POST", "/_bufferpool/trace/_start?max_events=200000")
    resp, wall = ba.search(client, index, body)
    trace = client.request("POST", "/_bufferpool/trace/_stop")
    if trace.get("dropped"):
        raise RuntimeError(f"trace dropped {trace['dropped']} events")
    return resp, wall, trace["events"]


def summarize(events, regions, block_size):
    """The trace has one event per load (size > 0) and one per wait on an in-flight load (size -1, waited_micros)."""
    by_region = collections.Counter()
    by_caller = collections.Counter()
    prefetched = collections.Counter()
    shared = 0
    offsets = collections.defaultdict(list)
    waits = 0
    waited_micros = 0
    loads = [e for e in events if e["size"] > 0]
    for e in events:
        if e["size"] <= 0:
            waits += 1
            waited_micros += e["waited_micros"]
    for e in loads:
        ext, region, flag = attribute(e, regions, block_size)
        by_region[region] += 1
        prefetched[region] += e["prefetch"]
        by_caller[(region, e["codec"], e["search"])] += 1
        shared += flag == "shared"
        offsets[region].append(e["offset"])
    waited = {"waits": waits, "waited_ms": round(waited_micros / 1000, 1)}
    order = {}
    for region, offs in offsets.items():
        ups = sum(1 for a, b in zip(offs, offs[1:]) if b > a)
        order[region] = {"loads": len(offs), "increasing_pairs": ups, "pairs": max(0, len(offs) - 1),
                         "first_block": min(offs) // block_size, "last_block": max(offs) // block_size}
    blocks = sorted({(e["file"], e["offset"] // block_size, bool(e["prefetch"])) for e in loads})
    # every load in trace order with its callers, for per-block attribution (sort queries: points blocks)
    load_list = [[e["file"], e["offset"] // block_size, e["codec"], e["search"], bool(e["prefetch"])] for e in loads]
    return {"loads": len(loads), "by_region": dict(by_region.most_common()), "shared_blocks": shared,
            "blocks": [[f, b, p] for f, b, p in blocks], "load_list": load_list,
            "prefetched": dict(prefetched), "waited": waited, "order": order,
            "by_caller": [{"region": r, "codec": c, "search": s, "loads": n} for (r, c, s), n in by_caller.most_common()]}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--layout", required=True)
    parser.add_argument("--bkd-layout", help="BkdLayout.java JSON: also attribute .kdi/.kdd loads to points fields")
    parser.add_argument("--docs", type=int, default=30_000_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--queries", default=",".join(ba.DEFAULT_QUERIES), help="comma-separated; 'sort' = bench_aggs.SORT_QUERIES")
    parser.add_argument("--mode", default="vecdec", help="agg_batch mode (off = stock)")
    parser.add_argument("--latency-ms", type=float, default=4)
    parser.add_argument("--url", default="http://localhost:9200")
    parser.add_argument("--fork", default=os.path.expanduser("~/workspace/lucene_experiments"))
    parser.add_argument("--out")
    parser.add_argument("--compare", help="dv_trace JSON of a run without prefetch: report blocks loaded here but not there")
    args = parser.parse_args()

    client = bp.Client(args.url)
    index = f"{ba.DATASET}_{args.docs}_{args.seed}_{bp.format_hash(args.fork)}"
    regions = load_regions(args.layout, args.bkd_layout)
    block_size = client.request("GET", "/_bufferpool/stats")["block_size"]
    results = []
    try:
        bp.set_latency(client, args.latency_ms)
        specs = []
        for s in args.queries.split(","):
            specs.extend(ba.SORT_QUERIES if s == "sort" else [s])
        for spec in specs:
            body = ba.query_body(spec)
            resp, wall, events = traced_search(client, index, body, args.mode)
            agg = summarize(events, regions, block_size)
            if ba.is_sort(spec):
                # a sort query has no aggregation to separate; "query alone" is the same request
                q_resp, qry = resp, agg
                hits = (resp["hits"].get("total") or {}).get("value", -1)
            else:
                query_only = {"size": 0, "track_total_hits": True, "query": body["query"]}
                q_resp, q_wall, q_events = traced_search(client, index, query_only, args.mode)
                qry = summarize(q_events, regions, block_size)
                hits = q_resp["hits"]["total"]["value"]
            results.append({"query": spec, "mode": args.mode, "took_ms": resp["took"], "wall_ms": wall,
                            "hits": hits, "query_only_took_ms": q_resp["took"],
                            "sort_values": [h.get("sort") for h in resp["hits"]["hits"]] if ba.is_sort(spec) else None,
                            "aggregation": agg, "query_only": qry})
            print(f"\n## {spec} ({args.mode}): {agg['loads']} loads, took {resp['took']} ms; query alone "
                  f"{qry['loads']} loads, took {q_resp['took']} ms, {hits:,} hits; "
                  f"shared blocks {agg['shared_blocks']}; {agg['waited']['waits']} waits, {agg['waited']['waited_ms']} ms waited")
            for region, n in agg["by_region"].items():
                o = agg["order"][region]
                print(f"  {region:<28} {n:>4} loads ({agg['prefetched'].get(region, 0)} prefetch; query alone "
                      f"{qry['by_region'].get(region, 0):>4})  blocks {o['first_block']}-{o['last_block']}, "
                      f"increasing {o['increasing_pairs']}/{o['pairs']}")
            for c in agg["by_caller"][:8]:
                print(f"    {c['loads']:>4}  {c['region']:<26} codec={c['codec']}  search={c['search']}")
    finally:
        bp.set_latency(client, 0)
        client.request("POST", "/_bufferpool/agg_batch?mode=off")
    if args.compare:
        ref = {r["query"]: r for r in json.load(open(args.compare))["results"]}
        print("\n## prefetched blocks vs blocks read without prefetch")
        for r in results:
            base = ref.get(r["query"])
            if base is None:
                continue
            read = {(f, b) for f, b, _ in base["aggregation"]["blocks"]}
            mine = {(f, b): p for f, b, p in r["aggregation"]["blocks"]}
            pre = [k for k, p in mine.items() if p]
            wasted = [k for k in pre if k not in read]
            missing = [k for k in read if k not in mine]
            r["prefetched_blocks"] = len(pre)
            r["prefetched_not_read"] = len(wasted)
            print(f"  {r['query']:<16} loaded {len(mine)} (prefetched {len(pre)}), read without prefetch {len(read)}; "
                  f"prefetched but never read {len(wasted)} {sorted(wasted)[:6]}; read there but not loaded here {len(missing)}")
    if args.out:
        with open(args.out, "w") as f:
            json.dump({"index": index, "block_size": block_size, "results": results}, f, indent=1)


if __name__ == "__main__":
    main()
