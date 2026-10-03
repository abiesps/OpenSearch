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

Sort queries (bench_aggs.py sort_*; groups sort and sort_tt) have no aggregation, so only the request itself is
traced. With --bkd-layout (validate/BkdLayout.java output) .kdi/.kdd loads are attributed to points fields too, and
every load is kept in trace order with its callers ("load_list") for per-leaf attribution (validate/sort_attr.py).

The variant (--variant, bench_aggs.py grammar: agg_batch mode, sort_opt switches, @split, @v1) is set before every
traced request. prefetched_not_read: blocks that a prefetch loaded during the request and that no read touched before
the trace stopped (same run, from the plugin trace), also counted by the code that requested the prefetch. Stock
Lucene itself prefetches some first pages that a query may never read (for example the value jump table of a numeric
field, Lucene90DocValuesProducer$VaryingBPVReader.<init>, when the values are read block after block). With --compare
REF (a dv_trace JSON of the reference variant): prefetched_not_read_beyond_reference (per requester, the unread blocks
beyond the reference's; must be 0 for every prefetching variant), prefetched_not_in_reference (prefetched here, not
loaded by the reference) and loads_not_in_reference by region (every block loaded here, by demand or prefetch, that
the reference did not load).

Usage: validate/dv_trace.py --layout LAYOUT.json [--bkd-layout BKD.json] [--queries dh:s50:7d,...|sort|sort_tt]
       [--variant V] [--dataset logs_v1] [--latency-ms 4] [--compare REF.json] [--out OUT.json]
"""

import argparse
import collections
import json
import os
import sys
import time

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(here))
import bench_aggs as ba  # noqa: E402
import bench_postings as bp  # noqa: E402

# after the response, let prefetches still in flight finish, so they count against this request
SETTLE_SECONDS = 0.2


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


def traced_search(client, index, body, variant):
    ba.set_variant(client, variant)
    client.request("POST", "/_bufferpool/cache/_clear")
    client.request("POST", "/_bufferpool/stats/_reset")
    client.request("POST", "/_bufferpool/trace/_start?max_events=200000")
    resp, wall = ba.search(client, index, body)
    time.sleep(SETTLE_SECONDS)
    trace = client.request("POST", "/_bufferpool/trace/_stop")
    if trace.get("dropped"):
        raise RuntimeError(f"trace dropped {trace['dropped']} events")
    if "prefetched_unread" not in trace:
        raise RuntimeError("the node's trace has no prefetched_unread; rebuild the node (refresh_lucene.sh)")
    return resp, wall, trace["events"], trace["prefetched_unread"]


def summarize(events, regions, block_size, unread):
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
    block_region = {}
    for e in loads:
        ext, region, flag = attribute(e, regions, block_size)
        by_region[region] += 1
        prefetched[region] += e["prefetch"]
        by_caller[(region, e["codec"], e["search"])] += 1
        shared += flag == "shared"
        offsets[region].append(e["offset"])
        block_region[(e["file"], e["offset"] // block_size)] = region
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
            "blocks": [[f, b, p, block_region[(f, b)]] for f, b, p in blocks], "load_list": load_list,
            "prefetched": dict(prefetched), "waited": waited, "order": order,
            "prefetched_not_read": unread["count"], "prefetched_unread_blocks": unread["blocks"],
            "prefetched_not_read_by_requester": unread.get("by_requester", {}),
            "by_caller": [{"region": r, "codec": c, "search": s, "loads": n} for (r, c, s), n in by_caller.most_common()]}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--layout", required=True)
    parser.add_argument("--bkd-layout", help="BkdLayout.java JSON: also attribute .kdi/.kdd loads to points fields")
    parser.add_argument("--docs", type=int, default=30_000_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--dataset", default=ba.DEFAULT_DATASET, choices=sorted(ba.DATASETS))
    parser.add_argument("--queries", default=",".join(ba.DEFAULT_QUERIES),
                        help="comma-separated specs or groups (sort, sort_tt)")
    parser.add_argument("--variant", help="bench_aggs.py variant expression (default: --mode)")
    parser.add_argument("--mode", default="vecdec", help="agg_batch mode when --variant is not given (off = stock)")
    parser.add_argument("--latency-ms", type=float, default=4)
    parser.add_argument("--url", default="http://localhost:9200")
    parser.add_argument("--fork", default=os.path.expanduser("~/workspace/lucene_experiments"))
    parser.add_argument("--out")
    parser.add_argument("--compare", help="dv_trace JSON of the reference variant: report blocks loaded here but not there")
    args = parser.parse_args()

    name = args.variant or ("stock" if args.mode == "off" else args.mode)
    try:
        variant = ba.resolve_variant(name, args.dataset)
        specs = ba.expand_queries(args.queries)
    except ValueError as e:
        sys.exit(str(e))
    client = bp.Client(args.url)
    index = ba.index_name(variant["dataset"], args.docs, args.seed, args.fork)
    if not client.exists(index):
        sys.exit(f"index [{index}] does not exist")
    regions = load_regions(args.layout, args.bkd_layout)
    block_size = client.request("GET", "/_bufferpool/stats")["block_size"]
    results = []
    try:
        bp.set_latency(client, args.latency_ms)
        for spec in specs:
            field = ba.query_field(variant, spec)
            body = ba.query_body(spec, field)
            resp, wall, events, unread = traced_search(client, index, body, variant)
            agg = summarize(events, regions, block_size, unread)
            if ba.is_sort(spec):
                # a sort query has no aggregation to separate; "query alone" is the same request
                q_resp, qry = resp, agg
                hits = (resp["hits"].get("total") or {}).get("value", -1)
            else:
                query_only = {"size": 0, "track_total_hits": True, "query": body["query"]}
                q_resp, q_wall, q_events, q_unread = traced_search(client, index, query_only, variant)
                qry = summarize(q_events, regions, block_size, q_unread)
                hits = q_resp["hits"]["total"]["value"]
            results.append({"query": spec, "variant": name, "mode": variant["agg_mode"] or "off", "index": index,
                            "field": field, "sort_opt": variant["sort_opt"], "took_ms": resp["took"], "wall_ms": wall,
                            "hits": hits, "query_only_took_ms": q_resp["took"],
                            "sort_values": [h.get("sort") for h in resp["hits"]["hits"]] if ba.is_sort(spec) else None,
                            "prefetched_not_read": agg["prefetched_not_read"],
                            "aggregation": agg, "query_only": qry})
            print(f"\n## {spec} ({name}): {agg['loads']} loads, took {resp['took']} ms; query alone "
                  f"{qry['loads']} loads, took {q_resp['took']} ms, {hits:,} hits; "
                  f"shared blocks {agg['shared_blocks']}; {agg['waited']['waits']} waits, {agg['waited']['waited_ms']} ms waited; "
                  f"prefetched_not_read {agg['prefetched_not_read']} {agg['prefetched_unread_blocks'][:6]} "
                  f"{agg['prefetched_not_read_by_requester']}")
            for region, n in agg["by_region"].items():
                o = agg["order"][region]
                print(f"  {region:<28} {n:>4} loads ({agg['prefetched'].get(region, 0)} prefetch; query alone "
                      f"{qry['by_region'].get(region, 0):>4})  blocks {o['first_block']}-{o['last_block']}, "
                      f"increasing {o['increasing_pairs']}/{o['pairs']}")
            for c in agg["by_caller"][:8]:
                print(f"    {c['loads']:>4}  {c['region']:<26} codec={c['codec']}  search={c['search']}")
    finally:
        bp.set_latency(client, 0)
        ba.reset_variant(client)
    if args.compare:
        ref = {r["query"]: r for r in json.load(open(args.compare))["results"]}
        print(f"\n## blocks loaded here vs blocks loaded by the reference ({args.compare})")
        for r in results:
            base = ref.get(r["query"])
            if base is None:
                continue
            read = {(x[0], x[1]) for x in base["aggregation"]["blocks"]}
            mine = {(x[0], x[1]): x for x in r["aggregation"]["blocks"]}
            pre = [k for k, x in mine.items() if x[2]]
            wasted = [k for k in pre if k not in read]
            extra = collections.Counter(x[3] for k, x in mine.items() if k not in read)
            missing = [k for k in read if k not in mine]
            r["prefetched_blocks"] = len(pre)
            r["prefetched_not_in_reference"] = len(wasted)
            r["loads_not_in_reference"] = dict(extra.most_common())
            # unread prefetches beyond the reference's own, per requesting code (stock Lucene requests some first pages
            # that a query may never read; those appear in the reference too)
            ref_unread = base["aggregation"].get("prefetched_not_read_by_requester", {})
            mine_unread = r["aggregation"]["prefetched_not_read_by_requester"]
            r["prefetched_not_read_beyond_reference"] = sum(max(0, n - ref_unread.get(k, 0)) for k, n in mine_unread.items())
            print(f"  {r['query']:<16} loaded {len(mine)} (prefetched {len(pre)}), reference {len(read)}; "
                  f"prefetched_not_in_reference {len(wasted)} {sorted(wasted)[:6]}; "
                  f"loads_not_in_reference {dict(extra.most_common())}; in reference but not loaded here {len(missing)}; "
                  f"prefetched_not_read {r['prefetched_not_read']} (beyond reference {r['prefetched_not_read_beyond_reference']})")
    if args.out:
        with open(args.out, "w") as f:
            json.dump({"index": index, "variant": name, "block_size": block_size, "results": results}, f, indent=1)


if __name__ == "__main__":
    main()
