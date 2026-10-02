#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Aggregation benchmark (Phase C): the most common log-analytics aggregations on doc values, cold and warm.

Synthetic log corpus, reproducible from the seed (dataset logs_v1). One segment, _source disabled.
  @timestamp  date, 7 days from START spread evenly over the docs (about 20 ms apart at 30M docs), plus up to 1 s of
              jitter. Ingested in time order by one bulk thread; the merged segment holds long time-ordered runs whose
              order the merges permuted (like a merged log index), and docs within about 1 s are shuffled by the jitter.
              doc_order() reports both.
  service     keyword, 50 values (svc00..svc49), Zipf weights 1/(i+1)
  status      keyword: 200 85%, 201 5%, 404 6%, 500 3%, 503 1%
  latency     long, log-normal milliseconds (median 50, sigma 1), at most 60,000
  sel         keyword without doc values, query terms only: s50 / s10 / s1 in 50% / 10% / 1% of docs (independent)

Queries are size 0 aggregations behind a bool filter of a term on sel and a range on @timestamp, the shape of a
Discover or dashboard panel. A term clause keeps date_histogram off the filter-rewrite (BKD) fast path, which only
applies to a match-all or a lone range query without sub-aggregations.
  dh       date_histogram on @timestamp (1h buckets over 7 days, 10m over 1 day)
  dh_avg   the same with avg(latency) per bucket
  terms    terms(service, size 10) with avg(latency) per bucket

Before measuring, every variant must return the same aggregation result as the first variant.

Usage:
  benchmark/bench_aggs.py --docs N                 # ingest once (reused afterwards), then measure
  benchmark/bench_aggs.py --docs N --ingest-only
  benchmark/bench_aggs.py --docs N --queries dh:s10:7d,terms:s50:7d --modes warm
Only the Python standard library is used.
"""

import argparse
import bisect
import datetime
import json
import math
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench_postings as bp  # noqa: E402  (Client, stats helpers)

DATASET = "logs_v1"
BATCH = 20_000
START_MS = int(datetime.datetime(2026, 9, 1, tzinfo=datetime.timezone.utc).timestamp() * 1000)
SPAN_MS = 7 * 24 * 3600 * 1000
SERVICES = [f"svc{i:02d}" for i in range(50)]
SERVICE_CUM = []
_acc = 0.0
for _i in range(len(SERVICES)):
    _acc += 1.0 / (_i + 1)
    SERVICE_CUM.append(_acc)
STATUS = [("200", 0.85), ("201", 0.05), ("404", 0.06), ("500", 0.03), ("503", 0.01)]
STATUS_CUM = [sum(p for _, p in STATUS[: i + 1]) for i in range(len(STATUS))]
SEL = [("s50", 0.5), ("s10", 0.1), ("s1", 0.01)]
WINDOWS = {"7d": (0, SPAN_MS, "1h"), "1d": (3 * 86_400_000, 4 * 86_400_000, "10m")}

# (variant, POST /_bufferpool/agg_batch mode; None = stock code path)
#   runend  Lucene DenseConjunctionBulkScorer keeps each clause's doc ID run end (CollectExperiments.setCacheRunEnd)
#   vec     runend + OpenSearch batch collection (BatchCollection): chunked bulk value reads in avg, single-bucket
#           runs of date_histogram handed to the sub-aggregation as a stream
#   vecdec  vec + Lucene bulk doc-values reads decode the packed span between the first and last doc in one pass
#           (CollectExperiments.setBulkDecode)
#   pf      vecdec + doc-values prefetch, one node ahead, doc-ID aligned, proven by a look-ahead iterator of the query
#           (DocValuesPrefetch)
#   pfs     pf + look-ahead clauses other than terms evaluated once per segment and shared by the planners
#   pfl     pf + look-ahead built as a leapfrog conjunction (bit-set clauses advanced, not tested doc by doc)
#   pfsl    pf + both
#   pfw     vecdec + doc-values prefetch proven by the main scorer's own matches: a run-ahead buffer between the scorer
#           and the aggregation collectors (131,072 doc IDs; no second scorer)
#   pfwg    pfw + gate: collection waits at a planner's next requested doc until the read after its node is known
#   pfwc    pfwg, but docs pass straight through (no buffer) while the nodes planners enter and the next ones are cached
VARIANTS_ALL = [("stock", None), ("runend", "runend"), ("vec", "vec"), ("vecdec", "vecdec"), ("pf", "pf"), ("pfs", "pfs"),
                ("pfl", "pfl"), ("pfsl", "pfsl"), ("pfw", "pfw"), ("pfwg", "pfwg"), ("pfwc", "pfwc")]
VARIANTS = VARIANTS_ALL[:1]
DEFAULT_QUERIES = [
    "dh:s50:7d", "dh:s10:7d", "dh:s1:7d", "dh:s10:1d",
    "dh_avg:s50:7d", "dh_avg:s10:7d", "dh_avg:s10:1d",
    "terms:s50:7d", "terms:s10:7d", "terms:s1:7d", "terms:s10:1d",
]


def mapping():
    return {
        "dynamic": "strict",
        "_source": {"enabled": False},
        "properties": {
            "@timestamp": {"type": "date"},
            "service": {"type": "keyword"},
            "status": {"type": "keyword"},
            "latency": {"type": "long"},
            "sel": {"type": "keyword", "doc_values": False},
        },
    }


def settings():
    s = bp.index_settings()
    # merges only join adjacent segments, so doc ID order stays ingest (time) order
    s["index.merge.policy"] = "log_byte_size"
    return s


def bulk_bodies(index, num_docs, seed):
    action = json.dumps({"index": {"_index": index}})
    total_s = SERVICE_CUM[-1]
    step = SPAN_MS / num_docs
    for b in range(math.ceil(num_docs / BATCH)):
        rng = random.Random(seed * 1_000_003 + b)
        lines = []
        for i in range(b * BATCH, min((b + 1) * BATCH, num_docs)):
            doc = {
                "@timestamp": START_MS + int(i * step) + rng.randrange(1000),
                "service": SERVICES[bisect.bisect_left(SERVICE_CUM, rng.random() * total_s)],
                "status": STATUS[min(len(STATUS) - 1, bisect.bisect_left(STATUS_CUM, rng.random()))][0],
                "latency": min(60_000, int(math.exp(rng.gauss(math.log(50), 1.0)))),
            }
            sel = [t for t, p in SEL if rng.random() < p]
            if sel:
                doc["sel"] = sel
            lines.append(action)
            lines.append(json.dumps(doc))
        yield ("\n".join(lines) + "\n").encode()


def ingest(client, index, num_docs, seed):
    print(f"creating [{index}] and ingesting {num_docs:,} docs (seed {seed}, one bulk thread) ...", flush=True)
    client.request("PUT", f"/{index}", {"settings": settings(), "mappings": mapping()})
    start = last = time.time()
    done = 0
    for body in bulk_bodies(index, num_docs, seed):
        resp = client.request("POST", "/_bulk", body, content_type="application/x-ndjson", timeout=1200)
        if resp.get("errors"):
            first = next(i for i in resp["items"] if "error" in i["index"])
            raise RuntimeError(f"bulk error: {first}")
        done += len(resp["items"])
        if time.time() - last > 10:
            last = time.time()
            print(f"  {done:,} docs ({done / max(time.time() - start, 1e-3):,.0f}/s)", flush=True)
    print(f"  {done:,} docs in {time.time() - start:.0f}s", flush=True)
    client.request("POST", f"/{index}/_refresh")
    print("  force merging to one segment ...", flush=True)
    start = time.time()
    bp.force_merge(client, index)
    print(f"  merged in {time.time() - start:.0f}s", flush=True)


def segment_info(client, index):
    segments = client.request("GET", f"/{index}/_segments")["indices"][index]["shards"]["0"][0]["segments"]
    if len(segments) != 1:
        raise RuntimeError(f"[{index}] has {len(segments)} segments, expected 1")
    seg = next(iter(segments.values()))
    return {"segments": 1, "segment_docs": seg["num_docs"], "segment_bytes": seg["size_in_bytes"]}


def doc_order(client, index, num_docs, samples=5, size=5000):
    """
    Time order of docs by doc ID, at a few places in the segment: the share of adjacent pairs less than 2 s apart (local
    order up to the jitter), and the doc IDs where @timestamp jumps by more than a minute (boundaries of time runs),
    found by sampling every 100,000th doc.
    """
    local = total = 0
    for s in range(samples):
        after = (num_docs - size) * s // max(1, samples - 1) - 1
        body = {"size": size, "sort": [{"_doc": "asc"}], "docvalue_fields": [{"field": "@timestamp", "format": "epoch_millis"}],
                "stored_fields": "_none_"}
        if after >= 0:
            body["search_after"] = [after]
        hits = client.request("POST", f"/{index}/_search?request_cache=false", body)["hits"]["hits"]
        ts = [int(float(h["fields"]["@timestamp"][0])) for h in hits]
        local += sum(1 for a, b in zip(ts, ts[1:]) if abs(b - a) < 2000)
        total += len(ts) - 1
    step = 100_000
    sampled = []
    for doc in range(0, num_docs, step):
        body = {"size": 1, "sort": [{"_doc": "asc"}], "docvalue_fields": [{"field": "@timestamp", "format": "epoch_millis"}],
                "stored_fields": "_none_"}
        if doc > 0:
            body["search_after"] = [doc - 1]
        h = client.request("POST", f"/{index}/_search?request_cache=false", body)["hits"]["hits"][0]
        sampled.append((h["sort"][0], int(float(h["fields"]["@timestamp"][0]))))
    expected_gap = SPAN_MS / num_docs * step
    breaks = [d for (_, a), (d, b) in zip(sampled, sampled[1:]) if abs(b - a - expected_gap) > 60_000]
    return {"adjacent_within_2s": local / max(1, total), "time_run_starts": breaks}


def parse_query(spec):
    agg, sel, window = spec.split(":")
    if agg not in ("dh", "dh_avg", "terms") or sel not in {t for t, _ in SEL} or window not in WINDOWS:
        raise ValueError(f"bad query spec {spec}")
    return agg, sel, window


def query_body(spec):
    agg, sel, window = parse_query(spec)
    lo, hi, interval = WINDOWS[window]
    query = {"bool": {"filter": [
        {"term": {"sel": sel}},
        {"range": {"@timestamp": {"gte": START_MS + lo, "lt": START_MS + hi, "format": "epoch_millis"}}},
    ]}}
    avg = {"avg_latency": {"avg": {"field": "latency"}}}
    if agg == "terms":
        aggs = {"a": {"terms": {"field": "service", "size": 10}, "aggs": avg}}
    else:
        aggs = {"a": {"date_histogram": {"field": "@timestamp", "fixed_interval": interval, "min_doc_count": 1}}}
        if agg == "dh_avg":
            aggs["a"]["aggs"] = avg
    return {"size": 0, "track_total_hits": False, "query": query, "aggs": aggs}


_variant = "unset"


def set_variant(client, mode):
    global _variant
    if mode == _variant:
        return
    if mode is not None or _variant not in ("unset", None):
        client.request("POST", f"/_bufferpool/agg_batch?mode={mode or 'off'}")
    _variant = mode


def search(client, index, body):
    start = time.perf_counter()
    resp = client.request("POST", f"/{index}/_search?request_cache=false", body)
    return resp, (time.perf_counter() - start) * 1000


def result_of(resp):
    """The aggregation result for comparison: bucket keys, doc counts and avg values, exactly as returned."""
    out = []
    for b in resp["aggregations"]["a"]["buckets"]:
        avg = b.get("avg_latency", {}).get("value")
        out.append((b["key"], b["doc_count"], avg))
    return out


def check_same_results(client, index, spec):
    ref = None
    for variant, mode in VARIANTS:
        set_variant(client, mode)
        resp, _ = search(client, index, query_body(spec))
        got = result_of(resp)
        if not got:
            raise RuntimeError(f"{variant} {spec}: no buckets")
        if ref is None:
            ref = got
        elif got != ref:
            diff = [(a, b) for a, b in zip(got, ref) if a != b][:5]
            raise RuntimeError(f"{variant} {spec}: result differs from {VARIANTS[0][0]}: {diff}")
    return ref


def io_snapshot(client):
    files = client.request("GET", "/_bufferpool/stats")["files"]
    return {k: v for k, v in files.items() if v["requests"] or v["loads"] or v["prefetch_loads"]}


def measure(client, index, spec, runs, cold, expected):
    out = {v: {"tooks": [], "walls": [], "loads": [], "io": None} for v, _ in VARIANTS}
    body = query_body(spec)
    if not cold:
        for _, mode in VARIANTS:  # two warm-ups per variant: page in the blocks and let C2 compile the loop
            set_variant(client, mode)
            search(client, index, body)
            search(client, index, body)
    for i in range(runs):
        order = VARIANTS[i % len(VARIANTS):] + VARIANTS[: i % len(VARIANTS)]
        for variant, mode in order:
            set_variant(client, mode)
            if cold:
                client.request("POST", "/_bufferpool/cache/_clear")
            client.request("POST", "/_bufferpool/stats/_reset")
            resp, wall = search(client, index, body)
            if result_of(resp) != expected:
                raise RuntimeError(f"{variant} {spec}: result changed during measurement")
            io = io_snapshot(client)
            r = out[variant]
            r["tooks"].append(resp["took"])
            r["walls"].append(wall)
            r["loads"].append(bp.summarize_io(io)[0])
            r["io"] = io
    return out


def run_matrix(client, index, specs, modes, latency, runs):
    results = []
    try:
        bp.set_latency(client, latency)
        for spec in specs:
            expected = check_same_results(client, index, spec)
            for mode in modes:
                started = time.time()
                by_variant = measure(client, index, spec, runs, mode == "cold", expected)
                metric = "tooks" if mode == "cold" else "walls"
                base = by_variant[VARIANTS[0][0]][metric]
                for variant, vmode in VARIANTS:
                    r = by_variant[variant]
                    loads, bytes_loaded, by_type = bp.summarize_io(r["io"])
                    results.append({
                        "latency_ms": latency, "query": spec, "variant": variant, "mode": mode,
                        "buckets": len(expected), "docs_in_buckets": sum(c for _, c, _ in expected),
                        "took_ms": r["tooks"], "wall_ms": r["walls"], "loads_per_run": r["loads"],
                        "took": bp.distribution(r["tooks"]), "wall": bp.distribution(r["walls"]),
                        "loads": loads, "bytes_loaded": bytes_loaded, "loads_by_type": by_type, "io": r["io"],
                        "median_change_vs_first": None if variant == VARIANTS[0][0] else bp.median_change_ci(base, r[metric]),
                    })
                print(f"  {spec:<16} {mode}: {runs} x {len(VARIANTS)} runs in {time.time() - started:.0f}s", flush=True)
    finally:
        bp.set_latency(client, 0)
        set_variant(client, None)
    return results


def print_report(meta, results):
    print(f"\n## {meta['docs']:,} docs, {meta['segment_bytes'] / 2**20:,.0f} MiB segment, {meta['runs']} runs per variant, "
          f"{meta['latency_ms']:g} ms per cold miss")
    print("cold: server took; warm: client wall. IOs = blocks loaded per query, by file type.")
    names = [v for v, _ in VARIANTS]
    rows = {}
    for r in results:
        rows.setdefault((r["query"], r["mode"]), {})[r["variant"]] = r
    for (spec, mode), cells in rows.items():
        key = "took" if mode == "cold" else "wall"
        first = cells[names[0]]
        parts = []
        for v in names:
            c = cells[v]
            change = "" if v == names[0] else f" ({bp.fmt_ci(c['median_change_vs_first'])})"
            types = ",".join(f"{t}:{n}" for t, n in sorted(c["loads_by_type"].items()))
            parts.append(f"{v}: p50 {c[key]['p50']:.1f}ms{change} IOs {c['loads']} [{types}]")
        print(f"{mode:4} {spec:<16} docs {first['docs_in_buckets']:>11,} buckets {first['buckets']:>4} | " + " | ".join(parts))


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.abspath(os.path.join(here, "..", "..", ".."))
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:9200")
    parser.add_argument("--docs", type=int, required=True, help="docs in the index (one merged segment)")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--latency-ms", type=float, default=4, help="simulated storage latency per cold miss")
    parser.add_argument("--modes", default="cold,warm")
    parser.add_argument("--queries", default=",".join(DEFAULT_QUERIES))
    parser.add_argument("--fork", default=os.path.join(os.path.dirname(repo), "lucene_experiments"))
    parser.add_argument("--ingest-only", action="store_true")
    parser.add_argument("--variants", help="comma-separated variants (the first is the reference); known: "
                        + ",".join(v[0] for v in VARIANTS_ALL))
    parser.add_argument("--out", default=os.path.join(os.path.expanduser("~"), "bufferpool-bench", "results"))
    args = parser.parse_args()
    global VARIANTS
    if args.variants:
        known = dict(VARIANTS_ALL)
        wanted = args.variants.split(",")
        unknown = [w for w in wanted if w not in known]
        if unknown:
            sys.exit(f"unknown variants {unknown}; known: {list(known)}")
        VARIANTS = [(w, known[w]) for w in wanted]
    specs = args.queries.split(",")
    for s in specs:
        parse_query(s)
    client = bp.Client(args.url)
    tag = bp.format_hash(args.fork)
    if tag is None:
        sys.exit(f"cannot find the format sources under --fork {args.fork}")
    index = f"{DATASET}_{args.docs}_{args.seed}_{tag}"
    if not client.exists(index):
        ingest(client, index, args.docs, args.seed)
    else:
        print(f"reusing [{index}]", flush=True)
    count = client.request("GET", f"/{index}/_count")["count"]
    if count != args.docs:
        raise RuntimeError(f"[{index}] has {count} docs, expected {args.docs}")
    seg = segment_info(client, index)
    seg["timestamp_in_order"] = doc_order(client, index, args.docs)
    if args.ingest_only:
        print(json.dumps(seg))
        return
    stats = client.request("GET", "/_bufferpool/stats")
    meta = {
        "time": datetime.datetime.now().isoformat(timespec="seconds"),
        "index": index, "docs": args.docs, "seed": args.seed, "runs": args.runs, "latency_ms": args.latency_ms,
        "block_size": stats["block_size"], "opensearch_head": bp.git_head(repo), "lucene_fork_head": bp.git_head(args.fork),
        "suite": "aggs", "variants": [list(v) for v in VARIANTS], "load_average": os.getloadavg(), **seg,
    }
    print(json.dumps(meta), flush=True)
    results = run_matrix(client, index, specs, args.modes.split(","), args.latency_ms, args.runs)
    os.makedirs(args.out, exist_ok=True)
    out = os.path.join(args.out, f"aggs_{datetime.datetime.now():%Y%m%d_%H%M%S}.json")
    with open(out, "w") as f:
        json.dump({"runs": [{"meta": meta, "results": results}]}, f, indent=1)
    print_report(meta, results)
    print(f"\nraw results: {out}")


if __name__ == "__main__":
    main()
