#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Top-k disjunction benchmark (Phase B): BM25 top-k OR queries on a text field, stock Lucene104 vs Lucene104DualNav.

One index holds the same text in two fields:
  body       "meta": {"postings_format": "Lucene104Baseline"}   stock Lucene104 postings, freqs and norms
  body_dual  "meta": {"postings_format": "Lucene104DualNav"}    copied from body at index time (copy_to)

Synthetic corpus, reproducible from the seed. Docs are generated in bulk batches of BATCH docs, and each batch is
indexed by one write thread, so it stays contiguous in doc ID order after the force merge.
  - query terms and the share of docs they appear in: t50 50%, t20 20%, t5 5%, t1 1%, t01 0.1%
  - term frequency: 1, except in "hot" batches of that term (HOT_SHARE of the batches, chosen per
    term), where it is 1 + a geometric draw with mean HOT_MEAN_TF, at most 30. Hot batches model topical clusters:
    they are the blocks with high max scores, so block-max pruning has something to skip.
  - document length: 16 to 128 filler tokens "z", log-uniform, so norms (and BM25 length normalization) vary. The
    range is narrow enough that a short doc with tf 1 cannot outscore a hot doc, so non-hot blocks have lower max
    scores (in v1, 4 to 128 tokens and tf 1-2, every block had a near-max doc and nothing could be skipped).

Queries are bool should of 2-3 term clauses with size k and track_total_hits false, so Lucene runs them with
MaxScoreBulkScorer (block-max pruning). Before measuring, every variant must return the same top-k scores, and a
warm check with _id fetched must return the same docs. For reference, the same query run exhaustively (size 0,
track_total_hits true) gives the IOs of reading every block.

Usage:
  benchmark/bench_topk.py --probe 2000000      # ingest a small index to measure bytes per doc, then delete it
  benchmark/bench_topk.py --docs N             # ingest once (reused afterwards), then measure
Only the Python standard library is used.
"""

import argparse
import concurrent.futures
import datetime
import json
import math
import os
import random
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench_postings as bp  # noqa: E402  (Client, stats helpers)

DATASET = "topk_v2"  # v1: filler 4-128 and tf 1-2 outside hot batches gave every block a near-max score
BATCH = 20_000
HOT_SHARE = 0.02
HOT_MEAN_TF = 6.0
FILLER_MIN, FILLER_MAX = 16, 128
TERMS = [("t50", 0.5), ("t20", 0.2), ("t5", 0.05), ("t1", 0.01), ("t01", 0.001)]

# (name, terms, k)
QUERIES = [
    ("t50 OR t20", ["t50", "t20"], 10),
    ("t50 OR t5", ["t50", "t5"], 10),
    ("t50 OR t1", ["t50", "t1"], 10),
    ("t20 OR t5 OR t1", ["t20", "t5", "t1"], 10),
    ("t5 OR t1 OR t01", ["t5", "t1", "t01"], 10),
    ("t50 OR t20 (k=100)", ["t50", "t20"], 100),
]
# (variant, field, Lucene104DualNav read mode or None, top-k prefetch)
# top-k prefetch: 0 = off, "nN" = norms N cache blocks ahead for every window, "nNf" = only eligible windows (max score
# from .nav impacts can beat the current threshold), "...dD" = also each clause's postings D blocks ahead (node-aligned)
VARIANTS_ALL = [
    ("baseline", "body", None, 0),
    ("dual_doc", "body_dual", "doc", 0),
    ("dual_nav", "body_dual", "nav", 0),
    ("baseline_norms2", "body", None, "n2"),
    ("dual_nav_norms2", "body_dual", "nav", "n2"),
    ("dual_nav_norms2_filter", "body_dual", "nav", "n2f"),
    ("dual_nav_doc1", "body_dual", "nav", "n0d1"),
    ("dual_nav_norms2_doc1", "body_dual", "nav", "n2d1"),
]
VARIANTS = VARIANTS_ALL[:3]

_topk = None


def set_topk(client, spec):
    """Sets the top-k prefetch switch (see VARIANTS_ALL); the disjunction (BooleanScorer) prefetch stays off."""
    global _topk
    if spec == _topk:
        return
    if not spec:
        client.request("POST", "/_bufferpool/topk_prefetch?norms_blocks=0")
    else:
        norms, _, doc = spec[1:].partition("d")
        filt = "true" if norms.endswith("f") else "false"
        blocks = int(norms.rstrip("f"))
        client.request("POST", f"/_bufferpool/topk_prefetch?norms_blocks={blocks}&filter={filt}&doc_blocks={int(doc or 0)}")
    _topk = spec


def mapping():
    def text(fmt, **extra):
        return {
            "type": "text",
            "analyzer": "whitespace",
            "index_options": "freqs",
            "norms": True,
            "meta": {"postings_format": fmt},
            **extra,
        }

    return {
        "dynamic": "strict",
        "_source": {"enabled": False},
        "properties": {"body": text(bp.BASELINE_FORMAT, copy_to="body_dual"), "body_dual": text(bp.DUAL_FORMAT)},
    }


def hot_batches(seed, num_batches):
    """Per term, the set of batch numbers where the term has high frequencies."""
    out = {}
    for ti, (term, _) in enumerate(TERMS):
        rng = random.Random(seed * 7919 + ti)
        n = max(1, round(HOT_SHARE * num_batches))
        out[term] = set(rng.sample(range(num_batches), n))
    return out


def bulk_bodies(index, num_docs, seed):
    num_batches = math.ceil(num_docs / BATCH)
    hot = hot_batches(seed, num_batches)
    action = json.dumps({"index": {"_index": index}})
    log_lo, log_hi = math.log(FILLER_MIN), math.log(FILLER_MAX + 1)
    p_geo = 1.0 / HOT_MEAN_TF
    for b in range(num_batches):
        rng = random.Random(seed * 1_000_003 + b)
        lines = []
        for _ in range(min(BATCH, num_docs - b * BATCH)):
            words = []
            for term, p in TERMS:
                if rng.random() < p:
                    if b in hot[term]:
                        tf = 1 + min(29, int(math.log(1.0 - rng.random()) / math.log(1.0 - p_geo)))
                    else:
                        tf = 1
                    words.extend([term] * tf)
            words.append(" ".join(["z"] * int(math.exp(rng.uniform(log_lo, log_hi)))))
            lines.append(action)
            lines.append(json.dumps({"body": " ".join(words)}))
        yield ("\n".join(lines) + "\n").encode()


def ingest(client, index, num_docs, seed, threads):
    print(f"creating [{index}] and ingesting {num_docs:,} docs (seed {seed}) ...", flush=True)
    client.request("PUT", f"/{index}", {"settings": bp.index_settings(), "mappings": mapping()})
    start = time.time()
    done = 0

    def send(body):
        resp = client.request("POST", "/_bulk", body, content_type="application/x-ndjson", timeout=1200)
        if resp.get("errors"):
            first = next(i for i in resp["items"] if "error" in i["index"])
            raise RuntimeError(f"bulk error: {first}")
        return len(resp["items"])

    with concurrent.futures.ThreadPoolExecutor(threads) as pool:
        pending = set()
        last = 0
        for body in bulk_bodies(index, num_docs, seed):
            pending.add(pool.submit(send, body))
            if len(pending) >= 2 * threads:
                finished, pending = concurrent.futures.wait(pending, return_when=concurrent.futures.FIRST_COMPLETED)
                for f in finished:
                    done += f.result()
                if time.time() - last > 10:
                    last = time.time()
                    print(f"  {done:,} docs ({done / max(time.time() - start, 1e-3):,.0f}/s)", flush=True)
        for f in concurrent.futures.as_completed(pending):
            done += f.result()
    print(f"  {done:,} docs in {time.time() - start:.0f}s", flush=True)
    client.request("POST", f"/{index}/_refresh")
    print("  force merging to one segment ...", flush=True)
    start = time.time()
    bp.force_merge(client, index)
    print(f"  merged in {time.time() - start:.0f}s", flush=True)


def segment_info(client, index):
    segments = client.request("GET", f"/{index}/_segments")["indices"][index]["shards"]["0"][0]["segments"]
    if len(segments) != 1:
        bp.force_merge(client, index)
        segments = client.request("GET", f"/{index}/_segments")["indices"][index]["shards"]["0"][0]["segments"]
    if len(segments) != 1:
        raise RuntimeError(f"[{index}] has {len(segments)} segments after force merge")
    seg = next(iter(segments.values()))
    return {"segments": 1, "segment_docs": seg["num_docs"], "segment_bytes": seg["size_in_bytes"]}


def query_body(field, terms, k, exhaustive=False, with_ids=False):
    clauses = [{"term": {field: t}} for t in terms]
    body = {"query": {"bool": {"should": clauses}}}
    if exhaustive:
        body.update({"size": 0, "track_total_hits": True})
    else:
        body.update({"size": k, "track_total_hits": False})
        if not with_ids:
            body["stored_fields"] = "_none_"  # no fetch-phase IO: scores only
    return body


def search(client, index, body):
    start = time.perf_counter()
    resp = client.request("POST", f"/{index}/_search?request_cache=false", body)
    return resp, (time.perf_counter() - start) * 1000


def check_same_results(client, index, terms, k):
    """Warm, not measured: every variant must return the same docs and scores, in the same order."""
    ref = None
    for variant, field, mode, blocks in VARIANTS:
        bp.set_read_mode(client, mode)
        set_topk(client, blocks)
        resp, _ = search(client, index, query_body(field, terms, k, with_ids=True))
        got = [(h["_id"], h["_score"]) for h in resp["hits"]["hits"]]
        if len(got) != k:
            raise RuntimeError(f"{variant} {terms}: {len(got)} hits, expected {k}")
        if ref is None:
            ref = got
        elif got != ref:
            raise RuntimeError(f"{variant} {terms}: top-{k} differs from baseline:\n{got}\n{ref}")
    return ref


def io_snapshot(client):
    files = client.request("GET", "/_bufferpool/stats")["files"]
    return {k: v for k, v in files.items() if v["requests"] or v["loads"] or v["prefetch_loads"]}


def exhaustive_reference(client, index, terms):
    """Cold IOs of reading every block of the clauses (BooleanScorer, size 0), per variant field."""
    ref = {}
    for variant, field, mode, blocks in VARIANTS:
        bp.set_read_mode(client, mode)
        set_topk(client, 0)
        client.request("POST", "/_bufferpool/cache/_clear")
        client.request("POST", "/_bufferpool/stats/_reset")
        resp, _ = search(client, index, query_body(field, terms, 0, exhaustive=True))
        loads, _, by_type = bp.summarize_io(io_snapshot(client))
        ref[variant] = {"hits": resp["hits"]["total"]["value"], "took": resp["took"], "loads": loads, "loads_by_type": by_type}
    return ref


def measure(client, index, terms, k, runs, cold, expected_scores):
    out = {v: {"tooks": [], "walls": [], "loads": [], "io": None} for v, _, _, _ in VARIANTS}
    if not cold:
        for _, field, mode, blocks in VARIANTS:
            bp.set_read_mode(client, mode)
            set_topk(client, blocks)
            search(client, index, query_body(field, terms, k))
    for i in range(runs):
        order = VARIANTS[i % len(VARIANTS):] + VARIANTS[: i % len(VARIANTS)]
        for variant, field, mode, blocks in order:
            bp.set_read_mode(client, mode)
            set_topk(client, blocks)
            if cold:
                client.request("POST", "/_bufferpool/cache/_clear")
            client.request("POST", "/_bufferpool/stats/_reset")
            resp, wall = search(client, index, query_body(field, terms, k))
            scores = [h["_score"] for h in resp["hits"]["hits"]]
            if scores != expected_scores:
                raise RuntimeError(f"{variant} {terms}: scores changed during measurement")
            if blocks:
                time.sleep(0.02)
            io = io_snapshot(client)
            r = out[variant]
            r["tooks"].append(resp["took"])
            r["walls"].append(wall)
            r["loads"].append(bp.summarize_io(io)[0])
            r["io"] = io
    bp.set_read_mode(client, "doc")
    set_topk(client, 0)
    return out


def run_matrix(client, index, latencies, runs):
    results = []
    try:
        for latency in latencies:
            bp.set_latency(client, latency)
            for qname, terms, k in QUERIES:
                ref = check_same_results(client, index, terms, k)
                expected = [s for _, s in ref]
                exhaustive = exhaustive_reference(client, index, terms)
                for mode in ("cold", "warm"):
                    started = time.time()
                    by_variant = measure(client, index, terms, k, runs, mode == "cold", expected)
                    metric = "tooks" if mode == "cold" else "walls"
                    base = by_variant["baseline"][metric]
                    for variant, _, _, _ in VARIANTS:
                        r = by_variant[variant]
                        loads, bytes_loaded, by_type = bp.summarize_io(r["io"])
                        results.append(
                            {
                                "latency_ms": latency,
                                "query": qname,
                                "k": k,
                                "variant": variant,
                                "mode": mode,
                                "top_scores": expected[:3],
                                "took_ms": r["tooks"],
                                "wall_ms": r["walls"],
                                "loads_per_run": r["loads"],
                                "took": bp.distribution(r["tooks"]),
                                "wall": bp.distribution(r["walls"]),
                                "loads": loads,
                                "bytes_loaded": bytes_loaded,
                                "loads_by_type": by_type,
                                "io": r["io"],
                                "exhaustive": exhaustive[variant],
                                "median_change_vs_baseline": None
                                if variant == "baseline"
                                else bp.median_change_ci(base, r[metric]),
                            }
                        )
                    print(f"  {latency:g}ms {qname:<20} {mode}: {runs} x {len(VARIANTS)} runs in {time.time() - started:.0f}s",
                          flush=True)
    finally:
        bp.set_latency(client, 0)
        bp.set_read_mode(client, "doc")
        set_topk(client, 0)
    return results


def print_report(meta, results):
    print(f"\n## {meta['docs']:,} docs, {meta['segment_bytes'] / 2**20:,.0f} MiB segment, {meta['runs']} runs per variant")
    print("IOs = blocks loaded per query (by type); full = IOs of the same query run exhaustively (every block).")
    names = [v for v, _, _, _ in VARIANTS]
    rows = {}
    for r in results:
        rows.setdefault((r["latency_ms"], r["query"], r["mode"]), {})[r["variant"]] = r
    for (lat, qname, mode), cells in rows.items():
        key = "took" if mode == "cold" else "wall"
        parts = []
        for v in names:
            c = cells[v]
            change = "" if v == "baseline" else f" ({bp.fmt_ci(c['median_change_vs_baseline'])})"
            types = ",".join(f"{t}:{n}" for t, n in sorted(c["loads_by_type"].items()))
            parts.append(f"{v}: {c[key]['p50']:.1f}ms{change} IOs {c['loads']} [{types}] full {c['exhaustive']['loads']}")
        print(f"{lat:g}ms {mode:4} {qname:<20} " + " | ".join(parts))


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.abspath(os.path.join(here, "..", "..", ".."))
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:9200")
    parser.add_argument("--docs", type=int, help="docs in the index (one merged segment)")
    parser.add_argument("--probe", type=int, help="ingest this many docs into a throwaway index, report bytes/doc, delete it")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--latencies-ms", default="4")
    parser.add_argument("--bulk-threads", type=int, default=4)
    parser.add_argument("--fork", default=os.path.join(os.path.dirname(repo), "lucene_experiments"))
    parser.add_argument("--ingest-only", action="store_true")
    parser.add_argument("--variants", help="comma-separated variants (baseline always kept); default: "
                        + ",".join(v[0] for v in VARIANTS_ALL[:3]))
    parser.add_argument("--out", default=os.path.join(os.path.expanduser("~"), "bufferpool-bench", "results"))
    args = parser.parse_args()
    global VARIANTS
    if args.variants:
        wanted = set(args.variants.split(",")) | {"baseline"}
        unknown = wanted - {v[0] for v in VARIANTS_ALL}
        if unknown:
            sys.exit(f"unknown variants {sorted(unknown)}; known: {[v[0] for v in VARIANTS_ALL]}")
        VARIANTS = [v for v in VARIANTS_ALL if v[0] in wanted]
    client = bp.Client(args.url)
    tag = bp.format_hash(args.fork)
    if tag is None:
        sys.exit(f"cannot find the format sources under --fork {args.fork}")

    if args.probe:
        index = f"{DATASET}_probe_{args.probe}_{args.seed}_{tag}"
        if client.exists(index):
            client.request("DELETE", f"/{index}")
        ingest(client, index, args.probe, args.seed, args.bulk_threads)
        seg = segment_info(client, index)
        per_doc = seg["segment_bytes"] / seg["segment_docs"]
        print(f"probe: {seg['segment_bytes'] / 2**20:,.1f} MiB for {seg['segment_docs']:,} docs = {per_doc:.1f} bytes/doc; "
              f"1 GiB needs about {2**30 / per_doc:,.0f} docs")
        client.request("DELETE", f"/{index}")
        return

    if not args.docs:
        sys.exit("pass --docs N (or --probe N)")
    index = f"{DATASET}_{args.docs}_{args.seed}_{tag}"
    if not client.exists(index):
        ingest(client, index, args.docs, args.seed, args.bulk_threads)
    else:
        print(f"reusing [{index}]", flush=True)
    count = client.request("GET", f"/{index}/_count")["count"]
    if count != args.docs:
        raise RuntimeError(f"[{index}] has {count} docs, expected {args.docs}")
    seg = segment_info(client, index)
    if args.ingest_only:
        print(json.dumps(seg))
        return
    stats = client.request("GET", "/_bufferpool/stats")
    meta = {
        "time": datetime.datetime.now().isoformat(timespec="seconds"),
        "index": index,
        "docs": args.docs,
        "seed": args.seed,
        "runs": args.runs,
        "block_size": stats["block_size"],
        "opensearch_head": bp.git_head(repo),
        "lucene_fork_head": bp.git_head(args.fork),
        "format_tag": tag,
        "suite": "topk",
        "corpus": {"batch": BATCH, "hot_share": HOT_SHARE, "hot_mean_tf": HOT_MEAN_TF, "filler": [FILLER_MIN, FILLER_MAX],
                   "terms": TERMS},
        "variants": [list(v) for v in VARIANTS],
        **seg,
    }
    print(json.dumps({k: v for k, v in meta.items() if k != "corpus"}), flush=True)
    results = run_matrix(client, index, [float(x) for x in args.latencies_ms.split(",")], args.runs)
    os.makedirs(args.out, exist_ok=True)
    out = os.path.join(args.out, f"postings_topk_{datetime.datetime.now():%Y%m%d_%H%M%S}.json")
    with open(out, "w") as f:
        json.dump({"runs": [{"meta": meta, "results": results}]}, f, indent=1)
    print_report(meta, results)
    print(f"\nraw results: {out}")


if __name__ == "__main__":
    main()
