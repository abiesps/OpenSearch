#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Postings layout benchmark: default Lucene104 postings vs Lucene104Nav (skip data in its own .nav file).

One index holds the same values in two keyword fields:
  tag      "meta": {"postings_format": "Lucene104Baseline"}  stock Lucene104 postings, in files of its own
  tag_nav  "meta": {"postings_format": "Lucene104Nav"}       skip data moved to .nav
so both formats are measured on the same data, in the same single segment, on the same node.

Each doc gets each term below independently with the given probability:
  d50 (50%), d10 (10%), r2 (1e-2), r3 (1e-3), r4 (1e-4), r5 (1e-5), r6 (1e-6)

Queries are bool filters of two terms (a rare lead clause advancing a dense clause), plus d50 AND d10 as a control
where both lists are dense. Each query runs --runs times per field (default 5), alternating the baseline and nav field on every iteration:
  cold  block cache cleared before every run, so every block the query needs is loaded
  warm  the same query again, cache kept
The report gives p50/p90/p99 of the server-side took, and the change of the median with a 95% bootstrap CI.
with the plugin's simulated per-load latency (bufferpool.simulated_load_latency) set to each value in --latencies-ms.
Load counts come from GET /_bufferpool/stats, per file type (for example Lucene104_0.doc vs Lucene104Nav_0.doc/.nav).

Reuse: the index is ingested and force-merged once. Its name includes DATASET_VERSION and a hash of the Nav format's
writer source in the Lucene fork, so it is rebuilt only when the data or the on-disk format changes (or with --reingest). Query-time-only changes
reuse the existing index.

Usage:
  benchmark/start_node.sh                      # once; restarts keep the data
  benchmark/bench_postings.py                  # ingest if needed, then run the matrix (20M docs)
  benchmark/bench_postings.py --sizes 128mb,500mb,1gb
  benchmark/bench_postings.py --docs 7500000,20000000 --latencies-ms 0,4 --runs 5
Only the Python standard library is used.
"""

import argparse
import concurrent.futures
import datetime
import hashlib
import json
import os
import random
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request

TERMS = [("d50", 0.5), ("d10", 0.1), ("r2", 1e-2), ("r3", 1e-3), ("r4", 1e-4), ("r5", 1e-5), ("r6", 1e-6)]
# conjunction suite: bool filter of two terms (rare lead AND dense)
QUERIES_AND = [
    ("r6 AND d50", ["r6", "d50"]),
    ("r5 AND d50", ["r5", "d50"]),
    ("r4 AND d50", ["r4", "d50"]),
    ("r3 AND d50", ["r3", "d50"]),
    ("r2 AND d50", ["r2", "d50"]),
    ("r4 AND d10", ["r4", "d10"]),
    ("d10 AND d50 (control)", ["d10", "d50"]),
]
# disjunction suite: exhaustive OR (size 0, all hits counted, no scores) -> Lucene BooleanScorer
QUERIES_OR = [
    ("d50 OR d10", ["d50", "d10"]),
    ("d50 OR d10 OR r2", ["d50", "d10", "r2"]),
    ("d10 OR r2", ["d10", "r2"]),
    ("d50 OR r4", ["d50", "r4"]),
    ("r2 OR r3 OR r4", ["r2", "r3", "r4"]),
    ("r4 OR r5 OR r6", ["r4", "r5", "r6"]),
]
QUERIES = QUERIES_AND
# (variant, field, Lucene104DualNav read mode or None, disjunction prefetch in cache blocks per clause).
# All dual variants read the same files.
VARIANTS_AND = [
    ("baseline", "tag", None, 0),
    ("nav", "tag_nav", None, 0),
    ("dual_doc", "tag_dual", "doc", 0),
    ("dual_nav", "tag_dual", "nav", 0),
]
VARIANTS_OR = [
    ("baseline", "tag", None, 0),
    ("dual_doc", "tag_dual", "doc", 0),
    ("dual_nav", "tag_dual", "nav", 0),
    ("dual_nav_pf1", "tag_dual", "nav", 1),
    ("dual_nav_pf4", "tag_dual", "nav", 4),
    ("dual_nav_pf16", "tag_dual", "nav", 16),
]
VARIANTS = VARIANTS_AND
SUITE = "and"
NAV_FORMAT = "Lucene104Nav"
DUAL_FORMAT = "Lucene104DualNav"
# stock Lucene104 under its own name, so the baseline field does not share files with _id (see the plugin)
BASELINE_FORMAT = "Lucene104Baseline"
# Bump when the mapping, index settings or data generation change, so a new index is built.
DATASET_VERSION = 3
FORK_FORMAT_FILES = [
    "lucene/core/src/java/org/apache/lucene/codecs/lucene104/Lucene104DualNavPostingsWriter.java",
    "lucene/core/src/java/org/apache/lucene/codecs/lucene104/Lucene104NavPostingsWriter.java",
    "lucene/core/src/java/org/apache/lucene/codecs/lucene104/Lucene104NavPostingsFormat.java",
]


class Client:
    def __init__(self, url):
        self.url = url.rstrip("/")

    def request(self, method, path, body=None, content_type="application/json", timeout=600):
        data = None
        if body is not None:
            data = body if isinstance(body, bytes) else json.dumps(body).encode()
        req = urllib.request.Request(self.url + path, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", content_type)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            if method == "HEAD":
                return None
            raise RuntimeError(f"{method} {path} -> {e.code}: {e.read().decode()[:2000]}") from None

    def exists(self, index):
        req = urllib.request.Request(f"{self.url}/{index}", method="HEAD")
        try:
            with urllib.request.urlopen(req, timeout=30):
                return True
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return False
            raise


def format_hash(fork):
    """Hash of the Nav format's writer and format sources: changes when the on-disk layout can change."""
    h = hashlib.sha1()
    for rel in FORK_FORMAT_FILES:
        path = os.path.join(fork, rel)
        if not os.path.exists(path):
            return None
        with open(path, "rb") as f:
            h.update(f.read())
    return h.hexdigest()[:8]


def git_head(repo):
    try:
        return subprocess.check_output(["git", "-C", repo, "rev-parse", "--short", "HEAD"], text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def mapping():
    def field(fmt):
        return {"type": "keyword", "doc_values": False, "meta": {"postings_format": fmt}}

    return {
        "dynamic": "strict",
        "_source": {"enabled": False},
        "properties": {"tag": field(BASELINE_FORMAT), "tag_nav": field(NAV_FORMAT), "tag_dual": field(DUAL_FORMAT)},
    }


def index_settings():
    return {
        "index.number_of_shards": 1,
        "index.number_of_replicas": 0,
        "index.store.type": "bufferpoolfs",
        "index.refresh_interval": "-1",
        "index.translog.durability": "async",
        "index.queries.cache.enabled": False,
        "index.requests.cache.enable": False,
        # keep each segment file separate so the IO counters can tell .doc, .nav, .tim, ... apart
        "index.compound_format": False,
    }


def bulk_bodies(index, num_docs, seed, batch):
    rng = random.Random(seed)
    action = json.dumps({"index": {"_index": index}})
    lines = []
    for i in range(num_docs):
        tags = [t for t, p in TERMS if rng.random() < p]
        # "d50" etc. need no JSON escaping
        values = ",".join(f'"{t}"' for t in tags)
        lines.append(action)
        lines.append(f'{{"tag":[{values}],"tag_nav":[{values}],"tag_dual":[{values}]}}')
        if len(lines) >= 2 * batch:
            yield ("\n".join(lines) + "\n").encode()
            lines = []
    if lines:
        yield ("\n".join(lines) + "\n").encode()


def ingest(client, index, args):
    print(f"creating [{index}] and ingesting {args.docs:,} docs (seed {args.seed}) ...", flush=True)
    client.request("PUT", f"/{index}", {"settings": index_settings(), "mappings": mapping()})
    start = time.time()
    done = 0

    def send(body):
        resp = client.request("POST", "/_bulk", body, content_type="application/x-ndjson")
        if resp.get("errors"):
            first = next(i for i in resp["items"] if "error" in i["index"])
            raise RuntimeError(f"bulk error: {first}")
        return len(resp["items"])

    with concurrent.futures.ThreadPoolExecutor(args.bulk_threads) as pool:
        pending = set()
        for body in bulk_bodies(index, args.docs, args.seed, args.bulk_size):
            pending.add(pool.submit(send, body))
            if len(pending) >= 2 * args.bulk_threads:
                finished, pending = concurrent.futures.wait(pending, return_when=concurrent.futures.FIRST_COMPLETED)
                for f in finished:
                    done += f.result()
                rate = done / max(time.time() - start, 1e-3)
                print(f"\r  {done:,} docs ({rate:,.0f}/s)", end="", flush=True)
        for f in concurrent.futures.as_completed(pending):
            done += f.result()
    print(f"\r  {done:,} docs in {time.time() - start:.0f}s", flush=True)
    client.request("POST", f"/{index}/_refresh")
    print("  force merging to one segment ...", flush=True)
    start = time.time()
    force_merge(client, index)
    print(f"  merged in {time.time() - start:.0f}s", flush=True)


def force_merge(client, index):
    client.request("POST", f"/{index}/_forcemerge?max_num_segments=1", timeout=7200)
    # refresh_interval is -1, so the merged segment is only searchable after a refresh
    client.request("POST", f"/{index}/_refresh")


def ensure_index(client, index, args):
    if args.reingest and client.exists(index):
        client.request("DELETE", f"/{index}")
    if not client.exists(index):
        ingest(client, index, args)
    else:
        print(f"reusing [{index}]", flush=True)
    count = client.request("GET", f"/{index}/_count")["count"]
    if count != args.docs:
        raise RuntimeError(f"[{index}] has {count} docs, expected {args.docs}; rerun with --reingest")
    segments = client.request("GET", f"/{index}/_segments")["indices"][index]["shards"]["0"][0]["segments"]
    if len(segments) != 1:
        print(f"  [{index}] has {len(segments)} segments, force merging ...", flush=True)
        force_merge(client, index)
        segments = client.request("GET", f"/{index}/_segments")["indices"][index]["shards"]["0"][0]["segments"]
    if len(segments) != 1:
        raise RuntimeError(f"[{index}] still has {len(segments)} segments after force merge")
    seg = next(iter(segments.values()))
    return {"segments": len(segments), "segment_docs": seg["num_docs"], "segment_bytes": seg["size_in_bytes"]}


def file_sizes(client, index, data_dir):
    """Sizes of the segment files of the two test fields, by file type, read from the node's data directory."""
    settings = client.request("GET", f"/{index}/_settings/index.uuid")
    uuid = settings[index]["settings"]["index"]["uuid"]
    index_dir = os.path.join(data_dir, "nodes", "0", "indices", uuid, "0", "index")
    sizes = {}
    if os.path.isdir(index_dir):
        for name in os.listdir(index_dir):
            for fmt in (BASELINE_FORMAT, NAV_FORMAT, DUAL_FORMAT):
                marker = f"_{fmt}_"
                if marker in name:
                    key = name[name.index(marker) + 1 :]
                    sizes[key] = sizes.get(key, 0) + os.path.getsize(os.path.join(index_dir, name))
    return sizes


def set_latency(client, ms):
    client.request(
        "PUT", "/_cluster/settings", {"transient": {"bufferpool.simulated_load_latency": f"{int(round(ms * 1000))}micros"}}
    )


def run_query(client, index, field, terms):
    clauses = [{"term": {field: t}} for t in terms]
    # and: filter conjunction; or: exhaustive disjunction (size 0 + all hits counted -> no scores)
    bool_query = {"filter": clauses} if SUITE == "and" else {"should": clauses, "minimum_should_match": 1}
    body = {"size": 0, "track_total_hits": True, "query": {"bool": bool_query}}
    start = time.perf_counter()
    resp = client.request("POST", f"/{index}/_search?request_cache=false", body)
    wall_ms = (time.perf_counter() - start) * 1000
    return resp["hits"]["total"]["value"], resp["took"], wall_ms


def set_read_mode(client, mode):
    if mode is not None:
        client.request("POST", f"/_bufferpool/dual_nav/_mode?mode={mode}")


_prefetch_blocks = None


def set_prefetch(client, blocks):
    global _prefetch_blocks
    if blocks != _prefetch_blocks:
        client.request("POST", f"/_bufferpool/disjunction_prefetch?blocks={blocks}")
        _prefetch_blocks = blocks


def measure_all(client, index, terms, runs, cold):
    """
    Runs the query for every variant `runs` times, rotating the variant order on every iteration so that drift over
    time (GC, thermals, background merges) affects all variants equally. Returns, per variant: hits, took and client
    wall time per run, blocks loaded per run, and the IO counters of the last run.
    """
    out = {v: {"hits": None, "tooks": [], "walls": [], "loads": [], "io": None} for v, _, _, _ in VARIANTS}
    if not cold:
        for _, field, mode, blocks in VARIANTS:
            set_read_mode(client, mode)
            set_prefetch(client, blocks)
            run_query(client, index, field, terms)  # make sure every block the query needs is cached
    for i in range(runs):
        order = VARIANTS[i % len(VARIANTS):] + VARIANTS[: i % len(VARIANTS)]
        for variant, field, mode, blocks in order:
            set_read_mode(client, mode)
            set_prefetch(client, blocks)
            if cold:
                client.request("POST", "/_bufferpool/cache/_clear")
            client.request("POST", "/_bufferpool/stats/_reset")
            h, took, wall = run_query(client, index, field, terms)
            if blocks:
                # let prefetches that were issued but not used finish, so they count against this run
                time.sleep(0.02)
            files = client.request("GET", "/_bufferpool/stats")["files"]
            io = {k: v for k, v in files.items() if v["requests"] or v["loads"] or v["prefetch_loads"]}
            r = out[variant]
            if r["hits"] is not None and h != r["hits"]:
                raise RuntimeError(f"{field} {terms}: hit count changed between runs ({r['hits']} vs {h})")
            r["hits"] = h
            r["tooks"].append(took)
            r["walls"].append(wall)
            r["loads"].append(summarize_io(io)[0])
            r["io"] = io
    set_read_mode(client, "doc")
    set_prefetch(client, 0)
    return out


def percentile(values, q):
    """Nearest-rank percentile, q in [0, 100]."""
    ordered = sorted(values)
    k = max(0, min(len(ordered) - 1, int(round(q / 100 * len(ordered) + 0.5)) - 1))
    return ordered[k]


def distribution(values):
    return {
        "p50": statistics.median(values),
        "p90": percentile(values, 90),
        "p99": percentile(values, 99),
        "mean": statistics.fmean(values),
        "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
        "min": min(values),
        "max": max(values),
    }


def median_change_ci(base, nav, resamples=2000, seed=7):
    """
    Relative change of the median, (median(nav) - median(base)) / median(base), with a 95% bootstrap confidence
    interval. If the interval excludes 0, the difference is unlikely to be noise.
    """
    b_med = statistics.median(base)
    if b_med == 0:
        return None
    point = (statistics.median(nav) - b_med) / b_med
    rng = random.Random(seed)
    changes = []
    for _ in range(resamples):
        bs = statistics.median(rng.choices(base, k=len(base)))
        ns = statistics.median(rng.choices(nav, k=len(nav)))
        if bs > 0:
            changes.append((ns - bs) / bs)
    changes.sort()
    return {"change": point, "ci_low": changes[int(0.025 * len(changes))], "ci_high": changes[int(0.975 * len(changes)) - 1]}


def summarize_io(io):
    """Blocks loaded from storage by the query, demand and prefetch together, in total and per file extension."""
    loads = sum(v["loads"] + v["prefetch_loads"] for v in io.values())
    by_type = {}
    for key, v in io.items():
        ext = key.rsplit(".", 1)[-1]
        by_type[ext] = by_type.get(ext, 0) + v["loads"] + v["prefetch_loads"]
    return loads, sum(v["bytes_loaded"] for v in io.values()), by_type


# Doc counts that give a single merged segment of about the named size (about 17.7 bytes/doc, measured at 20M docs;
# most of it is _id and the other metadata fields). Fixed counts keep index names stable, so each is ingested once.
SIZE_PRESETS = {"128mb": 7_500_000, "500mb": 29_500_000, "1gb": 60_500_000}


def run_matrix(client, index, latencies, runs):
    results = []
    try:
        for latency in latencies:
            set_latency(client, latency)
            for qname, terms in QUERIES:
                for mode in ("cold", "warm"):
                    started = time.time()
                    runs_by_variant = measure_all(client, index, terms, runs, mode == "cold")
                    hits = {v: r["hits"] for v, r in runs_by_variant.items()}
                    if len(set(hits.values())) != 1:
                        raise RuntimeError(f"{qname}: variants disagree on hits {hits}")
                    # cold: server-side took (ms, integer) is precise enough; warm queries take a few ms, so use the
                    # client wall time (sub-ms resolution, includes ~1ms of HTTP overhead that all variants pay)
                    metric = "tooks" if mode == "cold" else "walls"
                    base = runs_by_variant["baseline"][metric]
                    for variant, _, _, _ in VARIANTS:
                        r = runs_by_variant[variant]
                        loads, bytes_loaded, by_type = summarize_io(r["io"])
                        results.append(
                            {
                                "latency_ms": latency,
                                "query": qname,
                                "variant": variant,
                                "mode": mode,
                                "hits": r["hits"],
                                "took_ms": r["tooks"],
                                "wall_ms": r["walls"],
                                "loads_per_run": r["loads"],
                                "took": distribution(r["tooks"]),
                                "wall": distribution(r["walls"]),
                                "took_ms_median": statistics.median(r["tooks"]),
                                "wall_ms_median": statistics.median(r["walls"]),
                                "loads": loads,
                                "bytes_loaded": bytes_loaded,
                                "loads_by_type": by_type,
                                "io": r["io"],
                                "median_change_vs_baseline": None
                                if variant == "baseline"
                                else median_change_ci(base, r[metric]),
                            }
                        )
                    print(
                        f"  {latency:g}ms {qname:<22} {mode}: {runs} x {len(VARIANTS)} runs in {time.time() - started:.0f}s",
                        flush=True,
                    )
    finally:
        set_latency(client, 0)
        set_read_mode(client, "doc")
        set_prefetch(client, 0)
    return results


def run_one(client, args, docs, tag, repo, latencies):
    args.docs = docs
    index = f"postings_poc_v{DATASET_VERSION}_{docs}_{args.seed}_{tag}"
    segment = ensure_index(client, index, args)
    stats = client.request("GET", "/_bufferpool/stats")
    sizes = file_sizes(client, index, args.data_dir)
    meta = {
        "time": datetime.datetime.now().isoformat(timespec="seconds"),
        "index": index,
        "docs": docs,
        "seed": args.seed,
        "runs": args.runs,
        "block_size": stats["block_size"],
        "opensearch_head": git_head(repo),
        "lucene_fork_head": git_head(args.fork),
        "format_tag": tag,
        "suite": SUITE,
        "variants": [list(v) for v in VARIANTS],
        **segment,
        "file_sizes": sizes,
    }
    print(json.dumps({k: v for k, v in meta.items() if k != "file_sizes"}), flush=True)
    if sizes:
        print("postings files: " + ", ".join(f"{k}={v / 1024:,.0f}KiB" for k, v in sorted(sizes.items())), flush=True)
    return {"meta": meta, "results": run_matrix(client, index, latencies, args.runs)}


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.abspath(os.path.join(here, "..", "..", ".."))
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:9200")
    parser.add_argument("--docs", default="20000000", help="comma-separated doc counts; one index (one segment) each")
    parser.add_argument(
        "--sizes", help="comma-separated segment size presets instead of --docs: " + ", ".join(SIZE_PRESETS)
    )
    parser.add_argument("--suite", choices=["and", "or"], default="and", help="conjunction or disjunction queries")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--runs", type=int, default=5, help="runs per (query, field, mode), interleaved between fields")
    parser.add_argument("--latencies-ms", default="0,4", help="simulated per-load latencies to test, in ms")
    parser.add_argument("--fork", default=os.path.join(os.path.dirname(repo), "lucene_experiments"))
    parser.add_argument("--format-tag", help="override the format hash that names the index")
    parser.add_argument("--reingest", action="store_true", help="delete and rebuild the indices")
    parser.add_argument("--bulk-size", type=int, default=20_000)
    parser.add_argument("--bulk-threads", type=int, default=4)
    parser.add_argument("--data-dir", default=os.path.join(os.path.expanduser("~"), "bufferpool-bench", "data"))
    parser.add_argument("--out", default=os.path.join(os.path.expanduser("~"), "bufferpool-bench", "results"))
    args = parser.parse_args()
    global QUERIES, VARIANTS, SUITE
    SUITE = args.suite
    QUERIES, VARIANTS = (QUERIES_AND, VARIANTS_AND) if SUITE == "and" else (QUERIES_OR, VARIANTS_OR)

    client = Client(args.url)
    tag = args.format_tag or format_hash(args.fork)
    if tag is None:
        sys.exit(f"cannot find the Lucene104Nav sources under --fork {args.fork}; pass --format-tag")
    if args.sizes:
        unknown = [s for s in args.sizes.split(",") if s not in SIZE_PRESETS]
        if unknown:
            sys.exit(f"unknown size presets {unknown}; known: {list(SIZE_PRESETS)}")
        doc_counts = [SIZE_PRESETS[s] for s in args.sizes.split(",")]
    else:
        doc_counts = [int(d.replace("_", "")) for d in args.docs.split(",")]
    latencies = [float(x) for x in args.latencies_ms.split(",")]

    runs = [run_one(client, args, docs, tag, repo, latencies) for docs in doc_counts]

    os.makedirs(args.out, exist_ok=True)
    out = os.path.join(args.out, f"postings_{SUITE}_{datetime.datetime.now():%Y%m%d_%H%M%S}.json")
    with open(out, "w") as f:
        json.dump({"runs": runs}, f, indent=1)
    for run in runs:
        print_report(run["meta"], run["results"])
    if len(runs) > 1:
        print_size_summary(runs, max(latencies))
    print(f"\nraw results: {out}")


def fmt_ci(ci):
    if ci is None:
        return "n/a"
    sig = "" if ci["ci_low"] <= 0 <= ci["ci_high"] else " *"
    return f"{ci['change'] * 100:+.0f}% [{ci['ci_low'] * 100:+.0f}, {ci['ci_high'] * 100:+.0f}]{sig}"


def print_report(meta, results):
    print(
        f"\n## {meta['docs']:,} docs, {meta['segment_bytes'] / 2**20:,.0f} MiB segment, block size "
        f"{meta['block_size'] // 1024} KiB, {meta['runs']} runs per variant (interleaved)"
    )
    print("cold: server-side took (ms); warm: client wall time (ms, includes HTTP).")
    print("change = change of the median vs baseline with a 95% bootstrap CI; * = the CI excludes 0")
    names = [v for v, _, _, _ in VARIANTS]
    rows = {}
    for r in results:
        rows.setdefault((r["latency_ms"], r["query"], r["mode"]), {})[r["variant"]] = r
    for latency in sorted({k[0] for k in rows}):
        for mode in ("cold", "warm"):
            print(f"\n### {mode}, simulated load latency {latency:g} ms\n")
            print("| query | hits | " + " | ".join(f"{v} IOs, p50 ms (change)" for v in names) + " |")
            print("|---|---:|" + "---|" * len(names))
            for (lat, qname, m), cells in rows.items():
                if lat != latency or m != mode:
                    continue
                key = "took" if mode == "cold" else "wall"
                parts = []
                for v in names:
                    c = cells[v]
                    lp = c["loads_per_run"]
                    loads = f"{min(lp)}" if min(lp) == max(lp) else f"{min(lp)}-{max(lp)}"
                    change = "" if v == "baseline" else f" ({fmt_ci(c['median_change_vs_baseline'])})"
                    parts.append(f"{loads}, {c[key]['p50']:.1f}{change}")
                print(f"| {qname} | {cells['baseline']['hits']:,} | " + " | ".join(parts) + " |")


def print_size_summary(runs, latency):
    print(f"\n## across segment sizes, cold, simulated load latency {latency:g} ms\n")
    names = [v for v, _, _, _ in VARIANTS]
    print("| query | segment | " + " | ".join(f"{v} IOs / p50 ms" for v in names) + " | best |")
    print("|---|---:|" + "---|" * (len(names) + 1))
    for qname, _ in QUERIES:
        for run in runs:
            meta = run["meta"]
            cells = {r["variant"]: r for r in run["results"]
                     if r["query"] == qname and r["mode"] == "cold" and r["latency_ms"] == latency}
            best = min(names, key=lambda v: cells[v]["took"]["p50"])
            parts = []
            for v in names:
                c = cells[v]
                change = "" if v == "baseline" else f" ({c['median_change_vs_baseline']['change'] * 100:+.0f}%)"
                parts.append(f"{c['loads']} / {c['took']['p50']:g}{change}")
            print(f"| {qname} | {meta['segment_bytes'] / 2**20:,.0f} MiB | " + " | ".join(parts) + f" | {best} |")


if __name__ == "__main__":
    main()
