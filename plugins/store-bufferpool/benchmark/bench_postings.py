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
where both lists are dense. Each query runs:
  cold  block cache cleared before every run, so every block the query needs is loaded
  warm  the same query again, cache kept
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
QUERIES = [
    ("r6 AND d50", ["r6", "d50"]),
    ("r5 AND d50", ["r5", "d50"]),
    ("r4 AND d50", ["r4", "d50"]),
    ("r3 AND d50", ["r3", "d50"]),
    ("r2 AND d50", ["r2", "d50"]),
    ("r4 AND d10", ["r4", "d10"]),
    ("d10 AND d50 (control)", ["d10", "d50"]),
]
FIELDS = [("baseline", "tag"), ("nav", "tag_nav")]
NAV_FORMAT = "Lucene104Nav"
# stock Lucene104 under its own name, so the baseline field does not share files with _id (see the plugin)
BASELINE_FORMAT = "Lucene104Baseline"
# Bump when the mapping, index settings or data generation change, so a new index is built.
DATASET_VERSION = 2
FORK_FORMAT_FILES = [
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
        "properties": {"tag": field(BASELINE_FORMAT), "tag_nav": field(NAV_FORMAT)},
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
        lines.append(f'{{"tag":[{values}],"tag_nav":[{values}]}}')
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
            for fmt in (BASELINE_FORMAT, NAV_FORMAT):
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
    body = {
        "size": 0,
        "track_total_hits": True,
        "query": {"bool": {"filter": [{"term": {field: t}} for t in terms]}},
    }
    start = time.perf_counter()
    resp = client.request("POST", f"/{index}/_search?request_cache=false", body)
    wall_ms = (time.perf_counter() - start) * 1000
    return resp["hits"]["total"]["value"], resp["took"], wall_ms


def measure(client, index, field, terms, runs, cold):
    """Runs the query `runs` times; returns hits, took per run, and the IO counters of the last run."""
    tooks, walls, hits, io = [], [], None, None
    if not cold:
        run_query(client, index, field, terms)  # make sure every block the query needs is cached
    for _ in range(runs):
        if cold:
            client.request("POST", "/_bufferpool/cache/_clear")
        client.request("POST", "/_bufferpool/stats/_reset")
        h, took, wall = run_query(client, index, field, terms)
        io = {k: v for k, v in client.request("GET", "/_bufferpool/stats")["files"].items() if v["requests"] or v["loads"] or v["prefetch_loads"]}
        if hits is not None and h != hits:
            raise RuntimeError(f"{field} {terms}: hit count changed between runs ({hits} vs {h})")
        hits = h
        tooks.append(took)
        walls.append(wall)
    return hits, tooks, walls, io


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
                row_hits = {}
                for variant, field in FIELDS:
                    for mode in ("cold", "warm"):
                        hits, tooks, walls, io = measure(client, index, field, terms, runs, mode == "cold")
                        loads, bytes_loaded, by_type = summarize_io(io)
                        row_hits[variant] = hits
                        results.append(
                            {
                                "latency_ms": latency,
                                "query": qname,
                                "variant": variant,
                                "mode": mode,
                                "hits": hits,
                                "took_ms": tooks,
                                "took_ms_median": statistics.median(tooks),
                                "wall_ms_median": statistics.median(walls),
                                "loads": loads,
                                "bytes_loaded": bytes_loaded,
                                "loads_by_type": by_type,
                                "io": io,
                            }
                        )
                if row_hits["baseline"] != row_hits["nav"]:
                    raise RuntimeError(f"{qname}: baseline and nav disagree ({row_hits})")
    finally:
        set_latency(client, 0)
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
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--runs", type=int, default=3, help="runs per (query, field, mode); the median is reported")
    parser.add_argument("--latencies-ms", default="0,4", help="simulated per-load latencies to test, in ms")
    parser.add_argument("--fork", default=os.path.join(os.path.dirname(repo), "lucene_experiments"))
    parser.add_argument("--format-tag", help="override the format hash that names the index")
    parser.add_argument("--reingest", action="store_true", help="delete and rebuild the indices")
    parser.add_argument("--bulk-size", type=int, default=20_000)
    parser.add_argument("--bulk-threads", type=int, default=4)
    parser.add_argument("--data-dir", default=os.path.join(os.path.expanduser("~"), "bufferpool-bench", "data"))
    parser.add_argument("--out", default=os.path.join(os.path.expanduser("~"), "bufferpool-bench", "results"))
    args = parser.parse_args()

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
    out = os.path.join(args.out, f"postings_{datetime.datetime.now():%Y%m%d_%H%M%S}.json")
    with open(out, "w") as f:
        json.dump({"runs": runs}, f, indent=1)
    for run in runs:
        print_report(run["meta"], run["results"])
    if len(runs) > 1:
        print_size_summary(runs, max(latencies))
    print(f"\nraw results: {out}")


def print_size_summary(runs, latency):
    print(f"\n## across segment sizes, cold, simulated load latency {latency:g} ms\n")
    print("| query | segment | nav/doc KiB (base) | cold loads base → nav | cold took ms base → nav | change |")
    print("|---|---:|---:|---|---|---:|")
    for qname, _ in QUERIES:
        for run in runs:
            meta = run["meta"]
            cells = {(r["variant"]): r for r in run["results"]
                     if r["query"] == qname and r["mode"] == "cold" and r["latency_ms"] == latency}
            b, n = cells["baseline"], cells["nav"]
            base_doc = meta["file_sizes"].get(f"{BASELINE_FORMAT}_0.doc", 0) / 1024
            change = (n["took_ms_median"] - b["took_ms_median"]) / max(b["took_ms_median"], 1e-9) * 100
            print(
                f"| {qname} | {meta['segment_bytes'] / 2**20:,.0f} MiB | {base_doc:,.0f} "
                f"| {b['loads']} → {n['loads']} | {b['took_ms_median']:g} → {n['took_ms_median']:g} | {change:+.0f}% |"
            )


def print_report(meta, results):
    print(f"\n## {meta['docs']:,} docs, 1 segment, block size {meta['block_size'] // 1024} KiB, median of {meta['runs']} runs")
    rows = {}
    for r in results:
        rows.setdefault((r["latency_ms"], r["query"]), {})[(r["variant"], r["mode"])] = r
    for latency in sorted({k[0] for k in rows}):
        print(f"\n### simulated load latency {latency:g} ms\n")
        print("| query | hits | cold loads base → nav | cold KiB base → nav | cold took ms base → nav | warm took ms base → nav | nav cold loads doc / nav / tim+tip |")
        print("|---|---:|---|---|---|---|---|")
        for (lat, qname), cells in rows.items():
            if lat != latency:
                continue
            bc, nc = cells[("baseline", "cold")], cells[("nav", "cold")]
            bw, nw = cells[("baseline", "warm")], cells[("nav", "warm")]
            t = nc["loads_by_type"]
            print(
                f"| {qname} | {bc['hits']:,} | {bc['loads']} → {nc['loads']} "
                f"| {bc['bytes_loaded'] / 1024:,.0f} → {nc['bytes_loaded'] / 1024:,.0f} "
                f"| {bc['took_ms_median']:g} → {nc['took_ms_median']:g} "
                f"| {bw['took_ms_median']:g} → {nw['took_ms_median']:g} "
                f"| {t.get('doc', 0)} / {t.get('nav', 0)} / {t.get('tim', 0) + t.get('tip', 0)} |"
            )


if __name__ == "__main__":
    main()
