#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Warm latency of one bench_aggs.py query for a few variants, interleaved: in every round each variant is set, warmed up
(--warmups requests) and timed (--reps requests, client wall); the round's value is the median of its reps. Prints per
variant the median over rounds and every round's value. Used to re-check a warm-bar outlier of a bench_aggs.py run.

Usage: validate/qtime.py QUERY ROUNDS VARIANT [VARIANT ...] [--dataset logs_v3] [--fork WORKTREE]
  e.g. validate/qtime.py sort_desc:all:1d:500:tt 5 stock E
Variants use the bench_aggs.py grammar (agg_batch mode, sort_opt switches, @split, @v1).
"""

import argparse
import os
import statistics
import sys
import time

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(here))
import bench_aggs as ba  # noqa: E402
import bench_postings as bp  # noqa: E402


def main():
    repo = os.path.abspath(os.path.join(here, "..", "..", "..", ".."))
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("query")
    parser.add_argument("rounds", type=int)
    parser.add_argument("variants", nargs="+")
    parser.add_argument("--dataset", default=ba.DEFAULT_DATASET, choices=sorted(ba.DATASETS))
    parser.add_argument("--docs", type=int, default=30_000_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--reps", type=int, default=10)
    parser.add_argument("--url", default="http://localhost:9200")
    parser.add_argument("--fork", default=os.path.join(os.path.dirname(repo), "lucene_experiments"))
    args = parser.parse_args()
    try:
        variants = [ba.resolve_variant(v, args.dataset) for v in args.variants]
        ba.parse_query(args.query)
    except ValueError as e:
        sys.exit(str(e))
    client = bp.Client(args.url)
    for v in variants:
        v["index"] = ba.index_name(v["dataset"], args.docs, args.seed, args.fork)
        if not client.exists(v["index"]):
            sys.exit(f"index [{v['index']}] does not exist")
        v["body"] = ba.query_body(args.query, ba.query_field(v, args.query))
    load = ba.wait_for_load()
    res = {v["name"]: [] for v in variants}
    try:
        for _ in range(args.rounds):
            for v in variants:
                ba.set_variant(client, v)
                for _ in range(args.warmups):
                    ba.search(client, v["index"], v["body"])
                walls = []
                for _ in range(args.reps):
                    start = time.perf_counter()
                    ba.search(client, v["index"], v["body"])
                    walls.append((time.perf_counter() - start) * 1000)
                res[v["name"]].append(statistics.median(walls))
    finally:
        ba.reset_variant(client)
    print(f"{args.query}: {args.rounds} rounds x {args.reps} reps, load average {load[0]:.1f}")
    base = statistics.median(res[variants[0]["name"]])
    for v in variants:
        m = statistics.median(res[v["name"]])
        print(f"{args.query} {v['name']:28} median {m:7.2f} ms ({m - base:+6.2f} ms)  rounds "
              f"{[round(x, 1) for x in res[v['name']]]}")


if __name__ == "__main__":
    main()
