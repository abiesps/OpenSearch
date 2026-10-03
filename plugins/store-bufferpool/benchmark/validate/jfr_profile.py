#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
CPU profile of one bench_aggs.py query on the running node, from JFR execution samples of the search threads.

Starts a JFR recording on the node (jcmd, 1 ms Java sampling), runs the query in a loop for --seconds (warm), stops
the recording and prints, for the samples taken on search threads:
  - the bulk scorer and the collect entry points seen on the stacks (which code path ran),
  - the top frames by self time (leaf frame),
  - the top frames by total time (anywhere on the stack).

Usage: validate/jfr_profile.py dh:s50:7d [--docs 30000000] [--seconds 8] [--variant V] [--dataset logs_v3]
--variant takes a bench_aggs.py variant expression (agg_batch mode, sort_opt switches, @split, @v1); --mode MODE is
the older form of --variant MODE.
"""

import argparse
import collections
import json
import os
import re
import subprocess
import sys
import tempfile
import time

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(here))
import bench_aggs as ba  # noqa: E402
import bench_postings as bp  # noqa: E402

JFC = """<?xml version="1.0" encoding="UTF-8"?>
<configuration version="2.0">
  <event name="jdk.ExecutionSample"><setting name="enabled">true</setting><setting name="period">1 ms</setting></event>
</configuration>
"""
PATH_MARKERS = ("BulkScorer", "collect(", "collectRange", "DocIdStream", "forEach", "intoArray", "longValues")


def node_pid():
    out = subprocess.check_output(["pgrep", "-f", "org.opensearch.bootstrap.OpenSearch"], text=True).split()
    if len(out) != 1:
        sys.exit(f"expected one node process, found {out}")
    return out[0]


def parse_samples(jfr_file):
    """Yields (thread name, [frames leaf first]) for each execution sample."""
    text = subprocess.check_output(["jfr", "print", "--events", "jdk.ExecutionSample", "--stack-depth", "96", jfr_file],
                                   text=True)
    for block in text.split("jdk.ExecutionSample")[1:]:
        thread = re.search(r'sampledThread = "([^"]*)"', block)
        frames = re.findall(r"^\s+([\w$.<>]+\([^)]*\))\s+line:", block, re.M)
        if thread and frames:
            yield thread.group(1), frames


def short(frame):
    name, _, args = frame.partition("(")
    parts = name.split(".")
    return ".".join(parts[-2:]) + "(" + args


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("query")
    parser.add_argument("--docs", type=int, default=30_000_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--seconds", type=float, default=8)
    parser.add_argument("--variant", default=None, help="bench_aggs.py variant expression to set first (default: stock)")
    parser.add_argument("--mode", default=None, help="agg_batch mode to set first (older form of --variant)")
    parser.add_argument("--dataset", default=ba.DEFAULT_DATASET, choices=sorted(ba.DATASETS))
    parser.add_argument("--top", type=int, default=25)
    parser.add_argument("--url", default="http://localhost:9200")
    parser.add_argument("--fork", default=os.path.expanduser("~/workspace/lucene_experiments"))
    parser.add_argument("--out", help="write the summary as JSON here")
    args = parser.parse_args()

    name = args.variant or ("stock" if args.mode in (None, "off") else args.mode)
    try:
        variant = ba.resolve_variant(name, args.dataset)
        ba.parse_query(args.query)
    except ValueError as e:
        sys.exit(str(e))
    client = bp.Client(args.url)
    index = ba.index_name(variant["dataset"], args.docs, args.seed, args.fork)
    body = ba.query_body(args.query, ba.query_field(variant, args.query))
    ba.set_variant(client, variant)
    for _ in range(3):
        ba.search(client, index, body)

    pid = node_pid()
    tmp = tempfile.mkdtemp(prefix="jfr_aggs_")
    jfc = os.path.join(tmp, "cpu.jfc")
    with open(jfc, "w") as f:
        f.write(JFC)
    rec = os.path.join(tmp, "rec.jfr")
    subprocess.check_call(["jcmd", pid, "JFR.start", "name=aggs", f"settings={jfc}"], stdout=subprocess.DEVNULL)
    runs = 0
    walls = []
    try:
        end = time.time() + args.seconds
        while time.time() < end:
            _, wall = ba.search(client, index, body)
            walls.append(wall)
            runs += 1
    finally:
        subprocess.check_call(["jcmd", pid, "JFR.stop", "name=aggs", f"filename={rec}"], stdout=subprocess.DEVNULL)
        ba.reset_variant(client)

    self_counts = collections.Counter()
    total_counts = collections.Counter()
    path_counts = collections.Counter()
    n = 0
    for thread, frames in parse_samples(rec):
        if "search" not in thread:
            continue
        n += 1
        self_counts[short(frames[0])] += 1
        seen = set(short(f) for f in frames)
        for f in seen:
            total_counts[f] += 1
            if any(m in f for m in PATH_MARKERS):
                path_counts[f] += 1
    walls.sort()
    summary = {
        "query": args.query, "mode": name, "runs": runs, "median_wall_ms": walls[len(walls) // 2],
        "search_samples": n,
        "path": path_counts.most_common(args.top),
        "self": self_counts.most_common(args.top),
        "total": total_counts.most_common(args.top),
        "jfr": rec,
    }
    print(f"{args.query} mode={summary['mode']}: {runs} runs, median {summary['median_wall_ms']:.1f} ms, "
          f"{n} search-thread samples ({rec})")
    for title, key in (("code path (share of samples with the frame on the stack)", "path"),
                       ("self (leaf frame)", "self"), ("total (frame anywhere on the stack)", "total")):
        print(f"\n{title}:")
        for frame, c in summary[key]:
            print(f"  {100 * c / max(1, n):5.1f}%  {frame}")
    if args.out:
        with open(args.out, "w") as f:
            json.dump(summary, f, indent=1)


if __name__ == "__main__":
    main()
