#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Compares variants of one bench_aggs.py result file. Per (mode, query): each variant's p50 and p99 (cold: server took;
warm: client wall), its change of the median vs the first listed variant with the 95% bootstrap CI
(bench_postings.median_change_ci; * = the CI excludes 0) and the difference in ms, and the median blocks loaded per run
(loads by file type of the last run in brackets).

Usage: validate/cmp.py RESULT.json [stock,V,...]   (default: every variant of the run, in run order)
"""

import json
import os
import statistics
import sys

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(here))
import bench_postings as bp  # noqa: E402


def fmt_change(ci):
    if ci is None:
        return "n/a"
    sig = "*" if ci["ci_low"] > 0 or ci["ci_high"] < 0 else " "
    return f"{ci['change'] * 100:+5.1f}% [{ci['ci_low'] * 100:+5.1f}, {ci['ci_high'] * 100:+5.1f}]{sig}"


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    data = json.load(open(sys.argv[1]))
    for run in data["runs"]:
        meta = run["meta"]
        results = run["results"]
        names = sys.argv[2].split(",") if len(sys.argv) > 2 else [v[0] for v in meta["variants"]]
        print(f"opensearch {meta.get('opensearch_head')} lucene {meta.get('lucene_fork_head')} index {meta.get('index')} "
              f"load {meta.get('load_average')} runs {meta.get('runs')} latency {meta.get('latency_ms')} ms")
        by_key = {(r["query"], r["mode"], r["variant"]): r for r in results}
        queries = []
        for r in results:
            if r["query"] not in queries:
                queries.append(r["query"])
        modes = [m for m in ("cold", "warm") if any(r["mode"] == m for r in results)]
        base_name = names[0]
        for mode in modes:
            metric = "took_ms" if mode == "cold" else "wall_ms"
            print(f"\n## {mode} ({'server took' if mode == 'cold' else 'client wall'}), change vs {base_name}")
            for q in queries:
                base = by_key.get((q, mode, base_name))
                if base is None:
                    continue
                cols = []
                for n in names:
                    r = by_key.get((q, mode, n))
                    if r is None:
                        cols.append(f"{n}: -")
                        continue
                    vals = r[metric]
                    d = bp.distribution(vals)
                    loads = statistics.median(r["loads_per_run"])
                    types = ",".join(f"{t}:{c}" for t, c in sorted(r["loads_by_type"].items()))
                    cell = f"{n}: p50 {d['p50']:.1f} p99 {d['p99']:.1f} ms, loads {loads:g} [{types}]"
                    if n != base_name:
                        ci = bp.median_change_ci(base[metric], vals)
                        cell += f", {fmt_change(ci)} {d['p50'] - statistics.median(base[metric]):+.1f} ms"
                    cols.append(cell)
                load = base.get("load_average")
                print(f"{q:<28} " + " | ".join(cols) + (f" | load {load[0]:.1f}" if load else ""))


if __name__ == "__main__":
    main()
