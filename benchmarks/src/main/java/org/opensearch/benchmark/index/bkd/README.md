# vEB vs. regular BKD layout — block-seek POC

A self-contained model that compares how many distinct storage **block seeks** a BKD
(block KD-tree) navigation incurs under two on-disk node orderings:

- `LEVEL_ORDER` — flat breadth-first ordering. In a complete binary tree this places interior
  nodes level by level and all leaf blocks contiguously at the tail (left to right), which
  approximates how Lucene's `BKDWriter` serializes leaves today. This is the **regular BKD baseline**.
- `VAN_EMDE_BOAS` — the cache-oblivious recursive layout. The tree is split at half its height;
  the top sub-tree is stored contiguously, then each ~√N bottom sub-tree contiguously and
  recursively. A root-to-leaf descent touches `O(log_B N)` blocks for **any** block size `B`,
  without the layout knowing `B`.

## What it models

This is an **IO-locality model, not** a byte-exact reproduction of the Lucene points format.
Each tree node is a fixed `bytesPerNode` record placed in one simulated file; a "block" is a
fixed `blockSize` fetch unit (page / SSD block / remote-store range). A query's cost is the count
of *distinct* blocks the visited nodes fall into — i.e. the read operations a paged Directory would
issue with a cold cache. The salient contrast preserved is flat ordering vs. vEB recursive ordering.

## Running

```
./gradlew -p benchmarks run --args 'VebBkdLayoutBenchmark'
# or, from the uberjar:
java -jar benchmarks/build/distributions/opensearch-benchmarks.jar VebBkdLayoutBenchmark
```

## Reading the results

The benchmark reports JMH auxiliary counters per configuration:

- `blockSeeks` — total distinct blocks fetched
- `queries` — total queries
- `nodeVisits` — total nodes visited (the CPU-side work)

The figure of interest is **`blockSeeks / queries`** (average block seeks per query). Compare the
`layout = LEVEL_ORDER` rows against `layout = VAN_EMDE_BOAS`. `nodeVisits` stays roughly equal across
layouts, which is the point: vEB cuts IO **without** increasing instruction-count work.

Indicative deterministic counts from the model (16 B/node, 512 points/leaf):

| points | block | query | level-order seeks | vEB seeks | reduction |
|-------:|------:|-------|------------------:|----------:|----------:|
| 200 M  | 4 KiB | point | 13.0              | 3.8       | ~71%      |
| 200 M  | 64 KiB| point | 9.0               | 2.1       | ~77%      |
| 10 M   | 4 KiB | range | 9.5               | 3.0       | ~68%      |

## Caveats to carry into the strategy doc

- Large range scans read a contiguous leaf run, where flat in-order is already good; vEB wins on
  navigation and scattered point / small-range workloads.
- The vEB ordering is computed at segment-write time and makes bulk sequential scans slightly less
  linear — a read-latency-vs-write-complexity trade to weigh against an explicit IO-cost model.
- Next step toward a real prototype: implement the ordering in a Lucene `BKDWriter`/`BKDReader`
  variant (or a custom points format) and measure against `lucene-core`, rather than the model here.
