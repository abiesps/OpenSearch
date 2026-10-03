# IO optimization POC: plan and state

Living document for the postings IO POCs (strategy doc: "IO and memory optimization strategy in OpenSearch and Lucene",
Stage 3). Update it at the end of every step.

## Rules

- **One change at a time.** Measure each change against the baseline before starting the next.
- **Never remove data from stock files.** A new storage change keeps `.doc` (and the other stock files) byte-identical to
  stock Lucene and writes the new or relocated metadata as a duplicate in other files, so a reader can take it from either
  place.
- **Reuse ingested data.** Query-time changes and node settings must not need a re-ingest. Index names change only with
  `DATASET_VERSION` or the hash of the format writers (see `bench_postings.py`).
- **Iterations.** 5 per variant from now on (user decision; earlier runs used 100 and 10), variants rotated each
  iteration. Report p50/p99 and a 95% bootstrap CI for the median change.
- **Results doc.** Every result also goes into the Pippin doc https://pippin.amazon.dev/docs/k9mg0uBzxdeIFe/postings-poc
  (main body: summary per phase; Appendix: every table).

## Where things are

| What | Where |
|---|---|
| Lucene fork | `abiesps/lucene_experiments`, branch `poc-ioopt`, local `~/workspace/lucene_experiments` (base: tag `releases/lucene/10.5.1`) |
| OpenSearch | `abiesps/OpenSearch`, branch `poc-ioopt`, local `~/workspace/OpenSearch` (builds against the fork's `10.5.1-SNAPSHOT` from `~/.m2`; every Gradle command needs `-Drepos.mavenLocal=true`) |
| Publish fork jars | in the fork: `./gradlew :lucene:core:publishJarsPublicationToMavenLocal` (or `mavenToLocal` for all modules) |
| Node | `plugins/store-bufferpool/benchmark/start_node.sh` / `stop_node.sh`; data in `~/bufferpool-bench/data` survives restarts |
| Benchmark | `plugins/store-bufferpool/benchmark/bench_postings.py --sizes 128mb,500mb,1gb` (see README.md); aggregations: `bench_aggs.py --docs 30000000 --variants stock,runend,vec,vecdec` |
| Lucene refresh | `plugins/store-bufferpool/benchmark/refresh_lucene.sh`: publish all fork modules, restart, verify every node Lucene jar against `~/.m2` and the fork HEAD |
| CPU profile | `plugins/store-bufferpool/benchmark/validate/jfr_profile.py QUERY --mode MODE` (JFR on the node, code path + top frames) |
| Offline IO tracer | `plugins/store-bufferpool/benchmark/PostingsIoTrace.java` (term byte ranges, blocks touched per query) |
| Results | `~/bufferpool-bench/results/*.json` |

Postings formats in the fork (`lucene/core/.../codecs/lucene104/`):

| Format | Layout | Status |
|---|---|---|
| `Lucene104` (stock) | skip data interleaved in `.doc` | baseline; OpenSearch field alias `Lucene104Baseline` gives it its own files |
| `Lucene104Nav` | skip data moved out of `.doc` into `.nav` | change 1, measured; breaks the "never remove" rule, kept for reference |
| `Lucene104DualNav` | stock files unchanged + duplicate skip data (incl. impacts) in `.nav`, `.nsm`; per-term `.doc` length and `.nav` pointer in the terms dictionary; `ReadMode.DOC` (stock reader) or `ReadMode.NAV` | change 2, measured; use this for new work |

Plugin endpoints (node-local): `GET /_bufferpool/stats`, `POST /_bufferpool/stats/_reset`, `POST /_bufferpool/cache/_clear`,
`POST /_bufferpool/trace/_start|_stop`, `GET /_bufferpool/trace`, `POST /_bufferpool/dual_nav/_mode?mode=doc|nav`.
Settings: `bufferpool.cache.size`, `bufferpool.cache.block_size` (default 128kb, restart), `bufferpool.simulated_load_latency`
(dynamic; a delay added to each cache miss only).

Indices kept (seed 42, one segment, `tag` / `tag_nav` / `tag_dual` keyword fields, terms `d50 d10 r2..r6`):
`postings_poc_v3_{7500000,29500000,60500000}_42_f246f5c2` (137 / 532 / 1,073 MiB). Older two-field v2 indices also exist.

## What we learned so far

1. **Cold latency ≈ number of dependent block loads × storage latency** (about 4.8 ms per load at 4 ms simulated). Warm is
   1–26 ms, cold 35–690 ms.
2. **Change 1 (`Lucene104Nav`), cold, 100 iterations:** the rarest conjunction (`r6 AND d50`) is 20–29% faster, the rest
   −9% to +9%, at about 5% extra postings bytes.
3. **Calibrated cost model (per term, per segment):** k = lead term docFreq (the dense list is advanced once per lead
   doc), B = cache blocks of the dense list (`.doc` length / block size), N = its `.nav` blocks.
   Nav path ≈ B·(1 − (1 − 1/B)^k) + N blocks, scan ≈ B. It picks the right winner in 21/21 cases (mean error 0.5
   blocks); offline and in-node traces match block for block.
4. **Change 2 (`Lucene104DualNav`):** doc mode = baseline (±3%); nav mode −18% to −31% on `r6 AND d50`, +5–9%
   elsewhere (stock headers stay in `.doc`, so targets spread over more bytes). With the model's per-term pick the
   dual format keeps the full gain and is never worse than +3% (20/21 picks correct; the miss is a 1 ms difference).
5. **Scorers Lucene 10.5.1 uses:** conjunctions → `ConjunctionBulkScorer` (advance loop); exhaustive OR →
   `BooleanScorer` (4,096-doc windows, `intoBitSet` per clause, one clause after another); top-k OR →
   `MaxScoreBulkScorer` (outer windows from impacts via `advanceShallow`/`getMaxScore`, essential/non-essential split).
6. **Window vs block size:** at 128 KiB blocks one cache block of `d50` covers about 890K docs (about 217 windows of
   4,096 docs). Planning prefetch one window at a time would almost never overlap IO. Prefetch must look ahead by
   cache blocks per clause, not by one window.
7. **Impacts:** both nav formats keep each block's impacts (competitive freq/norm pairs) in `.nav` (DualNav also in
   `.doc`). Max scores are computed at query time from them. Impacts are not in a separate file yet. Keyword fields
   have no freqs/norms, so their impacts cannot prune anything.

## Phase A: nav-planned prefetch for exhaustive OR (A1–A4 done)

Goal: turn the one-at-a-time cold loads of an exhaustive OR into concurrent loads, using `.nav` to know exactly which
`.doc` blocks each clause needs next. Scoring stays in 4,096-doc windows; the collector contract is unchanged.

| Step | Change | Done when |
|---|---|---|
| A1 | Lucene API: `DocIdSetIterator.prefetchDocs(int fromDoc, int toDoc)`, a no-op hint by default; the term scorer's iterator and impacts wrappers pass it through | compiles, stock behavior unchanged |
| A2 | `Lucene104DualNav` nav-mode enum implements it: a separate planning cursor over `.nav` (does not move the enum), collects payload ranges of blocks with docs in `[fromDoc, toDoc)`, merges adjacent ranges, calls `docIn.prefetch` | unit test: prefetched ranges cover exactly the blocks later read |
| A3 | `BooleanScorer`: per clause keep a prefetch horizon; when a window reaches it, hint the range covering the next N cache blocks of that clause. N is a POC knob (static setter, REST toggle) | same hits as today; trace shows prefetch loads ahead of demand |
| A4 | Benchmark: exhaustive OR queries (`size: 0`, `track_total_hits: true`, 2–3 SHOULD clauses; dense+dense, dense+sparse, sparse+sparse) on the existing v3 indices. Variants: baseline, dual_doc, dual_nav, dual_nav+prefetch; sweep N = 1, 4, 8, 16 | 100 iterations, cold and warm, IOs, p50/p99, unused prefetched blocks |
| A5 | Prefetch pool: split a prefetch request into per-block tasks (today one task loads its blocks one after another), re-run the OR suite | larger look-ahead no longer loses at 137/532 MiB |
| A6 | Cut warm planning overhead (up to +8% warm on dense ORs at 1 GB) | warm within ±3% |

Expected: `d50 OR d10 OR r2` at 1 GB loads about 112 blocks one at a time (~540 ms); with 8 loads in flight (the
prefetch pool size) it could approach 70–80 ms. The simulation has no EFS queueing or slot limits, so treat the
result as an upper bound.

### Phase A results (10 iterations, cold, 4 ms per miss; file `postings_or_20260929_224221.json`)

- First run was invalid: `FilterPostingsEnum` did not forward `prefetchAhead`, and OpenSearch wraps every postings enum
  (query cancellation), so prefetch was a no-op. Fixed in fork commit `16294a3751` with a test that fails without it.
- Dense ORs at 1 GB: `d50 OR d10` 540 → 133 ms (−75%, pf16), `d50 OR d10 OR r2` 565 → 134 ms (−76%), `d10 OR r2`
  239 → 78 ms (−68%, pf4), `d50 OR r4` 475 → 178 ms (−63%, pf4). Gain grows with segment size.
- IOs unchanged by prefetch (no wasted blocks); the nav path costs +6–7 IOs per query. Sparse ORs: −16% to +7%.
- pf4 beats pf16 at 137/532 MiB; likely because one prefetch request is loaded serially by one task (see A5).
- Warm: up to +8% on dense ORs at 1 GB (planning CPU).

## Phase B: top-k OR (B1 done)

### B1 results (index `topk_v2_30700000_42_f246f5c2`, 709 MiB, 5 runs, cold 4 ms; `postings_topk_20260930_003050.json`)

- Corpus: `bench_topk.py` (text fields `body` = Lucene104Baseline, `body_dual` = DualNav via copy_to; terms t50/t20/t5/t1/t01;
  tf 1 except 2% hot 20K-doc batches per term; 16-128 filler tokens). Queries: bool should, size k, track_total_hits false.
- Norms (`.nvd`) are 54-93% of cold IOs (89-235 blocks; the field's norms are 235 blocks), `.doc` 17-74. Cold top-k is
  2-14x slower than the exhaustive count of the same query, which reads no norms. Warm top-k is 3-7 ms.
- Pruning skips candidates (1.5K-14K collected of 1.8M-18M matches) but no 128 KiB `.doc` block: top-k reads the same
  `.doc` blocks as the exhaustive query. DualNav doc/nav = baseline (-4% to +3%). k=100 = k=10 in IOs.
- OpenSearch's CancellableBulkScorer chunks read the same blocks as one Lucene call (validate/TopkBlocks.java).
- Next: T1-T4 below. 8 KiB nodes, A5, A6 parked.



### Top-k prefetch design (agreed)

- `.nvm` is loaded into heap when the segment opens, so it needs no prefetch. The cost is `.nvd`: one dense stream per
  field (1 byte per doc, `normsOffset + doc`), shared by all clauses on that field.
- Before scoring, prefetch the first `.nav` node of each clause (term metadata stores where `.nav` starts, not its
  length, so "the whole region" would need a format change).
- Scoring stays in 4,096-doc windows. Before each window, a planner looks ahead and decides, per upcoming window, whether
  it is eligible: sum over clauses of the max score from the clause's impacts (read through a second impacts enum, which
  in DualNav nav mode reads only `.nav`) >= the current `minCompetitiveScore`. The threshold only rises, so an
  ineligible window stays ineligible.
- For eligible windows only, request whole storage blocks (nodes), one to two ahead per stream as in Phase 3b: the norm
  blocks of the field, and later (T4) the `.doc` nodes of the clauses. Requests never overlap.
- Variants to measure: norms prefetch filtered by eligibility, norms prefetch without the filter (shows what the
  filter saves), then norms + `.doc`. Report prefetched blocks never read.

### T1-T3 results (5 runs, cold 4 ms; `postings_topk_20260930_114724.json`; fork `8ff4f53b06`)

- Norms prefetch 2 blocks ahead, every window: -35% to -58% cold (e.g. t20 OR t5 OR t1 1,553 -> 648 ms stock,
  703 ms DualNav). Works on stock postings too. Warm within noise. 2-term queries load 235 norm blocks but read 88-100.
- With the eligibility filter (level-0 impact ranges): 2-term queries load 91-97 norm blocks (no waste) but are 2-14%
  slower than unfiltered, and warm costs +1-8 ms. 3-term queries: no saving, t20 OR t5 OR t1 slower (935 ms, 65 norm
  demand loads). Likely cause: Lucene's outer windows use coarser bounds than the planner, so rejected ranges get read.
- A fixed 4,096-doc window with level-1 bounds rejected nothing (first version).
- .doc loads unchanged and serial (17-74 per query): next is T4.

### T4 results (5 runs, cold 4 ms; `postings_topk_20260930_163423.json`; fork `82aad05612`)

- Norms 2 blocks (unfiltered) + postings 1 node ahead (DualNav nav, node-aligned): -52% to -64% cold
  (t50 OR t20 825 -> 313 ms, t20 OR t5 OR t1 1,378 -> 491 ms). Postings prefetch adds 8-35% on top of norms alone;
  alone it gives -30% to -38% on 2-term queries, -5% to -15% on 3-term. `.doc` IOs unchanged, 0-1 demand loads.
- Warm k=10 within noise. Warm k=100 unusable in this run (machine load average 226 from other apps).
- Likely remaining bound: the norm stream (~2 in flight x 235 blocks). Next: norms 4 blocks ahead; cheap filter.
- Decision (user): block look-ahead stays at 1 and doc-ID aligned, to save IO; no deeper look-ahead experiments.
- Top-k work paused here (open item: a cheap norms filter aligned with Lucene's outer windows). Next area: doc values.

| Step | Change | Done when |
|---|---|---|
| T1 | Lucene: `NumericDocValues.prefetchNodes(fromDoc, toDoc, nodeBytes)` hint (no-op by default); dense Lucene90 norms request the whole nodes holding those norms, never twice | unit test: whole nodes, inside the field's region, no overlap |
| T2 | Lucene: `TopKPrefetch` switch; `TermScorer` gets a planning impacts enum (second enum on the same term) and the norms; `MaxScoreBulkScorer` planner: eligibility per 4,096-doc window, norms requested for eligible windows `docsAhead` ahead, filter optional; first `.nav` node prefetched at start | top-k identical with and without; with the filter off, every norm read was prefetched before it |
| T3 | Plugin endpoint + `bench_topk.py` variants; measure on `topk_v2` (5 runs) | results table + blocks prefetched but never read |
| T4 | `.doc` prefetch of the clauses in `MaxScoreBulkScorer`, node-aligned (Phase 3b planner), eligible windows only | measured on top of T3 |
| T5 | Storage (later): impacts in their own small file; skip index over impacts | only if T3/T4 show `.nav` reads on the critical path |

## Phase C: aggregations on doc values (C0-C2, C3a, C3c, C3d done; C3e done: pfwc within noise warm, pfsl-level cold)

Decisions (user):
- **Vectorization first.** Batch and vectorize collection, measure it, and only then build doc-values prefetch.
- **Navigation data stays on disk.** The value jump table and the IndexedDISI jump table are already together in the
  field's `.dvd` slice. For queries that iterate doc values, prefetch that slice before query execution instead of
  keeping a separate copy in heap.
- Prefetch, when it comes, is non-speculative: look-ahead 1, doc-ID aligned (same rule as postings).
- Tests run through OpenSearch with the changed Lucene as the dependency.

What stock already has (fork 10.5.1, OpenSearch `poc-ioopt`):
- Lucene: bulk `NumericDocValues.longValues(size, docs, values, default)` (dense Lucene90 overrides it);
  `DocIdStream.intoArray`; `ConstantScoreBulkScorer` and `DenseConjunctionBulkScorer` emit 4,096-doc
  `BitSetDocIdStream` windows; sparse conjunctions (`ConjunctionBulkScorer`) and `DefaultBulkScorer` collect per doc.
- OpenSearch: `LeafBucketCollector.collect(DocIdStream, owningBucketOrd)` and `collectRange` exist, but `avg` and
  `date_histogram` still read values per doc (`advanceExact` + `longValue`/`nextValue`) inside them, and bucket
  aggregations call their sub-aggregations per doc.

| Step | Change | Done when |
|---|---|---|
| C0 | `refresh_lucene.sh`: publish all fork modules, rebuild, restart, check every node lib jar against `~/.m2` and the fork head | script passes; mismatch fails it |
| C1 | Log-style corpus `logs_v1` + `bench_aggs.py` (Discover `date_histogram`, `terms(service)` + `avg(latency)`; cold and warm, 5 runs) | stock numbers, collect path per query, CPU profile |
| C2 | Vectorized batch collection: doc IDs in batches, bulk value decode, vectorized rounding / sum, batched bucket ords and sub-aggregation calls | same aggregation results as stock; warm and cold measured |
| C3 | Doc-values prefetch (non-speculative, `.dvd` jump tables first) | measured on top of C2 |

### C1: corpus and stock profile

- Index `logs_v1_30000000_42_f246f5c2`: 30M docs, 831 MiB, one segment, `_source` off, `log_byte_size` merges. Doc ID
  order is time order within 7 runs (merges permuted the runs; adjacent docs are less than 2 s apart).
  `@timestamp` and `latency` use the blocked (varying bits per value) numeric encoding.
- Queries: `bool filter [term sel:sX, range @timestamp]` + `dh` (date_histogram 1h over 7d, 10m over 1d), `dh_avg`
  (+ avg(latency)), `terms` (terms(service, 10) + avg(latency)). `s50`/`s10` run with `DenseConjunctionBulkScorer`
  (4,096-doc `BitSetDocIdStream` windows); top-level `date_histogram` uses `HistogramSkiplistLeafCollector`.
- JFR profile (`validate/jfr_profile.py`): for 7d ranges, 37-97% of warm CPU was not aggregation at all:
  `DenseConjunctionBulkScorer` called `docIDRunEnd()` on the range clause every window, and on a bit set that matches
  almost every doc it scans to the end of the run, O(maxDoc / 64) per 4,096-doc window. The rest was per-doc value
  reads (`VaryingBPVReader.getLongValue` + one bufferpool read per value), the terms ordinal hash, and the deferred
  (breadth-first) record and replay of the avg sub-aggregation.

### C2: changes (each behind a switch; `POST /_bufferpool/agg_batch?mode=off|runend|vec|vecdec`, cumulative)

| Mode | Where | Change |
|---|---|---|
| runend | Lucene `DenseConjunctionBulkScorer` | keep each clause's last run end while the clause is inside it (`CollectExperiments.setCacheRunEnd`); not for the collector's competitive iterator |
| vec | OpenSearch `BatchCollection` | `avg`: `collect(DocIdStream)` in 1,024-doc chunks, one `longValues` bulk read per chunk, exact long sum per chunk; `date_histogram` skip-list collector: a run of docs in one bucket goes to the sub-aggregation as a bounded `DocIdStream`; `terms` (global ords, single-valued): bulk `ordValues` per chunk, ordinal-to-bucket array cache (top level), `collectExistingBuckets` + new `LeafBucketCollector.collectBatch(docs, buckets, n)`; deferring collector records and replays in batches with a rebase table; `avg.collectBatch` sums per bucket locally |
| vecdec | Lucene `Lucene90DocValuesProducer` | bulk reads (`longValues`, new `SortedDocValues.ordValues`) of dense fields read the packed bytes of the doc span once and decode only the requested values (`PackedSpans.gather`, all widths, blocked/table/gcd/plain), when the span has at most 64 values per requested doc (`CollectExperiments.setBulkDecode`) |

Also: `BufferPoolIndexInput.readBytes(long, ...)` copied one byte at a time (Lucene's default); it now copies whole
blocks. Results are exact: integer sums are exact in longs and added to the compensated sum once per chunk and
bucket; every run checks bucket keys, doc counts and avg values for equality with stock.

### C2 results (5 runs, `aggs_20261001_092720.json`; fork `3098298c82`; load average 22-27)

p50 ms (cold: server took at 4 ms per miss; warm: client wall). `*` = 95% CI of the median change excludes 0. Docs = docs
in the returned buckets. IOs are the same in every mode.

| Query | Docs | Mode | stock | runend | vec | vecdec | IOs |
|---|---:|---|---:|---:|---:|---:|---:|
| dh:s50:7d | 15,004,856 | cold | 1,651 | 1,247 (-24%*) | 1,246 (-25%*) | 1,238 (-25%*) | 251 |
| dh:s50:7d | 15,004,856 | warm | 253 | 10.7 (-96%*) | 10.9 (-96%*) | 10.7 (-96%*) | 0 |
| dh:s10:7d | 3,000,344 | cold | 1,170 | 1,165 (-0%) | 1,169 (-0%) | 1,169 (-0%) | 237 |
| dh:s10:7d | 3,000,344 | warm | 12.6 | 12.8 (+1%) | 13.3 (+5%) | 12.6 (-0%) | 0 |
| dh:s1:7d | 300,008 | cold | 1,072 | 1,073 (+0%) | 1,076 (+0%) | 1,077 (+0%) | 220 |
| dh:s1:7d | 300,008 | warm | 6.3 | 6.2 (-1%) | 6.2 (-1%) | 6.4 (+2%) | 0 |
| dh:s10:1d | 428,670 | cold | 1,108 | 1,098 (-1%) | 1,093 (-1%) | 1,112 (+0%) | 221 |
| dh:s10:1d | 428,670 | warm | 27.3 | 21.5 (-21%) | 20.4 (-25%*) | 21.1 (-23%*) | 0 |
| dh_avg:s50:7d | 15,004,856 | cold | 4,112 | 3,777 (-8%*) | 3,342 (-19%*) | 3,094 (-25%*) | 605 |
| dh_avg:s50:7d | 15,004,856 | warm | 508 | 253 (-50%*) | 114 (-78%*) | 59.0 (-88%*) | 0 |
| dh_avg:s10:7d | 3,000,344 | cold | 3,068 | 3,043 (-1%) | 2,974 (-3%*) | 2,934 (-4%*) | 591 |
| dh_avg:s10:7d | 3,000,344 | warm | 69.8 | 64.7 (-7%*) | 37.9 (-46%) | 29.1 (-58%) | 0 |
| dh_avg:s10:1d | 428,670 | cold | 1,380 | 1,380 (+0%) | 1,360 (-1%) | 1,349 (-2%*) | 274 |
| dh_avg:s10:1d | 428,670 | warm | 33.0 | 27.9 (-15%*) | 23.6 (-29%*) | 23.0 (-30%*) | 0 |
| terms:s50:7d | 9,769,794 | cold | 4,409 | 4,539 (+3%) | 3,716 (-16%) | 3,732 (-15%*) | 625 |
| terms:s50:7d | 9,769,794 | warm | 740 | 495 (-33%*) | 282 (-62%*) | 239 (-68%*) | 0 |
| terms:s10:7d | 1,953,354 | cold | 3,302 | 3,395 (+3%) | 3,143 (-5%*) | 3,168 (-4%*) | 613 |
| terms:s10:7d | 1,953,354 | warm | 115 | 114 (-1%) | 71.2 (-38%*) | 62.4 (-46%*) | 0 |
| terms:s1:7d | 195,349 | cold | 2,926 | 2,953 (+1%) | 2,921 (-0%) | 2,908 (-1%) | 597 |
| terms:s1:7d | 195,349 | warm | 14.5 | 14.7 (+1%) | 11.2 (-23%) | 11.8 (-19%) | 0 |
| terms:s10:1d | 279,196 | cold | 1,167 | 1,155 (-1%) | 1,128 (-3%) | 1,125 (-4%) | 224 |
| terms:s10:1d | 279,196 | warm | 38.8 | 32.7 (-16%*) | 25.9 (-33%*) | 25.6 (-34%*) | 0 |

What it shows:
- Warm (CPU): -58% to -96% on the 7d queries at s50/s10, -19% to -34% at 1d and s1. Cold: only queries whose CPU was
  large gain (-15% to -25% at s50); the rest are within 4%, because cold time is about 4.8 ms x IOs, and the IOs do not
  change. That is what C3 (prefetch) is for: every 7d query reads all 209 `@timestamp` blocks and 354-377 `latency`
  or `service` blocks one dependent miss after another.
- No explicit SIMD yet. The gains come from removing per-doc calls (bulk reads, batch sub-aggregation calls, array
  instead of hash) and from not reading values one `RandomAccessInput` call at a time. Loops are simple enough for C2
  to unroll; whether it vectorizes them was not checked.
- Remaining warm profile, dh_avg:s50 vecdec (59 ms): `FixedBitSet.intoArray` (doc IDs out of the window bit set) 23%,
  `PackedSpans.gather` 22%, block switching in `VaryingBPVReader` 28%. terms:s50 vecdec (239 ms): the deferred
  avg's record (`PackedLongValues.Builder`) and replay are about 60%.
- Other CPU candidates for later: consume window bit sets word by word instead of materializing doc IDs; skip
  deferral when the sub-aggregations are cheap (avg); the user's change 3 (skipper sum/count per interval, so
  intervals fully inside the filter need no value reads).
- Explicit SIMD (Panama Vector API): parked (user decision). Warm-only gain on top of batching; it cannot change cold
  latency. Regression risks: short or sparse batches (s1, 1-day), scattered docs (NEON has no gather), heap-allocated
  vectors before C2 compiles the code, per-CPU differences in vector width and native instructions, changed
  floating-point sums, and a vector plus scalar path per width. Lucene's Panama doc-values decoder only vectorizes
  64-bit values with vectors of 32+ bytes, so on this Mac (16-byte NEON) it always runs scalar. If resumed: behind a
  switch, scalar below a minimum run length, check s1 and 1-day for regressions, measure on x86 too.

### C3a: where the cold reads go (`validate/DocValuesLayout.java` + `validate/dv_trace.py`; mode vecdec, 4 ms)

`DocValuesLayout.java` maps `.dvd`/`.dvs` byte ranges to field and region from the doc-values metadata.
`dv_trace.py` traces every block load of each query cold, maps it to a region, and runs the same query without
aggregations to separate the query's loads from the aggregation's. Files: `~/bufferpool-bench/results/c3a/`.
`.dvd` layout: `_seq_no` blocks 0-458, `@timestamp` 459-1032 (blocked, 1,832 value blocks, jump table in block 1032),
`service` ords 1032-1261 (8 bits), `status` ords 1261-1375 (4 bits), `latency` 1375-1728 (blocked, jump table in 1728);
skipper of `@timestamp` in `.dvs` blocks 0-1.

| Query | Took | Loads | Waited | Query alone | @timestamp values | latency values | service ords | points (kdd) | navigation |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| dh:s50:7d | 1,274 | 251 | 1,213 | 42 loads, 220 ms | 207 / 574 | – | – | 1 | 4 |
| dh:s10:7d | 1,225 | 237 | 1,175 | 31 loads, 169 ms | 207 / 574 | – | – | 1 | 4 |
| dh:s1:7d | 1,129 | 220 | 1,080 | 15 loads, 76 ms | 206 / 574 | – | – | 1 | 4 |
| dh:s10:1d | 1,152 | 221 | 1,083 | 136 loads, 707 ms | 83 / 574 | – | – | 110 | 4 |
| dh_avg:s50:7d | 3,324 | 605 | 2,994 | 42 loads, 221 ms | 207 / 574 | 352 / 354 | – | 1 | 6 |
| dh_avg:s10:7d | 3,034 | 591 | 2,905 | 31 loads, 172 ms | 207 / 574 | 352 / 354 | – | 1 | 6 |
| dh_avg:s10:1d | 1,419 | 274 | 1,335 | 136 loads, 712 ms | 83 / 574 | 51 / 354 | – | 110 | 6 |
| terms:s50:7d | 4,037 | 625 | 3,064 | 42 loads, 220 ms | – | 352 / 354 | 228 / 230 | 1 | 5 |
| terms:s10:7d | 3,373 | 613 | 3,010 | 31 loads, 165 ms | 1 / 574 | 352 / 354 | 228 / 230 | 0 | 7 |
| terms:s1:7d | 3,012 | 597 | 2,923 | 15 loads, 73 ms | 1 / 574 | 352 / 354 | 228 / 230 | 0 | 7 |
| terms:s10:1d | 1,207 | 224 | 1,086 | 136 loads, 715 ms | – | 51 / 354 | 34 / 230 | 110 | 5 |

(Took and waited in ms. "x / y" = blocks loaded / blocks of the region. Navigation = jump tables, skipper, terms data.)

Findings:
1. 90-97% of cold time is spent waiting on loads: every load is waited on, one after another.
2. Each doc-values field is read as one stream in strictly increasing block order (no backward jumps), so "next
   node" is well defined per field.
3. `latency` (avg) and `service` (terms ords) read every block of the matching doc range (352/354, 228/230), at every
   selectivity: even s1 (1% of docs) has a match in every 128 KiB node.
4. `@timestamp` under the skip-list date_histogram reads only 207 of 574 blocks: values are read only in skipper
   intervals that span a bucket boundary; the other intervals are counted from the skipper.
5. Navigation is 4-7 loads per query (about 20-35 ms). Prefetching it alone (C3b) is a small gain; it matters
   because C3c needs it (the jump table to map docs to nodes, the skipper to know which `@timestamp` nodes are read).
6. Query side: 1-day ranges run on points: 110 `.kdd` loads, about half the 1-day cold time (query alone 707-715 ms).
   Postings `.doc`: 4-34 sequential loads. Doc-values prefetch does not touch these.

Consequences for C3c:
- The proof must match what the collector reads. For fields read at every matching doc (`latency`, `service`), "the
  look-ahead iterator has a match in the node" is enough. For `@timestamp` under the skip-list collector, a node is
  read only if a match falls in a skipper interval that spans a bucket boundary, so the planner also checks the
  skipper; without that, about 367 of 574 nodes would be prefetched and never read.
- Fold C3b into C3c: prefetch the jump tables and the skipper when the leaf collector is created.
- Estimate (not measured): one node ahead keeps about 2 loads in flight per stream (as in Phase 3b), and streams run
  in parallel (8 prefetch threads). The longest stream then sets the time: about 352 x 4.8 / 2 = 0.85 s for 7-day
  dh_avg and terms (now 3.0-4.0 s), about 0.5 s for 7-day dh (now 1.1-1.3 s). 1-day queries stay bound by the 110
  points loads (about 0.5-0.7 s, now 1.1-1.4 s).

### C3c: doc-values prefetch (fork `690525375d`, `ab8781e6f8`; OpenSearch `3a7c3945537`; mode `pf` = vecdec + prefetch)

- Lucene: `NumericDocValues`/`SortedDocValues.nextPrefetchNodeDoc(doc, nodeBytes)` (first doc whose value starts in a
  later node) and `prefetchNodes` for dense Lucene90 numerics (packed, table, gcd, blocked) and dense sorted ordinals
  (`DocValuesNodes`). Blocked fields find blocks through the value jump table (prefetched on first use) and derive each
  block's bits per value from its length, so planning reads no value block; requests include the block header node.
  `DocValuesSkipper.prefetch()` requests the whole skipper. Each node is requested once. Test:
  `TestLucene90DocValuesPrefetch` checks the mapping against the bytes Lucene actually reads.
- OpenSearch `DocValuesPrefetch.Planner`: when collection reaches a doc in a new node of a field, it asks for the next
  node's first doc, finds the next doc at or after it that will be read, and requests that doc's node (one node
  ahead, doc-ID aligned). Proof of a read: a look-ahead scorer of the query (its own `Weight.scorer`), only advanced.
  The skip-list date_histogram adds a skipper check: no values are read in a level-0 interval that is dense and falls
  in one bucket. Wired into avg (latency), global-ordinal terms (service ords) and the skip-list collector (timestamp).

Validation (`validate/dv_trace.py --compare`, cold, all 11 queries): the blocks loaded with prefetch are exactly the
blocks loaded without it (plus one `.doc` block in the 1-day queries, read by the look-ahead scorer); prefetched but
never read: 0; aggregation results equal stock in every run.

Results (5 runs, `aggs_20261001_162306.json`, load average 4-7; p50 ms; `*` = CI excludes 0):

| Query | Mode | stock | vecdec | pf | pf vs vecdec | IOs |
|---|---|---:|---:|---:|---:|---:|
| dh:s50:7d | cold | 1,754 | 1,279 (-27%*) | 734 (-58%*) | -43% | 251 / 251 |
| dh:s50:7d | warm | 230 | 10.3 (-96%*) | 11.6 (-95%*) | +12% | 0 / 0 |
| dh:s10:7d | cold | 1,208 | 1,194 (-1%) | 681 (-44%*) | -43% | 237 / 237 |
| dh:s10:7d | warm | 12.6 | 12.2 (-3%) | 13.2 (+5%*) | +8% | 0 / 0 |
| dh:s1:7d | cold | 1,091 | 1,070 (-2%) | 595 (-45%*) | -44% | 220 / 220 |
| dh:s1:7d | warm | 4.9 | 5.6 (+14%) | 5.9 (+20%) | +5% | 0 / 0 |
| dh:s10:1d | cold | 1,166 | 1,129 (-3%*) | 984 (-16%*) | -13% | 221 / 222 |
| dh:s10:1d | warm | 24.0 | 18.0 (-25%*) | 39.1 (+63%*) | +117% | 0 / 0 |
| dh_avg:s50:7d | cold | 4,397 | 3,281 (-25%*) | 1,332 (-70%*) | -59% | 605 / 605 |
| dh_avg:s50:7d | warm | 426 | 52.5 (-88%*) | 53.7 (-87%*) | +2% | 0 / 0 |
| dh_avg:s10:7d | cold | 3,305 | 2,999 (-9%*) | 1,206 (-64%*) | -60% | 591 / 591 |
| dh_avg:s10:7d | warm | 55.3 | 23.7 (-57%*) | 25.8 (-53%*) | +9% | 0 / 0 |
| dh_avg:s10:1d | cold | 1,476 | 1,410 (-4%*) | 1,066 (-28%*) | -24% | 274 / 275 |
| dh_avg:s10:1d | warm | 28.9 | 19.3 (-33%*) | 58.0 (+101%*) | +200% | 0 / 0 |
| terms:s50:7d | cold | 4,836 | 4,150 (-14%*) | 1,954 (-60%*) | -53% | 625 / 625 |
| terms:s50:7d | warm | 698 | 218 (-69%*) | 221 (-68%*) | +2% | 0 / 0 |
| terms:s10:7d | cold | 3,626 | 3,303 (-9%*) | 1,710 (-53%*) | -48% | 613 / 613 |
| terms:s10:7d | warm | 105 | 54.1 (-48%*) | 55.1 (-47%*) | +2% | 0 / 0 |
| terms:s1:7d | cold | 2,995 | 2,974 (-1%*) | 1,689 (-44%*) | -43% | 597 / 597 |
| terms:s1:7d | warm | 14.2 | 10.6 (-25%) | 12.8 (-10%) | +21% | 0 / 0 |
| terms:s10:1d | cold | 1,208 | 1,151 (-5%*) | 1,006 (-17%*) | -13% | 224 / 225 |
| terms:s10:1d | warm | 35.8 | 22.3 (-38%*) | 60.7 (+69%*) | +172% | 0 / 0 |

Findings:
- Cold 7-day: -43% to -60% vs vecdec (-44% to -70% vs stock), with the same blocks loaded. Close to the C3a estimate for
  dh and dh_avg. terms is slower than the estimate because its two fields are read in two phases, not in parallel: the
  `service` ords during collection, then `latency` when the deferred avg is replayed.
- Cold 1-day: -13% to -24%; the 110 points (`.kdd`) loads of the query are not prefetched.
- Warm 7-day: within +2% to +12%. Warm 1-day: +63% to +200% (+21 to +39 ms). JFR on dh_avg:s10:1d: 42% of the time
  building the look-ahead scorers (each planner builds its own, and each runs the points intersection again), 28% in the
  planners' first advance: with the range as a bit set, Lucene's `BitSetConjunctionDISI` walks the `sel` postings doc by
  doc from doc 0 to the start of the 1-day range. The main query avoids both.
- Next for C3c: one shared look-ahead per segment, and a look-ahead that leapfrogs when a clause is a bit set.
### C3d: cheaper look-ahead (OpenSearch `946b6994555`, leak fix `049e9aee851`; fork `ab8781e6f8`)
Two fixes, each behind its own switch, so each one is measured alone and together
(`POST /_bufferpool/agg_batch?mode=pf|pfs|pfl|pfsl`; all include vecdec and pf):
- **shared (`pfs`)**: the planners of one search use a private `IndexSearcher` whose query cache keeps each non-term,
  non-Boolean clause's docs per segment. The points range intersection runs once per search for all planners instead of
  once per planner. A single shared iterator was rejected: planners advance to targets out of order, so one iterator
  would lose exactness. Materializing all matches was rejected: too much CPU on 7-day queries.
- **leapfrog (`pfl`)**: for a pure FILTER/MUST conjunction, build each clause's scorer with `get(Long.MAX_VALUE)` (its
  index structure, never a doc-values scan) and hide `BitSetIterator` behind `FilterDocIdSetIterator`, so
  `ConjunctionUtils.intersectIterators` leapfrogs with `advance` instead of `BitSetConjunctionDISI` walking the lead
  postings doc by doc. Falls back to the whole-query scorer when a clause is two-phase.
- Planners per query: `dh` 1 (timestamp), `dh_avg` 2 (timestamp, latency), `terms` 2 (service, latency). So shared can
  only help `dh_avg` and `terms`.
- Tests: `DocValuesPrefetchTests` (every share x leapfrog combination returns exactly the matches of the query under
  random out-of-order advances; release leaves nothing behind), `BatchCollectionTests` randomizes both switches
  (10 iterations each pass).
Leak found and fixed (`049e9aee851`): the first version used one `LRUQueryCache` per search. `LRUQueryCache` registers a
closed listener on each segment reader, so every per-search cache stayed reachable from the reader; the node ran out of
its 4 GB heap during JFR profiling (class histogram after 142 searches: 145 retained caches). The fix is a small
per-search cache with no reader listener, dropped when the search context closes (`SearchContext.addReleasable`).
Gauge `agg_prefetch_shared_searches` is 0 after the full benchmark; the histogram shows no retained cache. Latency
with the fix is the same as before it.
Validation (`validate/dv_trace.py --mode pfsl --compare trace_vecdec2.json`, cold, all 11 queries): prefetched but
never read: 0; no missing blocks; aggregation results equal in every mode. Extra loads vs vecdec: one `.doc` block in
the 1-day queries (as with pf) and one `.kdd` block in `terms:s10:7d` and `terms:s1:7d` (the look-ahead's points scorer).
Results (5 runs, `aggs_20261001_174255.json`, load average 5-6; p50 ms). A first run on the leaky build
(`aggs_20261001_171401.json`) gave the same relative deltas within a few percent; absolute times differ by up to 15%
between the runs because of machine load, so compare modes only within one run.
| Query | Mode | vecdec | pf | pfs | pfl | pfsl | IOs (vecdec / pfsl) |
|---|---|---:|---:|---:|---:|---:|---:|
| dh:s50:7d | cold | 1,444 | 824 | 829 | 817 | 830 | 251 / 251 |
| dh:s10:7d | cold | 1,355 | 766 | 754 | 764 | 769 | 237 / 237 |
| dh:s1:7d | cold | 1,216 | 664 | 662 | 663 | 665 | 220 / 220 |
| dh:s10:1d | cold | 1,228 | 1,025 | 1,044 | 1,007 | 1,003 | 221 / 222 |
| dh_avg:s50:7d | cold | 3,634 | 1,481 | 1,471 | 1,464 | 1,485 | 605 / 605 |
| dh_avg:s10:7d | cold | 3,287 | 1,315 | 1,313 | 1,322 | 1,321 | 591 / 591 |
| dh_avg:s10:1d | cold | 1,571 | 1,183 | 1,158 | 1,130 | 1,124 | 274 / 275 |
| terms:s50:7d | cold | 4,466 | 2,228 | 2,240 | 2,174 | 2,254 | 625 / 625 |
| terms:s10:7d | cold | 3,684 | 1,939 | 1,970 | 1,938 | 1,940 | 613 / 614 |
| terms:s1:7d | cold | 3,398 | 1,908 | 1,933 | 1,935 | 1,919 | 597 / 598 |
| terms:s10:1d | cold | 1,317 | 1,161 | 1,142 | 1,100 | 1,088 | 224 / 225 |
| dh:s50:7d | warm | 9 | 9 | 10 | 10 | 10 | 0 |
| dh:s10:7d | warm | 10 | 12 | 13 | 12 | 13 | 0 |
| dh:s1:7d | warm | 4 | 5 | 5 | 6 | 6 | 0 |
| dh:s10:1d | warm | 16 | 37 | 36 | 28 | 29 | 0 |
| dh_avg:s50:7d | warm | 54 | 57 | 56 | 56 | 56 | 0 |
| dh_avg:s10:7d | warm | 24 | 26 | 26 | 26 | 26 | 0 |
| dh_avg:s10:1d | warm | 19 | 62 | 49 | 48 | 35 | 0 |
| terms:s50:7d | warm | 209 | 210 | 210 | 212 | 213 | 0 |
| terms:s10:7d | warm | 55 | 56 | 56 | 56 | 56 | 0 |
| terms:s1:7d | warm | 10 | 11 | 11 | 11 | 11 | 0 |
| terms:s10:1d | warm | 21 | 62 | 50 | 47 | 34 | 0 |
Attribution: each change against the mode without it (median ratio; `*` = bootstrap 95% CI excludes 0; warm times
below 15 ms have 1 ms resolution, so +10% to +25% there is one tick and not significant):
| Query | Mode | pf vs vecdec | shared alone (pfs/pf) | leapfrog alone (pfl/pf) | shared after leapfrog (pfsl/pfl) | leapfrog after shared (pfsl/pfs) | both (pfsl/pf) | pfsl vs vecdec |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| dh:s10:1d | warm | +131%* | -3% | -24%* | +4% | -19%* | -22%* | +81%* |
| dh_avg:s10:1d | warm | +226%* | -21%* | -23%* | -27%* | -29%* | -44%* | +84%* |
| terms:s10:1d | warm | +195%* | -19%* | -24%* | -28%* | -32%* | -45%* | +62%* |
| dh:s10:1d | cold | -17%* | +2% | -2% | -0% | -4%* | -2%* | -18%* |
| dh_avg:s10:1d | cold | -25%* | -2% | -4%* | -1% | -3%* | -5%* | -28%* |
| terms:s10:1d | cold | -12%* | -2% | -5%* | -1% | -5%* | -6%* | -17%* |
| 7-day queries (8) | cold | -43% to -60%* | -2% to +2% | -2% to +1% | -0% to +4% | -2% to +2% | -0% to +1% | -43% to -60%* |
| 7-day queries (8) | warm | +0% to +25% | -2% to +11% | -2% to +20% | 0% to +8% | 0% to +20% | -2% to +20% | +2% to +50% (from pf itself; largest dh:s10:7d +3 ms*) |
Where the time goes (JFR, warm, 8 s loop per mode; search-thread samples split by stack: `build` = under
`DocValuesPrefetch.queryMatches`, `advance` = under `Planner.start/advance`, `main` = the rest; ms = share x median;
`validate/jfr_profile.py` + `validate/jfr_split.py`, files in `results/c3d/`; ratios from `validate/attribution.py`):
| Query | Mode | median | main | build | advance | samples in points code |
|---|---|---:|---:|---:|---:|---:|
| dh:s10:1d | vecdec | 19.7 | 19.7 | 0 | 0 | 80% |
| dh:s10:1d | pf | 39.3 | 17.0 | 13.8 | 8.5 | 70% |
| dh:s10:1d | pfs | 38.3 | 16.4 | 13.7 | 8.2 | 70% |
| dh:s10:1d | pfl | 32.2 | 17.4 | 14.3 | 0.5 | 89% |
| dh:s10:1d | pfsl | 31.6 | 17.0 | 14.1 | 0.5 | 88% |
| dh_avg:s10:1d | vecdec | 21.0 | 21.0 | 0 | 0 | 70% |
| dh_avg:s10:1d | pf | 63.2 | 19.7 | 27.1 | 16.4 | 64% |
| dh_avg:s10:1d | pfs | 50.4 | 19.7 | 14.1 | 16.6 | 54% |
| dh_avg:s10:1d | pfl | 47.7 | 19.5 | 27.4 | 0.8 | 86% |
| dh_avg:s10:1d | pfsl | 35.3 | 20.3 | 14.3 | 0.7 | 79% |
| terms:s10:1d | vecdec | 23.6 | 23.6 | 0 | 0 | 60% |
| terms:s10:1d | pf | 65.2 | 22.3 | 26.7 | 16.2 | 62% |
| terms:s10:1d | pfs | 52.2 | 22.3 | 13.7 | 16.4 | 51% |
| terms:s10:1d | pfl | 50.7 | 22.9 | 27.2 | 0.7 | 81% |
| terms:s10:1d | pfsl | 37.2 | 22.5 | 13.9 | 0.8 | 73% |
Findings:
- **shared** removes one points intersection per extra planner: build 27 ms -> 14 ms on the 2-planner queries
  (-19% to -21% warm), nothing on `dh` (1 planner). Its effect does not depend on leapfrog (-21% alone, -27% after).
- **leapfrog** removes the doc-by-doc lead walk: advance 8-16 ms -> 0.5-0.8 ms on every 1-day query (-23% to -24%
  warm alone, -19% to -32% combined). It also gives the small cold gain on 1-day queries (-2% to -5%): the planner
  reaches the range sooner, so the first prefetch is issued earlier.
- The two are independent and add up: both = -22% (dh) to -45% (dh_avg, terms) vs pf.
- 7-day queries: no change from either fix, cold or warm (the walk and the intersect are small next to collection).
- What is left on warm 1-day: +12 to +16 ms vs vecdec = exactly one points intersection (build, 14 ms) that the
  look-ahead runs in addition to the main query's own. The main query does not use the look-ahead's cache. Next fix
  candidate: run the range intersection once per search for the main query and the look-ahead (for example, the main
  query reads the same per-search clause cache, or the look-ahead reuses the main scorer's doc set). Expected: warm
  1-day within about 1 ms of vecdec.

### Status after C3d: look-ahead scorer rejected for the hot path
- Decision (user): no noticeable regression on the hot (warm) path is acceptable. C3c/C3d fail this: warm 1-day
  queries are +62% to +84% (+13 to +16 ms) vs vecdec with both fixes, and pf itself adds +2 to +3 ms warm on
  dh:s10:7d and dh:s1:7d. The cold gains (-43% to -60% on 7-day, -17% to -28% on 1-day) are not worth that.
- Root cause: the proof of a read came from a second scorer of the query. It re-runs the query (one points
  intersection, 14 ms on 1-day ranges) and advances it. The OR prefetch never needed this, because there the
  prefetched data (postings) is read by the iterator that owns it, so the structure alone proves the read. Doc values
  are read only at docs chosen by the query, so the proof needs future matches, and the main scorer only produces the
  current 4,096-doc window while a 128 KiB node spans 13-32 windows (~52k docs for `@timestamp`, ~85k `latency`,
  ~131k `service`).
- The pf/pfs/pfl/pfsl code stays in the branch behind its switches, so the C3c/C3d numbers stay reproducible.

### C3e plan: the main scorer runs ahead (no second scorer)
Same idea as the OR prefetch hook in `BooleanScorer`: hook the bulk scorer's window loop.
1. The bulk scorer computes window bit sets as today (`DenseConjunctionBulkScorer`, `ConstantScoreBulkScorer`), but
   keeps a small queue: it computes windows until it has seen matches up to the end of the next node, then collects
   the oldest window. Same windows, computed once, in a different order: no extra query work.
2. Each computed window is shown to the collector's planners through a new experimental `LeafCollector` call. A
   planner finds the first match in the next node of its field and requests that node: one node ahead, doc-ID
   aligned, proven by the query's own matches.
3. Residency check before each request: if the next node is already cached, no prefetch call is made, so a warm query
   only pays the check (per node, not per doc).
4. Per-doc bulk scorers (`DefaultBulkScorer`, sparse `ConjunctionBulkScorer`): buffer doc IDs ahead in an int array.
   The date_histogram skip-list collector keeps its skipper check and reads windows from the queue. Deleted docs and
   two-phase clauses are resolved when the windows are built, so matches stay exact.
- Memory: at most one node of windows per leaf (about 32 x 512 bytes = 16 KiB).
- Acceptance bar: every warm query within noise of vecdec (CI includes 0, or at most +1 ms); cold gains at least at
  the pfsl level; 0 prefetched-but-unread blocks; a microbenchmark of the per-node planner cost.
- Mode `pfw` (vecdec + window-driven prefetch), measured against vecdec and pfsl.

### C3e progress: prefetch proven by the main scorer (run-ahead)
Code (all pushed): Lucene fork `7460f3d5a5`, `4cbc71ac96` (`DocIdStream#intoBitSet`), `4574e3d861` (`FixedBitSet#intoArray`
fix); OpenSearch `cf9ac6c51e5` (pfw), `a2d98a1d078` (date_histogram fix), `97168906ca8` (form-preserving delivery, gate).
- **Design as built** (no Lucene bulk-scorer change). `AggregatorBase.getLeafCollector` of a top-level aggregation opens a
  `DocValuesPrefetch.RunAhead`; planners of that leaf tree register with it, and if any does, the leaf collector is
  wrapped. The wrapper copies each scorer window into a bit set (`DocIdStream#intoBitSet`, 64 words per window) and
  hands the docs to the real collectors `docs` doc IDs later (default 131,072). Planners find the first match at or
  after the next node in the buffer (`Matches.next`, or UNKNOWN until arrivals reach it, then a retry on arrival).
  Docs are handed on in the form the scorer used: one at a time, as streams or as ranges, so the collectors keep the
  stock code path. Memory: 2 x lag bits (32 KiB; 64 KiB with the gate) plus a 16k-doc ring for single docs.
- Deferred replay (terms -> avg): `BestBucketsDeferringCollector.replayBatch` replays through a ring of 16 replay chunks
  (`DocValuesPrefetch.Replay`), so the avg planner sees replayed docs ahead; no doc is decoded twice.
- **Gate (mode `pfwg`)**: collection waits at a planner's next requested doc until the read after that doc's node is known
  (up to the buffer capacity). Needed for date_histogram without a sub-aggregation, which reads `@timestamp` only in
  skipper intervals that span two buckets (1h buckets: about one read every 178k docs, more than the lag): without the
  gate the request for the next read goes out only lag docs before it, and there is almost no collection time in
  between, so no IO overlap.
- Planners search forward only (one cursor and a cached next read per planner), so a search never restarts inside a
  skipper interval the filter already skipped.
- Modes: `pfw` = vecdec + run-ahead; `pfwg` = pfw + gate (`POST /_bufferpool/agg_batch?mode=pfw|pfwg[&docs=N]`). The
  look-ahead-scorer modes (pf, pfs, pfl, pfsl) stay for comparison.
- Tests: `DocValuesRunAheadTests` (random windows, single docs and ranges; every match collected once, in order, in its
  arrival form; requests are exactly the first match of every node and go out before that doc is collected; replay
  ring), `BatchCollectionTests` randomizes run-ahead, lag and gate, plus `testShuffledTimeRuns` and
  `testPaddedWindowStreams`.

**Found on the way**
1. **Stock date_histogram bug (upstream).** `HistogramSkiplistLeafCollector.collect(DocIdStream)` advanced its skipper to
   the end of the docs it had consumed whenever `stream.mayHaveRemaining()` was true. Lucene's window streams are backed by
   4,096-bit sets and report remaining docs past the end of a shorter window; the next window can start there, and its
   first docs were counted in the bucket of a later skipper interval. On logs_v1 `dh:s50:7d` stock moved 6 docs between
   two buckets (checked against the same histogram with `hard_bounds`, which turns the skip list off). The code comes from
   upstream `da18cc6e635` (#19573). Fix `a2d98a1d078`: position the skipper at the stream's next doc;
   `testPaddedWindowStreams` fails 8 of 10 times without it. pfw found it because its streams end exactly at the last doc.
   All modes now match the no-skip-list reference on all dh queries. Worth an upstream issue.
2. **Lucene `FixedBitSet#intoArray`** scanned every word to the end of the range even when the array was full: O(range)
   per call. Fixed in the fork (`4574e3d861`); results unchanged.
3. **Flaky C2 test**: `BatchCollectionTests`' stream-run assertion failed about 2.5% of seeds because a random merge
   policy shuffled doc order (no skipper interval in one bucket). The test index now uses `newLogMergePolicy()`.
- Validation (`validate/dv_trace.py --mode pfw --compare trace_vecdec2.json`, cold, all 11 queries,
  `results/c3e/trace_pfw.json`): loaded blocks are exactly vecdec's (no extra `.doc` / `.kdd` block, unlike pfsl);
  prefetched but never read: 0.

**Measurements so far** (5 runs; each column is measured against vecdec in its own run, because machine load differed
between runs; `*` = bootstrap 95% CI excludes 0; wall-clock medians):
- (1) `aggs_20261001_203753.json`: first pfw, with the date_histogram fix, load average 8-9.
- (2) `aggs_20261001_204746.json`: + Lucene `FixedBitSet#intoArray` fix, load average 7-10.
- (3) `aggs_20261001_211753.json`: + form-preserving delivery and the word-skipping reader, and pfwg; load average
  9-20 (noisy).

| Query | Mode | vecdec (3) ms | pfsl (3) | pfw (1) | pfw (2) | pfw (3) | pfwg (3) |
|---|---|---:|---:|---:|---:|---:|---:|
| dh:s50:7d | cold | 1,271.6 | -41%* (-524.0 ms) | -16%* (-204.2 ms) | -17%* (-219.2 ms) | -16%* (-199.4 ms) | -41%* (-516.6 ms) |
| dh:s10:7d | cold | 1,188.0 | -44%* (-521.0 ms) | -13%* (-154.3 ms) | -12%* (-149.6 ms) | -13%* (-149.7 ms) | -39%* (-468.3 ms) |
| dh:s1:7d | cold | 1,091.7 | -45%* (-491.4 ms) | -8%* (-84.8 ms) | -8%* (-92.0 ms) | -9%* (-92.9 ms) | -40%* (-440.6 ms) |
| dh:s10:1d | cold | 1,115.6 | -18%* (-203.0 ms) | -18%* (-202.0 ms) | -18%* (-211.5 ms) | -19%* (-207.6 ms) | -18%* (-200.1 ms) |
| dh_avg:s50:7d | cold | 3,191.8 | -59%* (-1898.5 ms) | -59%* (-1849.1 ms) | -59%* (-1931.1 ms) | -58%* (-1844.8 ms) | -59%* (-1884.9 ms) |
| dh_avg:s10:7d | cold | 2,970.8 | -60%* (-1771.7 ms) | -59%* (-1744.2 ms) | -59%* (-1745.2 ms) | -58%* (-1732.8 ms) | -59%* (-1765.8 ms) |
| dh_avg:s10:1d | cold | 1,385.7 | -29%* (-398.5 ms) | -31%* (-415.4 ms) | -31%* (-438.3 ms) | -31%* (-424.3 ms) | -31%* (-430.2 ms) |
| terms:s50:7d | cold | 3,613.3 | -46%* (-1651.6 ms) | -25%* (-935.4 ms) | -27%* (-1053.1 ms) | -20%* (-739.1 ms) | -20%* (-739.6 ms) |
| terms:s10:7d | cold | 3,245.1 | -47%* (-1516.4 ms) | -47%* (-1686.8 ms) | -47%* (-1491.0 ms) | -47%* (-1528.3 ms) | -47%* (-1516.1 ms) |
| terms:s1:7d | cold | 2,972.6 | -43%* (-1271.3 ms) | -43%* (-1264.7 ms) | -43%* (-1280.3 ms) | -42%* (-1261.1 ms) | -43%* (-1265.7 ms) |
| terms:s10:1d | cold | 1,190.9 | -17%* (-208.3 ms) | -19%* (-212.1 ms) | -18%* (-205.1 ms) | -20%* (-240.4 ms) | -20%* (-239.1 ms) |
| dh:s50:7d | warm | 11.5 | +65%* (+7.4 ms) | +41%* (+4.6 ms) | +0% (+0.0 ms) | +24%* (+2.8 ms) | +18%* (+2.1 ms) |
| dh:s10:7d | warm | 13.1 | +19%* (+2.5 ms) | +8%* (+1.1 ms) | +3% (+0.3 ms) | +6% (+0.8 ms) | +3% (+0.4 ms) |
| dh:s1:7d | warm | 7.5 | +28% (+2.1 ms) | -2% (-0.1 ms) | -4% (-0.3 ms) | +29% (+2.2 ms) | +37% (+2.8 ms) |
| dh:s10:1d | warm | 17.8 | +86%* (+15.4 ms) | +1% (+0.2 ms) | +1% (+0.2 ms) | +6%* (+1.1 ms) | +3% (+0.5 ms) |
| dh_avg:s50:7d | warm | 63.2 | +5% (+3.3 ms) | +11%* (+6.6 ms) | +4% (+2.0 ms) | +29%* (+18.1 ms) | +31%* (+19.4 ms) |
| dh_avg:s10:7d | warm | 29.7 | +9%* (+2.5 ms) | +8%* (+2.3 ms) | +5%* (+1.4 ms) | -1% (-0.4 ms) | -1% (-0.3 ms) |
| dh_avg:s10:1d | warm | 20.1 | +68%* (+13.7 ms) | +3% (+0.7 ms) | +2% (+0.4 ms) | +4% (+0.8 ms) | +4% (+0.7 ms) |
| terms:s50:7d | warm | 221.4 | +3% (+6.2 ms) | +8%* (+16.4 ms) | +6% (+13.2 ms) | +11%* (+24.3 ms) | +13%* (+28.6 ms) |
| terms:s10:7d | warm | 58.0 | +6% (+3.8 ms) | +5%* (+2.8 ms) | +3%* (+1.6 ms) | +2% (+1.3 ms) | +1% (+0.5 ms) |
| terms:s1:7d | warm | 10.8 | +6% (+0.6 ms) | +34%* (+4.2 ms) | +39%* (+4.9 ms) | +19%* (+2.1 ms) | +19%* (+2.1 ms) |
| terms:s10:1d | warm | 22.9 | +57%* (+13.0 ms) | -1% (-0.4 ms) | -2% (-0.6 ms) | +1% (+0.2 ms) | +1% (+0.1 ms) |

Findings so far:
- **Warm 1-day: solved.** All three 1-day queries are within noise of vecdec in pfw and pfwg (+0.1 to +1.1 ms), against
  +13 to +15 ms for pfsl.
- **Cold: the gate brings dh back to the pfsl level** (-39% to -41% on dh 7-day, against -8% to -16% without it), and
  pfw/pfwg match pfsl on dh_avg and terms s10/s1 and on every 1-day query. Open: `terms:s50:7d` cold is -20% (pfsl -46%).
- **Each overhead fix, separately**:
  - Lucene `intoArray` fix ((1) -> (2)): dh:s50:7d warm +4.6 ms -> 0.0 ms. The skip-list collector reads one doc per
    skipper interval with a 1-element `intoArray`, which scanned the rest of the run-ahead range each time.
  - Form-preserving delivery + word-skipping reader ((2) -> (3)): terms:s1:7d warm +4.9 -> +2.1 ms (single docs were sent
    through the bulk path, which is slower for sparse docs). But the bit-by-bit reader made dense 7-day queries slower:
    dh_avg:s50:7d +2.0 -> +18 ms, terms:s50:7d +13 -> +24 ms (15M and 9.8M docs read one bit at a time instead of
    Lucene's branch-free dense word decoder).
- **Not yet acceptable**: warm dense 7-day queries (dh_avg s50 +18 ms, terms s50 +24 ms, dh s50 +2 ms) and terms s1 7d
  (+2 ms).
- Next: decode dense words branch-free in the run-ahead reader (in progress, uncommitted), re-measure; find the
  terms:s50:7d cold gap; JFR of pfw vs vecdec on the dense 7-day queries; microbenchmark of the per-node planner cost.

**C3e continued: overhead fixes, each measured** (OpenSearch `1d10d60d8b8`, `02b54288848`, `747abd4dd75`, `8f47295980b`)
- (4) `aggs_20261001_220507.json`, `1d10d60d8b8`, load 4-5: dense words decoded branch-free (same decoder as Lucene's
  `FixedBitSet`), and the stream position never moves past its end (a bug the tests caught). dh_avg:s50:7d warm +18 ->
  +7 ms, dh:s50:7d +2.8 -> +0.2 ms.
- (5) `aggs_20261001_222504.json`, `02b54288848`, load 5-8: single-doc arrivals take a fast path (set the bit, append to
  the ring; run bookkeeping only when a planner waits, a delivery is due or the buffer needs room), and the wrapper takes
  the scorer's `collect(doc)` directly. JFR before: arrival + delivery were about 20% of dh:s1:7d. terms:s1:7d warm
  +3.8 -> +0.8 ms, dh_avg:s50:7d +7 -> +0.7 ms.
- `747abd4dd75`: the replay ring may hold up to 128 chunks (allocated as needed, 1.5 MiB at most). With 16 chunks a dense
  replay (terms s50) had only about 32k doc IDs of lead, less than one `latency` node: terms:s50:7d cold -26% -> -48%
  (checked alone with 96 chunks before the commit).
- (6) `aggs_20261001_225751.json`, `8f47295980b`, load 5-8: the replay loop fills the ring's arrays itself, in its own
  method (as tight as the plain loop; JFR showed `PackedLongValues.Iterator.next` no longer inlined in the shared loop).
  terms:s50:7d warm, interleaved 5 x 10 runs: +12.5 -> +9.7 ms; independent of the lag (4,096 to 131,072 docs: +9 ms),
  so not a cache-footprint effect. Planner code is 0.1% of samples.

| Query | Mode | vecdec (6) ms | pfsl (6) | pfwg (4) | pfwg (5) | pfw (6) | pfwg (6) |
|---|---|---:|---:|---:|---:|---:|---:|
| dh:s50:7d | cold | 1,430.9 | -43%* (-609.8 ms) | -41%* (-520.1 ms) | -41%* (-527.8 ms) | -17%* (-238.1 ms) | -40%* (-576.4 ms) |
| dh:s10:7d | cold | 1,323.8 | -42%* (-557.4 ms) | -41%* (-485.4 ms) | -41%* (-484.8 ms) | -11%* (-149.5 ms) | -40%* (-523.0 ms) |
| dh:s1:7d | cold | 1,224.9 | -44%* (-539.0 ms) | -40%* (-440.8 ms) | -41%* (-442.8 ms) | -8%* (-94.6 ms) | -40%* (-489.6 ms) |
| dh:s10:1d | cold | 1,260.4 | -17%* (-219.7 ms) | -18%* (-200.2 ms) | -18%* (-205.9 ms) | -18%* (-229.6 ms) | -18%* (-222.8 ms) |
| dh_avg:s50:7d | cold | 3,584.2 | -59%* (-2105.5 ms) | -60%* (-1998.9 ms) | -60%* (-1895.8 ms) | -58%* (-2092.4 ms) | -60%* (-2136.8 ms) |
| dh_avg:s10:7d | cold | 3,350.0 | -60%* (-2004.4 ms) | -60%* (-1809.8 ms) | -59%* (-1734.8 ms) | -58%* (-1942.3 ms) | -59%* (-1993.1 ms) |
| dh_avg:s10:1d | cold | 1,539.8 | -29%* (-439.7 ms) | -31%* (-420.8 ms) | -32%* (-444.9 ms) | -30%* (-464.7 ms) | -30%* (-454.4 ms) |
| terms:s50:7d | cold | 4,280.3 | -49%* (-2077.3 ms) | -26%* (-998.0 ms) | -28%* (-1130.9 ms) | -48%* (-2050.4 ms) | -47%* (-2030.9 ms) |
| terms:s10:7d | cold | 3,605.5 | -47%* (-1712.4 ms) | -48%* (-1552.4 ms) | -47%* (-1517.1 ms) | -47%* (-1696.0 ms) | -47%* (-1703.8 ms) |
| terms:s1:7d | cold | 2,987.3 | -44%* (-1318.8 ms) | -43%* (-1268.3 ms) | -43%* (-1271.3 ms) | -42%* (-1257.0 ms) | -44%* (-1303.0 ms) |
| terms:s10:1d | cold | 1,112.1 | -17%* (-186.3 ms) | -18%* (-204.2 ms) | -19%* (-217.0 ms) | -17%* (-194.4 ms) | -19%* (-205.9 ms) |
| dh:s50:7d | warm | 10.2 | +19%* (+1.9 ms) | +7% (+0.7 ms) | +14% (+1.6 ms) | +8% (+0.9 ms) | +8% (+0.9 ms) |
| dh:s10:7d | warm | 11.0 | +24%* (+2.6 ms) | +7% (+0.9 ms) | -2% (-0.2 ms) | +14%* (+1.5 ms) | +13%* (+1.4 ms) |
| dh:s1:7d | warm | 6.5 | +22%* (+1.4 ms) | +78%* (+4.6 ms) | +27%* (+2.0 ms) | +32%* (+2.1 ms) | +27%* (+1.7 ms) |
| dh:s10:1d | warm | 16.0 | +83%* (+13.3 ms) | +0% (+0.1 ms) | +1% (+0.1 ms) | +7% (+1.1 ms) | +4% (+0.6 ms) |
| dh_avg:s50:7d | warm | 54.6 | +4%* (+2.0 ms) | +12%* (+6.7 ms) | +1% (+0.7 ms) | +4%* (+2.0 ms) | +4%* (+2.1 ms) |
| dh_avg:s10:7d | warm | 26.8 | +5%* (+1.5 ms) | -0% (-0.1 ms) | -2% (-0.7 ms) | -7% (-1.8 ms) | -8% (-2.0 ms) |
| dh_avg:s10:1d | warm | 19.7 | +68%* (+13.4 ms) | +3% (+0.7 ms) | +1% (+0.3 ms) | +2% (+0.5 ms) | -1% (-0.2 ms) |
| terms:s50:7d | warm | 214.2 | +2% (+4.4 ms) | +5% (+11.3 ms) | +4%* (+8.0 ms) | +3%* (+5.5 ms) | +4% (+7.8 ms) |
| terms:s10:7d | warm | 56.7 | +2% (+0.9 ms) | +0% (+0.2 ms) | -0% (-0.2 ms) | -1% (-0.4 ms) | -1% (-0.3 ms) |
| terms:s1:7d | warm | 13.1 | +17%* (+2.2 ms) | +36%* (+4.6 ms) | +6% (+0.8 ms) | +10%* (+1.3 ms) | +9%* (+1.1 ms) |
| terms:s10:1d | warm | 23.5 | +52%* (+12.1 ms) | -0% (-0.0 ms) | +1% (+0.2 ms) | +0% (+0.1 ms) | -3% (-0.6 ms) |

Status after (6):
- **Cold: pfwg now matches pfsl on every query** (dh 7-day -40%, dh_avg -59/-60%, terms -44/-47%, 1-day -18/-30%), with
  exactly vecdec's loads and no unread prefetch.
- **Warm 1-day: within noise** (-0.6 to +0.6 ms).
- **Warm residuals (pfwg)**: dh:s1:7d +1.7 ms (+27%*), dh:s10:7d +1.4 ms (+13%*), terms:s1:7d +1.1 ms (+9%*), dh_avg:s50:7d
  +2.1 ms (+4%*), dh:s50:7d +0.9 ms, terms:s50:7d +7.8 ms (+4%, CI includes 0). These are the buffer's own cost: copying
  matches in and out (about 1 ns per doc for windows, a few ns per doc for single docs), which shows on queries whose
  per-doc work is small. Not yet within the bar.
- Next: skip the buffer when the data is already cached. When collection enters a node, check (without IO) whether the
  next node of the field is in the cache; while it is, hand docs straight to the collectors (no copy, no delay), and
  start buffering at the first node that is not cached. Warm queries then pay one residency check per node.

**C3e: pass-through while cached (mode `pfwc` = pfwg + pass-through)** (Lucene fork `1373677cfb`; OpenSearch
`ff59b07d23a`, `f86006df955`, `8f0436bef13`)
- **Residency API (Lucene `1373677cfb`)**: `RandomAccessInput#isLoaded(offset, length)` returns `Optional<Boolean>`
  (empty = the input cannot tell); `NumericDocValues` / `SortedDocValues#isNodeLoaded(doc, nodeBytes)` answer for the
  node(s) holding a doc's value (`DocValuesNodes.isLoaded`; the Filter wrappers forward it). The bufferpool answers from
  its block cache (`BlockCache.contains`, `BufferPoolIndexInput.isLoaded`), no IO and no cache-entry touch. Default:
  unknown, which planners treat as not cached (so stock directories always buffer).
- **Design**: a run-ahead leaf (and a replay ring) starts in pass-through: docs go straight to the collectors in their
  arrival form, nothing is copied or delayed. Each planner checks, once per node it reads from, whether that node is
  cached. At the first read node that is not, the leaf starts buffering for good (that read is a miss either way) and the
  planner plans from the next node with the matches that arrive from then on.
- **Rejected: checking the next node by position** (first pfwc version, `ff59b07d23a`, run (7)): date_histogram reads
  `@timestamp` once per skipper interval that spans two buckets, so most `@timestamp` nodes are never read and never
  cached. The next node by position was then almost always "not cached", and every warm dh query switched to buffering
  (dh:s1:7d +28%*, dh:s10:7d +13%). Fix `f86006df955`: check the node being read. Warm run-ahead switches went from many
  to 0 on all 11 queries.
- **Replay without the ring in pass-through** (`8f0436bef13`): in run (8) the only residual was warm terms 7d (s50
  +10.4 ms / +5%*, s10 +4.1 ms). JFR (interleaved with vecdec, terms:s50:7d, about 7,500 samples each): the main
  collection was equal (+1.6 samples per query, noise), the deferred replay was +5.9 samples per query, all of it in
  the replay loop (`replayBatch` 31.4 -> `replayAhead` + `Replay.sealFull` 37.0): in pass-through every 1,024-doc chunk
  was still sealed in the ring and handed over at once. Now pass-through runs the plain replay loop
  (`replayPassThrough`, `Replay.passThrough` sets `arrived` and hands the chunk over); when a planner leaves
  pass-through, the rest of the segment continues in the ring loop from the next recorded doc. JFR after: replay
  71.1 vs 71.5 and collection 86.9 vs 86.4 samples per query (vecdec vs pfwc), equal. Interleaved 5 x 10:
  terms:s50:7d +9 ms -> +1.6 ms, terms:s10:7d +2 ms -> +0.3 ms. Test: `BestBucketsDeferringCollectorTests
  .testReplayAheadPassThrough` (random cached prefix, switch in the middle of a segment: every selected doc once, in
  order, rebased; requests are read docs, one per node, before collection).
- Validation (`validate/dv_trace.py --mode pfwc`, cold, `results/c3e/trace_pfwc2.json` after `8f0436bef13`): loads
  equal vecdec's on all 11 queries, prefetched but never read: 0.
- Runs: (7) `aggs_20261001_235736.json` (`ff59b07d23a`, load 14-109, noisy); (8) `aggs_20261002_001203.json`
  (`f86006df955`, load 5-9); (9) `aggs_20261002_022618.json` (node built from the `8f0436bef13` tree before the commit,
  so the file records `f86006df955`; load 5 until terms:s50:7d, then 260-290: the last three terms queries of (9) are not
  usable); (10) `aggs_20261002_104605.json` (`8f0436bef13`, terms queries only, load 3-6).

| Query | Mode | vecdec (9) ms | pfsl (9) | pfwg (9) | pfwc (7) | pfwc (8) | pfwc (9) |
|---|---|---:|---:|---:|---:|---:|---:|
| dh:s50:7d | cold | 1,271.6 | -44%* (-563.4 ms) | -42%* (-538.2 ms) | -40%* (-511.3 ms) | -40%* (-512.8 ms) | -41%* (-526.7 ms) |
| dh:s10:7d | cold | 1,177.2 | -44%* (-520.2 ms) | -41%* (-481.4 ms) | -41%* (-484.5 ms) | -41%* (-490.6 ms) | -42%* (-489.2 ms) |
| dh:s1:7d | cold | 1,074.5 | -45%* (-480.3 ms) | -41%* (-440.3 ms) | -41%* (-454.5 ms) | -41%* (-452.0 ms) | -42%* (-449.9 ms) |
| dh:s10:1d | cold | 1,124.0 | -17%* (-192.0 ms) | -19%* (-213.3 ms) | -18%* (-199.2 ms) | -18%* (-202.0 ms) | -17%* (-190.5 ms) |
| dh_avg:s50:7d | cold | 3,231.6 | -59%* (-1916.4 ms) | -60%* (-1930.4 ms) | -59%* (-1791.4 ms) | -60%* (-1977.9 ms) | -59%* (-1921.0 ms) |
| dh_avg:s10:7d | cold | 2,988.7 | -60%* (-1784.4 ms) | -60%* (-1781.5 ms) | -59%* (-1737.3 ms) | -59%* (-2007.0 ms) | -59%* (-1767.5 ms) |
| dh_avg:s10:1d | cold | 1,376.9 | -27%* (-378.3 ms) | -30%* (-418.6 ms) | -30%* (-415.1 ms) | -29%* (-463.6 ms) | -30%* (-409.0 ms) |
| terms:s50:7d | cold | 3,857.4 | -50%* (-1919.9 ms) | -49%* (-1872.2 ms) | -51%* (-2017.1 ms) | -53%* (-2337.0 ms) | -49%* (-1877.5 ms) |
| terms:s10:7d | cold | 3,696.2 | -47%* (-1749.4 ms) | -47%* (-1741.4 ms) | -47%* (-1514.1 ms) | -46%* (-1690.3 ms) | -47%* (-1736.0 ms) |
| terms:s1:7d | cold | 3,373.1 | -42% (-1404.1 ms) | -34% (-1133.1 ms) | -43%* (-1274.2 ms) | -42%* (-1433.9 ms) | -35% (-1173.1 ms) |
| terms:s10:1d | cold | 1,249.3 | -18%* (-227.8 ms) | -20%* (-251.6 ms) | -19%* (-217.9 ms) | -17%* (-214.2 ms) | -21%* (-260.2 ms) |
| dh:s50:7d | warm | 10.7 | +14% (+1.5 ms) | +8% (+0.9 ms) | +8% (+0.9 ms) | +1% (+0.1 ms) | -2% (-0.2 ms) |
| dh:s10:7d | warm | 13.2 | +12%* (+1.6 ms) | +3% (+0.3 ms) | +13% (+1.5 ms) | -2% (-0.2 ms) | -2% (-0.3 ms) |
| dh:s1:7d | warm | 7.0 | +62%* (+4.3 ms) | +30% (+2.1 ms) | +28%* (+2.0 ms) | +5% (+0.4 ms) | +4% (+0.3 ms) |
| dh:s10:1d | warm | 17.7 | +77%* (+13.7 ms) | +3% (+0.6 ms) | +0% (+0.0 ms) | +2% (+0.4 ms) | -2% (-0.4 ms) |
| dh_avg:s50:7d | warm | 57.2 | +6%* (+3.6 ms) | +11%* (+6.5 ms) | +5%* (+2.5 ms) | +0% (+0.3 ms) | +4%* (+2.5 ms) |
| dh_avg:s10:7d | warm | 26.8 | +9%* (+2.5 ms) | +5%* (+1.4 ms) | -2% (-0.5 ms) | -1% (-0.3 ms) | +3% (+0.9 ms) |
| dh_avg:s10:1d | warm | 19.9 | +67%* (+13.4 ms) | +0% (+0.0 ms) | +3% (+0.6 ms) | +0% (+0.1 ms) | -2% (-0.3 ms) |
| terms:s50:7d | warm | 211.4 | +1% (+2.2 ms) | +8%* (+15.9 ms) | +2% (+3.4 ms) | +5%* (+10.4 ms) | +1% (+1.8 ms) |
| terms:s10:7d | warm | 143.5 | +6%* (+8.5 ms) | +8%* (+11.4 ms) | +5% (+3.1 ms) | +7% (+4.1 ms) | +5% (+6.7 ms) |
| terms:s1:7d | warm | 24.8 | +2% (+0.5 ms) | -0% (-0.1 ms) | +10% (+1.0 ms) | +2% (+0.3 ms) | +24% (+5.9 ms) |
| terms:s10:1d | warm | 40.2 | +54%* (+21.8 ms) | -1% (-0.3 ms) | +0% (+0.0 ms) | -1% (-0.3 ms) | -1% (-0.2 ms) |

Run (10), terms queries again at low load:

| Query | Mode | vecdec (10) ms | pfsl (10) | pfwg (10) | pfwc (10) |
|---|---|---:|---:|---:|---:|
| terms:s50:7d | cold | 4,037.8 | -51%* (-2069.7 ms) | -51%* (-2063.4 ms) | -51%* (-2055.1 ms) |
| terms:s10:7d | cold | 3,284.4 | -47%* (-1554.2 ms) | -47%* (-1554.9 ms) | -47%* (-1558.6 ms) |
| terms:s1:7d | cold | 2,973.6 | -43%* (-1283.9 ms) | -43%* (-1273.0 ms) | -42%* (-1262.1 ms) |
| terms:s10:1d | cold | 1,160.4 | -15%* (-179.6 ms) | -18%* (-213.7 ms) | -18%* (-207.0 ms) |
| terms:s50:7d | warm | 208.4 | +2%* (+4.6 ms) | +5%* (+11.0 ms) | +1%* (+2.0 ms) |
| terms:s10:7d | warm | 55.0 | +3% (+1.5 ms) | +3% (+1.7 ms) | +0% (+0.1 ms) |
| terms:s1:7d | warm | 9.4 | +18%* (+1.7 ms) | +19%* (+1.8 ms) | +2% (+0.2 ms) |
| terms:s10:1d | warm | 21.3 | +60%* (+12.7 ms) | +1% (+0.3 ms) | +1% (+0.2 ms) |

Status after (10) (pfwc):
- **Warm: within noise of vecdec on all 11 queries.** dh / dh_avg in (9): -0.4 to +0.9 ms, except dh_avg:s50:7d
  +2.5 ms (+4%*), which was -0.8 ms interleaved over 5 x 10 runs and +0.3 ms over 10 x 10 (this change does not
  touch it; (8) had +0.3 ms);
  dh:s1:7d +0.2 ms interleaved (10 x 10). terms in (10): s50 7d +2.0 ms (+1%), s10 7d +0.1 ms, s1 7d +0.2 ms, s10 1d
  +0.2 ms. In the same runs pfsl is up to +77% and pfwg up to +30%.
- **Cold: equal to pfsl and pfwg**: dh 7-day -41/-42%, dh_avg 7-day -59%, terms 7-day -42% to -51%, 1-day -17% to
  -30%; same loads as vecdec, no unread prefetch.
- Attribution of the C3e steps (warm, against vecdec of the same run): buffer copy-in/out (pfw/pfwg) cost up to +2 ms on
  sparse dh and +8 to +16 ms on terms s50 7d; pass-through removes the main-collection part when the read nodes are
  cached ((8): dh within noise); replay pass-through removes the replay part ((10): terms s50 7d +10.4 -> +2.0 ms).
- A partly cached leaf: pass-through until the first uncached read node, buffering from there on (no switch back).
- Pippin doc updated: section "Phase 5 (C3e, done)", status line, appendix tables for runs (4)-(10).
- Next: microbenchmark of the per-node planner and pass-through cost; then points (`.kdd`) prefetch for 1-day ranges.

**Hot-path latency, stock vs vecdec vs pfwc** (interleaved `/tmp/qtime.py`, 5 rounds x 10 requests after 3 warm-up per
version, median wall ms; OpenSearch `a44ccc13989`, fork `1373677cfb`; load 4-5)

| Query | Stock ms | vecdec ms | pfwc ms | pfwc vs vecdec | pfwc vs stock |
|---|---:|---:|---:|---:|---:|
| dh:s50:7d | 237.7 | 10.5 | 10.1 | -0.3 ms (-3%) | -96% |
| dh:s10:7d | 12.2 | 11.6 | 11.3 | -0.2 ms (-2%) | -7% |
| dh:s1:7d | 5.0 | 5.5 | 5.8 | +0.2 ms (+4%) | +15% |
| dh:s10:1d | 23.1 | 17.4 | 17.9 | +0.5 ms (+3%) | -23% |
| dh_avg:s50:7d | 460.5 | 55.0 | 55.3 | +0.3 ms (+1%) | -88% |
| dh_avg:s10:7d | 58.7 | 25.5 | 25.7 | +0.2 ms (+1%) | -56% |
| dh_avg:s10:1d | 29.4 | 19.9 | 20.0 | +0.0 ms (+0%) | -32% |
| terms:s50:7d | 688.0 | 213.2 | 214.3 | +1.1 ms (+1%) | -69% |
| terms:s10:7d | 107.4 | 58.2 | 58.2 | -0.1 ms (-0%) | -46% |
| terms:s1:7d | 13.5 | 11.1 | 11.2 | +0.2 ms (+1%) | -16% |
| terms:s10:1d | 35.3 | 23.2 | 23.3 | +0.1 ms (+1%) | -34% |

- Open: dh:s1:7d is 0.8 ms slower than stock (0.6 ms from C2, 0.2 ms from prefetch); rounds overlap, needs more rounds.

## Stage 4: numeric sort and BKD (requested; in progress)

Goal: cold latency of `sort` on `@timestamp` (asc and desc, with and without filters). These queries do not use concurrent
segment search, so prefetch is the main lever. Steps:
1. Baseline: add sort queries to the benchmark; measure stock / vecdec / pfwc cold and warm; attribute cold loads by file
   and code path (does `NumericComparator` prune with points (`.kdi`/`.kdd`) or with the doc-values skipper?).
2. If the BKD path dominates: new points format (user design). `.kdi` keeps the inner-node tree and adds a leaf directory
   (per leaf, by leaf ID: tight min/max per index dimension, docCount, min/max docID, file pointer + length into `.kdd`
   and into a new `.kdv`); `.kdd` holds only docIDs, `.kdv` only values; `.kdm` adds the `.kdv` start and a flag.
   Traversal: prefetch the whole `.kdi` if it is at most 32-64 KB, otherwise child nodes as it descends; classify each
   leaf from the resident bounds (OUTSIDE: skip, INSIDE: docIDs only, CROSSES: docIDs + values); collect the surviving
   leaves' pointers; one coalesced prefetch; then read only what is needed.
   **Decision (user, POC, supersedes "keep stock files"): split as designed, behind a new field type.** Stock fields keep
   the stock points format, unchanged. A new field type (mapping type in OpenSearch, per-field points format in Lucene)
   writes the new layout: `.kdi` = inner-node tree + leaf directory, `.kdd` = docIDs only, `.kdv` = values only, `.kdm`
   + `.kdv` start and a flag. The corpus gets a twin of `@timestamp` with identical values in the new type, so every
   query can run on the stock field and on the new field over the same docs, same index, same segment. Needs a re-ingest.
   Queries to compare: `sort` on the timestamp field, ascending AND descending (with and without filters, size 10 and
   500), and the range filters of the aggregation queries (1-day and 7-day).

## Parked

- Cost model in code: `IOCost` interface on `IndexInput` (block size, miss cost, residency via sampled
  `containsKey`) plus an `expectedAdvances(k)` hint from `ConjunctionDISI`, so the DualNav reader picks doc/nav itself.
- Resident `.nav` experiment (keep `.nav` warm, `.doc` cold): IO counts say the nav path would then never lose.
- 8 KiB cache blocks.
- Explicit SIMD for aggregation collection (see Phase C2 notes).
