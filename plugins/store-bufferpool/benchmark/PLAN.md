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

## Phase C: aggregations on doc values (C0-C2 done)

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
- Candidates before or with C3: Panama gather/unpack for the common widths; consume window bit sets word by word
  instead of materializing doc IDs; skip deferral when the sub-aggregations are cheap (avg); the user's change 3
  (skipper sum/count per interval, so intervals fully inside the filter need no value reads).

## Parked

- Cost model in code: `IOCost` interface on `IndexInput` (block size, miss cost, residency via sampled
  `containsKey`) plus an `expectedAdvances(k)` hint from `ConjunctionDISI`, so the DualNav reader picks doc/nav itself.
- Resident `.nav` experiment (keep `.nav` warm, `.doc` cold): IO counts say the nav path would then never lose.
- 8 KiB cache blocks.
