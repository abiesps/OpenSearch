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
| Benchmark | `plugins/store-bufferpool/benchmark/bench_postings.py --sizes 128mb,500mb,1gb` (see README.md) |
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
- Next: norms prefetch for each window's doc range, then B2 (.doc prefetch of essential clauses). 8 KiB nodes, A5, A6 parked.



| Step | Change |
|---|---|
| B1 | New corpus: a text field with varied term frequencies and document lengths (BM25 impacts that differ per block), plus top-k OR queries; one re-ingest (bump `DATASET_VERSION`) |
| B2 | Impact-aware prefetch in `MaxScoreBulkScorer`: prefetch the essential clauses' blocks for upcoming outer windows; leave out blocks whose max score plus the other clauses' max scores is below `minCompetitiveScore` |
| B3 | Impacts in their own small file (duplicate, per the rule), so the navigation plane stays minimal |
| B4 | Skip index over impacts: skip non-competitive blocks with O(log n) reads instead of O(n) |

## Parked

- Cost model in code: `IOCost` interface on `IndexInput` (block size, miss cost, residency via sampled
  `containsKey`) plus an `expectedAdvances(k)` hint from `ConjunctionDISI`, so the DualNav reader picks doc/nav itself.
- Resident `.nav` experiment (keep `.nav` warm, `.doc` cold): IO counts say the nav path would then never lose.
- 8 KiB cache blocks.
