# Cold-path benchmark harness (EC2)

Measures every query family cold and warm on ONE data node that runs every arm (stock OpenSearch and the POC
binary, index on EBS gp3 or EFS), with JVM restarts as the unit, and computes the per-operation outcome verdict:
target (S2-CORE-EFS, S2-CORE+PLANNER-EFS) not worse than reference (S0-EBS). Method: the skill
`cold-path-io-latency-experiments` (sections 3 to 5). Entry points are listed in `../harness.json`.

## Pieces

| File | Runs on | What it does |
|---|---|---|
| `agent/coldpath_agent.py` | data node, root, stdlib | drop caches (pageout of the JVM's index-file mappings, `sync`, `echo 3 > drop_caches`), page-cache residency (mincore), `/proc/<pid>/io`, diskstats (EBS), NFS mountstats (EFS), READ-size trace (tracefs `nfs_initiate_read`), `read_ahead_kb` set/record, `du`, node restart by arm name (commands only from its own config) |
| `queries.py` | load generator | op set per corpus: every search op of the corpus' OSB workload (rendered with jinja2) plus generated ops for every required family, values discovered from the data with fixed rules (`corpora/<corpus>.json` names fields by role only) |
| `coldbench.py` | load generator | the session: interleaved arms (A B B A, or seeded random), restart per run, index/segment/size verification, switches with read-back, cold block (clear and verify before EVERY iteration), warm block, result pass, optional concurrent cold/warm; `--executor replay` (this client) or `--executor osb` (OpenSearch Benchmark) |
| `osb_cold.py` | load generator | derived OSB workload with the `coldpath-search` runner (clear + verify, then OSB's own Query runner) and `coldpath-warm` |
| `indexprep.py` | load generator | single-shard >= 30 GB copy (1 primary, 0 replicas), force-merge to N segments with verification, 1-segment clone, describe (store bytes, segments, du) |
| `analyze.py` | anywhere | statistics and the outcome verdict (below) |
| `selftest.py` | anywhere | end to end against a mock node and mock agent; `--osb-bin` also runs the OSB executor |

## Why the cold protocol is implemented outside OSB's schedule
OSB has no hook between iterations of a task, and it times a runner from its first to its last request on its own
client. The `coldpath-search` runner therefore clears and verifies with a separate stdlib client before it calls
OSB's Query runner, so OSB's service time covers only the search requests. The replay executor does the same with
its own client and records network time only (`wall_ms`) and the server's `took`.

## Cold protocol (before EVERY iteration of EVERY op)
1. wait until the bufferpool prefetch pool is idle;
2. `POST /_bufferpool/cache/_clear` (bufferpool arms); `POST /_cache/clear?query&fielddata&request`; searches use
   `request_cache=false`;
3. agent `/cache/drop`: `process_madvise(MADV_PAGEOUT)` on the JVM's mappings of index files (stock hybridfs mmaps
   Lucene files, and `drop_caches` does not evict pages that are mapped), `sync`, `echo 3 > drop_caches`;
4. verify: bufferpool `cached_blocks == 0`; page-cache residency of the arm's index files <= 1 MiB (mincore, EBS
   and EFS); after the query, if anything was read, the reads reached storage: EBS diskstats reads > 0 and JVM
   `read_bytes` > 0; EFS NFS READ ops > 0. With empty caches the first read of the iteration is a miss by
   construction. A sample that fails is marked `cold_ok=false` and excluded (`--strict` aborts).
5. IO sizes and kernel readahead (common-rules.md, kernel readahead section, REVISED ~20:00): before
   and after every arm run the agent sets and verifies `read_ahead_kb` on every layer of the arm's data path (NFS
   bdi on EFS; block device and any dm/LUKS layer on EBS, plus `blockdev --setra`): stock arms keep the as-mounted
   default (mmap readahead; EFS 15360 KiB, re-applied by the AL2023 udev rule on every mount), bufferpool arms run with
   0 (`--poc-read-ahead-kb`; the bufferpool announces each window with POSIX_FADV_WILLNEED, one window = one device
   read). A run whose value differs is refused. Every cold iteration
   traces the device read sizes (NFS READ RPCs on EFS, block read requests on EBS; `--no-read-size-trace` falls back
   to mountstats / diskstats); for bufferpool arms the device reads, bytes and size classes must equal the
   bufferpool's reads + prefetch_reads, bytes_read and reads_by_size (`device_reads_are_windows`), else the iteration
   fails verification and the run is marked invalid (`run_end.valid`, `session_end.invalid_runs`); `io_size_ok`
   (`--max-read-bytes`) applies to bufferpool arms only. The agent applies each storage's last mode again after a reboot.
   Order: Linux copies the bdi readahead into each file when the file is opened, so the arm's value is set BEFORE the
   configured indices are closed, the node restarts and the arm's indices are opened, and read back after the open
   (`readahead_after_open`; a change refuses the run). `--no-restart` and `probe` also close and re-open the indices
   after the set. `selftest_order.py` checks the call order and the value each opened file holds.
6. IO configuration (bufferpool arms): at the start of every run `/_bufferpool/stats` must report `block_size` 8192,
   `random_read_size` 32768 and `sequential_read_size` 131072 (`--bp-block-size`, `--bp-random-read-size`,
   `--bp-sequential-read-size`), else the run is refused; recorded as `io_config`. The plugin has no O_DIRECT mode.
7. Other open indices: the arms file's `other_indices` = `record` (default: listed in the run record and the log),
   `close` (closed before the arm's indices open; generic arms files) or `refuse`; names starting with `.` are
   never touched (`runguards.py`).
EFS server-side caching cannot be cleared from the client; it is part of the storage (as in AOSS) and is the same for
every EFS arm.

## Index state per run
Every index is closed before a node stops. On start the harness sets the arm's `index.store.type` on the closed index
and opens what the arm needs, so a stock node never opens a bufferpoolfs or split-format index.
A closed index stays allocated, so the stock binary cannot even START next to a closed index it cannot read (probe on
the big5-100 data node, `br-big5-100/closed-index-probe.txt`): (1) store type `bufferpoolfs` -> shard fails with
"Unknown store type", cluster red, and the stock node also rejects the store-type update; (2) split BKD format ->
"Could not load codec 'Lucene104SplitPoints'", no valid shard copy, red. Therefore (1) when a node with the bufferpool
plugin closes the indices, it sets every index that a stock arm opens back to the stock arms' store type (logged);
(2) an index whose `format` contains "split" (or `"poc_only": true`) must be opened only by agent arms whose data path
no stock arm's node uses (e.g. agent arms `POC-B-EBS` / `POC-B-EFS` with their own `data_path`); the session is refused
before any run otherwise (`runguards.check_format_isolation`, recorded as `format_isolation` in the session record). Before measuring it
verifies store type, docs, primaries, segments per shard (exact), primary store bytes (and `min_store_bytes`), and the
agent's `du`, and records them in the run record. Single-shard >= 30 GB runs use the same arms with
`--indices indices.single-shard.example.json` (and the 1-segment variant file).

## Statistics (`analyze.py`)
- Per-run median per op, then across runs. Change of the median with a bootstrap 95% CI resampling runs; exact
  Mann-Whitney p on run medians; Benjamini-Hochberg across ops; significance also needs |change| > A/A floor.
- A/A floor from two labels of one arm (`ARM@a,ARM@b`). Warm regression bar: CI above 0 and beyond the floor.
- Amdahl: cold median >= warm median per arm and op; a cold gain must not exceed the base's cold - warm budget.
- Warm steady state (second vs first half), reference-op drift (last vs first per run), result equality across
  arms (totals, hits with tie groups, aggregations) and across runs, cold verification counts, IO per request.
- Outcome (non-inferiority): per op, target/reference for cold p50, cold p90, warm p50 (cold p99 informational) by a
  two-level bootstrap (runs, then iterations); PASS when the CI upper bound <= 1 + delta, delta = max(5 %, the
  reference's A/A MDE for that op and statistic), and the BH-adjusted one-sided p < 0.05; WORSE when significantly
  above the margin; else INCONCLUSIVE (add runs; never a pass). Failing ops list the gap in ms and the IO that
  remains (demand loads per file type, NFS READ ops, RTT, READs in flight).

## Known gaps
- `prefetched_not_read` is not exported by `/_bufferpool/stats` (needs the bufferpool trace); reported as a gap.
- The bufferpool reads exactly one cache block per miss (`BlockCache.load` reads `blockSize` bytes; prefetch loads
  each missing block with its own read). There is no setting for an IO size larger than the block size, and the
  directory ignores IOContext / ReadAdvice. The 8 KiB block with 32 KiB random / 128 KiB sequential reads needs a
  plugin change (settings for block size, random IO size, sequential IO size).
