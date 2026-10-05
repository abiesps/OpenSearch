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
| `indexprep.py` | load generator | single-shard >= 30 GB copy (1 primary, 0 replicas), force-merge to N segments with verification, 1-segment clone, describe (store bytes, segments, du), `formats` (post-ingest segment format check, below) |
| `segformat_check.py`, `agent/coldpath_segformat.py` | load generator; data node (agent `GET /index/formats`, or `--dir` directly) | post-ingest segment format check: reads every shard's last commit from the segment files and proves each field's points and postings format from its per-field attribute and the format's files |
| `analyze.py` | anywhere | statistics and the outcome verdict (below) |
| `selftest.py` | anywhere | end to end against a mock node and mock agent; `--osb-bin` also runs the OSB executor |
| `agent/coldpath_efsconn.py` | data node (agent `GET /efs/connections`, `POST /efs/precondition`, and in every `/snapshot`) | EFS backend connection count: efs-proxy's established TCP connections to the mount target port 2049; pre-conditioning to a target count (below) |
| `selftest_efsconn.py` | anywhere (the O_DIRECT read on Linux only) | the connection count against a fake /proc, and the pre-conditioning rules |
| `selftest_segformat.py` | anywhere | the segment format check on real Lucene shards (`testdata/segformat`: a split BKD and Nav postings copy written by the plugin's codec service, and a stock control), the agent endpoint, `indexprep.py formats` logic and the coldbench verify step |
| `node_allocator.sh` | data node, root | the C memory allocator of every OpenSearch node unit: installs the pinned jemalloc (AL2023 package 5.2.1-7, sha256 checked) and writes the same systemd drop-in to every `opensearch-*.service` (below) |
| `arms_baseline.py` | anywhere | converts an arms file to the configuration set of the user decision of 2026-10-05 (below) |
| `selftest_baseline.py` | anywhere | `node_allocator.sh` under a fake root, the agent's allocator parsing, the converted templates, and coldbench end to end with the baseline build, the build and allocator checks and context arms |

## EFS backend connection count (common-rules "Amazon EFS connection count is a measured variable")
efs-proxy (efs-utils 3.3.2) starts a mount on one TCP connection to the mount target and adds 4 more only after one
3 s window at >= 300 MiB/s; it keeps 5 until the proxy incarnation restarts (a reconnect). The count is not
configurable (compile-time `DEFAULT_SCALE_UP_CONFIG`, optionally overridden by the server). High-concurrency read
latency depends on it (storage/storage-model.md section 4.2.2), so:
- every EFS sample records `io.efs_connections` = {start, end, proxy_pid} from the agent's `/snapshot`;
- `coldbench.py --efs-connections 5` (default) holds the count: before each EFS run and before every cold and measured
  warm sample whose mount is below 5, the agent reads a scratch file on the mount with O_DIRECT 1 MiB reads (no
  page-cache page, no index file; `<mountpoint>/coldpath-efs-precondition.bin`, 4 GiB, created once) until efs-proxy
  has scaled up, then waits without reading until the count has held for 30 s (a lost connection often restarts the
  proxy 7-25 s after a scale-up), reading again if it drops (up to 360 s, longer than efs-proxy's 300 s back-off
  after a failed search). A sample is valid only if start and
  end are both 5 or more (a 6th socket of the previous incarnation can linger) on the same proxy process (cold: `checks.efs_connections_ok`, part of `cold_ok`; warm:
  `efs_connections_ok`). A run whose mount cannot reach 5 is refused;
- `--efs-connections 1` is the sensitivity of a mount that never scaled up: it needs a fresh mount pinned to one
  connection (`storage/efs_conn_ctl.sh pin-on`, then remount); a mount above the target is refused, never lowered;
- `--efs-connections 0` records the count only; `analyze.py` refuses to compare EFS samples at different counts
  (select one with `--efs-connections N`) and drops EFS samples without a count unless
  `--keep-unknown-efs-connections` (labelled "connection state unknown", no EFS verdict).
## Why the cold protocol is implemented outside OSB's schedule
OSB has no hook between iterations of a task, and it times a runner from its first to its last request on its own
client. The `coldpath-search` runner therefore clears and verifies with a separate stdlib client before it calls
OSB's Query runner, so OSB's service time covers only the search requests. The replay executor does the same with
its own client and records network time only (`wall_ms`) and the server's `took`.

## Cold protocol (before EVERY iteration of EVERY op)
Cold means data-cold on a JIT-warm JVM (common-rules DECISION "cold means data-cold on a JIT-warm JVM"): before the
cold block of every JVM run, every op runs once, unmeasured (record `jit_warmup`; run record `cold_protocol`
`jit-warm`; `--no-jit-warmup` = the old `jit-cold` protocol). Then, before every cold iteration:
1. wait until the bufferpool prefetch pool is idle;
2. `POST /_bufferpool/cache/_clear` (bufferpool arms); `POST /_cache/clear?query&fielddata&request`, then the node's
   fielddata (it holds the global ordinals), query cache and request cache must report 0 bytes and 0 entries
   (`/_nodes/_local/stats/indices/fielddata,query_cache,request_cache`, check `search_caches_empty`); searches use
   `request_cache=false`. The fielddata clear only MARKS entries; the node drops them in its periodic sweep every
   `indices.cache.cleanup_interval` (default 1m), so every arm's node runs with `indices.cache.cleanup_interval: 1s`
   (S0 and POC alike; a cold run is refused otherwise, `runguards.check_cache_cleanup`), and the clear is re-posted
   every second for up to 5 s until all three read 0 (`clear.caches.wait_ms`);
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
   to mountstats / diskstats); for bufferpool arms the agent attributes every traced read to index-file data (EBS:
   FIEMAP extents of the arm's Lucene files; EFS: READ fileid = inode) or to other reads (file-system metadata after
   drop_caches, reported by size and count), and the data reads must be bufferpool windows: none crosses a 128 KiB
   file block or is larger than the largest window, data bytes <= bufferpool bytes_read, 128 KiB blocks touched <=
   bufferpool reads + prefetch_reads, data reads <= those reads + splits at file extent boundaries (XFS fragmentation;
   a window split into 4 KiB pages fails) (`device_reads_are_windows`, rule `attributed`; common-rules.md "DECISION
   ~22:00"; without the attribution the old strict equality applies), else the iteration
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

## Node failures (common-rules "Node start can fail on Amazon EFS with NoSuchFileException in the cluster-state commit")
A node start that fails before any measured iteration (the JVM exits, or its HTTP port never answers) is retried
`NODE_START_RETRIES` (2) times after 10 s; each retry is recorded as `node_start_retry` with the exception, whether the
JVM was gone, the agent's node status and the EFS backend connection count and efs-proxy pid of the arm's mount. After
the last retry the run stops (`node_start_failed`) and is re-queued at the end of the session (`run_requeued`, once).
A node that dies during a measured run discards that run (`run_discarded`; analyze.py drops its samples and results)
and re-queues it. A re-queued run gets the run id `<label>#r<round>.a<attempt>` and the op orders of the first attempt.
After a bufferpool arm's failure the stock store types of every stock data path are reset again
(`store_type_normalize` with `after`). A second failure records `run_failed` and the session continues. Errors that
are not node failures (the JVM still runs) stop the session as before.

Request timeout and storage incidents (big5-1000 host C, 2026-10-05: its Amazon EFS mount stalled, "nfs: server
127.0.0.1 not responding", and a phrase-query request outlived the client timeout):
- `--request-timeout S` (default 7200) is the client socket timeout of every OpenSearch request, one value for every
  configuration of a session (recorded in the session arguments). A request that outlives it raises
  `common.RequestTimeout`: the harness cancels the searches still running on the node (`POST _tasks/_cancel`),
  records `run_discarded` (reason `request timeout`) and re-queues the run like a node failure. A timeout is never a
  latency sample.
- After every run (and with every discard) the harness asks the agent (v3, `GET /storage/incidents`) for kernel NFS
  stall windows during the run ("nfs: server X not responding" until "nfs: server X OK", from `journalctl -k`) and
  records `storage_incident_check` and, if there were any, `storage_incident` with the windows. analyze.py excludes
  every sample whose request overlaps a window (1 s margin) from every comparison and verdict, and reports the count
  as sensitivity. An agent without the endpoint is recorded as `available: false`.

Node memory (common-rules "BLOCKER: native memory growth from one direct allocation per cache block"): during every
run a monitor thread polls the agent's `GET /node/memory` (v3) every `--mem-interval` s (10): node JVM VmRSS and
RssAnon against host MemTotal, and the JVM's MALLOC_ARENA_MAX from its environment. Each run records a `node_memory`
summary (maximum, a series every 60 s, MALLOC_ARENA_MAX). When either value passes `--mem-limit-pct` (70) the run ends
at the next operation boundary (`run_discarded`, reason `memory limit`) and is re-queued.

## Configuration set and allocator (common-rules "USER DECISION (2026-10-05) ... the bufferpool is the baseline")
Every configuration uses the bufferpool store with the same plugin settings and the bufferpool readahead rule
(read_ahead_kb 0 plus the window hint). `arms_baseline.py convert` turns an arms file into this set:
- `BASE-EBS`, `BASE-EFS` (and `-css` variants): `"build": "baseline"`, the artifact `baseline_bufferpool` (stock
  OpenSearch b44de786cef and stock Lucene with the plugin built for it), agent arms `BASE-EBS` / `BASE-EFS` (units
  `opensearch-base-ebs` / `-efs`) on the same data paths as `POC-EBS` / `POC-EFS`. Each copies `S1-<storage>` (index
  key, cluster settings) without proof-of-concept-only indices and without switches. coldbench posts no
  `base_switches` to them and does not read `sort_opt` (the baseline build has no experiment endpoint).
- `S2-*` (changes on) and `S1-*` (all changes off, the attribution reference): `"build": "poc"`, artifact
  `poc_iosize_conc2`.
- stock memory-mapping arms (old `S0-*`) move to `"context_arms"`: a session runs one only when `--arm-list` names
  it; the run record says `context_arm: true`; no outcome uses it. Without a named context arm there is no stock arm,
  so the shared stock-format indices keep store type `bufferpoolfs` (no reset to hybridfs).
- `"builds"`: at every run start the node must report `build_target` (`stock` for the baseline, `poc`) in
  `GET /_bufferpool/stats` and a `build_hash` starting with the recorded one in `GET /`; else the run is refused.
- `"same_bufferpool_settings": true`: every bufferpool configuration on one storage must report the same block size,
  read sizes, read hint, prefetch node size and prefetch scheduler settings as the first run of the session there.
- `"outcome"`: reference `BASE-EBS` for the targets on Amazon EFS; `same_storage` lists each storage's comparison
  against the baseline on the same storage (run `analyze.py --ni-ref R --ni-target T --ni-aa AA` for each).
The format-isolation guard counts the baseline build's nodes as stock binaries (no split codec): a split or other
proof-of-concept-only index needs its own data path (`POC-B-*`), as before.

Allocator (user decision "For memory fragmentation use jemalloc"): `node_allocator.sh install` then
`node_allocator.sh apply` on every data node (root). Every `opensearch-*.service` gets the same drop-in
`90-coldpath-allocator.conf` (`UnsetEnvironment=MALLOC_ARENA_MAX`, `LD_PRELOAD=/usr/lib64/libjemalloc.so.2`,
`MALLOC_CONF=background_thread:true`); earlier `MALLOC_ARENA_MAX` lines in unit files or drop-ins are removed (backup
under `/var/lib/coldpath/allocator-backup/`), and `/etc/coldpath/allocator.json` records the setup. Agent v4 adds
`allocator` to `GET /node/memory`: the JVM's `LD_PRELOAD`, `MALLOC_CONF`, `MALLOC_ARENA_MAX` (from
`/proc/<pid>/environ`), the libjemalloc files mapped in `/proc/<pid>/maps` with their sha256, and the name
(`jemalloc` when a libjemalloc file is mapped, else `glibc`). coldbench checks it at every node start against the
arms file's `"allocator"` block (or `--allocator jemalloc|glibc|any`), requires every run of a session to have the
same allocator, and records it in the `run` record and in `node_memory`. A run on a node without jemalloc, or with an
agent before v4, is refused before it measures. The validation (resident memory per cache fill for glibc, glibc with
2 arenas and jemalloc, and the latency comparison) is in
`.agents/tasks/baseline-bufferpool-2026-10-05/jemalloc-validation.md`.

## Index state per run
Aborted sessions (common-rules "Store type left on the other data path after an aborted session"): (a) a SIGTERM or
any error during a run triggers an exit trap that closes the configured indices on the running node and resets their
store type when that node has the bufferpool plugin; (b) a SIGKILL cannot be trapped, so before the first run every
session starts, for each data path a scheduled stock arm uses, a POC arm of the arms file on the same path, resets
foreign store types of the stock indices, retries failed allocations (`_cluster/reroute?retry_failed=true`) and waits
for green (record `store_type_normalize`; `--no-normalize-store-types` skips it).
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

## Segment format check (split BKD and Nav postings copies)
The codec name never proves a format: every Lucene104 segment of a bufferpoolfs index is named
`Lucene104SplitPoints`, also when no field asks for the split format. After every ingest, force-merge, clone or
restore of a split-format or Nav-postings index, and of its control copy, and before any result of it is reported,
run the check:

    indexprep.py formats --url U --agent A --token-file T --agent-arm POC-B-EBS --index nyc_taxis_split --from-mapping
    indexprep.py formats --url U --agent A --token-file T --agent-arm POC-B-EBS --index nyc_taxis_split \
        --points pickup_datetime=Lucene90Split --points dropoff_datetime=Lucene90Split
    indexprep.py formats --url U --agent A --token-file T --agent-arm POC-B-EBS --index nyc_taxis_ctrl --control
    # without the agent, on the data node itself:
    python3 agent/coldpath_segformat.py --dir /data/ebs/opensearch-b/nodes/0/indices/UUID --postings tag_nav=Lucene104Nav

For a copy meant to have a format, every segment that holds an expected field must carry the per-field attribute
(`PerFieldPointsFormat.format=Lucene90Split`, `PerFieldPostingsFormat.format=Lucene104Nav`) and the format's files
(`<segment>_Lucene90Split_0.kdm`, `.kdi`, `.kdd`; `<segment>_Lucene104Nav_<suffix>.nav`, inside the compound file when
the segment is compound), and at least one segment must hold the field. A control copy (`--control`) must have
neither format in any attribute or file name. Any mismatch prints the segments and exits with status 1. `--from-mapping`
takes the expected fields from the index mapping's `meta.points_format` / `meta.postings_format` entries. In an
indices file, `"formats": {"points": {...}, "postings": {...}}`, `"formats": "mapping"` or `"formats": "control"` on an
index makes `coldbench.py` run the same check in the verify step of every run, which then fails before measuring. The
agent must have `coldpath_segformat.py` installed next to it (`../harness.json`, install); an older agent answers 404 and
the check stops with that message.

## Statistics (`analyze.py`)
- One analysis uses one cold protocol: sessions whose runs have different `cold_protocol` are refused together.
  `--cold-skip-iters 1` leaves out cold iteration 0 (the primary result of `jit-cold` sessions; protocol
  `jit-cold/skip-iter1`).
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
