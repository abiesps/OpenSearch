# Generic workloads and luceneutil (harness extension)

Extends the cold-path harness (README.md) to the other non-vector OpenSearch Benchmark workloads and to Lucene's
luceneutil, with the same arms, IO configuration, cold protocol, statistics and decision rule. Plan:
`.agents/tasks/ec2-big5-2026-10-04/GENERIC-PLAN.md` (sections 1, 4, 5). Pins: opensearch-benchmark-workloads
`29dd7e1f681df035c87a5fba0e31d0cc0ce46a4f`, luceneutil `99217ab641d0dd172a89f22f079aa85227f5b7fe`.

The big5 / nyc_taxis / http_logs / pmc paths are unchanged: their profiles do not set `"osb_import": "ext"`, their
`queries/*.osb.json` re-render byte-identical, and `canonical()` digests of responses without the new hit keys are
the old digests (both are checked by `selftest_generic.py`).

## Pieces

| File | What it does |
|---|---|
| `corpora/<w>.json` | profiles of clickbench, eventdata, geonames, geopoint, geopointshape, geoshape, nested, noaa, percolator, so: field roles, geo fields, nested paths, `families_not_applicable` (each with its reason), index names and document counts, corpus base URLs where the workload has none, split-BKD applicability, single-shard rule |
| `osb_import_ext.py` | every op of the workload's `operations/*.json`: search ops, `raw-request` POST `_search` ops (clickbench DSL), param-source ops materialized from the workload's own `workload.py` (seed 4, 5 distinct bodies each, `osb:<name>#k`), bulk ops listed as ingest ops, the rest listed as not measured with the reason (clickbench PPL: no SQL plugin in S0 or POC). Records the clickbench `timeout` removal per op |
| `families_ext.py` | generated families from discovered values: the standard ones (queries.generate where a time field exists, the same rules without dates for geonames, date_range for agg:range without numeric fields), geo (bbox, distance, polygon, geo_shape envelope/polygon, geohash/geotile grids, geo_bounds, geo_centroid, geo-distance sort), nested (range, term, inner_hits, sort, aggregations), scroll; coverage with not-applicable families |
| `canonical_ext.py` | result equality for percolator slots, highlight fragments, nested inner_hits, matched_queries; `terminated_early` per sample |
| `indices_ext.py` | multi-index targets (`members`: geoshape's three indices; per-member close / store type / open / verify, comma-joined query target, residency over all member UUIDs) and not-applicable arms (recorded as gaps) |
| `ingest_osb.py` | derived ingest-only OSB workload from the workload's own index.json, corpora and bulk ops (shard/replica override, split-BKD meta, renames), OSB run with indexing metrics, and the interleaved S0/POC ingest check |
| `arms_generic.py` | arms and indices files per workload from the main arms file (same arms, switches, outcome); B arms not applicable where B cannot apply, CORE without B on the stock-format index |
| `luceneutil/` | luceneutil setup, patches, switch table from the fork, interleaved N-arm driver, conversion to a coldbench session (below) |
| `selftest_generic.py` (+ `selftest_generic_ingest.py`) | parts (a)-(g), stdlib + jinja2, no AWS |

## Commands

```
# op set (after ingest, before any measurement): pre-registered ops of one workload
python3 queries.py build --corpus geonames --osb-workloads OSBW --osb-commit 29dd7e1f681df035c87a5fba0e31d0cc0ce46a4f \
    --url http://DATA:9200 --index geonames --out ops/geonames.json --check-coverage
python3 queries.py build --corpus geoshape ... --index osmlinestrings,osmmultilinestrings,osmpolygons --out ops/geoshape.json
# ingest (stock-format index, S0) and the ingest check (fresh indices, S0 POC POC S0 S0 POC)
python3 ingest_osb.py render --osb-workloads OSBW --corpus eventdata --shards 6 --out work/eventdata/derived
python3 ingest_osb.py run --derived work/eventdata/derived --url http://DATA:9200 --out results/eventdata/ingest-s0.json
python3 ingest_osb.py check --osb-workloads OSBW --corpus eventdata --shards 6 --url http://DATA:9200 \
    --agent http://DATA:9700 --token-file TOKEN --sequence S0-EBS,POC-EBS,POC-EBS,S0-EBS,S0-EBS,POC-EBS \
    --out results/eventdata/ingest-check
python3 ingest_osb.py check ... --procedure update          # geonames, geopoint, geopointshape: index-update
# B index (split BKD date fields, POC binary): eventdata, so, nested, noaa only
python3 ingest_osb.py render ... --rename eventdata=eventdata_split --split-fields @timestamp --out work/eventdata/derived-b
# arms (generated from the branch's arms file); single-shard >= 30 GB and 1-segment variants
python3 arms_generic.py build --base arms.json --corpus geoshape --segments 10 --out arms.geoshape.json
python3 arms_generic.py indices --corpus clickbench --layout single --segments 10 --out indices.clickbench.1s.json
# sessions and analysis: coldbench.py run / analyze.py exactly as for big5 (README.md, ../harness.json)
python3 selftest_generic.py --osb-workloads OSBW [--lucene-fork LUCENE_FORK --fork-commit origin/bkd-split]
```

## What is kept from the main harness
- IO configuration: no O_DIRECT (buffered reads), random reads 32 KiB, sequential reads 128 KiB, 8 KiB cache block:
  these are node and arm settings of the main arms file and the POC artifact; `arms_generic.py` copies the arms,
  base switches and cluster settings unchanged, and the sessions use the same `--read-ahead-kb 128`,
  `--max-read-bytes 131072` and `--nfs-trace` IO-size checks.
- Cold protocol before every iteration: idle prefetch pool, `POST /_bufferpool/cache/_clear`, `POST /_cache/clear`,
  agent pageout + sync + `drop_caches`, verification (cached_blocks 0, mincore residency of every member's files,
  reads reached the device or the NFS server), `request_cache=false` on every op (also on `cache: true` ops).
- Result equality for every op in every arm (`canonical()` + `canonical_ext`), `timed_out` and `_shards.failed`
  fail the sample.

## luceneutil (branch g-luceneutil)
- `setup.sh BASE POC_COMMIT`: luceneutil at the pin with `patches/0000-0003`, Lucene stock = `releases/lucene/10.5.1`
  (the fork's base) and POC = the fork at POC_COMMIT, each with a private patched luceneutil copy (POC also
  `patches/poc`), both merge-bases recorded, the wikimediumall line file (URL read from luceneutil's
  `initial_setup.py`), `localconstants.py`, the Lucene builds, and the switch-table check.
- `patches/0000`: luceneutil main tracks Lucene main (11.x APIs) and does not compile against Lucene 10.5.1 at the pin
  (8 errors: PriorityQueue, CompoundFormat.setShouldUseCompoundFile, BpVectorReorderer). No luceneutil commit
  compiles against 10.5.1 unpatched (checked back to 2025-07). The patch restores luceneutil's own pre-#510 code
  (merge-policy noCFSRatio), uses `lessThan` for the priority queue and rejects `-bp` (vector reordering, Lucene main
  only; not used here). Applied identically to the stock and POC copies. Verified: the patched sources compile with
  javac against the lucene-*-10.5.1 release jars from Maven Central.
- `patches/0001` ColdpathSwitches: `-Dcoldpath.switches=<Class>.<setter>=<value>,...` set by reflection, read back
  through `getX`/`isX`, printed; unknown class, setter, value or read-back = exit 3 (arm not available).
  `switches.json` lists the 18 setters of the six experiment classes, generated by `gen_switches.py` from the fork's
  source (`--check` compares it with a commit).
- `patches/0002` strict per-task cold mode (`-Dcoldpath.cold.agent=...`, one task at a time): before every task,
  outside its timed region, agent `/cache/drop?pageout=1` and `/cache/residency` (mincore of `<index>/index/`), agent
  snapshots before and after; one JSON line per task. The taxonomy directory (`<index>/facets/`) is paged out and
  dropped by the same calls, but the residency check covers `index/` only (agent rule): a recorded gap.
- `patches/0003` + `patches/poc/0001`: `-Dcoldpath.pointsFormat=Lucene90Split:lastMod,timesecnum,dayOfYear` writes
  those 1-D points fields in the split BKD format under the codec name Lucene104SplitPoints (POC Lucene only;
  stock exits "not available"). The DualNav variant uses luceneutil's own `postingsFormat`.
- `run_luceneutil.py plan|run|index|copy`: N interleaved arms through luceneutil's own API (rotation per JVM
  iteration, same seeds), modes warm / cold-luceneutil / cold-strict, EBS and EFS index copies (copied with sha256
  verification), readahead set to 128 KiB through the agent, luceneutil's simpleReport and verifyScores /
  verifyCounts per arm vs the base. `analyze_luceneutil.py` converts the logs into a coldbench session for analyze.py.
- Not testable in luceneutil (OpenSearch plugin code): C, D-a, D-b, K1, E/E8, C2/C3e; the bufferpool 8 / 32 /
  128 KiB configuration applies to the OpenSearch arms, luceneutil reads through MMapDirectory and the page cache.

## Known gaps
- Rendering needs jinja2 (installed with opensearch-benchmark). The ingest driver and the luceneutil driver were
  tested by rendering and static checks here; their first real runs are on the EC2 hosts.
- `queries/geonames.osb.summary.json` replaces the 15 large-terms bodies (45,587 terms, 0.78 MB each) by sha256,
  size and term count; the full bodies are re-rendered on the host (deterministic, checked by the self-test).
