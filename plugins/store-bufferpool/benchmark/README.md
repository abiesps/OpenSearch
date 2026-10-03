# Postings layout benchmark

Compares stock `Lucene104` postings with `Lucene104Nav` (skip data moved from `.doc` into its own `.nav` file) on the
same data, in the same single segment, through the `bufferpoolfs` block cache.

`Lucene104Nav` lives in the Lucene fork (`abiesps/lucene_experiments`, branch `bufferpool`). This branch builds against
that fork's `10.5.1-SNAPSHOT` jars from `~/.m2`, so every Gradle command needs `-Drepos.mavenLocal=true`.

## Setup

```
# in the Lucene fork, after any Lucene change
./gradlew mavenToLocal

# in OpenSearch: start one node (data in ~/bufferpool-bench/data survives restarts)
plugins/store-bufferpool/benchmark/start_node.sh           # BLOCK_SIZE=8kb, CACHE_SIZE, HEAP to change
plugins/store-bufferpool/benchmark/stop_node.sh
```

## Run

```
plugins/store-bufferpool/benchmark/bench_postings.py                       # 20M docs
plugins/store-bufferpool/benchmark/bench_postings.py --sizes 128mb,500mb,1gb
```

Each dataset is ingested and force-merged to one segment the first time only. The index name holds the doc count, a
dataset version and a hash of the `Lucene104Nav` writer source, so a new index is built only when the data or the
on-disk format changes. Query-time changes and node settings (block size, cache size, latency) reuse the indices.

For each query and field, a cold run clears the block cache first; a warm run repeats the query with the cache kept.
`bufferpool.simulated_load_latency` (dynamic) adds a fixed delay to every block load to model remote storage.
Per-file-type load counts come from `GET /_bufferpool/stats`. Raw results go to `~/bufferpool-bench/results`.

## Mapping

```
"tag":     { "type": "keyword", "meta": { "postings_format": "Lucene104Baseline" } }
"tag_nav": { "type": "keyword", "meta": { "postings_format": "Lucene104Nav" } }
```

`Lucene104Baseline` is stock `Lucene104` under another name, so the baseline field gets files of its own instead of
sharing them with `_id`.

## Sort experiments (`sort_opt`)

The cold-path sort switches of the Lucene fork and of OpenSearch are set JVM-wide with
`POST /_bufferpool/sort_opt?bkd_prefetch=&bkd_chunks=&whole_index=&whole_index_bytes=&index_child_prefetch=&approx_single=&approx_bool=&skipper_range=&sort_prefetch=&sort_docs=&clamp=&sample_docs=&skipper_mode=&run_cap=`
(absent = unchanged; a bad value returns 400 and changes nothing) and read with `GET /_bufferpool/sort_opt`.
`bench_aggs.py --variants` takes expressions `tok(+tok|~tok)*` over a base (`stock` or an `agg_batch` mode such as
`vecdec`), atoms `A`, `childpf`, `C`, `Da`, `Db`, `E`, `K1`, `K2`, `K3`, `K4f`, `K4s`, `@split` (the twin field
`@timestamp_split`), `@v1` (dataset `logs_v1`), and aliases `B0`, `B`, `ALL`, `ALLf`, `stock-v1`; `~X` drops X (for
example `ALL~A`). Before every run the full `sort_opt` set is posted, defaults included, so no switch leaks between
variants. `validate/dv_trace.py`, `validate/qtime.py` and `validate/jfr_profile.py` take the same `--variant`
expressions, and `validate/cmp.py` compares the variants of one result file. When the Lucene fork lives in a worktree,
restart the node with `FORK=<worktree> ./refresh_lucene.sh --skip-publish` (after `./gradlew mavenToLocal` there), so
the jar check compares against that worktree's HEAD.
