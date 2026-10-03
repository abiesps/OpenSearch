#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Aggregation benchmark (Phase C): the most common log-analytics aggregations on doc values, cold and warm.

Synthetic log corpus, reproducible from the seed (dataset logs_v1). One segment, _source disabled.
  @timestamp  date, 7 days from START spread evenly over the docs (about 20 ms apart at 30M docs), plus up to 1 s of
              jitter. Ingested in time order by one bulk thread; the merged segment holds long time-ordered runs whose
              order the merges permuted (like a merged log index), and docs within about 1 s are shuffled by the jitter.
              doc_order() reports both.
  service     keyword, 50 values (svc00..svc49), Zipf weights 1/(i+1)
  status      keyword: 200 85%, 201 5%, 404 6%, 500 3%, 503 1%
  latency     long, log-normal milliseconds (median 50, sigma 1), at most 60,000
  sel         keyword without doc values, query terms only: s50 / s10 / s1 in 50% / 10% / 1% of docs (independent)
Dataset logs_v3 is the same corpus (same values per doc) plus the twin field
  @timestamp_split  date with the same value as @timestamp, skip list on, "meta": {"points_format": "Lucene90Split"}
                    (the split BKD points format of the Lucene fork, picked by the plugin's PointsFormatSelectingCodec),
and "index.search.concurrent_segment_search.mode": "none", so both fields run the same non-concurrent sort path
(OpenSearch turns concurrent search off only for the name @timestamp). Its index name also carries a hash of the split
format's writer sources, so a twin written by an older split writer is never benchmarked.

Queries are size 0 aggregations behind a bool filter of a term on sel and a range on @timestamp, the shape of a
Discover or dashboard panel. A term clause keeps date_histogram off the filter-rewrite (BKD) fast path, which only
applies to a match-all or a lone range query without sub-aggregations.
  dh       date_histogram on @timestamp (1h buckets over 7 days, 10m over 1 day)
  dh_avg   the same with avg(latency) per bucket
  terms    terms(service, size 10) with avg(latency) per bucket

Before measuring, every variant must return the same aggregation result as the first variant.

Sort queries (Stage 4; SORT_QUERIES, selected with --queries or --queries sort): top hits sorted on @timestamp, no
aggregation, the shape of Discover. Spec sort_ORDER:SEL:WINDOW[:SIZE][:nt|:tt]
  ORDER   desc or asc ("sort": [{"@timestamp": ORDER}])
  SEL     s50 / s10 / s1: bool filter [term sel, range]; all: bool filter [range] only (match_all if WINDOW is all);
          bare: the range as the top-level query (OpenSearch's approximation framework applies to it, and to match_all)
  WINDOW  7d / 1d range on @timestamp, or all (no range)
  SIZE    hits returned, default 10 (Discover asks for 500)
  nt      "track_total_hits": false; default is OpenSearch's 10,000 (Lucene starts skipping non-competitive docs only
          after that many hits are counted); tt: "track_total_hits": true (exact count: no skipping at all)
Timed runs ask for no stored fields (the hits carry only their sort values), so the fetch phase reads no .fdt. Before
measuring, every variant must return the same hits as the first variant: _id order, sort values, and hits.total value
and relation. A variant on another index (@v1) has other _ids, so there each hit is compared by a fingerprint of its
@timestamp, service, status and latency doc values; hits with equal sort values may come in another order there (doc
order differs between indices), so within a run of equal sort values the fingerprints are compared as a multiset, and
in the last such run (the top-k cut) only the sort values count.

Query groups for --queries: sort (the 48 SORT_QUERIES), sort_tt (their 24 order x sel/window x size shapes with :tt).
Any spec may end in :f=split: sort and range on the twin field @timestamp_split (aggregation specs: only the range).

Variants (--variants, comma-separated, the first is the reference): an expression tok(+tok|~tok)*. The first token is
a base: stock, an agg_batch mode (runend, vec, vecdec, pf, pfs, pfl, pfsl, pfw, pfwg, pfwc) or an atom or alias.
'+X' adds atom or alias X, '~X' removes it. Atoms (POST /_bufferpool/sort_opt switches):
  A (bkd_prefetch), childpf (index_child_prefetch), C (skipper_range), Da (approx_single), Db (approx_bool),
  E (sort_prefetch), K1 (clamp), K2 (sample_docs=65536), K3 (run_cap), K4f (skipper_mode=fallback),
  K4s (skipper_mode=first), @split (sort and range on @timestamp_split), @v1 (dataset logs_v1).
Aliases: B0=@split, B=@split+A, ALL=A+C+Da+Db+E+K1+K2+K3+K4s, ALLf=ALL~K4s+K4f, stock-v1=@v1. So B+childpf is
@split+A+childpf and ALL~A leaves A out. Before every run the variant's agg_batch mode and the full sort_opt set
(defaults for every absent switch) are posted, so no switch leaks from one variant into the next.

Usage:
  benchmark/bench_aggs.py --docs N --ingest        # ingest once (reused afterwards), then measure
  benchmark/bench_aggs.py --docs N --ingest-only
  benchmark/bench_aggs.py --docs N --queries dh:s10:7d,terms:s50:7d --modes warm
  benchmark/bench_aggs.py --docs N --queries sort,sort_tt --variants stock,A,E,ALL
Without --ingest a missing index is an error. Before each (query, mode) block the run waits while the 1-minute load
average is above 10 and records it in each result row. Only the Python standard library is used.
"""

import argparse
import bisect
import datetime
import hashlib
import json
import math
import os
import random
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench_postings as bp  # noqa: E402  (Client, stats helpers)

DATASET = "logs_v1"
BATCH = 20_000
START_MS = int(datetime.datetime(2026, 9, 1, tzinfo=datetime.timezone.utc).timestamp() * 1000)
SPAN_MS = 7 * 24 * 3600 * 1000
SERVICES = [f"svc{i:02d}" for i in range(50)]
SERVICE_CUM = []
_acc = 0.0
for _i in range(len(SERVICES)):
    _acc += 1.0 / (_i + 1)
    SERVICE_CUM.append(_acc)
STATUS = [("200", 0.85), ("201", 0.05), ("404", 0.06), ("500", 0.03), ("503", 0.01)]
STATUS_CUM = [sum(p for _, p in STATUS[: i + 1]) for i in range(len(STATUS))]
SEL = [("s50", 0.5), ("s10", 0.1), ("s1", 0.01)]
WINDOWS = {"7d": (0, SPAN_MS, "1h"), "1d": (3 * 86_400_000, 4 * 86_400_000, "10m")}

# (variant, POST /_bufferpool/agg_batch mode; None = stock code path)
#   runend  Lucene DenseConjunctionBulkScorer keeps each clause's doc ID run end (CollectExperiments.setCacheRunEnd)
#   vec     runend + OpenSearch batch collection (BatchCollection): chunked bulk value reads in avg, single-bucket
#           runs of date_histogram handed to the sub-aggregation as a stream
#   vecdec  vec + Lucene bulk doc-values reads decode the packed span between the first and last doc in one pass
#           (CollectExperiments.setBulkDecode)
#   pf      vecdec + doc-values prefetch, one node ahead, doc-ID aligned, proven by a look-ahead iterator of the query
#           (DocValuesPrefetch)
#   pfs     pf + look-ahead clauses other than terms evaluated once per segment and shared by the planners
#   pfl     pf + look-ahead built as a leapfrog conjunction (bit-set clauses advanced, not tested doc by doc)
#   pfsl    pf + both
#   pfw     vecdec + doc-values prefetch proven by the main scorer's own matches: a run-ahead buffer between the scorer
#           and the aggregation collectors (131,072 doc IDs; no second scorer)
#   pfwg    pfw + gate: collection waits at a planner's next requested doc until the read after its node is known
#   pfwc    pfwg, but docs pass straight through (no buffer) while the nodes planners enter and the next ones are cached
VARIANTS_ALL = [("stock", None), ("runend", "runend"), ("vec", "vec"), ("vecdec", "vecdec"), ("pf", "pf"), ("pfs", "pfs"),
                ("pfl", "pfl"), ("pfsl", "pfsl"), ("pfw", "pfw"), ("pfwg", "pfwg"), ("pfwc", "pfwc")]
AGG_MODES = {name: mode for name, mode in VARIANTS_ALL}

# Datasets (--dataset). An entry: whether the mapping has the twin field SPLIT_FIELD.
DATASETS = {"logs_v1": {"twin": False}, "logs_v3": {"twin": True}}
DEFAULT_DATASET = "logs_v1"
TIME_FIELD = "@timestamp"
SPLIT_FIELD = "@timestamp_split"
# sources of the split points format's writer (under --fork): the logs_v3 index name carries their hash
SPLIT_FORMAT_FILES = [
    "lucene/core/src/java/org/apache/lucene/util/bkd/SplitBKDWriter.java",
    "lucene/core/src/java/org/apache/lucene/codecs/lucene90/Lucene90SplitPointsWriter.java",
    "lucene/core/src/java/org/apache/lucene/codecs/perfield/PerFieldPointsFormat.java",
]

# POST /_bufferpool/sort_opt parameters with their defaults (every switch off = stock).
SORT_OPT_DEFAULTS = {
    "bkd_prefetch": "false", "bkd_chunks": "8", "whole_index": "false", "whole_index_bytes": "65536",
    "index_child_prefetch": "false", "approx_single": "false", "approx_bool": "false", "skipper_range": "false",
    "sort_prefetch": "false", "sort_docs": "65536", "clamp": "false", "sample_docs": "0", "skipper_mode": "off",
    "run_cap": "false",
}
# variant atoms: sort_opt switches, plus @split (twin field) and @v1 (dataset)
SORT_OPT_ATOMS = {
    "A": ("bkd_prefetch", "true"), "childpf": ("index_child_prefetch", "true"), "C": ("skipper_range", "true"),
    "Da": ("approx_single", "true"), "Db": ("approx_bool", "true"), "E": ("sort_prefetch", "true"),
    "K1": ("clamp", "true"), "K2": ("sample_docs", "65536"), "K3": ("run_cap", "true"),
    "K4f": ("skipper_mode", "fallback"), "K4s": ("skipper_mode", "first"),
}
TARGET_ATOMS = {"@split", "@v1"}
VARIANT_ALIASES = {
    "B0": "@split", "B": "@split+A", "ALL": "A+C+Da+Db+E+K1+K2+K3+K4s", "ALLf": "ALL~K4s+K4f", "stock-v1": "@v1",
}


def _variant_atoms(expr, depth=0):
    """The set of atoms (and at most one agg_batch mode, as 'mode:NAME') an expression stands for."""
    if depth > 8:
        raise ValueError(f"variant alias loop at {expr}")
    if expr in VARIANT_ALIASES:
        return _variant_atoms(VARIANT_ALIASES[expr], depth + 1)
    tokens = re.split(r"([+~])", expr)
    atoms = set()
    for i in range(0, len(tokens), 2):
        tok, op = tokens[i], "+" if i == 0 else tokens[i - 1]
        if tok in VARIANT_ALIASES:
            these = _variant_atoms(VARIANT_ALIASES[tok], depth + 1)
        elif tok in SORT_OPT_ATOMS or tok in TARGET_ATOMS:
            these = {tok}
        elif tok == "stock" and i == 0:
            these = set()
        elif tok in AGG_MODES and AGG_MODES[tok] is not None and i == 0:
            these = {"mode:" + tok}
        else:
            raise ValueError(f"unknown variant token [{tok}] in [{expr}]; agg modes {list(AGG_MODES)} are a base (first "
                             f"token) only; atoms {sorted(SORT_OPT_ATOMS) + sorted(TARGET_ATOMS)}; aliases "
                             f"{sorted(VARIANT_ALIASES)}")
        atoms = atoms | these if op == "+" else atoms - these
    return atoms


def resolve_variant(name, dataset):
    """
    A variant expression -> {"name", "dataset", "field", "agg_mode" (None = stock), "sort_opt" (every parameter)}.
    dataset is the run's --dataset; @v1 replaces it.
    """
    atoms = _variant_atoms(name)
    modes = sorted(a[len("mode:"):] for a in atoms if a.startswith("mode:"))
    sort_opt = dict(SORT_OPT_DEFAULTS)
    set_by = {}
    for a in sorted(atoms & set(SORT_OPT_ATOMS)):
        key, value = SORT_OPT_ATOMS[a]
        if key in set_by:
            raise ValueError(f"variant [{name}]: {set_by[key]} and {a} both set {key}")
        set_by[key] = a
        sort_opt[key] = value
    ds = "logs_v1" if "@v1" in atoms else dataset
    if ds not in DATASETS:
        raise ValueError(f"variant [{name}]: unknown dataset {ds}; known {list(DATASETS)}")
    if "@split" in atoms and not DATASETS[ds]["twin"]:
        raise ValueError(f"variant [{name}]: dataset {ds} has no {SPLIT_FIELD}")
    return {"name": name, "dataset": ds, "field": SPLIT_FIELD if "@split" in atoms else TIME_FIELD,
            "agg_mode": modes[0] if modes else None, "sort_opt": sort_opt}


def split_format_hash(fork):
    """First 8 hex digits of the SHA-1 over SPLIT_FORMAT_FILES under fork (None if one is missing)."""
    h = hashlib.sha1()
    for rel in SPLIT_FORMAT_FILES:
        path = os.path.join(fork, rel)
        if not os.path.exists(path):
            return None
        with open(path, "rb") as f:
            h.update(f.read())
    return h.hexdigest()[:8]


def index_name(dataset, docs, seed, fork):
    """
    The index of a dataset; its name carries the postings format hash of --fork, and for a dataset with the twin field
    also the split points format hash.
    """
    tag = bp.format_hash(fork)
    if tag is None:
        raise RuntimeError(f"cannot find the format sources under --fork {fork}")
    name = f"{dataset}_{docs}_{seed}_{tag}"
    if DATASETS[dataset]["twin"]:
        split_tag = split_format_hash(fork)
        if split_tag is None:
            raise RuntimeError(f"cannot find the split points format sources under --fork {fork}")
        name += f"_{split_tag}"
    return name


VARIANTS = [resolve_variant("stock", DEFAULT_DATASET)]
DEFAULT_QUERIES = [
    "dh:s50:7d", "dh:s10:7d", "dh:s1:7d", "dh:s10:1d",
    "dh_avg:s50:7d", "dh_avg:s10:7d", "dh_avg:s10:1d",
    "terms:s50:7d", "terms:s10:7d", "terms:s1:7d", "terms:s10:1d",
]
SORT_QUERIES = [
    f"sort_{order}:{sel}:{window}{size}{tth}"
    for order in ("desc", "asc")
    for sel, window in (("s10", "7d"), ("s10", "1d"), ("all", "7d"), ("all", "1d"), ("bare", "1d"), ("all", "all"))
    for size in ("", ":500")
    for tth in ("", ":nt")
]
# the 24 order x sel/window x size shapes of SORT_QUERIES with an exact hit count
SORT_TT_QUERIES = [q + ":tt" for q in SORT_QUERIES if not q.endswith(":nt")]
QUERY_GROUPS = {"sort": SORT_QUERIES, "sort_tt": SORT_TT_QUERIES}
SPLIT_SUFFIX = ":f=split"


def expand_queries(arg):
    """Comma-separated specs and group names (sort, sort_tt) -> list of specs, each checked."""
    specs = []
    for s in arg.split(","):
        specs.extend(QUERY_GROUPS.get(s, [s]))
    for s in specs:
        parse_query(s)
    return specs


def spec_field(spec):
    """(spec without :f=split, the field its sort and range use when the spec asks for the twin, else None)."""
    if spec.endswith(SPLIT_SUFFIX):
        return spec[: -len(SPLIT_SUFFIX)], SPLIT_FIELD
    return spec, None


def query_field(variant, spec):
    """The field a variant's query of spec sorts and filters on: the twin if the variant or the spec asks for it."""
    return spec_field(spec)[1] or variant["field"]


def mapping(dataset=DEFAULT_DATASET):
    m = {
        "dynamic": "strict",
        "_source": {"enabled": False},
        "properties": {
            "@timestamp": {"type": "date"},
            "service": {"type": "keyword"},
            "status": {"type": "keyword"},
            "latency": {"type": "long"},
            "sel": {"type": "keyword", "doc_values": False},
        },
    }
    if DATASETS[dataset]["twin"]:
        # skip_list: the default skip list applies only to the name @timestamp (or the index sort field)
        m["properties"][SPLIT_FIELD] = {"type": "date", "skip_list": True, "meta": {"points_format": "Lucene90Split"}}
    return m


def settings(dataset=DEFAULT_DATASET):
    s = bp.index_settings()
    # merges only join adjacent segments, so doc ID order stays ingest (time) order
    s["index.merge.policy"] = "log_byte_size"
    if DATASETS[dataset]["twin"]:
        # concurrent search is turned off only for the name @timestamp; the twin must run the same path
        s["index.search.concurrent_segment_search.mode"] = "none"
    return s


def bulk_bodies(index, num_docs, seed, dataset=DEFAULT_DATASET):
    action = json.dumps({"index": {"_index": index}})
    twin = DATASETS[dataset]["twin"]
    total_s = SERVICE_CUM[-1]
    step = SPAN_MS / num_docs
    for b in range(math.ceil(num_docs / BATCH)):
        rng = random.Random(seed * 1_000_003 + b)
        lines = []
        for i in range(b * BATCH, min((b + 1) * BATCH, num_docs)):
            doc = {
                "@timestamp": START_MS + int(i * step) + rng.randrange(1000),
                "service": SERVICES[bisect.bisect_left(SERVICE_CUM, rng.random() * total_s)],
                "status": STATUS[min(len(STATUS) - 1, bisect.bisect_left(STATUS_CUM, rng.random()))][0],
                "latency": min(60_000, int(math.exp(rng.gauss(math.log(50), 1.0)))),
            }
            if twin:
                doc[SPLIT_FIELD] = doc["@timestamp"]
            sel = [t for t, p in SEL if rng.random() < p]
            if sel:
                doc["sel"] = sel
            lines.append(action)
            lines.append(json.dumps(doc))
        yield ("\n".join(lines) + "\n").encode()


def ingest(client, index, num_docs, seed, dataset=DEFAULT_DATASET):
    print(f"creating [{index}] and ingesting {num_docs:,} docs (seed {seed}, dataset {dataset}, one bulk thread) ...",
          flush=True)
    client.request("PUT", f"/{index}", {"settings": settings(dataset), "mappings": mapping(dataset)})
    start = last = time.time()
    done = 0
    for body in bulk_bodies(index, num_docs, seed, dataset):
        resp = client.request("POST", "/_bulk", body, content_type="application/x-ndjson", timeout=1200)
        if resp.get("errors"):
            first = next(i for i in resp["items"] if "error" in i["index"])
            raise RuntimeError(f"bulk error: {first}")
        done += len(resp["items"])
        if time.time() - last > 10:
            last = time.time()
            print(f"  {done:,} docs ({done / max(time.time() - start, 1e-3):,.0f}/s)", flush=True)
    print(f"  {done:,} docs in {time.time() - start:.0f}s", flush=True)
    client.request("POST", f"/{index}/_refresh")
    print("  force merging to one segment ...", flush=True)
    start = time.time()
    bp.force_merge(client, index)
    print(f"  merged in {time.time() - start:.0f}s", flush=True)


def segment_info(client, index):
    segments = client.request("GET", f"/{index}/_segments")["indices"][index]["shards"]["0"][0]["segments"]
    if len(segments) != 1:
        raise RuntimeError(f"[{index}] has {len(segments)} segments, expected 1")
    seg = next(iter(segments.values()))
    return {"segments": 1, "segment_docs": seg["num_docs"], "segment_bytes": seg["size_in_bytes"]}


def doc_order(client, index, num_docs, samples=5, size=5000):
    """
    Time order of docs by doc ID, at a few places in the segment: the share of adjacent pairs less than 2 s apart (local
    order up to the jitter), and the doc IDs where @timestamp jumps by more than a minute (boundaries of time runs),
    found by sampling every 100,000th doc.
    """
    local = total = 0
    for s in range(samples):
        after = (num_docs - size) * s // max(1, samples - 1) - 1
        body = {"size": size, "sort": [{"_doc": "asc"}], "docvalue_fields": [{"field": "@timestamp", "format": "epoch_millis"}],
                "stored_fields": "_none_"}
        if after >= 0:
            body["search_after"] = [after]
        hits = client.request("POST", f"/{index}/_search?request_cache=false", body)["hits"]["hits"]
        ts = [int(float(h["fields"]["@timestamp"][0])) for h in hits]
        local += sum(1 for a, b in zip(ts, ts[1:]) if abs(b - a) < 2000)
        total += len(ts) - 1
    step = 100_000
    sampled = []
    for doc in range(0, num_docs, step):
        body = {"size": 1, "sort": [{"_doc": "asc"}], "docvalue_fields": [{"field": "@timestamp", "format": "epoch_millis"}],
                "stored_fields": "_none_"}
        if doc > 0:
            body["search_after"] = [doc - 1]
        h = client.request("POST", f"/{index}/_search?request_cache=false", body)["hits"]["hits"][0]
        sampled.append((h["sort"][0], int(float(h["fields"]["@timestamp"][0]))))
    expected_gap = SPAN_MS / num_docs * step
    breaks = [d for (_, a), (d, b) in zip(sampled, sampled[1:]) if abs(b - a - expected_gap) > 60_000]
    return {"adjacent_within_2s": local / max(1, total), "time_run_starts": breaks}


def is_sort(spec):
    return spec.startswith("sort_")


def parse_sort(spec):
    """
    sort_ORDER:SEL:WINDOW[:SIZE][:nt|:tt][:f=split] -> (order, sel, window, size, track_total_hits or None for the
    default).
    """
    parts = spec_field(spec)[0].split(":")
    order = parts[0][len("sort_"):]
    if len(parts) < 3 or order not in ("desc", "asc"):
        raise ValueError(f"bad query spec {spec}")
    sel, window, rest = parts[1], parts[2], parts[3:]
    size, tth = 10, None
    for p in rest:
        if p == "nt":
            tth = False
        elif p == "tt":
            tth = True
        elif p.isdigit():
            size = int(p)
        else:
            raise ValueError(f"bad query spec {spec}")
    sels = {t for t, _ in SEL} | {"all", "bare"}
    if sel not in sels or window not in set(WINDOWS) | {"all"} or (sel == "bare" and window == "all"):
        raise ValueError(f"bad query spec {spec}")
    return order, sel, window, size, tth


def parse_query(spec):
    if is_sort(spec):
        return parse_sort(spec)
    parts = spec_field(spec)[0].split(":")
    if len(parts) != 3:
        raise ValueError(f"bad query spec {spec}")
    agg, sel, window = parts
    if agg not in ("dh", "dh_avg", "terms") or sel not in {t for t, _ in SEL} or window not in WINDOWS:
        raise ValueError(f"bad query spec {spec}")
    return agg, sel, window


FINGERPRINT_FIELDS = [{"field": TIME_FIELD, "format": "epoch_millis"}, "service", "status", "latency"]


def sort_body(spec, with_ids=False, field=None, fingerprint=False):
    """
    The sort request of spec, sorting and filtering on field (default: @timestamp, or the twin for :f=split). With
    with_ids the hits carry _id; with fingerprint also the doc values of FINGERPRINT_FIELDS.
    """
    field = field or spec_field(spec)[1] or TIME_FIELD
    order, sel, window, size, tth = parse_sort(spec)
    rng = None
    if window != "all":
        lo, hi, _ = WINDOWS[window]
        rng = {"range": {field: {"gte": START_MS + lo, "lt": START_MS + hi, "format": "epoch_millis"}}}
    if sel == "bare":
        query = rng
    elif sel == "all":
        query = {"bool": {"filter": [rng]}} if rng else {"match_all": {}}
    else:
        query = {"bool": {"filter": [{"term": {"sel": sel}}] + ([rng] if rng else [])}}
    body = {"size": size, "query": query, "sort": [{field: order}]}
    if tth is not None:
        body["track_total_hits"] = tth
    if not with_ids:
        body["stored_fields"] = "_none_"
    if fingerprint:
        body["docvalue_fields"] = FINGERPRINT_FIELDS
    return body


def query_body(spec, field=None):
    """
    The timed request of spec. field (default: @timestamp, or the twin for :f=split) is the sort and range field of a
    sort spec, and the range field of an aggregation spec (the aggregation stays on @timestamp).
    """
    field = field or spec_field(spec)[1] or TIME_FIELD
    if is_sort(spec):
        return sort_body(spec, field=field)
    agg, sel, window = parse_query(spec)
    lo, hi, interval = WINDOWS[window]
    query = {"bool": {"filter": [
        {"term": {"sel": sel}},
        {"range": {field: {"gte": START_MS + lo, "lt": START_MS + hi, "format": "epoch_millis"}}},
    ]}}
    avg = {"avg_latency": {"avg": {"field": "latency"}}}
    if agg == "terms":
        aggs = {"a": {"terms": {"field": "service", "size": 10}, "aggs": avg}}
    else:
        aggs = {"a": {"date_histogram": {"field": "@timestamp", "fixed_interval": interval, "min_doc_count": 1}}}
        if agg == "dh_avg":
            aggs["a"]["aggs"] = avg
    return {"size": 0, "track_total_hits": False, "query": query, "aggs": aggs}


def set_variant(client, variant):
    """
    Posts the variant's agg_batch mode and its full sort_opt set (defaults for absent switches), every time, and checks
    that the node reports the values it was sent.
    """
    client.request("POST", f"/_bufferpool/agg_batch?mode={variant['agg_mode'] or 'off'}")
    query = "&".join(f"{k}={v}" for k, v in variant["sort_opt"].items())
    got = client.request("POST", f"/_bufferpool/sort_opt?{query}")
    wrong = {k: got.get(k) for k, v in variant["sort_opt"].items() if str(got.get(k)).lower() != v}
    if wrong:
        raise RuntimeError(f"variant {variant['name']}: node reports sort_opt {wrong}, sent {variant['sort_opt']}")


def reset_variant(client):
    """Back to stock: agg_batch off and every sort_opt switch at its default."""
    set_variant(client, resolve_variant("stock", DEFAULT_DATASET))


def wait_for_load(limit=10.0, pause=30):
    """Waits while the 1-minute load average is above limit (the machine is shared); returns the load average."""
    while os.getloadavg()[0] > limit:
        print(f"  load average {os.getloadavg()[0]:.1f} > {limit:g}, waiting {pause}s ...", flush=True)
        time.sleep(pause)
    return os.getloadavg()


def search(client, index, body):
    start = time.perf_counter()
    resp = client.request("POST", f"/{index}/_search?request_cache=false", body)
    return resp, (time.perf_counter() - start) * 1000


def result_of(resp):
    """
    The aggregation result for comparison: bucket keys, doc counts and avg values, exactly as returned. For a query
    without aggregations: the total hits (value, relation) and the sort values of the hits, in order.
    """
    if "aggregations" not in resp:
        total = resp["hits"].get("total")
        return [(total["value"], total["relation"]) if total else None] + [tuple(h["sort"]) for h in resp["hits"]["hits"]]
    out = []
    for b in resp["aggregations"]["a"]["buckets"]:
        avg = b.get("avg_latency", {}).get("value")
        out.append((b["key"], b["doc_count"], avg))
    return out


def fingerprint(hit):
    """The doc values of FINGERPRINT_FIELDS of a hit: the same doc in another index has another _id but these values."""
    f = hit.get("fields", {})
    return tuple(tuple(str(v) for v in f.get(x["field"] if isinstance(x, dict) else x, [])) for x in FINGERPRINT_FIELDS)


def tie_groups(hits):
    """[(sort values, sorted fingerprints)] per run of hits with equal sort values, in order."""
    groups = []
    for sort, fp in hits:
        if groups and groups[-1][0] == sort:
            groups[-1][1].append(fp)
        else:
            groups.append((sort, [fp]))
    return [(s, sorted(fps)) for s, fps in groups]


def same_docs_other_index(got, ref):
    """
    Hits of two indices with the same documents: same sort values in order; within each run of equal sort values the
    same fingerprints (in any order: ties follow doc order, which differs between indices), except the last run, which
    the top-k cut may split differently.
    """
    if [s for s, _ in got] != [s for s, _ in ref]:
        return False
    g, r = tie_groups(got), tie_groups(ref)
    return g[:-1] == r[:-1]


def check_same_results(client, spec):
    """
    Runs spec once per variant (on the variant's index and field) and compares with the first variant: the timed
    request's result (aggregation buckets; or hits.total value and relation and the sort values), and for sort specs
    the hits of a request that also fetches _id and the fingerprint doc values (the timed one does not): the same _id
    order on the same index, the same fingerprints (see same_docs_other_index) on another index.
    """
    ref = None
    for v in VARIANTS:
        set_variant(client, v)
        field = query_field(v, spec)
        resp, _ = search(client, v["index"], query_body(spec, field))
        got = result_of(resp)
        if is_sort(spec):
            id_resp, _ = search(client, v["index"], sort_body(spec, with_ids=True, field=field, fingerprint=True))
            hits = id_resp["hits"]["hits"]
            if len(got) < 2 or [tuple(h["sort"]) for h in hits] != got[1:] or result_of(id_resp)[0] != got[0]:
                raise RuntimeError(f"{v['name']} {spec}: no hits, or the hits with _id differ from the hits without")
            got = (got, [h["_id"] for h in hits], [(tuple(h["sort"]), fingerprint(h)) for h in hits])
        elif not got:
            raise RuntimeError(f"{v['name']} {spec}: no buckets")
        if ref is None:
            ref, ref_index = got, v["index"]
            continue
        if not is_sort(spec):
            same = got == ref
        elif got[0] != ref[0]:
            same = False
        elif v["index"] == ref_index:
            same = got[1] == ref[1] and got[2] == ref[2]
        else:
            same = same_docs_other_index(got[2], ref[2])
        if not same:
            if is_sort(spec) and got[0][0] != ref[0][0]:
                diff = [("hits.total", got[0][0], ref[0][0])]
            else:
                if not is_sort(spec):
                    a, b = got, ref
                elif v["index"] == ref_index and got[1] != ref[1]:
                    a, b = got[1], ref[1]
                else:
                    a, b = got[2], ref[2]
                diff = [(i, x, y) for i, (x, y) in enumerate(zip(a, b)) if x != y][:5] or [("length", len(a), len(b))]
            raise RuntimeError(f"{v['name']} {spec}: result differs from {VARIANTS[0]['name']}: {diff}")
    return ref[0] if is_sort(spec) else ref


def result_summary(spec, expected):
    """(buckets, docs in buckets) of an aggregation; for a sort query (hits returned, total hits value)."""
    if is_sort(spec):
        return len(expected) - 1, expected[0][0] if expected[0] else -1
    return len(expected), sum(c for _, c, _ in expected)


def io_type(key):
    """
    The file type of a stats key (BlockCache drops the segment name): its extension, but the split points format's
    files (Lucene90Split_0.kdd, ...) count apart from the stock points files, as split.kdd, split.kdi, ...
    """
    ext = key.rsplit(".", 1)[-1]
    return "split." + ext if key.startswith("Lucene90Split_") else ext


def summarize_io(io):
    """bench_postings.summarize_io with the file types of io_type."""
    loads = sum(v["loads"] + v["prefetch_loads"] for v in io.values())
    by_type = {}
    for key, v in io.items():
        t = io_type(key)
        by_type[t] = by_type.get(t, 0) + v["loads"] + v["prefetch_loads"]
    return loads, sum(v["bytes_loaded"] for v in io.values()), by_type


def io_snapshot(client):
    files = client.request("GET", "/_bufferpool/stats")["files"]
    return {k: v for k, v in files.items() if v["requests"] or v["loads"] or v["prefetch_loads"]}


def measure(client, spec, runs, cold, expected):
    out = {v["name"]: {"tooks": [], "walls": [], "loads": [], "io": None} for v in VARIANTS}
    bodies = {v["name"]: query_body(spec, query_field(v, spec)) for v in VARIANTS}
    if not cold:
        for v in VARIANTS:  # two warm-ups per variant: page in the blocks and let C2 compile the loop
            set_variant(client, v)
            search(client, v["index"], bodies[v["name"]])
            search(client, v["index"], bodies[v["name"]])
    for i in range(runs):
        order = VARIANTS[i % len(VARIANTS):] + VARIANTS[: i % len(VARIANTS)]
        for v in order:
            variant = v["name"]
            set_variant(client, v)
            if cold:
                client.request("POST", "/_bufferpool/cache/_clear")
            client.request("POST", "/_bufferpool/stats/_reset")
            resp, wall = search(client, v["index"], bodies[variant])
            if result_of(resp) != expected:
                raise RuntimeError(f"{variant} {spec}: result changed during measurement")
            io = io_snapshot(client)
            r = out[variant]
            r["tooks"].append(resp["took"])
            r["walls"].append(wall)
            r["loads"].append(summarize_io(io)[0])
            r["io"] = io
    return out


def run_matrix(client, specs, modes, latency, runs):
    results = []
    first = VARIANTS[0]["name"]
    try:
        bp.set_latency(client, latency)
        for spec in specs:
            expected = check_same_results(client, spec)
            for mode in modes:
                load = wait_for_load()
                started = time.time()
                by_variant = measure(client, spec, runs, mode == "cold", expected)
                metric = "tooks" if mode == "cold" else "walls"
                base = by_variant[first][metric]
                buckets, docs_in_buckets = result_summary(spec, expected)
                for v in VARIANTS:
                    variant = v["name"]
                    r = by_variant[variant]
                    loads, bytes_loaded, by_type = summarize_io(r["io"])
                    results.append({
                        "latency_ms": latency, "query": spec, "variant": variant, "mode": mode,
                        "index": v["index"], "field": query_field(v, spec), "agg_mode": v["agg_mode"],
                        "sort_opt": v["sort_opt"], "load_average": load,
                        "buckets": buckets, "docs_in_buckets": docs_in_buckets,
                        "took_ms": r["tooks"], "wall_ms": r["walls"], "loads_per_run": r["loads"],
                        "took": bp.distribution(r["tooks"]), "wall": bp.distribution(r["walls"]),
                        "loads": loads, "bytes_loaded": bytes_loaded, "loads_by_type": by_type, "io": r["io"],
                        "median_change_vs_first": None if variant == first else bp.median_change_ci(base, r[metric]),
                    })
                print(f"  {spec:<16} {mode}: {runs} x {len(VARIANTS)} runs in {time.time() - started:.0f}s "
                      f"(load {load[0]:.1f})", flush=True)
    finally:
        bp.set_latency(client, 0)
        reset_variant(client)
    return results


def print_report(meta, results):
    print(f"\n## {meta['docs']:,} docs, {meta['segment_bytes'] / 2**20:,.0f} MiB segment, {meta['runs']} runs per variant, "
          f"{meta['latency_ms']:g} ms per cold miss")
    print("cold: server took; warm: client wall. IOs = blocks loaded per query, by file type.")
    names = [v["name"] for v in VARIANTS]
    rows = {}
    for r in results:
        rows.setdefault((r["query"], r["mode"]), {})[r["variant"]] = r
    for (spec, mode), cells in rows.items():
        key = "took" if mode == "cold" else "wall"
        first = cells[names[0]]
        parts = []
        for v in names:
            c = cells[v]
            change = "" if v == names[0] else f" ({bp.fmt_ci(c['median_change_vs_first'])})"
            types = ",".join(f"{t}:{n}" for t, n in sorted(c["loads_by_type"].items()))
            parts.append(f"{v}: p50 {c[key]['p50']:.1f}ms{change} IOs {c['loads']} [{types}]")
        print(f"{mode:4} {spec:<16} docs {first['docs_in_buckets']:>11,} buckets {first['buckets']:>4} | " + " | ".join(parts))


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.abspath(os.path.join(here, "..", "..", ".."))
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:9200")
    parser.add_argument("--docs", type=int, required=True, help="docs in the index (one merged segment)")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--latency-ms", type=float, default=4, help="simulated storage latency per cold miss")
    parser.add_argument("--modes", default="cold,warm")
    parser.add_argument("--queries", default=",".join(DEFAULT_QUERIES))
    parser.add_argument("--fork", default=os.path.join(os.path.dirname(repo), "lucene_experiments"))
    parser.add_argument("--dataset", default=DEFAULT_DATASET, choices=sorted(DATASETS))
    parser.add_argument("--ingest", action="store_true", help="ingest a missing index (otherwise it is an error)")
    parser.add_argument("--ingest-only", action="store_true", help="ingest if missing (implies --ingest), then stop")
    parser.add_argument("--variants", help="comma-separated variant expressions (the first is the reference), see above")
    parser.add_argument("--out", default=os.path.join(os.path.expanduser("~"), "bufferpool-bench", "results"))
    args = parser.parse_args()
    global VARIANTS
    try:
        VARIANTS = [resolve_variant(w, args.dataset) for w in (args.variants or "stock").split(",")]
        specs = expand_queries(args.queries)
    except ValueError as e:
        sys.exit(str(e))
    names = [v["name"] for v in VARIANTS]
    if len(set(names)) != len(names):
        sys.exit(f"duplicate variants in {names}")
    client = bp.Client(args.url)
    try:
        main_index = index_name(args.dataset, args.docs, args.seed, args.fork)
        for v in VARIANTS:
            v["index"] = index_name(v["dataset"], args.docs, args.seed, args.fork)
    except RuntimeError as e:
        sys.exit(str(e))
    indices = {}
    datasets = {main_index: args.dataset, **{v["index"]: v["dataset"] for v in VARIANTS}}
    for index in [main_index] + [v["index"] for v in VARIANTS]:
        if index in indices:
            continue
        if not client.exists(index):
            if not (args.ingest or args.ingest_only):
                existing = sorted(i["index"] for i in client.request("GET", "/_cat/indices/logs_*?format=json"))
                sys.exit(f"index [{index}] does not exist; pass --ingest to build it. Existing logs_* indices: {existing}")
            ingest(client, index, args.docs, args.seed, datasets[index])
        else:
            print(f"reusing [{index}]", flush=True)
        count = client.request("GET", f"/{index}/_count")["count"]
        if count != args.docs:
            raise RuntimeError(f"[{index}] has {count} docs, expected {args.docs}")
        seg = segment_info(client, index)
        seg["timestamp_in_order"] = doc_order(client, index, args.docs)
        indices[index] = seg
    if args.ingest_only:
        print(json.dumps(indices))
        return
    stats = client.request("GET", "/_bufferpool/stats")
    meta = {
        "time": datetime.datetime.now().isoformat(timespec="seconds"),
        "index": main_index, "dataset": args.dataset, "docs": args.docs, "seed": args.seed, "runs": args.runs,
        "latency_ms": args.latency_ms,
        "block_size": stats["block_size"], "opensearch_head": bp.git_head(repo), "lucene_fork_head": bp.git_head(args.fork),
        "suite": "aggs", "variants": [[v["name"], v] for v in VARIANTS], "load_average": os.getloadavg(),
        **indices[main_index], "indices": indices,
    }
    print(json.dumps(meta), flush=True)
    results = run_matrix(client, specs, args.modes.split(","), args.latency_ms, args.runs)
    os.makedirs(args.out, exist_ok=True)
    out = os.path.join(args.out, f"aggs_{datetime.datetime.now():%Y%m%d_%H%M%S}.json")
    with open(out, "w") as f:
        json.dump({"runs": [{"meta": meta, "results": results}]}, f, indent=1)
    print_report(meta, results)
    print(f"\nraw results: {out}")


if __name__ == "__main__":
    main()
