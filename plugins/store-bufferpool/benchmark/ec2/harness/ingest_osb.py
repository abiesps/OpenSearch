#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Ingest driver for every OSB workload of the generic validation (and usable for the others): writes a derived,
ingest-only OSB workload from the workload's OWN index.json files, corpora and bulk operations, runs it with
OpenSearch Benchmark, and records the indexing metrics. The index bodies are rendered with jinja2 like OSB does,
then the recorded overrides are applied: number_of_shards / number_of_replicas, optional index renames, and the
split-BKD mapping meta {"points_format": "Lucene90Split"} on the named 1-D date fields (the B index). Everything else
(mappings, analysis, codec, index sort, the workload's bulk-size, ingest-percentage, conflicts) is the workload's own.

  ingest_osb.py render --osb-workloads DIR --corpus eventdata --out DIR/derived [--shards 6] [--replicas 0]
        [--rename eventdata=eventdata_split] [--split-fields @timestamp] [--procedure append|update]
        [--clients 8] [--bulk-size N]
  ingest_osb.py run --derived DIR/derived --url http://DATA:9200 --out ingest-1.json [--osb-bin opensearch-benchmark]
  ingest_osb.py check --osb-workloads DIR --corpus eventdata --url ... --agent ... --token-file ... \
        --sequence S0-EBS,POC-EBS,POC-EBS,S0-EBS,S0-EBS,POC-EBS --shards 6 --out results/eventdata/ingest-check
Test procedure "coldpath-ingest" (append): delete-index, create-index, cluster-health green, every bulk op of the
workload (geoshape: its three corpora into their three indices) with the given clients, then refresh. "update": the
workload's index-update op (OSB generates ids with conflicts=random, on-conflict=index, conflict-probability=25:
appends with updates in between) instead of the append op.
`run` records: OSB's own results (throughput docs/s, bulk latency percentiles, service time) from its CSV, the
wall time, and per index `_stats` (docs, indexing index_time, merges total_time and count, refresh and flush
total_time, store size), segment count and primaries.
`check` is the ingest check of the generic plan: fresh indices for every entry of --sequence (each a binary x
storage agent arm, restarted through the agent), interleaved (S0, POC, POC, S0, S0, POC: the spread within a
binary is the ingest A/A floor). The first entry keeps the workload's index names when --keep-first is given (it
becomes the stock-format index every S0/S1 arm shares); every other entry ingests into <index>__ichk<k> and those
indices, and only those, are deleted after their metrics are recorded (disk space: clickbench is 60-100 GB per copy).
`run` and `check` refuse to start when an index of the workload's own name already exists (the derived procedure
deletes it first; common-rules: never re-ingest when br-<branch>/ingest.json exists), unless --force-recreate.
"""
import argparse
import csv
import io
import json
import os
import re
import shutil
import subprocess
import sys
import time

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
import osb_import_ext  # noqa: E402
from common import JsonClient, wait_until  # noqa: E402

SPLIT_META = {"points_format": "Lucene90Split"}
CHECK_SUFFIX = "__ichk"


def load_profile(corpus):
    return json.load(open(os.path.join(here, "corpora", corpus + ".json")))


def render_workload_json(workload_dir, params):
    class _Helpers:  # OSB's benchmark.helpers for templates that use it without importing it (clickbench)
        @staticmethod
        def collect(parts):
            return ""

    env = osb_import_ext.jinja_env(workload_dir)
    text = env.get_template("workload.json").render(benchmark=_Helpers(), **params)
    return json.loads(text)


def set_field_meta(mappings, dotted, meta):
    """Adds meta to a (possibly nested / object) field of the mapping; the field must be a date or numeric type."""
    node = mappings
    parts = dotted.split(".")
    for i, p in enumerate(parts):
        props = node.get("properties")
        if props is None or p not in props:
            raise KeyError(f"field {dotted} not in the mapping (at {'.'.join(parts[:i + 1])})")
        node = props[p]
    if node.get("type") not in ("date", "date_nanos", "long", "integer", "short", "byte", "double", "float"):
        raise ValueError(f"field {dotted} has type {node.get('type')}: the split points format is for 1-D points fields")
    node["meta"] = {**node.get("meta", {}), **meta}
    return node


def derive(osb_workloads, corpus, out, shards=None, replicas=0, renames=None, split_fields=None, procedure="append",
           clients=8, bulk_size=None, params=None):
    """Writes the derived workload into `out`; returns its derivation record."""
    profile = load_profile(corpus)
    wd = os.path.join(osb_workloads, profile["osb"]["workload"])
    params = dict(params or {})
    renames = dict(renames or {})
    wl = render_workload_json(wd, params)
    items = []
    for rel in osb_import_ext.ops_files(wd, {"ops_files": ["operations/*.json"]}):
        items += osb_import_ext.render_items(wd, rel, params)
    bulk = [o for o in items if o.get("operation-type") == "bulk"]
    if procedure == "update":
        chosen = [o for o in bulk if o["name"] == "index-update"]
        if not chosen:
            raise RuntimeError(f"{corpus}: no index-update op (only {[o['name'] for o in bulk]})")
    else:
        chosen = [o for o in bulk if o["name"] != "index-update"]
    if not chosen:
        raise RuntimeError(f"{corpus}: no bulk op")
    if os.path.exists(out):
        shutil.rmtree(out)
    os.makedirs(out)
    indices, overrides = [], []
    for idx in wl["indices"]:
        body_rel = idx["body"]
        env = osb_import_ext.jinja_env(wd)
        body = json.loads(env.get_template(body_rel).render(**params))
        st = body.setdefault("settings", {})
        if shards is not None:
            st.pop("number_of_shards", None)
            st["index.number_of_shards"] = int(shards)
            overrides.append(f"{idx['name']}: index.number_of_shards={shards}")
        st.pop("number_of_replicas", None)
        st["index.number_of_replicas"] = int(replicas)
        overrides.append(f"{idx['name']}: index.number_of_replicas={replicas}")
        for f in (split_fields or []):
            set_field_meta(body["mappings"], f, SPLIT_META)
            overrides.append(f"{idx['name']}: mapping {f} meta {json.dumps(SPLIT_META)}")
        name = renames.get(idx["name"], idx["name"])
        fname = f"index-{name}.json"
        with open(os.path.join(out, fname), "w") as f:
            json.dump(body, f, indent=1, sort_keys=True)
        indices.append({"name": name, "body": fname, "source_index": idx["name"], "source_body": body_rel})
    base_urls = profile.get("corpus_base_urls", {})
    corpora = []
    for c in wl["corpora"]:
        c = json.loads(json.dumps(c))
        if not c.get("base-url"):
            if c["name"] not in base_urls:
                raise RuntimeError(f"{corpus}: corpus {c['name']} has no base-url and the profile gives none")
            c["base-url"] = base_urls[c["name"]]
            overrides.append(f"corpus {c['name']}: base-url {c['base-url']} (the workload has none)")
        if c.get("target-index"):
            c["target-index"] = renames.get(c["target-index"], c["target-index"])
        for d in c["documents"]:
            if d.get("target-index"):
                d["target-index"] = renames.get(d["target-index"], d["target-index"])
        corpora.append(c)
    ops = []
    for o in chosen:
        o = json.loads(json.dumps(o))
        if bulk_size is not None:
            o["bulk-size"] = int(bulk_size)
            overrides.append(f"op {o['name']}: bulk-size={bulk_size}")
        ops.append(o)
    names = [i["name"] for i in indices]
    schedule = [{"operation": {"operation-type": "delete-index"}},
                {"operation": {"operation-type": "create-index"}},
                {"name": "check-cluster-health", "operation": {"operation-type": "cluster-health", "index": ",".join(names),
                 "request-params": {"wait_for_status": "green", "wait_for_no_relocating_shards": "true"},
                 "retry-until-success": True}}]
    for o in ops:
        schedule.append({"operation": o["name"], "warmup-time-period": 0, "clients": int(clients)})
    schedule.append({"name": "refresh-after-ingest", "operation": {"operation-type": "refresh", "index": ",".join(names)}})
    derived = {"version": 2, "description": f"coldpath derived ingest-only workload of {corpus} ({procedure})",
               "indices": [{"name": i["name"], "body": i["body"]} for i in indices], "corpora": corpora,
               "operations": ops, "test_procedures": [{"name": "coldpath-ingest", "default": True, "schedule": schedule}]}
    with open(os.path.join(out, "workload.json"), "w") as f:
        json.dump(derived, f, indent=1)
    rec = {"corpus": corpus, "workload": profile["osb"]["workload"], "procedure": procedure, "indices": indices,
           "bulk_ops": [o["name"] for o in ops], "clients": int(clients), "overrides": overrides, "params": params,
           "corpora": [{"name": c["name"], "base-url": c.get("base-url"),
                        "documents": [{k: d.get(k) for k in ("source-file", "document-count", "compressed-bytes",
                                                             "uncompressed-bytes", "target-index")} for d in c["documents"]]}
                       for c in corpora],
           "expected_docs": {i["name"]: _expected_docs(corpora, i["source_index"], i["name"], len(indices)) for i in indices}}
    with open(os.path.join(out, "derivation.json"), "w") as f:
        json.dump(rec, f, indent=1)
    return rec


def _expected_docs(corpora, source, name, n_indices):
    total = 0
    for c in corpora:
        for d in c["documents"]:
            tgt = d.get("target-index") or c.get("target-index")
            if tgt in (source, name) or (tgt is None and n_indices == 1):
                total += d.get("document-count") or 0
    return total


def osb_results(path):
    """OSB CSV results -> {task: {metric: [value, unit]}} (and the global metrics under task '')."""
    out = {}
    if not os.path.exists(path):
        return out
    for row in csv.reader(io.StringIO(open(path).read())):
        if len(row) < 4 or row[0] == "Metric":
            continue
        metric, task, value, unit = row[:4]
        try:
            v = float(value)
        except ValueError:
            v = value
        out.setdefault(task, {})[metric] = [v, unit]
    return out


def index_metrics(client, names):
    out = {}
    for n in names:
        st = client.request("GET", f"/{n}/_stats/docs,indexing,merge,refresh,flush,store,segments")["_all"]["primaries"]
        row = client.request("GET", f"/_cat/indices/{n}?format=json&bytes=b&h=pri,rep,docs.count,pri.store.size")[0]
        segs = client.request("GET", f"/_cat/segments/{n}?format=json&h=shard,prirep,segment")
        # _cat/indices docs.count counts Lucene documents, which include nested child documents (nested: 29.4M
        # Lucene docs for 11.2M top-level docs); the workload's document-count is the top-level count (_count API)
        out[n] = {"docs": client.request("GET", f"/{n}/_count")["count"], "lucene_docs": int(row["docs.count"]),
                  "primaries": int(row["pri"]), "replicas": int(row["rep"]),
                  "pri_store_bytes": int(row["pri.store.size"]), "segments": sum(1 for s in segs if s["prirep"] in ("p", "primary")),
                  "index_time_ms": st["indexing"]["index_time_in_millis"], "index_total": st["indexing"]["index_total"],
                  "merge_total_time_ms": st["merges"]["total_time_in_millis"], "merges_total": st["merges"]["total"],
                  "merge_throttled_ms": st["merges"].get("total_throttled_time_in_millis"),
                  "refresh_total_time_ms": st["refresh"]["total_time_in_millis"],
                  "flush_total_time_ms": st["flush"]["total_time_in_millis"],
                  "store_bytes": st["store"]["size_in_bytes"]}
    return out


def refuse_existing(client, names, force_recreate=False):
    """
    The derived test procedure starts with delete-index. An index that already exists under one of `names` (the
    workload's own name: built by ec2-stock-ingest or an earlier run) is never deleted and rebuilt unless
    force_recreate is given (common-rules: when br-<branch>/ingest.json exists, never re-ingest). The ingest-check
    copies <index>__ichk<k> are this script's own and are always re-created. Returns the protected names that exist.
    """
    existing = []
    for n in names:
        if re.search(re.escape(CHECK_SUFFIX) + r"\d+$", n):
            continue
        status, _ = client.raw("GET", f"/_cat/indices/{n}?format=json&h=index&expand_wildcards=all")
        if status == 200:
            existing.append(n)
        elif status != 404:
            raise RuntimeError(f"cannot tell whether index {n} exists: HTTP {status}")
    if existing and not force_recreate:
        raise RuntimeError(f"refusing to delete and re-create existing index {existing}: the derived workload starts "
                           "with delete-index (common-rules: never re-ingest when br-<branch>/ingest.json exists); "
                           "pass --force-recreate to rebuild it on purpose")
    return existing


def run_osb(derived, url, out_json, osb_bin, user_tag="", force_recreate=False):
    import osb_cold  # the same OSB subcommand detection as the cold executor

    rec = json.load(open(os.path.join(derived, "derivation.json")))
    recreated = refuse_existing(JsonClient(url), [i["name"] for i in rec["indices"]], force_recreate)
    results = os.path.splitext(out_json)[0] + ".osb.csv"
    host = url.split("://", 1)[1].rstrip("/")
    cmd = [osb_bin, osb_cold.osb_subcommand(osb_bin), "--pipeline=benchmark-only", f"--workload-path={derived}",
           "--test-procedure=coldpath-ingest", f"--target-hosts={host}", "--kill-running-processes", "--on-error=abort",
           f"--results-file={results}", "--results-format=csv"]
    if user_tag:
        cmd.append(f"--user-tag={user_tag}")
    if url.startswith("https"):
        cmd.append("--client-options=use_ssl:true,verify_certs:false")
    t0 = time.time()
    p = subprocess.run(cmd, capture_output=True, text=True)
    wall = time.time() - t0
    log = os.path.splitext(out_json)[0] + ".osb.log"
    with open(log, "w") as f:
        f.write(" ".join(cmd) + "\n" + p.stdout + "\n" + p.stderr)
    if p.returncode != 0 or "[ERROR]" in p.stdout:
        raise RuntimeError(f"OSB ingest failed (rc {p.returncode}); see {log}:\n" + (p.stdout + p.stderr)[-3000:])
    client = JsonClient(url)
    names = [i["name"] for i in rec["indices"]]
    metrics = index_metrics(client, names)
    bad = {n: (m["docs"], rec["expected_docs"][n]) for n, m in metrics.items()
           if rec["expected_docs"].get(n) and m["docs"] != rec["expected_docs"][n] and rec["procedure"] == "append"}
    out = {"derivation": rec, "osb_cmd": cmd, "wall_s": wall, "osb": osb_results(results), "indices": metrics,
           "recreated_existing": recreated,
           "doc_count_ok": not bad, "doc_count_mismatch": bad,
           "node": client.request("GET", "/"), "t": time.time()}
    with open(out_json, "w") as f:
        json.dump(out, f, indent=1)
    if bad:
        raise RuntimeError(f"doc count differs from the workload's document-count: {bad}")
    return out


def _parse_kv(items):
    out = {}
    for it in items or []:
        k, v = it.split("=", 1)
        out[k] = v
    return out


def cmd_render(a):
    rec = derive(a.osb_workloads, a.corpus, a.out, a.shards, a.replicas, _parse_kv(a.rename),
                 [f for f in (a.split_fields or "").split(",") if f], a.procedure, a.clients, a.bulk_size, _parse_kv(a.param))
    print(json.dumps(rec, indent=1))


def cmd_run(a):
    out = run_osb(a.derived, a.url, a.out, a.osb_bin, a.user_tag, a.force_recreate)
    print(json.dumps({k: out[k] for k in ("wall_s", "indices", "doc_count_ok")}, indent=1))


def _close_open_indices(client):
    rows = client.request("GET", "/_cat/indices?format=json&h=index,status&expand_wildcards=open")
    names = [r["index"] for r in rows if r["status"] == "open" and not r["index"].startswith(".")]
    for n in names:
        client.request("POST", f"/{n}/_close?wait_for_active_shards=0")
    return names


def cmd_check(a):
    profile = load_profile(a.corpus)
    seq = a.sequence.split(",")
    os.makedirs(a.out, exist_ok=True)
    token = open(a.token_file).read().strip() if a.token_file else ""
    agent = JsonClient(a.agent, timeout=900.0, headers={"X-Coldpath-Token": token})
    client = JsonClient(a.url)
    names = [i["name"] for i in profile["indices"]]
    summary = []
    for k, arm in enumerate(seq):
        keep = k == 0 and a.keep_first
        suffix = "" if keep else f"{CHECK_SUFFIX}{k}"
        renames = {n: n + suffix for n in names} if suffix else {}
        derived = os.path.join(a.out, f"derived-{k}")
        derive(a.osb_workloads, a.corpus, derived, a.shards, 0, renames,
               [f for f in (a.split_fields or "").split(",") if f], a.procedure, a.clients, a.bulk_size)
        try:
            closed = _close_open_indices(client)
        except Exception:  # noqa: BLE001 - node not running yet
            closed = []
        res = agent.request("POST", "/node/restart", {"arm": arm})
        wait_until(lambda: client.request("GET", "/", timeout=5), 900, 1.0, "OpenSearch HTTP")
        client.request("GET", "/_cluster/health?wait_for_status=green&timeout=900s", timeout=930)
        out_json = os.path.join(a.out, f"ingest-{k}-{arm}.json")
        r = run_osb(derived, a.url, out_json, a.osb_bin, f"coldpath_ingest_check:{k},arm:{arm}", a.force_recreate)
        r["arm"], r["closed_before_restart"], r["agent_restart"] = arm, closed, res
        json.dump(r, open(out_json, "w"), indent=1)
        summary.append({"k": k, "arm": arm, "file": out_json, "wall_s": r["wall_s"], "kept": keep,
                        "indices": r["indices"], "osb_bulk": {t: v for t, v in r["osb"].items() if t}})
        if suffix:
            for n in renames.values():
                if not n.endswith(suffix):
                    raise RuntimeError(f"refusing to delete {n}: not an ingest-check index")
                client.request("DELETE", f"/{n}")
    json.dump({"corpus": a.corpus, "sequence": seq, "runs": summary}, open(os.path.join(a.out, "summary.json"), "w"), indent=1)
    print(json.dumps(summary, indent=1))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("render", "check"):
        p = sub.add_parser(name)
        p.add_argument("--osb-workloads", required=True)
        p.add_argument("--corpus", required=True)
        p.add_argument("--shards", type=int)
        p.add_argument("--split-fields", help="comma list of 1-D date fields that get the split-BKD meta (B index)")
        p.add_argument("--procedure", choices=["append", "update"], default="append")
        p.add_argument("--clients", type=int, default=8, help="bulk clients (the workloads' bulk_indexing_clients default)")
        p.add_argument("--bulk-size", type=int, help="default: the workload's own bulk-size")
        p.add_argument("--out", required=True)
    r = sub.choices["render"]
    r.add_argument("--replicas", type=int, default=0)
    r.add_argument("--rename", action="append", help="OLD=NEW index name")
    r.add_argument("--param", action="append", help="workload parameter NAME=VALUE (rendered into the templates)")
    c = sub.choices["check"]
    c.add_argument("--url", required=True)
    c.add_argument("--agent", required=True)
    c.add_argument("--token-file")
    c.add_argument("--sequence", default="S0-EBS,POC-EBS,POC-EBS,S0-EBS,S0-EBS,POC-EBS")
    c.add_argument("--keep-first", action="store_true", help="the first ingest keeps the workload's index names")
    c.add_argument("--osb-bin", default="opensearch-benchmark")
    c.add_argument("--force-recreate", action="store_true", help="--keep-first: delete and rebuild an existing index "
                   "of the workload's name (refused without it)")
    u = sub.add_parser("run")
    u.add_argument("--derived", required=True)
    u.add_argument("--url", required=True)
    u.add_argument("--out", required=True)
    u.add_argument("--osb-bin", default="opensearch-benchmark")
    u.add_argument("--user-tag", default="")
    u.add_argument("--force-recreate", action="store_true", help="delete and rebuild an existing index of the "
                   "derived workload (refused without it)")
    a = ap.parse_args()
    {"render": cmd_render, "run": cmd_run, "check": cmd_check}[a.cmd](a)


if __name__ == "__main__":
    main()
