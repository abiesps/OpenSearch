#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Index preparation for coldbench, run once per corpus before any measurement (never between arms).

  indexprep.py single-shard --url U --source big5 --target big5_1s [--max-docs N] [--min-size-gb 30]
      creates TARGET with 1 primary and 0 replicas, the source's mappings and analysis settings, refresh off during the
      copy, then _reindex from SOURCE (all docs; or a contiguous time slice with --range-gte/--range-lt, optionally
      capped by --max-docs); restores the refresh interval, refreshes, and checks the primary store size (>= 30 GB)
  indexprep.py forcemerge --url U --index X --segments N
      _forcemerge?max_num_segments=N, waits, then verifies that EVERY shard has exactly N searchable segments
      (fewer means the shard had too few docs/segments for N: reported as an error, choose another N)
  indexprep.py clone --url U --source X --target Y [--segments 1]
      write-blocks X, _clone X -> Y (hard links: identical files), then force-merges Y to --segments (the 1-segment
      variant of the laptop layout) and verifies it
  indexprep.py describe --url U --index X [--agent A --token-file T --agent-arm S0-EBS]
      docs, primaries, replicas, primary/total store bytes (_cat/indices?bytes=b), segments per shard, and the
      on-disk du of the shard directories through the agent
  indexprep.py formats --url U --agent A --token-file T --agent-arm POC-EBS --index X
          (--points FIELD=Lucene90Split ... --postings FIELD=Lucene104Nav ... | --from-mapping | --control)
      post-ingest segment format check (run it after the ingest and after every force-merge or clone, before any
      result of the index is reported): the agent reads every shard's last commit from the segment files, and every
      segment that holds an expected field must carry the per-field attribute (PerFieldPointsFormat.format,
      PerFieldPostingsFormat.format) and the format's files (_Lucene90Split_0.kdm/kdi/kdd, _Lucene104Nav_N.nav);
      --control: no segment may have either format. Never judged from the codec name. Exit status 1 on any mismatch.
      Without an agent, run agent/coldpath_segformat.py --dir on the data node itself. See segformat_check.py.
Every command prints JSON, so the result can be stored next to the session.
"""
import argparse
import json
import os
import sys
import time

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
from common import JsonClient  # noqa: E402
import segformat_check  # noqa: E402

GB = 1 << 30


def wait_task(c, task, poll=10):
    while True:
        t = c.request("GET", f"/_tasks/{task}")
        if t.get("completed"):
            if t.get("error") or (t.get("response") or {}).get("failures"):
                raise RuntimeError(f"task {task} failed: {json.dumps(t)[:2000]}")
            return t
        st = t["task"].get("status", {})
        print(f"  {task}: created {st.get('created')} of {st.get('total')}", file=sys.stderr, flush=True)
        time.sleep(poll)


def segments_per_shard(c, index):
    per = {}
    for s in c.request("GET", f"/_cat/segments/{index}?format=json&h=shard,prirep,segment,searchable"):
        if s["prirep"] in ("p", "primary") and str(s.get("searchable")).lower() == "true":
            per[int(s["shard"])] = per.get(int(s["shard"]), 0) + 1
    return dict(sorted(per.items()))


def describe(c, index, agent=None, agent_arm=None):
    row = c.request("GET", f"/_cat/indices/{index}?format=json&bytes=b&h=index,uuid,health,status,pri,rep,docs.count,"
                           f"pri.store.size,store.size")[0]
    out = {"index": index, "uuid": row["uuid"], "health": row["health"], "status": row["status"],
           "primaries": int(row["pri"]), "replicas": int(row["rep"]), "docs": int(row["docs.count"]),
           "pri_store_bytes": int(row["pri.store.size"]), "store_bytes": int(row["store.size"]),
           "pri_store_gb": round(int(row["pri.store.size"]) / GB, 2), "segments_per_shard": segments_per_shard(c, index)}
    if agent:
        out["du"] = agent.request("GET", f"/index/du?arm={agent_arm}&uuids={row['uuid']}")
    return out


def forcemerge(c, index, n):
    t0 = time.time()
    c.request("POST", f"/{index}/_forcemerge?max_num_segments={n}&wait_for_completion=true", timeout=6 * 3600)
    while True:  # wait_for_completion can return on an HTTP timeout; wait until no merge runs
        st = c.request("GET", f"/{index}/_stats/merge")["_all"]["primaries"]["merges"]
        if st["current"] == 0:
            break
        time.sleep(10)
    c.request("POST", f"/{index}/_refresh")
    per = segments_per_shard(c, index)
    bad = {s: k for s, k in per.items() if k != n}
    if bad:
        raise RuntimeError(f"{index}: after force merge to {n}, shards with another segment count: {bad}")
    return {"index": index, "segments": n, "segments_per_shard": per, "elapsed_s": round(time.time() - t0, 1)}


def cmd_single_shard(a, c):
    src = c.request("GET", f"/{a.source}")[a.source]
    idx_settings = src["settings"]["index"]
    settings = {"index.number_of_shards": 1, "index.number_of_replicas": 0, "index.refresh_interval": "-1"}
    for k in ("codec", "sort", "query"):
        if k in idx_settings:
            settings[f"index.{k}"] = idx_settings[k]
    if "analysis" in idx_settings:
        settings["index.analysis"] = idx_settings["analysis"]
    c.request("PUT", f"/{a.target}", {"settings": settings, "mappings": src["mappings"]})
    body = {"source": {"index": a.source, "size": 5000}, "dest": {"index": a.target}}
    if a.range_gte or a.range_lt:
        rng = {k: v for k, v in (("gte", a.range_gte), ("lt", a.range_lt)) if v}
        body["source"]["query"] = {"range": {a.time_field: rng}}
    if a.max_docs:
        body["max_docs"] = a.max_docs
    r = c.request("POST", "/_reindex?wait_for_completion=false&refresh=false", body)
    wait_task(c, r["task"])
    c.request("PUT", f"/{a.target}/_settings", {"index.refresh_interval": idx_settings.get("refresh_interval", "1s")})
    c.request("POST", f"/{a.target}/_refresh")
    out = describe(c, a.target)
    out["source_docs"] = int(c.request("GET", f"/{a.source}/_count")["count"])
    parts = []
    if a.range_gte or a.range_lt:
        parts.append(f"{a.time_field} in [{a.range_gte or '-inf'}, {a.range_lt or '+inf'})")
    if a.max_docs:
        parts.append(f"at most {a.max_docs} docs in scroll order")
    out["slice"] = ", ".join(parts) or "full corpus"
    if a.min_size_gb and out["pri_store_bytes"] < a.min_size_gb * GB:
        out["error"] = f"primary store {out['pri_store_gb']} GB < required {a.min_size_gb} GB"
    return out


def cmd_clone(a, c):
    c.request("PUT", f"/{a.source}/_settings", {"index.blocks.write": True})
    c.request("POST", f"/{a.source}/_clone/{a.target}?wait_for_active_shards=all",
              {"settings": {"index.number_of_replicas": 0, "index.blocks.write": None}})
    c.request("GET", f"/_cluster/health/{a.target}?wait_for_status=green&timeout=3600s", timeout=3700)
    out = {"clone": describe(c, a.target)}
    if a.segments:
        out["forcemerge"] = forcemerge(c, a.target, a.segments)
        out["final"] = describe(c, a.target)
    return out


def cmd_formats(a, c, agent):
    if agent is None:
        raise SystemExit("formats: --agent and --token-file are required (or run agent/coldpath_segformat.py --dir on the node)")
    if a.control:
        spec = "control"
    elif a.from_mapping:
        spec = "mapping"
    else:
        spec = {"points": segformat_check.sf.parse_pairs(a.points), "postings": segformat_check.sf.parse_pairs(a.postings)}
    rows = c.request("GET", f"/_cat/indices/{a.index}?format=json&h=index,uuid")
    if not rows:
        return {"error": f"no index matches {a.index}"}
    results = [segformat_check.check_index(c, agent, a.agent_arm, r["index"], r["uuid"], spec)
               for r in sorted(rows, key=lambda r: r["index"])]
    out = {"ok": all(r["ok"] for r in results), "indices": results}
    if not out["ok"]:
        out["error"] = "; ".join(f"{r['index']}: {e}" for r in results for e in r["errors"][:5])
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("single-shard")
    s.add_argument("--source", required=True)
    s.add_argument("--target", required=True)
    s.add_argument("--max-docs", type=int)
    s.add_argument("--time-field", default="@timestamp")
    s.add_argument("--range-gte", help="slice: contiguous time range start (inclusive)")
    s.add_argument("--range-lt", help="slice: time range end (exclusive)")
    s.add_argument("--min-size-gb", type=float, default=30)
    f = sub.add_parser("forcemerge")
    f.add_argument("--index", required=True)
    f.add_argument("--segments", type=int, required=True)
    k = sub.add_parser("clone")
    k.add_argument("--source", required=True)
    k.add_argument("--target", required=True)
    k.add_argument("--segments", type=int, default=1)
    d = sub.add_parser("describe")
    d.add_argument("--index", required=True)
    fm = sub.add_parser("formats")
    fm.add_argument("--index", required=True, help="index name, comma list or pattern (_cat/indices)")
    fm.add_argument("--points", action="append", help="FIELD=FORMAT every segment holding FIELD must use")
    fm.add_argument("--postings", action="append", help="FIELD=FORMAT every segment holding FIELD must use")
    fm.add_argument("--from-mapping", action="store_true", help="expected fields from the mapping's meta entries")
    fm.add_argument("--control", action="store_true", help="a control copy: no segment may have either format")
    for p in (s, f, k, d, fm):
        p.add_argument("--url", default="http://localhost:9200")
        p.add_argument("--agent")
        p.add_argument("--token-file")
        p.add_argument("--agent-arm", default="S0-EBS")
    a = ap.parse_args()
    c = JsonClient(a.url, timeout=6 * 3600)
    if a.cmd == "single-shard":
        out = cmd_single_shard(a, c)
    elif a.cmd == "forcemerge":
        out = forcemerge(c, a.index, a.segments)
    elif a.cmd == "clone":
        out = cmd_clone(a, c)
    else:
        agent = None
        if a.agent:
            agent = JsonClient(a.agent, headers={"X-Coldpath-Token": open(a.token_file).read().strip()})
        out = cmd_formats(a, c, agent) if a.cmd == "formats" else describe(c, a.index, agent, a.agent_arm)
    print(json.dumps(out, indent=1))
    if out.get("error"):
        sys.exit(1)


if __name__ == "__main__":
    main()
