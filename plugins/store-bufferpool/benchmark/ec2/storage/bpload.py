#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""Bufferpool load latency on EBS and EFS (storage branch). Runs as root on the storage data node.

Per storage arm (POC binary poc_iosize2, S1 = every experiment switch off, 8 KiB / 32 KiB / 128 KiB, read_hint auto):
  read_ahead_kb=0 on the storage BEFORE the JVM starts (common-rules.md), start the unit, verify the IO config,
  post the S1 reset calls, open the index. Then per pass (CSS none, CSS all) every big5 op once, in a seeded random
  order, each cold: wait for an idle prefetch pool, bufferpool cache clear, _cache/clear, sync + drop_caches,
  verify cached_blocks == 0, then snapshot /_bufferpool/stats, device counters and the device read-size trace
  around the query. One JVM restart per round; rounds alternate EBS and EFS.
Load latency per window = delta load_time_micros / delta (reads + prefetch_reads) per file type (the time of
BlockCache.readWindow: WILLNEED hint + one pread of the window + copy into 8 KiB blocks).
"""
import argparse, json, os, random, subprocess, sys, time, urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# in the repository the harness agent modules live in ../harness/agent; on the host they are copied next to this file
sys.path.insert(1, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "harness", "agent"))
import storbench as sb  # counters, readahead and trace helpers
import coldpath_readattr  # ec2-bench harness/agent/coldpath_readattr.py (shared device check, 516992ef534)
import coldpath_agent  # ec2-bench 78c12153000 harness/agent: drop_until_empty (pageout + sync + drop_caches, mincore 0)

DATA_PATH = {"ebs": "/data/ebs/opensearch", "efs": "/mnt/efs/storage-branch/opensearch"}

ES = "http://127.0.0.1:9200"
S1_RESET = [
    "/_bufferpool/agg_batch?mode=off",
    "/_bufferpool/sort_opt?bkd_prefetch=false&whole_index=false&index_child_prefetch=false&approx_single=false&approx_bool=false&skipper_range=false&sort_prefetch=false&clamp=false&sample_docs=0&skipper_mode=off&run_cap=false",
    "/_bufferpool/disjunction_prefetch?blocks=0",
    "/_bufferpool/topk_prefetch?norms_blocks=0&doc_blocks=0",
    "/_bufferpool/dual_nav/_mode?mode=doc",
]


def req(method, path, body=None, timeout=600):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(ES + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        return json.loads(resp.read() or b"{}")


def wait_up(timeout=300):
    end = time.time() + timeout
    while time.time() < end:
        try:
            h = req("GET", "/_cluster/health?wait_for_status=yellow&timeout=5s")
            if h.get("status") in ("green", "yellow"):
                return h
        except Exception:
            pass
        time.sleep(2)
    raise SystemExit("node did not come up")


def start(storage):
    for s in ("ebs", "efs"):
        subprocess.run(["systemctl", "stop", f"opensearch-poc-{s}"], check=True)
    sb.set_ra(storage, 0)
    if storage == "ebs":
        subprocess.run(["blockdev", "--setra", "0", f"/dev/{sb.EBS_DEV}"], check=True)
    subprocess.run(["systemctl", "start", f"opensearch-poc-{storage}"], check=True)
    wait_up()


def bp_stats():
    return req("GET", "/_bufferpool/stats")


def idle():
    for _ in range(600):
        s = bp_stats()
        if s.get("in_flight_reads", 0) == 0 and s.get("pending_prefetch_tasks", 0) == 0:
            return s
        time.sleep(0.05)
    raise SystemExit("prefetch pool not idle")


FIELDS = ["requests", "loads", "prefetch_requests", "prefetch_loads", "reads", "prefetch_reads", "bytes_read",
          "bytes_overread", "bytes_loaded", "blocks_inserted", "window_blocks", "waits", "wait_time_micros",
          "load_time_micros"]


def delta_files(a, b):
    out = {}
    for ft, s1 in b.get("files", {}).items():
        s0 = a.get("files", {}).get(ft, {})
        d = {k: s1.get(k, 0) - s0.get(k, 0) for k in FIELDS}
        rs1, rs0 = s1.get("reads_by_size", {}), s0.get("reads_by_size", {})
        d["reads_by_size"] = {k: v - rs0.get(k, 0) for k, v in rs1.items() if v - rs0.get(k, 0)}
        if d["reads"] or d["prefetch_reads"]:
            out[ft] = d
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--ops", required=True, help="harness queries/big5.osb.json")
    ap.add_argument("--index", default="big5")
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--seed", type=int, default=20261004)
    ap.add_argument("--limit", type=int, default=0, help="smoke test: first N ops only")
    ap.add_argument("--storages", default="ebs,efs")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    ops = [o for o in json.load(open(a.ops))["ops"] if o.get("type") == "search"]
    if a.limit:
        ops = ops[:a.limit]
    stores = a.storages.split(",")
    log = open(os.path.join(a.out, "bpload.jsonl"), "a")
    for rnd in range(1, a.rounds + 1):
        for storage in (stores if rnd % 2 else stores[::-1]):
            start(storage)
            st = bp_stats()
            cfg = {k: st.get(k) for k in ("block_size", "random_read_size", "sequential_read_size", "read_hint",
                                           "prefetch_task_per_window", "cache_size_bytes")}
            if (st["block_size"], st["random_read_size"], st["sequential_read_size"]) != (8192, 32768, 131072) \
                    or st["read_hint"].lower() != "willneed":
                raise SystemExit(f"IO config wrong: {cfg}")
            for p in S1_RESET:
                req("POST", p)
            req("POST", f"/{a.index}/_open")
            req("GET", f"/_cluster/health/{a.index}?wait_for_status=green&timeout=300s")
            ra = int(open(sb.ra_path(storage)).read())
            if ra != 0:
                raise SystemExit(f"readahead {ra} != 0 after open")
            segs = req("GET", f"/_cat/segments/{a.index}?format=json&h=shard,segment")
            uuid = req("GET", f"/_cat/indices/{a.index}?format=json&h=uuid")[0]["uuid"]
            index_dirs = [os.path.join(DATA_PATH[storage], "nodes", "0", "indices", uuid)]
            if not os.path.isdir(index_dirs[0]):
                raise SystemExit(f"index dir missing: {index_dirs[0]}")
            ebs_dev = open(f"/sys/block/{sb.EBS_DEV}/dev").read().strip()
            agent = coldpath_agent.Agent({"data_path": DATA_PATH[storage]})
            agst = agent._storage(DATA_PATH[storage], None)
            meta = {"type": "run", "round": rnd, "storage": storage, "io_config": cfg, "read_ahead_kb": ra,
                    "segments": len(segs), "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
            log.write(json.dumps(meta) + "\n"); log.flush()
            for css in ("none", "all"):
                req("PUT", "/_cluster/settings", {"persistent": {"search.concurrent_segment_search.mode": css}})
                order = ops[:]
                random.Random(a.seed * 100 + rnd * 10 + (css == "all")).shuffle(order)
                for op in order:
                    idle()
                    req("POST", "/_bufferpool/cache/_clear")
                    req("POST", "/_cache/clear?query=true&fielddata=true&request=true")
                    drop = agent.drop_until_empty(True, agst, {uuid}, 5)
                    if drop["resident_bytes"] != 0:
                        raise SystemExit(f"page cache not empty: {drop['resident_bytes']} bytes resident")
                    s0 = idle()
                    if s0.get("cached_blocks", 0) != 0:
                        raise SystemExit("cache not empty after clear")
                    dev0 = sb.ebs_stat() if storage == "ebs" else sb.efs_read_stat()
                    sb.trace_start(storage)
                    t0 = time.time()
                    err = None
                    try:
                        r = req("POST", f"/{a.index}/_search?request_cache=false", op["body"])
                        took = r.get("took")
                    except Exception as e:
                        err, took = str(e)[:300], None
                    wall = (time.time() - t0) * 1000
                    s1 = idle()
                    tr = sb.trace_stop(storage, raw=True)
                    if storage == "ebs":
                        attr = coldpath_readattr.attribute_block(tr["requests"], index_dirs, ebs_dev)
                    else:
                        attr = coldpath_readattr.attribute_nfs(tr["requests"], index_dirs)
                    bpr = sum(f["reads"] + f["prefetch_reads"] for f in delta_files(s0, s1).values())
                    bpb = sum(f["bytes_read"] for f in delta_files(s0, s1).values())
                    check = {"cross_window_ok": attr["cross_window"] == 0,
                             "max_ok": (attr["data_max_bytes"] or 0) <= 131072,
                             "bytes_ok": attr["data_bytes"] <= bpb,
                             "windows_ok": attr["windows"] <= bpr,
                             "reads_ok": attr["data_reads"] <= bpr + attr["extent_splits"],
                             "ra_ok": int(open(sb.ra_path(storage)).read()) == 0}
                    check["valid"] = all(check.values())
                    dev1 = sb.ebs_stat() if storage == "ebs" else sb.efs_read_stat()
                    if storage == "ebs":
                        dev = {k: dev1[k] - dev0[k] for k in dev0}
                    else:
                        dev = {k: dev1[k] - dev0[k] for k in dev0 if k != "xprt"}
                    rec = {"type": "op", "round": rnd, "storage": storage, "css": css, "op": op["name"],
                           "took_ms": took, "wall_ms": round(wall, 1), "error": err,
                           "files": delta_files(s0, s1), "device": dev, "device_size_hist": tr["hist"],
                           "trace_overrun": tr["overrun"], "attribution": attr,
                           "drop_rounds": drop["rounds"], "resident_bytes_before": drop["resident_bytes"], "device_check": check,
                           "max_prefetch_reads_in_flight": s1.get("max_prefetch_reads_in_flight"),
                           "read_hint_errors": s1.get("read_hint_errors", 0) - s0.get("read_hint_errors", 0)}
                    log.write(json.dumps(rec) + "\n"); log.flush()
                    nreads = sum(f["reads"] + f["prefetch_reads"] for f in rec["files"].values())
                    lt = sum(f["load_time_micros"] for f in rec["files"].values())
                    print(f"r{rnd} {storage} css={css} {op['name']} took={took} reads={nreads} "
                          f"load_us/read={lt / nreads if nreads else 0:.0f} dev_ok={check['valid']} err={err}", flush=True)
            req("POST", f"/{a.index}/_close")
    for s in ("ebs", "efs"):
        subprocess.run(["systemctl", "stop", f"opensearch-poc-{s}"], check=True)
    sb.set_ra("ebs", 128); sb.set_ra("efs", 15360)
    subprocess.run(["blockdev", "--setra", "256", f"/dev/{sb.EBS_DEV}"], check=True)


if __name__ == "__main__":
    main()
