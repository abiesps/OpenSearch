#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
End-to-end self-test of the harness without AWS: a mock OpenSearch node (the REST calls coldbench makes, with a
fake block cache that models cold misses) and a mock agent, then queries.py build (fixed profile values),
coldbench.py run (A/A + three arms, one of them not available), and analyze.py. Also checks that the analysis finds
the planted effect (S2-X-EFS 40 % faster cold than S1-EFS, unchanged warm), no effect in A/A, the outcome verdict
(S2-X-EFS PASS and S1-EFS WORSE against S0-EBS), EFS reads verified by NFS counters, equal results, and that a
broken clear is caught by the cold verification. Python >= 3.8, stdlib only.

  selftest.py [--keep DIR]
"""
import argparse
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

here = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable

VALUES = {
    "time": {"min_ms": 1672531200000, "max_ms": 1675209600000},
    "keyword": {"process.name": {"cardinality": 30, "high": "kernel", "mid": "sshd", "low": "cron", "five": ["kernel", "sshd", "cron", "a", "b"]},
                "cloud.region": {"cardinality": 5, "high": "us-east-1", "mid": "us-west-2", "low": "eu-west-1", "five": ["us-east-1", "eu-west-1"]}},
    "numeric": {"metrics.size": {"5.0": 10.0, "40.0": 100.0, "50.0": 120.0, "60.0": 140.0, "95.0": 900.0}},
    "text": {"message": {"terms": ["monkey", "jackal", "bear"], "mid": "zebra", "phrase": "monkey jackal"}},
}


class Mock:
    def __init__(self):
        self.lock = threading.Lock()
        self.binary = "POC-EFS"
        self.nfs_reads = 0
        self.readahead = {}
        self.trace_from = 0
        self.split_reads = False
        self.cached = set()
        self.page_cache = set()
        self.files = {}
        self.sort_opt = {"bkd_prefetch": False}
        self.indices = {"big5": {"status": "open", "uuid": "u-stock", "store_type": "bufferpoolfs"},
                        "big5_split": {"status": "open", "uuid": "u-split", "store_type": "bufferpoolfs"}}
        # strict_store (selftest.py): like real nodes, a stock node is red next to a closed bufferpoolfs index and
        # rejects its store-type update; an index with "nodes" exists only on those agent arms' data paths
        self.strict_store = False
        # agent arm -> data path (agent /health storages)
        self.data_paths = {"S0-EBS": "/data/ebs/opensearch", "S0-EFS": "/mnt/efs/opensearch", "POC-EBS": "/data/ebs/opensearch",
                           "POC-EFS": "/mnt/efs/opensearch", "POC-B-EFS": "/mnt/efs/opensearch-b"}
        self.cluster = {}
        self.read_bytes = 0
        self.disk_reads = 0
        self.pid = 100
        self.broken_clear = False
        self.rng = random.Random(3)
        # what /_bufferpool/stats reports as the node's IO configuration
        self.bp_io = {"block_size": 8192, "random_read_size": 32768, "sequential_read_size": 131072}
        # call log (selftest_order.py) and a per-storage bdi readahead that index files copy at _open, like Linux
        self.calls = []
        self.bdi = {}
        self.opened = []

    def search(self, index, body):
        key = json.dumps(body, sort_keys=True)
        blocks = [f"{index}:{key}:{i}" for i in range(4)]
        miss = [b for b in blocks if b not in self.cached]
        bp = self.binary.startswith("POC")
        efs = self.binary.endswith("EFS")
        cold = bool(miss) if bp else any(b not in self.page_cache for b in blocks)
        if bp:
            f = self.files.setdefault("_0.kdd", {"requests": 0, "loads": 0, "prefetch_requests": 0, "prefetch_loads": 0,
                                                  "bytes_loaded": 0, "load_time_micros": 0, "reads": 0,
                                                  "prefetch_reads": 0, "bytes_read": 0, "reads_by_size": {}})
            f["requests"] += len(blocks)
            f["loads"] += len(miss)
            f["bytes_loaded"] += 8192 * len(miss)
            # one storage read per missing block (read size = block size in this mock)
            f["reads"] += len(miss)
            f["bytes_read"] += 8192 * len(miss)
            if miss:
                f["reads_by_size"]["8192"] = f["reads_by_size"].get("8192", 0) + len(miss)
            self.cached.update(blocks)
        new_dev = [b for b in blocks if b not in self.page_cache]
        if efs:
            self.nfs_reads += len(new_dev)  # NFS reads are not in /proc/<pid>/io read_bytes
        else:
            self.read_bytes += 8192 * len(new_dev)
            self.disk_reads += len(new_dev)
        self.page_cache.update(blocks)
        base = (80.0 if efs else 50.0) if cold else 5.0
        if cold and self.sort_opt.get("bkd_prefetch"):
            base *= 0.6
        took = max(1, round(base * (1 + self.rng.gauss(0, 0.03))))
        hits = [{"_id": f"d{i}", "_score": 1.0, "sort": [i]} for i in range(3)]
        return {"took": took, "timed_out": False, "_shards": {"failed": 0}, "hits": {"total": {"value": 3, "relation": "eq"}, "hits": hits},
                "aggregations": {"a": {"buckets": [{"key": "x", "doc_count": 3}]}} if "aggs" in body else None}


def make_os_handler(m):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def send(self, status, obj):
            if isinstance(obj, dict):
                obj = {k: v for k, v in obj.items() if v is not None}
            data = json.dumps(obj).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def body(self):
            n = int(self.headers.get("Content-Length") or 0)
            return json.loads(self.rfile.read(n)) if n else {}

        def handle_any(self, method):
            u = urllib.parse.urlsplit(self.path)
            q = dict(urllib.parse.parse_qsl(u.query))
            p = u.path
            b = self.body()  # OpenSearch Benchmark sends GET with a body
            with m.lock:
                m.calls.append(("os", method, p))
                bp = m.binary.startswith("POC")
                visible = ({n: i for n, i in m.indices.items() if m.binary in i.get("nodes", {m.binary})}
                           if m.strict_store else m.indices)
                if p == "/":
                    return self.send(200, {"name": "n1", "cluster_name": "mock", "binary": m.binary, "version": {
                        "distribution": "opensearch", "number": "3.3.0", "build_type": "tar", "build_hash": "mock",
                        "build_date": "2026-01-01T00:00:00Z", "build_snapshot": False, "lucene_version": "10.3.0",
                        "minimum_wire_compatibility_version": "2.19.0", "minimum_index_compatibility_version": "2.0.0"}})
                if p.startswith("/_cluster/health"):
                    # a closed index stays allocated: the stock binary fails its shard on an unknown store type
                    if m.strict_store and not bp and any(i["store_type"] == "bufferpoolfs" for i in visible.values()):
                        return self.send(408, {"status": "red", "timed_out": True})
                    return self.send(200, {"status": "green"})
                if p.startswith("/_cat/plugins"):
                    return self.send(200, [{"component": "store-bufferpool"}] if bp else [])
                if p.startswith("/_nodes/_local/stats/thread_pool"):
                    tp = {"bufferpool_prefetch": {"active": 0, "queue": 0, "rejected": 0, "completed": 1}} if bp else {}
                    return self.send(200, {"nodes": {"n1": {"thread_pool": tp}}})
                if p.startswith("/_nodes"):
                    # enough of nodes info / stats for OpenSearch Benchmark's internal telemetry too
                    gc = {"collectors": {"young": {"collection_count": 0, "collection_time_in_millis": 0},
                                         "old": {"collection_count": 0, "collection_time_in_millis": 0}}}
                    zero = {"count": 0, "memory_in_bytes": 0, "stored_fields_memory_in_bytes": 0, "doc_values_memory_in_bytes": 0,
                            "terms_memory_in_bytes": 0, "norms_memory_in_bytes": 0, "points_memory_in_bytes": 0}
                    node = {"name": "n1", "host": "127.0.0.1", "ip": "127.0.0.1", "version": "3.3.0", "roles": ["data"],
                            "attributes": {}, "plugins": [], "modules": [],
                            "os": {"name": "Linux", "version": "6.1", "available_processors": 4},
                            "jvm": {"version": "21.0.4", "vm_vendor": "Amazon.com Inc.", "gc": gc,
                                    "mem": {"pools": {"young": {"peak_used_in_bytes": 0}, "old": {"peak_used_in_bytes": 0}}}},
                            "indices": {"segments": zero, "merges": {"total_time_in_millis": 0, "total_throttled_time_in_millis": 0},
                                        "refresh": {"total_time_in_millis": 0}, "flush": {"total_time_in_millis": 0},
                                        "indexing": {"index_time_in_millis": 0, "throttle_time_in_millis": 0},
                                        "store": {"size_in_bytes": 0}, "translog": {"size_in_bytes": 0}},
                            "process": {"cpu": {"percent": 0}}, "thread_pool": {}, "breakers": {}, "transport": {},
                            "fs": {"total": {}}}
                    return self.send(200, {"_nodes": {"total": 1}, "cluster_name": "mock", "nodes": {"n1": node}})
                if p.startswith("/_all/_stats") or p.endswith("/_stats") or p.startswith("/_stats"):
                    return self.send(200, {"_all": {"primaries": {}, "total": {"store": {"size_in_bytes": 0},
                                     "translog": {"size_in_bytes": 0}, "segments": {"count": 0}}}, "indices": {}})
                if p == "/_cluster/settings":
                    if method == "PUT":
                        for k, v in b.get("persistent", {}).items():
                            if v is None:
                                m.cluster.pop(k, None)
                            else:
                                m.cluster[k] = v
                    return self.send(200, {"persistent": dict(m.cluster), "transient": {}})
                if p == "/_cat/indices":
                    return self.send(200, [{"index": n, "status": i["status"]} for n, i in dict.items(visible)
                                           if "open" not in q.get("expand_wildcards", "open") or i["status"] == "open"])
                if p.startswith("/_cat/indices/"):
                    name = p.split("/")[3]
                    if name not in visible:
                        return self.send(404, {"error": "index_not_found_exception"})
                    i = m.indices[name]
                    return self.send(200, [{"index": name, "status": i["status"], "uuid": i["uuid"], "docs.count": "1000",
                                            "pri": "2", "rep": "0", "pri.store.size": "1000000", "store.size": "1000000"}])
                if p.startswith("/_cat/segments/"):
                    return self.send(200, [{"shard": str(s), "prirep": "p", "segment": "_0", "searchable": "true"} for s in range(2)])
                if p == "/_cache/clear":
                    return self.send(200, {"_shards": {"failed": 0}})
                if p.startswith("/_bufferpool/"):
                    if not bp:
                        return self.send(404, {"error": "no handler"})
                    if p == "/_bufferpool/cache/_clear":
                        if not m.broken_clear:
                            m.cached.clear()
                    elif p == "/_bufferpool/sort_opt":
                        if method == "POST":
                            unknown = [k for k in q if k not in ("bkd_prefetch",)]
                            if unknown:
                                return self.send(400, {"error": f"unrecognized parameters {unknown}"})
                            for k, v in q.items():
                                m.sort_opt[k] = v == "true"
                        return self.send(200, dict(m.sort_opt))
                    return self.send(200, {**m.bp_io, "cached_blocks": len(m.cached), "files": m.files,
                                           "agg_prefetch_requests": 0, "sort_prefetch_requests": 0})
                if p == "/_search/scroll":
                    return self.send(200, {"took": 1, "hits": {"hits": []}, "_shards": {"failed": 0}})
                parts = p.strip("/").split("/")
                name = parts[0]
                if name in visible:
                    i = m.indices[name]
                    if len(parts) == 1 or parts[1] == "_settings":
                        if method == "PUT":
                            if m.strict_store and not bp and i["store_type"] == "bufferpoolfs":
                                return self.send(400, {"error": "Unknown store type [bufferpoolfs]"})
                            i["store_type"] = b["index.store.type"]
                            return self.send(200, {"acknowledged": True})
                        return self.send(200, {name: {"settings": {"index": {"store": {"type": i["store_type"]}}}}})
                    if parts[1] == "_close":
                        i["status"] = "close"
                        return self.send(200, {"acknowledged": True})
                    if parts[1] == "_open":
                        if m.binary.startswith("S0") and i["store_type"] == "bufferpoolfs":
                            return self.send(500, {"error": "unknown store type"})
                        i["status"] = "open"
                        # the index files copy the storage's readahead when they are opened (Linux f_ra)
                        storage = "EFS" if m.binary.endswith("EFS") else "EBS"
                        m.opened.append({"index": name, "node": m.binary, "readahead": m.bdi.get(storage, "default")})
                        return self.send(200, {"acknowledged": True})
                    if parts[1] == "_count":
                        return self.send(200, {"count": 1000})
                    if parts[1] == "_search":
                        if i["status"] != "open":
                            return self.send(400, {"error": "index closed"})
                        return self.send(200, m.search(name, b))
                return self.send(404, {"error": f"mock: no route {method} {p}"})

        def do_GET(self):  # noqa: N802
            self.handle_any("GET")

        def do_POST(self):  # noqa: N802
            self.handle_any("POST")

        def do_PUT(self):  # noqa: N802
            self.handle_any("PUT")

        def do_DELETE(self):  # noqa: N802
            self.handle_any("DELETE")
    return H


def make_agent_handler(m):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def send(self, status, obj):
            data = json.dumps(obj).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def handle_any(self, method):
            if self.headers.get("X-Coldpath-Token") != "selftest-token-0123456789":
                return self.send(403, {"error": "token"})
            n = int(self.headers.get("Content-Length") or 0)
            b = json.loads(self.rfile.read(n)) if n else {}
            u = urllib.parse.urlsplit(self.path)
            q = dict(urllib.parse.parse_qsl(u.query))
            efs = q.get("arm", m.binary).endswith("EFS")
            with m.lock:
                m.calls.append(("agent", method, u.path, q.get("mode"), b.get("arm")))
                if u.path == "/health":
                    return self.send(200, {"version": "1", "storages": {k: {"data_path": v} for k, v in m.data_paths.items()}})
                if u.path == "/snapshot":
                    disk = None if efs else {"device": "nvme1n1", "reads": m.disk_reads, "sectors_read": m.read_bytes // 512,
                                             "read_ms": m.disk_reads, "weighted_io_ms": m.disk_reads, "in_flight": 0}
                    nfs = {"normal_read_bytes": 8192 * m.nfs_reads, "ops": {"READ": {
                        "ops": m.nfs_reads, "trans": m.nfs_reads, "timeouts": 0, "bytes_sent": 0, "bytes_recv": 8192 * m.nfs_reads,
                        "queue_ms": 0, "rtt_ms": 2 * m.nfs_reads, "execute_ms": 2 * m.nfs_reads}}} if efs else None
                    return self.send(200, {"t_mono": __import__("time").monotonic(), "pid": m.pid,
                                           "proc_io": {"read_bytes": m.read_bytes, "rchar": m.read_bytes},
                                           "disk": disk, "nfs": nfs})
                if u.path == "/cache/drop":
                    if not m.broken_clear:
                        m.page_cache.clear()
                    return self.send(200, {"pageout_ms": 0.1, "sync_ms": 0.1, "drop_ms": 0.1, "pageout": {"mappings": 0},
                                           "meminfo_before": {"Cached": 1}, "meminfo_after": {"Cached": 0}})
                if u.path == "/cache/residency":
                    res = 4096 * len(m.page_cache)
                    return self.send(200, {"files": 4, "bytes": 1 << 30, "resident_bytes": res, "by_ext": {},
                                           "top_resident": [], "elapsed_ms": 1.0})
                if u.path == "/index/du":
                    return self.send(200, {"u": {"apparent_bytes": 1000000, "allocated_bytes": 1003520, "lucene_bytes": 999000}})
                if u.path in ("/readahead", "/readahead/mode"):
                    arm = q.get("arm")
                    default = 15360 if arm.endswith("EFS") else 128
                    if u.path == "/readahead/mode":
                        m.readahead[arm] = q["mode"]
                        m.bdi["EFS" if arm.endswith("EFS") else "EBS"] = q["mode"]
                    cur = m.bdi.get("EFS" if arm.endswith("EFS") else "EBS", "default")  # readahead is per device
                    v = default if cur == "default" else int(cur)
                    mode = q.get("mode")
                    want = None if mode is None else (default if mode == "default" else int(mode))
                    return self.send(200, {"nfs": arm.endswith("EFS"), "mode": mode, "ok": None if mode is None else v == want,
                                           "layers": [{"key": "bdi:0:53" if arm.endswith("EFS") else "blk:nvme1n1",
                                                       "read_ahead_kb": v, "default_kb": default, "target_kb": want}]})
                if u.path.startswith("/trace/") and u.path.endswith("/_start"):
                    m.trace_from = m.nfs_reads + m.disk_reads
                    return self.send(200, {})
                if u.path.startswith("/trace/") and u.path.endswith("/_stop"):
                    n = m.nfs_reads + m.disk_reads - m.trace_from
                    if m.split_reads:  # the device split every read in two (what read_ahead_kb=0 does)
                        return self.send(200, {"reads": 2 * n, "bytes_hist": {"4096": 2 * n} if n else {},
                                               "max_bytes": 4096 if n else None, "total_bytes": 8192 * n})
                    return self.send(200, {"reads": n, "bytes_hist": {"8192": n} if n else {}, "max_bytes": 8192 if n else None,
                                           "total_bytes": 8192 * n, "source": u.path.split("/")[2]})
                if u.path == "/host":
                    return self.send(200, {"uname": "mock", "device": "nvme1n1"})
                if u.path == "/node/status":
                    return self.send(200, {"pid": m.pid, "last_arm": m.binary})
                if u.path == "/node/restart":
                    m.binary = b["arm"]
                    m.pid += 1
                    m.cached.clear()
                    m.sort_opt = {"bkd_prefetch": False}
                    return self.send(200, {"arm": m.binary, "pid": m.pid})
                return self.send(404, {"error": "no route"})

        def do_GET(self):  # noqa: N802
            self.handle_any("GET")

        def do_POST(self):  # noqa: N802
            self.handle_any("POST")
    return H


def serve(handler):
    s = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=s.serve_forever, daemon=True).start()
    return s, f"http://127.0.0.1:{s.server_address[1]}"


def run(cmd):
    print("+", " ".join(cmd), flush=True)
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        print(p.stdout[-4000:], p.stderr[-4000:])
        raise SystemExit(f"FAILED: {' '.join(cmd)}")
    return p.stdout


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep")
    ap.add_argument("--osb-bin", help="opensearch-benchmark executable: also test the OSB executor")
    a = ap.parse_args()
    tmp = a.keep or tempfile.mkdtemp(prefix="coldbench-selftest-")
    os.makedirs(tmp, exist_ok=True)
    # discovery keeps only analyzer tokens as term candidates (http_logs: "anime" inside "/anime_1.gif" is no term)
    sys.path.insert(0, here)
    import queries
    assert queries.indexed_ranked({"anime": 3, "get": 5, "gif": 4}, {"get", "gif", "anime_1.gif"}) == ["get", "gif"]
    m = Mock()
    m.strict_store = True
    m.indices["big5_split"]["nodes"] = {"POC-B-EFS"}  # the split index has its own data path (format isolation)
    _, os_url = serve(make_os_handler(m))
    _, agent_url = serve(make_agent_handler(m))
    token = os.path.join(tmp, "token")
    open(token, "w").write("selftest-token-0123456789\n")
    vals = os.path.join(tmp, "values.json")
    json.dump(VALUES, open(vals, "w"))
    ops = os.path.join(tmp, "ops.json")
    run([PY, os.path.join(here, "queries.py"), "build", "--corpus", "big5", "--profile-values", vals, "--out", ops])
    opsd = json.load(open(ops))
    assert not opsd["missing_families"], opsd["missing_families"]
    small = {**opsd, "ops": [o for o in opsd["ops"] if o["name"] in (
        opsd["reference_op"], "gen:term_process.name_high", "gen:agg_composite", "gen:search_after_@timestamp_desc",
        "gen:scroll_match_all_doc", "gen:phrase_message", "gen:sort_metrics.size_asc")]}
    json.dump(small, open(ops, "w"))
    arms = {
        "indices": {"stock": {"name": "big5", "segments_per_shard": 1, "shards": 2},
                    "split": {"name": "big5_split", "segments_per_shard": 1, "format": "split BKD"}},
        "base_switches": [{"method": "POST", "path": "/_bufferpool/sort_opt?bkd_prefetch=false", "verify": {"bkd_prefetch": "false"}}],
        "outcome": {"reference": "S0-EBS", "targets": ["S2-X-EFS", "S1-EFS"], "aa": "S0-EBS@a,S0-EBS@b", "delta_min": 0.05},
        "arms": {
            "S0-EBS": {"node": "S0-EBS", "bufferpool": False, "index": "stock", "open": ["stock"], "store_types": {"stock": "hybridfs"}},
            "S1-EFS": {"node": "POC-EFS", "bufferpool": True, "index": "stock", "open": ["stock"],
                       "store_types": {"stock": "bufferpoolfs"},
                       "cluster_settings": {"search.concurrent_segment_search.mode": "none"}},
            "S2-X-EFS": {"node": "POC-B-EFS", "bufferpool": True, "index": "split", "open": ["split"],
                         "store_types": {"split": "bufferpoolfs"},
                         "switches": [{"method": "POST", "path": "/_bufferpool/sort_opt?bkd_prefetch=true", "verify": {"bkd_prefetch": "true"}}]},
            "S2-NA-EFS": {"node": "POC-EFS", "bufferpool": True, "index": "stock", "open": ["stock"],
                          "switches": [{"method": "POST", "path": "/_bufferpool/sort_opt?not_in_binary=1"}]},
        }}
    arms_f = os.path.join(tmp, "arms.json")
    json.dump(arms, open(arms_f, "w"))
    common = ["--arms", arms_f, "--ops", ops, "--url", os_url, "--agent", agent_url, "--token-file", token,
              "--cold-iters", "3", "--warm-warmup", "2", "--warm-iters", "6"]
    s1 = os.path.join(tmp, "session-aa")
    run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S1-EFS@a,S1-EFS@b,S0-EBS@a,S0-EBS@b",
         "--rounds", "4", "--out", s1, "--strict"])
    s2 = os.path.join(tmp, "session-arms")
    run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S0-EBS,S1-EFS,S2-X-EFS,S2-NA-EFS", "--rounds", "4",
         "--modes", "cold,warm,ccold,cwarm", "--clients", "3", "--concurrent-batches", "2", "--concurrent-seconds", "1",
         "--out", s2, "--strict"])
    # an index without index.store.type (the node default, as OSB creates it) closed while a stock node runs: the
    # stock arm starts without a store-type reset (http_logs: the ingest left logs-* unset)
    for i in m.indices.values():
        i["status"] = "close"
    m.indices["big5"]["store_type"] = None
    m.binary = "S0-EBS"
    run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S0-EBS", "--rounds", "1", "--modes", "cold",
         "--cold-iters", "1", "--no-results", "--out", os.path.join(tmp, "session-unset"), "--strict"])
    assert m.indices["big5"]["store_type"] == "hybridfs", m.indices["big5"]
    out = os.path.join(tmp, "analysis")
    report = run([PY, os.path.join(here, "analyze.py"), s1, s2, "--base", "S1-EFS", "--compare", "S1-EFS@b,S2-X-EFS,S0-EBS",
                  "--aa", "S1-EFS@a,S1-EFS@b", "--ni-ref", "S0-EBS,S0-EBS@a,S0-EBS@b", "--ni-aa", "S0-EBS@a,S0-EBS@b",
                  "--boot", "2000", "--ni-boot", "1000", "--warm-metric", "took_ms", "--out", out])
    res = json.load(open(os.path.join(out, "analysis.json")))
    x_cold = res["comparisons"]["S2-X-EFS:cold"]
    assert all(r["faster"] for r in x_cold), [(r["op"], r["change"], r["q"], r["floor"]) for r in x_cold]
    assert all(-0.45 < r["change"] < -0.35 for r in x_cold), [r["change"] for r in x_cold]
    assert not any(r["significant"] for r in res["comparisons"]["S1-EFS@b:cold"]), "A/A shows an effect"
    assert not any(r["regression_bar"] for r in res["comparisons"]["S2-X-EFS:warm"]), "planted warm effect is zero"
    assert "Not available" in report and "S2-NA-EFS" in report, "unavailable arm not reported"
    ni = res["noninferiority"]["targets"]
    assert all(st["_verdict"] == "PASS" for st in ni["S2-X-EFS"].values()), {op: {k: v.get("verdict") if isinstance(v, dict) else v for k, v in st.items()} for op, st in ni["S2-X-EFS"].items()}
    assert all(st["_verdict"] == "WORSE" for st in ni["S1-EFS"].values()), "S1-EFS is 1.6x the reference cold"
    nfs_checked = [json.loads(l) for l in open(os.path.join(s2, "samples.jsonl"))]
    nfs_checked = [r for r in nfs_checked if r.get("mode") == "cold" and r.get("arm") == "S2-X-EFS"]
    assert nfs_checked and all(r["checks"].get("reads_reached_nfs_server") for r in nfs_checked), "EFS reads not verified by NFS counters"
    assert all(r["checks"].get("io_size_ok") for r in nfs_checked), "NFS read size check missing"
    # kernel readahead per arm session: stock arms as mounted, bufferpool arms 0 (common-rules), read sizes traced
    assert m.readahead.get("POC-EFS") == "0" and m.readahead.get("S0-EBS") == "default", m.readahead
    runs = [json.loads(l) for l in open(os.path.join(s2, "samples.jsonl"))]
    assert all(r["readahead"]["ok"] for r in runs if r["type"] == "run" and r.get("available")), "readahead not verified"
    assert all(r["readahead"]["ok"] for r in runs if r["type"] == "run_end"), "readahead not re-checked at run end"
    # set before the index is opened: each file keeps the readahead it was opened with (selftest_order.py: the order)
    assert m.opened and all(o["readahead"] == ("default" if o["node"].startswith("S0") else "0") for o in m.opened), m.opened
    assert all(r["readahead_after_open"]["ok"] for r in runs if r["type"] == "run" and r.get("available"))
    assert all(r["io_config"]["ok"] for r in runs if r["type"] == "run" and r.get("available") and r["io_config"])
    ebs = [r for r in runs if r.get("mode") == "cold" and r.get("arm") == "S0-EBS"]
    assert ebs and all(r["io"].get("block_read_sizes", {}).get("reads") for r in ebs), "EBS read sizes not traced"
    assert all("io_size_ok" not in r["checks"] for r in ebs), "stock arms keep the default readahead: no size limit"
    p = subprocess.run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S1-EFS", "--rounds", "1",
                        "--read-ahead-kb", "128", "--out", os.path.join(tmp, "session-ra")], capture_output=True, text=True)
    assert p.returncode != 0 and "no longer used" in p.stderr, "--read-ahead-kb must be refused"
    assert m.readahead.get("POC-EFS") == "0", "POC arms run with read_ahead_kb 0"
    bp_cold = [r for r in runs if r.get("mode") == "cold" and r.get("arm") == "S2-X-EFS"]
    assert bp_cold and all(r["checks"].get("device_reads_are_windows") for r in bp_cold), "device reads vs bufferpool reads"
    assert all(r["io"]["device_vs_bufferpool"]["device_reads"] == r["io"]["device_vs_bufferpool"]["bufferpool_reads"]
               for r in bp_cold)
    # device reads that are not bufferpool windows make the run invalid
    m.split_reads = True
    s5 = os.path.join(tmp, "session-split")
    run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S1-EFS", "--rounds", "1", "--modes", "cold",
         "--out", s5])
    recs5 = [json.loads(l) for l in open(os.path.join(s5, "samples.jsonl"))]
    assert all(not r["cold_ok"] and r["checks"]["device_reads_are_windows"] is False for r in recs5
               if r.get("mode") == "cold" and r["io"].get("bp", {}).get("reads")), "split device reads not caught"
    assert [r for r in recs5 if r["type"] == "run_end"][0]["valid"] is False, "run not marked invalid"
    assert [r for r in recs5 if r["type"] == "session_end"][0]["invalid_runs"] == ["S1-EFS#r0"]
    m.split_reads = False

    # a POC arm leaves the stock index closed with the stock arms' store type, so the next stock arm starts green
    assert m.indices["big5"]["store_type"] in ("hybridfs", "bufferpoolfs")
    log2 = open(os.path.join(s2, "session.log")).read()
    assert "big5: store type bufferpoolfs -> hybridfs" in log2, "store type not reset before a stock arm"
    # a split (POC-only) index on a stock arm's data path is refused before any run
    bad = json.loads(json.dumps(arms))
    bad["arms"]["S2-X-EFS"]["node"] = "POC-EBS"  # the data path of the stock arm S0-EBS
    bad_f = os.path.join(tmp, "arms-bad.json")
    json.dump(bad, open(bad_f, "w"))
    p = subprocess.run([PY, os.path.join(here, "coldbench.py"), "run", *[bad_f if x == arms_f else x for x in common],
                        "--arm-list", "S2-X-EFS", "--rounds", "1", "--out", os.path.join(tmp, "session-iso")],
                       capture_output=True, text=True)
    assert p.returncode != 0 and "also a stock arm's data path" in p.stderr, (p.stdout[-1500:], p.stderr[-1500:])
    eq = res["equality"]
    assert eq["across"] and all(e["equal"] for e in eq["across"]), eq
    assert not res["amdahl"], res["amdahl"]
    if a.osb_bin:
        # the same arms through OpenSearch Benchmark (coldpath-search runner): samples land in the same schema
        s4 = os.path.join(tmp, "session-osb")
        run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S0-EBS,S2-X-EFS", "--rounds", "2",
             "--executor", "osb", "--osb-bin", a.osb_bin, "--out", s4, "--strict"])
        recs = [json.loads(l) for l in open(os.path.join(s4, "samples.jsonl"))]
        osb = [r for r in recs if r.get("executor") == "osb"]
        n_ops = len(small["ops"]) + 1
        cold = [r for r in osb if r["mode"] == "cold"]
        warm = [r for r in osb if r["mode"] == "warm"]
        assert len(cold) == 2 * 2 * n_ops * 3, len(cold)
        assert len(warm) == 2 * 2 * n_ops * 6, len(warm)
        assert all(r["cold_ok"] for r in cold), [r["checks"] for r in cold if not r["cold_ok"]][:3]
        assert sum(r["type"] == "osb_summary" for r in recs) == 2 * 2 * 2
        run([PY, os.path.join(here, "analyze.py"), s4, "--base", "S0-EBS", "--boot", "500", "--ni-boot", "300",
             "--ni-ref", "S0-EBS", "--ni-target", "S2-X-EFS", "--warm-metric", "took_ms", "--out", os.path.join(tmp, "analysis-osb")])
    # a broken clear must be caught
    m.broken_clear = True
    s3 = os.path.join(tmp, "session-broken")
    p = subprocess.run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S1-EFS", "--rounds", "1",
                        "--modes", "cold", "--out", s3, "--strict"], capture_output=True, text=True)
    assert p.returncode != 0 and "cold verification failed" in p.stderr, (p.stdout[-2000:], p.stderr[-2000:])
    print(report[-3000:])
    print(f"\nSELFTEST PASS ({tmp})")
    if not a.keep:
        shutil.rmtree(tmp)


if __name__ == "__main__":
    main()
