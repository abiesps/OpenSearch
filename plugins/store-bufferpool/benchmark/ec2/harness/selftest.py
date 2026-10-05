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
import time
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
        # node failures (common-rules "Node start can fail on Amazon EFS ..."): fail_starts {agent arm: n} makes the
        # next n starts of that arm exit at once; crash_after_searches makes the node exit after that many searches
        self.fail_starts = {}
        self.down = False
        self.crash_after_searches = None
        self.searches = 0
        # agent arm -> data path (agent /health storages)
        self.data_paths = {"S0-EBS": "/data/ebs/opensearch", "S0-EFS": "/mnt/efs/opensearch", "POC-EBS": "/data/ebs/opensearch",
                           "POC-EFS": "/mnt/efs/opensearch", "POC-B-EFS": "/mnt/efs/opensearch-b"}
        self.cluster = {}
        self.read_bytes = 0
        self.disk_reads = 0
        self.pid = 100
        self.broken_clear = False
        self.until_empty_drops = 0
        self.rng = random.Random(3)
        # what /_bufferpool/stats reports as the node's IO configuration
        self.bp_io = {"block_size": 8192, "random_read_size": 32768, "sequential_read_size": 131072}
        # call log (selftest_order.py) and a per-storage bdi readahead that index files copy at _open, like Linux
        self.calls = []
        self.bdi = {}
        self.opened = []
        # search caches: aggregations build fielddata / global ordinals; _cache/clear?fielddata drops them
        self.fielddata = 0
        self.fielddata_at_clear = []
        self.broken_cache_clear = False
        # like IndicesFieldDataCache: _cache/clear only marks fielddata; the periodic sweep (every
        # indices.cache.cleanup_interval) drops it, here sweep_delay_s after the clear
        self.sweep_delay_s = 0.3
        self.fd_marked_at = None
        self.node_settings = {"indices.cache.cleanup_interval": "1s"}
        self.events = []  # ("search", body key) / ("cache_clear",) in call order
        # EFS backend connections of efs-proxy: a fresh mount has 1; /efs/precondition scales it to the target; a
        # reconnect (efs_drop_at = the EFS snapshot number at which it happens) brings it back to 1
        self.efs_conns = 1
        self.proxy_pid = 4000
        self.preconditions = 0
        self.efs_snapshots = 0
        self.efs_drop_at = None
        # request timeout: the next slow_searches searches answer after slow_search_s (outside the lock)
        self.slow_searches = 0
        self.slow_search_s = 0.0
        # kernel NFS stall windows the agent reports (GET /storage/incidents), as (start, end) epoch pairs
        self.nfs_stalls = []

    def search(self, index, body):
        key = json.dumps(body, sort_keys=True)
        self.events.append(("search", key))
        if "aggs" in body:
            self.fielddata += 4096  # global ordinals of the aggregated keyword field
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
            if p.endswith("/_search") and m.slow_searches > 0:
                with m.lock:
                    m.slow_searches -= 1
                time.sleep(m.slow_search_s)
            with m.lock:
                if m.down:  # the JVM is gone: the connection drops without a response
                    self.close_connection = True
                    return
                if p.endswith("/_search") and m.crash_after_searches is not None:
                    m.searches += 1
                    if m.searches > m.crash_after_searches:
                        m.crash_after_searches, m.down = None, True
                        self.close_connection = True
                        return
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
                if p.startswith("/_nodes/_local/settings"):
                    return self.send(200, {"nodes": {"n1": {"settings": dict(m.node_settings)}}})
                if p.startswith("/_nodes/_local/stats/indices/"):
                    if m.fd_marked_at is not None and time.monotonic() - m.fd_marked_at >= m.sweep_delay_s:
                        m.fielddata, m.fd_marked_at = 0, None
                    return self.send(200, {"nodes": {"n1": {"indices": {
                        "fielddata": {"memory_size_in_bytes": m.fielddata},
                        "query_cache": {"memory_size_in_bytes": 0, "cache_size": 0},
                        "request_cache": {"memory_size_in_bytes": 0}}}}})
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
                    m.fielddata_at_clear.append(m.fielddata)
                    m.events.append(("cache_clear",))
                    if q.get("fielddata") == "true" and not m.broken_cache_clear and m.fd_marked_at is None:
                        m.fd_marked_at = time.monotonic()
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
                    if efs:
                        m.efs_snapshots += 1
                        if m.efs_drop_at is not None and m.efs_snapshots >= m.efs_drop_at:
                            m.efs_conns, m.efs_drop_at = 1, None
                    return self.send(200, {"t_mono": __import__("time").monotonic(), "pid": m.pid,
                                           "proc_io": {"read_bytes": m.read_bytes, "rchar": m.read_bytes},
                                           "disk": disk, "nfs": nfs,
                                           "efs_connections": {"count": m.efs_conns, "proxy_pid": m.proxy_pid} if efs else None})
                if u.path == "/efs/connections":
                    return self.send(200, {"efs_connections": {"count": m.efs_conns, "proxy_pid": m.proxy_pid} if efs else None})
                if u.path == "/storage/incidents":
                    t0, t1 = float(q["since"]), float(q["until"])
                    w = [{"server": "127.0.0.1", "start": a0, "end": min(b0, t1), "open_end": b0 > t1}
                         for a0, b0 in m.nfs_stalls if a0 <= t1 and b0 >= t0]
                    return self.send(200, {"available": True, "since": t0, "until": t1, "windows": w, "lines": []})
                if u.path == "/efs/precondition":
                    before, target = m.efs_conns, int(q["target"])
                    m.preconditions += 1
                    m.efs_conns = max(before, target) if target > 1 else before
                    return self.send(200, {"ok": m.efs_conns >= target, "count": m.efs_conns, "count_before": before,
                                           "read_MBps": 600.0, "elapsed_s": 4.0, "skipped": False})
                if u.path == "/cache/drop":
                    if not m.broken_clear:
                        m.page_cache.clear()
                    out = {"pageout_ms": 0.1, "sync_ms": 0.1, "drop_ms": 0.1, "pageout": {"mappings": 0},
                           "meminfo_before": {"Cached": 1}, "meminfo_after": {"Cached": 0}}
                    if "until_empty" in q:  # the agent's drop_until_empty: residency after the rounds
                        m.until_empty_drops += 1
                        res = 4096 * len(m.page_cache)
                        out.update(rounds=1 if res == 0 else int(q["until_empty"]), resident_bytes=res, files=4,
                                   bytes=1 << 30, round_detail=[{"resident_bytes": res, "top_resident": []}])
                    return self.send(200, out)
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
                    return self.send(200, {"pid": None if m.down else m.pid, "last_arm": m.binary})
                if u.path == "/node/restart":
                    m.binary = b["arm"]
                    m.pid += 1
                    m.down = m.fail_starts.get(m.binary, 0) > 0
                    if m.down:
                        m.fail_starts[m.binary] -= 1
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
    import coldbench
    # a session split into invocations with --round-offset keeps the round orders and gets distinct run ids
    labs = ["S0-EBS@a", "S0-EBS@b", "S1-EFS@a", "S1-EFS@b"]
    whole = coldbench.schedule(labs, 5, "random", "coldpath")
    parts = coldbench.schedule(labs, 2, "random", "coldpath") + coldbench.schedule(labs, 3, "random", "coldpath", 2)
    assert whole == parts and len({f"{lab}#r{r}" for r, lab in parts}) == len(parts), (whole, parts)
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
    # store type left on a stock data path by a session killed while a POC arm ran (SIGKILL: no exit trap). The
    # pre-start check starts the POC arm of the same path, resets the store type, then the stock arm starts green;
    # without the check the stock arm cannot start (red, and a stock node cannot reset the store type)
    arms_pe = {**arms, "arms": {"S0-EBS": arms["arms"]["S0-EBS"],
                                "S1-EBS": {"node": "POC-EBS", "bufferpool": True, "index": "stock", "open": ["stock"],
                                           "store_types": {"stock": "bufferpoolfs"}}}}
    arms_pe_f = os.path.join(tmp, "arms-pe.json")
    json.dump(arms_pe, open(arms_pe_f, "w"))
    common_pe = [c if c != arms_f else arms_pe_f for c in common]
    one_s0 = ["--arm-list", "S0-EBS", "--rounds", "1", "--modes", "cold", "--cold-iters", "1", "--no-results", "--strict"]
    m.indices["big5"]["store_type"], m.binary = "bufferpoolfs", "S0-EBS"
    p = subprocess.run([PY, os.path.join(here, "coldbench.py"), "run", *common_pe, *one_s0, "--no-normalize-store-types",
                        "--out", os.path.join(tmp, "session-dirty-nocheck")], capture_output=True, text=True)
    assert p.returncode != 0 and m.indices["big5"]["store_type"] == "bufferpoolfs", (p.returncode, p.stderr[-800:])
    m.calls.clear()
    run([PY, os.path.join(here, "coldbench.py"), "run", *common_pe, *one_s0, "--out", os.path.join(tmp, "session-dirty")])
    restarts = [c[4] for c in m.calls if c[:3] == ("agent", "POST", "/node/restart")]
    assert restarts[:2] == ["POC-EBS", "S0-EBS"], restarts
    norm = [json.loads(x) for x in open(os.path.join(tmp, "session-dirty", "samples.jsonl"))
            if '"store_type_normalize"' in x][0]
    assert norm["paths"][0]["poc_node"] == "POC-EBS" and norm["paths"][0]["reset"] == [
        {"index": "big5", "from": "bufferpoolfs", "to": "hybridfs"}], norm
    assert m.indices["big5"]["store_type"] == "hybridfs", m.indices["big5"]
    # exit trap: SIGTERM while a POC arm runs resets the store type on the running node before the process exits
    m.indices["big5"]["store_type"], m.binary = "hybridfs", "POC-EBS"
    proc = subprocess.Popen([PY, os.path.join(here, "coldbench.py"), "run", *common_pe, "--arm-list", "S1-EBS", "--rounds",
                             "3", "--modes", "cold,warm", "--no-normalize-store-types",
                             "--out", os.path.join(tmp, "session-term")], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    t0 = time.monotonic()
    while not (m.indices["big5"]["status"] == "open" and m.indices["big5"]["store_type"] == "bufferpoolfs"):
        assert time.monotonic() - t0 < 60 and proc.poll() is None, "S1-EBS did not open big5 as bufferpoolfs"
        time.sleep(0.05)
    proc.terminate()
    err = proc.communicate(timeout=60)[1]
    assert proc.returncode != 0 and "session aborted" in open(os.path.join(tmp, "session-term", "session.log")).read(), err[-800:]
    assert m.indices["big5"]["status"] == "close" and m.indices["big5"]["store_type"] == "hybridfs", m.indices["big5"]
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
    # cold = data-cold on a JIT-warm JVM: an unmeasured warm-up of every op precedes the cold block of every run, and
    # the clear before every cold iteration empties fielddata / global ordinals (built by the warm-up's aggregations)
    for run_rec in [r for r in runs if r["type"] == "run" and r.get("available")]:
        rid = run_rec["run_id"]
        seq = [r["type"] if r["type"] != "sample" else r["mode"] for r in runs if r.get("run_id") == rid]
        assert run_rec["cold_protocol"] == "jit-warm" and "jit_warmup" in seq and seq.index("jit_warmup") < seq.index("cold"), seq
        assert next(r for r in runs if r["type"] == "jit_warmup" and r["run_id"] == rid)["ops"] == len(small["ops"]) + 1
    colds = [r for r in runs if r.get("mode") == "cold"]
    assert colds and all(r["clear"]["caches"]["empty"] and r["checks"]["search_caches_empty"]
                         and r["clear"]["caches"]["fielddata_bytes"] == 0 for r in colds), "search caches not empty"
    assert max(m.fielddata_at_clear) > 0, "the warm-up and the aggregations built fielddata that the clear dropped"
    # the clear only marks fielddata; the harness waits for the node's periodic sweep (0.3 s in the mock)
    assert any(r["clear"]["caches"]["wait_ms"] >= 250 for r in colds), "the clear did not wait for the fielddata sweep"
    assert all(r["cache_cleanup"]["ok"] for r in runs if r["type"] == "run" and r.get("available")
               and r["args"]["modes"] != "warm"), "cache cleanup interval not recorded"
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
    # EFS backend connection count (common-rules "Amazon EFS connection count is a measured variable"): the fresh mount
    # (1 connection) was pre-conditioned to 5 before the first EFS run; every EFS sample records 5 at start and end
    assert m.preconditions >= 1, "the mount at 1 connection was not pre-conditioned"
    s1_recs = [json.loads(l) for l in open(os.path.join(s1, "samples.jsonl"))]
    for recs_ in (runs, s1_recs):
        efs_s = [r for r in recs_ if r["type"] == "sample" and r["mode"] in ("cold", "warm") and r["arm"].endswith("EFS")]
        assert efs_s and all(r["io"]["efs_connections"]["start"] == r["io"]["efs_connections"]["end"] == 5 for r in efs_s)
        assert all(r["checks"]["efs_connections_ok"] for r in efs_s if r["mode"] == "cold")
        assert all(r["efs_connections_ok"] for r in efs_s if r["mode"] == "warm")
        assert not any("efs_connections" in r.get("io", {}) for r in recs_ if r["type"] == "sample" and r.get("arm") == "S0-EBS")
        assert all(r["efs_connections_target"] == 5 and r["efs_precondition_start"]["ok"] for r in recs_
                   if r["type"] == "run" and r.get("available") and r["arm"].endswith("EFS"))
    # a reconnect in the middle of a session (count back to 1): the sample that sees it is invalid, the next sample
    # is pre-conditioned again and valid
    n_pre = m.preconditions
    m.efs_drop_at = m.efs_snapshots + 9
    s8 = os.path.join(tmp, "session-efs-drop")
    run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S1-EFS", "--rounds", "1", "--modes", "cold,warm",
         "--no-results", "--out", s8])
    r8 = [json.loads(l) for l in open(os.path.join(s8, "samples.jsonl"))]
    s8s = [r for r in r8 if r["type"] == "sample"]
    bad8 = [r for r in s8s if (r["checks"]["efs_connections_ok"] if r["mode"] == "cold" else r["efs_connections_ok"]) is False]
    assert len(bad8) == 1 and m.preconditions == n_pre + 1, (len(bad8), m.preconditions, n_pre)
    assert bad8[0]["mode"] != "cold" or bad8[0]["cold_ok"] is False
    assert any("efs_precondition" in r.get("clear", {}) or "efs_precondition" in r for r in s8s), "re-pre-conditioning not recorded"
    # record-only sessions (--efs-connections 0) keep whatever count the mount has; analyze.py refuses to compare EFS
    # samples at different counts unless one is selected
    m.efs_conns = 1
    s9 = os.path.join(tmp, "session-efs-record")
    run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S1-EFS", "--rounds", "1", "--modes", "cold,warm",
         "--efs-connections", "0", "--no-results", "--out", s9])
    r9 = [json.loads(l) for l in open(os.path.join(s9, "samples.jsonl"))]
    assert all(r["io"]["efs_connections"]["start"] == 1 and "efs_connections_ok" not in r and
               "efs_connections_ok" not in r.get("checks", {}) for r in r9 if r["type"] == "sample")
    p = subprocess.run([PY, os.path.join(here, "analyze.py"), s2, s9, "--base", "S1-EFS", "--boot", "200", "--ni-boot", "100",
                        "--out", os.path.join(tmp, "analysis-efs-mixed")], capture_output=True, text=True)
    assert p.returncode != 0 and "different backend connection counts" in p.stderr, p.stderr[-800:]
    run([PY, os.path.join(here, "analyze.py"), s2, s9, "--base", "S1-EFS", "--efs-connections", "5", "--boot", "200",
         "--ni-boot", "100", "--out", os.path.join(tmp, "analysis-efs-5")])
    a5 = json.load(open(os.path.join(tmp, "analysis-efs-5", "analysis.json")))
    assert a5["efs_connections"]["kept_states"] == ["5"], a5["efs_connections"]
    # a 6th socket of the previous incarnation can stay open while it closes: 6 is the scaled-up level too
    m.efs_conns = 6
    s10 = os.path.join(tmp, "session-efs-6")
    run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S1-EFS", "--rounds", "1", "--modes", "cold,warm",
         "--no-results", "--out", s10, "--strict"])
    r10 = [json.loads(l) for l in open(os.path.join(s10, "samples.jsonl"))]
    assert all((r["checks"]["efs_connections_ok"] if r["mode"] == "cold" else r["efs_connections_ok"]) for r in r10
               if r["type"] == "sample"), "6 backend connections is the scaled-up level"
    run([PY, os.path.join(here, "analyze.py"), s2, s10, "--base", "S1-EFS", "--boot", "200", "--ni-boot", "100",
         "--out", os.path.join(tmp, "analysis-efs-6")])
    assert json.load(open(os.path.join(tmp, "analysis-efs-6", "analysis.json")))["efs_connections"]["kept_states"] == ["5"]
    # the 1-connection sensitivity needs a fresh pinned mount: a mount above the target is refused, never lowered
    m.efs_conns = 5
    p = subprocess.run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S1-EFS", "--rounds", "1",
                        "--modes", "cold", "--efs-connections", "1", "--out", os.path.join(tmp, "session-efs-1")],
                       capture_output=True, text=True)
    assert p.returncode != 0 and "cannot be lowered without a remount" in p.stderr, p.stderr[-800:]
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
    assert m.until_empty_drops > 0, "cold clears use the agent's /cache/drop?until_empty"
    # a residency tolerance above 0 is refused (common-rules "Agent pageout bug")
    p = subprocess.run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S1-EFS", "--rounds", "1",
                        "--modes", "cold", "--out", os.path.join(tmp, "session-tol"), "--residency-tolerance", "4096"],
                       capture_output=True, text=True)
    assert p.returncode != 0 and "only 0 is allowed" in (p.stdout + p.stderr), (p.stdout[-1000:], p.stderr[-1000:])
    # pre-registered per-op iteration caps: same for every arm, the capped op keeps its runs, others unchanged
    caps_f = os.path.join(tmp, "caps.json")
    json.dump({"ops": {"gen:phrase_message": {"cold_iters": 1, "warm_warmup": 2, "warm_iters": 3}}}, open(caps_f, "w"))
    s6 = os.path.join(tmp, "session-caps")
    run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S0-EBS,S1-EFS", "--rounds", "2",
         "--modes", "cold,warm", "--op-caps", caps_f, "--out", s6, "--strict"])
    recs6 = [json.loads(l) for l in open(os.path.join(s6, "samples.jsonl"))]
    cnt = {}
    for r in recs6:
        if r["type"] == "sample":
            k = (r["run_id"], r["mode"], r["op"])
            cnt[k] = cnt.get(k, 0) + 1
    runs6 = sorted({k[0] for k in cnt})
    assert len(runs6) == 4, runs6
    for rid in runs6:
        assert cnt[(rid, "cold", "gen:phrase_message")] == 1 and cnt[(rid, "warm", "gen:phrase_message")] == 3, cnt
        assert cnt[(rid, "cold", "gen:term_process.name_high")] == 3 and cnt[(rid, "warm", "gen:term_process.name_high")] == 6, cnt
    assert [r for r in recs6 if r["type"] == "session"][0]["op_caps"] == {"gen:phrase_message": {"cold_iters": 1, "warm_warmup": 2, "warm_iters": 3}}
    assert all(r["op_caps"] for r in recs6 if r["type"] == "run" and r.get("available"))
    for bad, why in (({"gen:range_@timestamp_1d": {"cold_iters": 1, "warm_warmup": 2, "warm_iters": 3}}, "reference op"),
                     ({"gen:phrase_message": {"cold_iters": 0, "warm_warmup": 2, "warm_iters": 3}}, "at least 1"),
                     ({"no:such_op": {"cold_iters": 1, "warm_warmup": 2, "warm_iters": 3}}, "not an op")):
        json.dump({"ops": bad}, open(caps_f, "w"))
        p = subprocess.run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S1-EFS", "--rounds", "1",
                            "--op-caps", caps_f, "--out", os.path.join(tmp, "session-caps-bad")], capture_output=True, text=True)
        assert p.returncode != 0 and why in p.stderr, (why, p.stderr[-800:])
    # a broken clear must be caught
    m.broken_clear = True
    s3 = os.path.join(tmp, "session-broken")
    p = subprocess.run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S1-EFS", "--rounds", "1",
                        "--modes", "cold", "--out", s3, "--strict"], capture_output=True, text=True)
    assert p.returncode != 0 and "cold verification failed" in p.stderr, (p.stdout[-2000:], p.stderr[-2000:])
    m.broken_clear = False
    # fielddata / global ordinals that survive the cache clear must be caught too
    m.broken_cache_clear = True
    s6 = os.path.join(tmp, "session-fielddata")
    p = subprocess.run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S1-EFS", "--rounds", "1",
                        "--modes", "cold", "--out", s6, "--strict"], capture_output=True, text=True)
    assert p.returncode != 0 and "search_caches_empty': False" in p.stderr, (p.stdout[-2000:], p.stderr[-2000:])
    m.broken_cache_clear = False
    # the node's default cache cleanup interval (1m) leaves marked fielddata cached: the run is refused
    m.node_settings = {}
    s6b = os.path.join(tmp, "session-cleanup-default")
    p = subprocess.run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S1-EFS", "--rounds", "1",
                        "--modes", "cold", "--out", s6b, "--strict"], capture_output=True, text=True)
    assert p.returncode != 0 and "indices.cache.cleanup_interval is the default 1m" in p.stderr, (p.stdout[-2000:], p.stderr[-2000:])
    m.node_settings = {"indices.cache.cleanup_interval": "1s"}
    # the old protocol stays selectable and is recorded
    s7 = os.path.join(tmp, "session-jitcold")
    run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S1-EFS", "--rounds", "1", "--modes", "cold",
         "--no-jit-warmup", "--no-results", "--out", s7, "--strict"])
    r7 = [json.loads(l) for l in open(os.path.join(s7, "samples.jsonl"))]
    assert not any(r["type"] == "jit_warmup" for r in r7) and [r for r in r7 if r["type"] == "run"][0]["cold_protocol"] == "jit-cold"
    # analyze.py never mixes cold protocols in one analysis; --cold-skip-iters leaves out the first cold iterations
    p = subprocess.run([PY, os.path.join(here, "analyze.py"), s2, s7, "--base", "S1-EFS", "--boot", "200", "--ni-boot", "100",
                        "--out", os.path.join(tmp, "analysis-mixed")], capture_output=True, text=True)
    assert p.returncode != 0 and "different cold protocols" in p.stderr, p.stderr[-800:]
    run([PY, os.path.join(here, "analyze.py"), s7, "--base", "S1-EFS", "--cold-skip-iters", "1", "--boot", "200",
         "--ni-boot", "100", "--out", os.path.join(tmp, "analysis-skip")])
    a7 = json.load(open(os.path.join(tmp, "analysis-skip", "analysis.json")))
    assert a7["cold_protocol"] == "jit-cold/skip-iter1" and a7["cold_skipped"]["S1-EFS"] == len(small["ops"]) + 1, a7["cold_skipped"]
    x = coldbench.parse_xprt("xprt:\ttcp 0 0 70 0 4 19792764 19792728 0 704170016 0 66 4622218 261745959")
    assert x["connect_count"] == 70 and x["sends"] == 19792764 and coldbench.parse_xprt(None) is None, x
    # node start retries and re-queued runs (common-rules "Node start can fail on Amazon EFS with NoSuchFileException
    # in the cluster-state commit"): the wait between retries is shortened for the test
    coldbench_env = {**os.environ, "COLDBENCH_NODE_START_RETRY_WAIT_S": "0.2"}
    for i in m.indices.values():
        i["status"] = "close"
    m.indices["big5"]["store_type"], m.binary, m.down = "hybridfs", "S0-EBS", False

    def session(name, arm_list, rounds=1):
        out = os.path.join(tmp, name)
        p = subprocess.run([PY, os.path.join(here, "coldbench.py"), "run", *common_pe, "--arm-list", arm_list, "--rounds",
                            str(rounds), "--modes", "cold", "--cold-iters", "1", "--out", out], capture_output=True,
                           text=True, env=coldbench_env)
        assert p.returncode == 0, p.stderr[-1500:]
        return out, [json.loads(x) for x in open(os.path.join(out, "samples.jsonl"))]
    m.fail_starts = {"S0-EBS": 1}  # one failed start: retried, the run is measured as usual
    _, recs = session("session-start-retry", "S0-EBS")
    retries = [r for r in recs if r["type"] == "node_start_retry"]
    assert len(retries) == 1 and retries[0]["jvm_gone"] and retries[0]["node"] == "S0-EBS", retries
    assert [r["run_id"] for r in recs if r["type"] == "run_end"] == ["S0-EBS#r0"], [r["type"] for r in recs]
    m.fail_starts = {"S0-EBS": 3}  # three failed starts: 2 retries fail, the run is re-queued and measured at the end
    _, recs = session("session-start-requeue", "S0-EBS,S1-EBS")
    assert sum(r["type"] == "node_start_retry" for r in recs) == 2 and any(r["type"] == "node_start_failed" for r in recs)
    assert [r["run_id"] for r in recs if r["type"] == "run_requeued"] == ["S0-EBS#r0"]
    assert [r["run_id"] for r in recs if r["type"] == "run_end"] == ["S1-EBS#r0", "S0-EBS#r0.a1"], \
        [(r["type"], r.get("run_id")) for r in recs]
    order = lambda rid: [r["op"] for r in recs if r.get("run_id") == rid and r.get("mode") == "cold"]  # noqa: E731
    m.fail_starts = {}
    # the node dies in the middle of the first run's cold block (after the JIT warm-up of every op and 3 cold
    # iterations): discarded, re-queued, measured again
    m.searches, m.crash_after_searches = 0, len(small["ops"]) + 1 + 3
    out_c, recs = session("session-crash", "S1-EBS")
    disc = [r for r in recs if r["type"] == "run_discarded"]
    assert len(disc) == 1 and disc[0]["run_id"] == "S1-EBS#r0", [r["type"] for r in recs]
    assert [r["run_id"] for r in recs if r["type"] == "run_end"] == ["S1-EBS#r0.a1"]
    first = order("S1-EBS#r0")
    again = order("S1-EBS#r0.a1")
    assert first and len(again) > len(first) and again[:len(first)] == first, (first, again)  # same op order
    assert any(r["type"] == "store_type_normalize" and r.get("after") == "S1-EBS#r0" for r in recs), "no store-type reset"
    assert m.indices["big5"]["store_type"] == "hybridfs", m.indices["big5"]
    sys.path.insert(0, here)
    import analyze as _an
    d = _an.Data([out_c], "took_ms", "wall_ms")
    assert d.discarded == {"S1-EBS#r0"} and not any(k[2] == "S1-EBS#r0" for k in d.samples) and \
        any(k[2] == "S1-EBS#r0.a1" for k in d.samples), sorted({k[2] for k in d.samples})
    # a request that outlives --request-timeout is never a latency sample: the run is discarded, the search is
    # cancelled on the node, and the run is re-queued and measured again
    m.searches, m.crash_after_searches = 0, None
    m.slow_searches, m.slow_search_s = 1, 3.0
    out_t = os.path.join(tmp, "session-timeout")
    p = subprocess.run([PY, os.path.join(here, "coldbench.py"), "run", *common_pe, "--arm-list", "S0-EBS", "--rounds", "1",
                        "--modes", "cold", "--cold-iters", "1", "--request-timeout", "1", "--out", out_t],
                       capture_output=True, text=True, env=coldbench_env)
    assert p.returncode == 0, p.stderr[-1500:]
    recs = [json.loads(x) for x in open(os.path.join(out_t, "samples.jsonl"))]
    disc = [r for r in recs if r["type"] == "run_discarded"]
    assert len(disc) == 1 and disc[0]["reason"].startswith("request timeout") and disc[0]["request_timeout_s"] == 1, disc
    assert [r["run_id"] for r in recs if r["type"] == "run_end"] == ["S0-EBS#r0.a1"], [r["type"] for r in recs]
    assert ("os", "POST", "/_tasks/_cancel") in m.calls, "the timed-out search was not cancelled"
    assert recs[0]["args"]["request_timeout"] == 1.0
    p = subprocess.run([PY, os.path.join(here, "coldbench.py"), "run", *common_pe, "--arm-list", "S0-EBS", "--rounds", "1",
                        "--modes", "cold", "--request-timeout", "0", "--out", os.path.join(tmp, "session-timeout0")],
                       capture_output=True, text=True, env=coldbench_env)
    assert p.returncode != 0 and "--request-timeout must be > 0" in p.stderr, p.stderr[-500:]
    # a kernel NFS stall window during a run: recorded as a storage incident; analyze.py excludes the samples inside
    # it from every verdict (sensitivity only) and keeps the others
    now = time.time()
    m.nfs_stalls = [(now - 3600.0, now + 3600.0)]
    out_i, recs = session("session-incident", "S1-EBS")
    m.nfs_stalls = []
    inc = [r for r in recs if r["type"] == "storage_incident"]
    assert len(inc) == 1 and inc[0]["windows"][0]["server"] == "127.0.0.1", [r["type"] for r in recs]
    assert any(r["type"] == "storage_incident_check" and r["available"] for r in recs)
    di = _an.Data([out_i], "took_ms", "wall_ms")
    assert di.incidents and sum(di.incident_samples.values()) > 0 and not di.samples, (di.incident_samples, len(di.samples))
    _, recs = session("session-no-incident", "S1-EBS")
    assert not [r for r in recs if r["type"] == "storage_incident"]
    assert [r["windows"] for r in recs if r["type"] == "storage_incident_check"] == [0]
    import importlib.util
    spec = importlib.util.spec_from_file_location("coldpath_agent_t", os.path.join(here, "agent", "coldpath_agent.py"))
    ag = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ag)
    j = ("1000.000 h kernel: nfs: server 127.0.0.1 not responding, still trying\n"
         "1010.500 h kernel: nfs: server 127.0.0.1 OK\n"
         "1100.000 h kernel: something else\n"
         "1200.000 h kernel: nfs: server 127.0.0.1 not responding, still trying\n")
    r = ag.nfs_incidents(900.0, 1300.0, journal=j)
    assert r["windows"] == [{"server": "127.0.0.1", "start": 1000.0, "end": 1010.5, "open_end": False},
                            {"server": "127.0.0.1", "start": 1200.0, "end": 1300.0, "open_end": True}], r
    print(report[-3000:])
    print(f"\nSELFTEST PASS ({tmp})")
    if not a.keep:
        shutil.rmtree(tmp)


if __name__ == "__main__":
    main()
