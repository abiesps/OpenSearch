#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Cold/warm benchmark session on ONE data node that runs every arm, run from the load generator.

Unit of measurement = one JVM run of one arm. A session is R rounds; each round runs every arm once, in an
interleaved order (round 0 forward, round 1 reversed, ... = A B B A A B for two arms; or seeded random), and every
arm run is a fresh JVM (the agent restarts the node with that arm's binary). Inside a run:
  1. verify: cluster green, the arm's indices open with the arm's store type, segment count per shard as expected,
     doc counts, the per-field formats of every segment for an index with a "formats" entry (segformat_check.py, read
     by the agent from the segment files), every switch the arm posts reads back as sent (an unknown switch = arm NOT
     AVAILABLE, a recorded gap)
  2. cold block: ops in a seeded random order with the reference op first and last; before EVERY iteration of EVERY op:
       wait for an idle bufferpool prefetch pool, POST /_bufferpool/cache/_clear (bufferpool arms),
       POST /_cache/clear (query, fielddata, request), agent /cache/drop (pageout of the JVM's index-file mappings,
       sync, echo 3 > drop_caches), then VERIFY: bufferpool cached_blocks == 0, page-cache residency of the arm's
       index files <= --residency-tolerance (agent mincore; works for EBS and EFS files), and after the query: if the
       query read anything, the reads reached storage: on EBS diskstats reads > 0 and JVM /proc/<pid>/io
       read_bytes > 0; on EFS NFS READ ops > 0 (mountstats, what nfsiostat reads). Every search runs with
       request_cache=false. Samples that fail verification carry cold_ok=false (--strict aborts).
  3. warm block: ops in a new random order (reference first and last); per op W warm-up then M measured iterations.
  4. result pass: one more execution per op (unmeasured) whose canonical result (total hits, hit ids/scores/sort
     values, aggregations) is stored for the cross-arm equality check in analyze.py.
  5. optional concurrent blocks (--clients N): cold batches (clear, then N clients start one op each together) and a
     warm closed loop of N clients for --concurrent-seconds.
Every sample stores took (server) and wall (client) ms and the IO deltas of its iteration: bufferpool per-file-type
requests / demand loads / prefetch loads / bytes / load time, JVM /proc/<pid>/io, and data-device diskstats.

  coldbench.py run --arms arms.json --ops ops/big5.json --arm-list S0,S1,S2-CORE --rounds 5 \
      --url http://DATA:9200 --agent http://DATA:9700 --token-file TOKEN --out results/big5/session-1
  coldbench.py run ... --arm-list S1@a,S1@b      (A/A: two labels of the same arm)
  coldbench.py probe --arms arms.json --arm S1 --ops ... --op NAME   (one verified cold iteration, no restart)
  coldbench.py plan  --ops ... --arm-list ... --rounds 5             (expected wall time of a session)
"""
import argparse
import hashlib
import json
import math
import os
import random
import sys
import threading
import time
import urllib.parse

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
import canonical_ext  # noqa: E402 - generic workloads: percolate slots, highlight, inner_hits
import indices_ext  # noqa: E402 - generic workloads: multi-index targets, not-applicable arms
import runguards  # noqa: E402 - IO configuration and other-open-indices checks at every arm start
import segformat_check  # noqa: E402 - segment formats proven from the files (index "formats" entry)
from common import HttpError, JsonClient, JsonlWriter, RequestTimeout, wait_until  # noqa: E402

SCHEMA = 1
FLOAT_DIGITS = 12
DROP_ROUNDS = 5  # agent /cache/drop?until_empty: at most this many pageout + sync + drop rounds per cold clear
# EFS pre-conditioning: long enough to outlast efs-proxy's 300 s back-off after a failed scale-up search
EFS_PRECONDITION_TIMEOUT_S = 360
EFS_CONNECTIONS_DEFAULT = 5  # efs-proxy's scaled-up count (max_multiplexed_connections), the steady busy-node state


# ---------------------------------------------------------------- query execution and canonical results
def _round(v):
    if isinstance(v, float):
        if v == 0 or math.isnan(v) or math.isinf(v):
            return v
        return float(f"{v:.{FLOAT_DIGITS}g}")
    if isinstance(v, dict):
        return {k: _round(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_round(x) for x in v]
    return v


def _agg_items(aggs):
    n = 0
    if isinstance(aggs, dict):
        for k, v in aggs.items():
            if k == "buckets" and isinstance(v, (list, dict)):
                n += len(v)
            n += _agg_items(v)
    elif isinstance(aggs, list):
        for v in aggs:
            n += _agg_items(v)
    return n


def canonical(pages, typ):
    """Comparable result of an op: total hits, hits (id, score, sort) in order, aggregations (floats to 12 digits)."""
    first = pages[0]
    total = first.get("hits", {}).get("total")
    hits = [[h.get("_id"), _round(h.get("_score")), _round(h.get("sort"))] for p in pages for h in p.get("hits", {}).get("hits", [])]
    out = {"total": [total["value"], total["relation"]] if isinstance(total, dict) else total,
           "aggs": _round(first.get("aggregations")), "aggs_size": _agg_items(first.get("aggregations"))}
    if typ == "scroll":
        # many thousands of ids: keep the count and an order-free digest (scroll order is not defined across arms)
        ids = sorted(str(h[0]) for h in hits)
        out["hits_count"] = len(ids)
        out["hits_digest"] = hashlib.sha256("\n".join(ids).encode()).hexdigest()
    else:
        out["hits"] = hits
        extras = canonical_ext.hit_extras(pages, _round)  # None unless hits carry percolate/highlight/inner_hits parts
        if extras is not None:
            out["hits_ext"] = extras
    out["digest"] = hashlib.sha256(json.dumps(out, sort_keys=True).encode()).hexdigest()[:16]
    return out


def _qs(params):
    p = {"request_cache": "false", **{k: v for k, v in params.items() if k != "request_cache"}}
    return urllib.parse.urlencode(p)


def execute(client, index, op):
    """
    Runs one op. Returns took_ms (sum over pages), wall_ms (client time of the timed requests only), canonical, pages.
    scroll: first page + (pages-1) scroll requests (clear-scroll after the timing); search_after: pages requests,
    each continuing after the last hit of the previous page.
    """
    typ = op.get("type", "search")
    body = dict(op["body"])
    pages, took, wall = [], 0.0, 0.0
    if typ == "search":
        r = client.request("POST", f"/{index}/_search?{_qs(op.get('params', {}))}", body)
        wall += client.last_wall_s
        pages.append(r)
        took += r.get("took", 0)
    elif typ == "scroll":
        params = {**op.get("params", {}), "scroll": "1m", "size": str(op.get("page_size", 1000))}
        r = client.request("POST", f"/{index}/_search?{_qs(params)}", body)
        wall += client.last_wall_s
        pages.append(r)
        took += r.get("took", 0)
        sid = r.get("_scroll_id")
        try:
            for _ in range(op.get("pages", 10) - 1):
                if not r.get("hits", {}).get("hits"):
                    break
                r = client.request("POST", "/_search/scroll", {"scroll": "1m", "scroll_id": sid})
                wall += client.last_wall_s
                pages.append(r)
                took += r.get("took", 0)
                sid = r.get("_scroll_id", sid)
        finally:
            if sid:
                client.raw("DELETE", "/_search/scroll", {"scroll_id": [sid]})
    elif typ == "search_after":
        for _ in range(op.get("pages", 3)):
            r = client.request("POST", f"/{index}/_search?{_qs(op.get('params', {}))}", body)
            wall += client.last_wall_s
            pages.append(r)
            took += r.get("took", 0)
            hits = r.get("hits", {}).get("hits", [])
            if not hits:
                break
            body = {**body, "search_after": hits[-1]["sort"]}
    else:
        raise ValueError(f"op {op['name']}: unknown type {typ}")
    for p in pages:
        if p.get("timed_out") or (p.get("_shards", {}).get("failed") or 0) > 0:
            raise RuntimeError(f"op {op['name']}: timed out or shard failures: {json.dumps(p.get('_shards'))[:500]}")
    out = {"took_ms": took, "wall_ms": wall * 1e3, "canonical": canonical(pages, typ), "requests": len(pages)}
    flags = canonical_ext.response_flags(pages)
    if flags:
        out["flags"] = flags
    return out


# ---------------------------------------------------------------- the data node
# Client socket timeout per OpenSearch request (search, cache clear, settings). One value for every configuration of
# a session (it is recorded with the session arguments); raise it for operations whose single execution can
# exceed 2 h (big5 1,000 GB phrase query on Amazon EFS), so a slow request finishes instead of failing the run.
REQUEST_TIMEOUT_DEFAULT_S = 7200.0


class Node:
    def __init__(self, url, agent_url, token, residency_tolerance, timeout=REQUEST_TIMEOUT_DEFAULT_S):
        self.url = url
        self.os = JsonClient(url, timeout=timeout)
        self.agent = JsonClient(agent_url, timeout=900.0, headers={"X-Coldpath-Token": token}) if agent_url else None
        self.agent_url, self.agent_token = agent_url, token
        if residency_tolerance != 0:
            # common-rules "Agent pageout bug": a cold iteration counts only with 0 resident index pages
            raise SystemExit(f"--residency-tolerance {residency_tolerance}: only 0 is allowed (cold = no index page resident)")
        self.residency_tolerance = residency_tolerance

    def up(self):
        try:
            self.os.request("GET", "/", timeout=5)
            return True
        except Exception:  # noqa: BLE001
            self.os.close()
            return False

    def jvm_gone(self):
        """True when the agent sees no node JVM (it exited while starting or while measuring); False when it does or
        there is no agent to ask."""
        if not self.agent:
            return False
        try:
            return self.agent.request("GET", "/node/status").get("pid") is None
        except Exception:  # noqa: BLE001 - an agent that does not answer says nothing about the node
            return False

    def wait_green(self, timeout_s=900):
        deadline = time.monotonic() + timeout_s
        while not self.up():
            # a node that exited (EFS: NoSuchFileException in the cluster-state commit) never comes up: fail at once
            # instead of after timeout_s, so start_arm can retry it
            if self.jvm_gone():
                raise NodeDown("the node JVM exited before its HTTP port answered")
            if time.monotonic() > deadline:
                raise TimeoutError(f"timed out after {timeout_s}s waiting for OpenSearch HTTP")
            time.sleep(1.0)
        self.os.request("GET", f"/_cluster/health?wait_for_status=green&wait_for_no_relocating_shards=true"
                               f"&wait_for_no_initializing_shards=true&timeout={timeout_s}s", timeout=timeout_s + 30)

    def prefetch_pool(self):
        n = next(iter(self.os.request("GET", "/_nodes/_local/stats/thread_pool")["nodes"].values()))
        return n["thread_pool"].get("bufferpool_prefetch")

    def scheduler_state(self):
        """The prefetch scheduler's idle fields, or None on a build without the scheduler."""
        s = self.bp_stats().get("prefetch_scheduler")
        if s is None:
            return None
        return {k: s[k] for k in ("pending", "queued", "active_workers", "demand_reads_in_flight")}

    def wait_prefetch_idle(self, timeout_s=120):
        """Wait until no prefetch runs or waits. With the prefetch scheduler (budget scope total) items can be held in
        its queue with no worker, so an idle thread pool is not enough: also require prefetch_scheduler.pending == 0
        and demand_reads_in_flight == 0. Builds without the scheduler keep the thread-pool check only."""
        t0 = time.monotonic()
        has_scheduler = None
        while True:
            p = self.prefetch_pool()
            s = None
            if p is None:
                return (time.monotonic() - t0) * 1e3, p
            if p["active"] == 0 and p["queue"] == 0:
                if has_scheduler is not False:
                    s = self.scheduler_state()
                    has_scheduler = s is not None
                if s is None or (s["pending"] == 0 and s["demand_reads_in_flight"] == 0):
                    return (time.monotonic() - t0) * 1e3, p
            if time.monotonic() - t0 > timeout_s:
                if s is None and has_scheduler is not False:
                    s = self.scheduler_state()
                raise TimeoutError(f"bufferpool_prefetch not idle after {timeout_s}s: pool {p}, scheduler {s}")
            time.sleep(0.005)

    def bp_stats(self):
        return self.os.request("GET", "/_bufferpool/stats")

    def snapshot(self, bufferpool, agent_arm):
        s = {"agent": self.agent.request("GET", f"/snapshot?arm={urllib.parse.quote(agent_arm)}") if self.agent else None}
        s["bp"] = self.bp_stats() if bufferpool else None
        return s


def _delta_files(a, b):
    """Bufferpool per-file counters b - a, summed per file extension."""
    out = {}
    fa = (a or {}).get("files", {})
    for key, v in ((b or {}).get("files") or {}).items():
        ext = key.rsplit(".", 1)[-1]
        prev = fa.get(key, {})
        e = out.setdefault(ext, {})
        for k, x in v.items():
            if isinstance(x, dict):  # reads_by_size: count per size class
                h = e.setdefault(k, {})
                for sk, sx in x.items():
                    h[sk] = h.get(sk, 0) + sx - (prev.get(k) or {}).get(sk, 0)
                e[k] = {sk: sx for sk, sx in h.items() if sx}
            else:
                e[k] = e.get(k, 0) + x - prev.get(k, 0)
    return {ext: e for ext, e in out.items() if any(e.values())}


def io_delta(pre, post):
    d = {}
    if pre.get("bp") is not None:
        d["bp_files"] = _delta_files(pre["bp"], post["bp"])
        tot = {}
        for e in d["bp_files"].values():
            for k, x in e.items():
                if isinstance(x, dict):
                    h = tot.setdefault(k, {})
                    for sk, sx in x.items():
                        h[sk] = h.get(sk, 0) + sx
                else:
                    tot[k] = tot.get(k, 0) + x
        d["bp"] = tot
        for k in ("agg_prefetch_requests", "sort_prefetch_requests", "agg_prefetch_planners", "sort_prefetch_planners"):
            if k in post["bp"]:
                d[k] = post["bp"][k] - pre["bp"].get(k, 0)
    pa, pb = (pre.get("agent") or {}), (post.get("agent") or {})
    if pa.get("proc_io") and pb.get("proc_io") and pa.get("pid") == pb.get("pid"):
        d["proc_io"] = {k: pb["proc_io"][k] - pa["proc_io"].get(k, 0) for k in pb["proc_io"]}
    if pa.get("disk") and pb.get("disk"):
        a, b = pa["disk"], pb["disk"]
        dd = {k: b[k] - a[k] for k in b if isinstance(b[k], int) and k != "in_flight"}
        dt_ms = (pb["t_mono"] - pa["t_mono"]) * 1e3
        dd["window_ms"] = dt_ms
        dd["read_bytes"] = dd.get("sectors_read", 0) * 512
        dd["r_await_ms"] = dd["read_ms"] / dd["reads"] if dd.get("reads") else None
        dd["aqu_sz"] = dd["weighted_io_ms"] / dt_ms if dt_ms > 0 else None
        d["disk"] = dd
    if pa.get("nfs") and pb.get("nfs"):
        a, b = pa["nfs"], pb["nfs"]
        nd = {k: b[k] - a[k] for k in ("normal_read_bytes", "direct_read_bytes", "server_read_bytes", "read_pages")
              if isinstance(b.get(k), int) and isinstance(a.get(k), int)}
        for op, v in b.get("ops", {}).items():
            w = a.get("ops", {}).get(op, {})
            nd[op] = {k: v[k] - w.get(k, 0) for k in v}
        r = nd.get("READ", {})
        # bytes received per READ RPC (payload plus a ~100-byte RPC reply header): the per-read EFS request size
        nd["read_avg_bytes"] = r["bytes_recv"] / r["ops"] if r.get("ops") else None
        nd["read_rtt_avg_ms"] = r["rtt_ms"] / r["ops"] if r.get("ops") else None
        nd["read_exe_avg_ms"] = r["execute_ms"] / r["ops"] if r.get("ops") else None
        dt_ms = (pb["t_mono"] - pa["t_mono"]) * 1e3
        # average READs in flight over the window (Little's law: total execute time / window)
        nd["read_in_flight"] = r["execute_ms"] / dt_ms if r.get("ops") and dt_ms > 0 else None
        nd["window_ms"] = dt_ms
        d["nfs"] = nd
    ea, eb = pa.get("efs_connections"), pb.get("efs_connections")
    if ea is not None or eb is not None:
        # efs-proxy's backend TCP connections to the mount target at the start and the end of the sample
        # (common-rules.md "Amazon EFS connection count is a measured variable")
        d["efs_connections"] = {"start": (ea or {}).get("count"), "end": (eb or {}).get("count"),
                                "proxy_pid": [(ea or {}).get("proxy_pid"), (eb or {}).get("proxy_pid")]}
    return d


def warm_efs_fields(it, io):
    """Fields of a warm sample record: efs_connections_ok (EFS arms with a target) and a pre-conditioning, if one ran."""
    out = {}
    eok = efs_connections_ok(io, it.efs_target) if it.efs else None
    if eok is not None:
        out["efs_connections_ok"] = eok
    pc = it.take_efs_precondition()
    if pc is not None:
        out["efs_precondition"] = pc
    return out


def efs_level(count, target):
    """True if a backend connection count is at the target level: exactly 1 for target 1; at least the target above 1
    (efs-proxy multiplexes 5 connections; a 6th socket of the previous incarnation can stay open while it closes)."""
    if count is None:
        return False
    return count == 1 if target == 1 else count >= target


def efs_connections_ok(io, target):
    """None if not an EFS sample or no target; else start and end at the target level, on the same efs-proxy process."""
    if not target or io.get("nfs") is None:
        return None
    e = io.get("efs_connections")
    if e is None:
        return False  # an EFS sample without the count: connection state unknown
    return efs_level(e["start"], target) and efs_level(e["end"], target) and e["proxy_pid"][0] == e["proxy_pid"][1]


# ---------------------------------------------------------------- NFS transport
XPRT_TCP = ("srcport", "bind_count", "connect_count", "connect_time", "idle_time", "sends", "recvs", "bad_xids",
            "req_u", "bklog_u", "max_slots", "sending_u", "pending_u")


def parse_xprt(line):
    """mountstats 'xprt: tcp ...' line -> {field: int} (Linux nfs-utils mountstats field order), None if absent."""
    if not line or not isinstance(line, str):
        return None
    parts = line.split()
    if len(parts) < 3 or parts[0] != "xprt:" or parts[1] != "tcp":
        return {"raw": line}
    out = {"proto": "tcp", "raw": line}
    for k, v in zip(XPRT_TCP, parts[2:]):
        try:
            out[k] = int(v)
        except ValueError:
            out[k] = v
    return out


# ---------------------------------------------------------------- arms
class ArmUnavailable(RuntimeError):
    pass


class NodeDown(RuntimeError):
    """The node JVM is not running (it exited while starting or during a run)."""


class NodeStartFailed(RuntimeError):
    """A node start failed after NODE_START_RETRIES retries; the run is re-queued at the end of the session."""


class MemoryLimitExceeded(RuntimeError):
    """The node's resident or anonymous memory passed --mem-limit-pct of host memory (common-rules "BLOCKER: native
    memory growth ..."): the run ends at the next operation boundary as run_discarded and is re-queued."""


class MemoryMonitor:
    """
    Polls the agent's GET /node/memory every `interval` s on its own connection during one run: node JVM VmRSS and
    RssAnon against host MemTotal. Keeps the maximum, a series every `keep_every` s, and flags `exceeded` when either
    passes `limit_pct`; the run loop checks the flag at every operation boundary (check()). An agent without the
    endpoint leaves the monitor unavailable (recorded), never a zero.
    """

    def __init__(self, node, interval=10.0, limit_pct=70.0, keep_every=60.0):
        self.interval, self.limit_pct, self.keep_every = interval, limit_pct, keep_every
        self.client = JsonClient(node.agent_url, timeout=30.0, headers={"X-Coldpath-Token": node.agent_token})
        self.stop_ev = threading.Event()
        self.lock = threading.Lock()
        self.first, self.last, self.max_rss_pct, self.max_anon_pct, self.max_rss_kb = None, None, 0.0, 0.0, 0
        self.samples, self.errors, self.series, self.exceeded, self.available = 0, 0, [], None, True
        self._last_kept = 0.0
        self.thread = threading.Thread(target=self._loop, name="node-memory", daemon=True)

    def start(self):
        self.poll()
        if self.available:
            self.thread.start()
        return self

    def poll(self):
        try:
            m = self.client.request("GET", "/node/memory")
        except HttpError as e:
            if e.status == 404:
                self.available = False
            with self.lock:
                self.errors += 1
            return None
        except Exception:  # noqa: BLE001 - a missed poll is counted, the next one may work
            with self.lock:
                self.errors += 1
            return None
        now = time.time()
        with self.lock:
            self.samples += 1
            self.first = self.first or {**m, "t": now}
            self.last = {**m, "t": now}
            rp, ap = m.get("rss_pct") or 0.0, m.get("anon_pct") or 0.0
            self.max_rss_pct, self.max_anon_pct = max(self.max_rss_pct, rp), max(self.max_anon_pct, ap)
            self.max_rss_kb = max(self.max_rss_kb, m.get("vm_rss_kb") or 0)
            if now - self._last_kept >= self.keep_every:
                self.series.append({"t": round(now, 1), "rss_kb": m.get("vm_rss_kb"), "anon_kb": m.get("rss_anon_kb")})
                self._last_kept = now
            if self.exceeded is None and max(rp, ap) > self.limit_pct:
                self.exceeded = {"t": now, "rss_pct": rp, "anon_pct": ap, "vm_rss_kb": m.get("vm_rss_kb"),
                                 "rss_anon_kb": m.get("rss_anon_kb"), "limit_pct": self.limit_pct}
        return m

    def _loop(self):
        while not self.stop_ev.wait(self.interval):
            self.poll()

    def stop(self):
        self.stop_ev.set()
        if self.thread.is_alive():
            self.thread.join(timeout=self.interval + 35)
        self.client.close()
        return self.summary()

    def summary(self):
        with self.lock:
            return {"available": self.available, "interval_s": self.interval, "limit_pct": self.limit_pct,
                    "samples": self.samples, "errors": self.errors, "max_rss_pct": round(self.max_rss_pct, 2),
                    "max_anon_pct": round(self.max_anon_pct, 2), "max_rss_kb": self.max_rss_kb, "first": self.first,
                    "last": self.last, "series": self.series, "exceeded": self.exceeded,
                    "malloc_arena_max": (self.first or {}).get("malloc_arena_max"),
                    "allocator": (self.first or {}).get("allocator")}

    def check(self):
        with self.lock:
            e = self.exceeded
        if e:
            raise MemoryLimitExceeded(f"node memory above {e['limit_pct']:.0f} % of host memory (RSS {e['rss_pct']:.1f} %, "
                                      f"anonymous {e['anon_pct']:.1f} %, VmRSS {e['vm_rss_kb']} kB)")


class RunDiscarded(RuntimeError):
    """The node failed during a measured run; the run's samples are discarded and the run is re-queued."""


# Node start on EFS can fail with NoSuchFileException in the cluster-state commit (common-rules.md, "Node start can fail
# on Amazon EFS ..."): a start that fails before any measured iteration is retried after NODE_START_RETRY_WAIT_S.
NODE_START_RETRIES = 2
NODE_START_RETRY_WAIT_S = float(os.environ.get("COLDBENCH_NODE_START_RETRY_WAIT_S", "10"))  # env: self-test only
# a run that could not start, or whose node failed while measuring, is re-queued once at the end of the session
RUN_REQUEUES = 1


def load_arms(path, indices_override=None, labels=None):
    """
    Arms file; --indices replaces its [indices] (e.g. the single-shard >= 30 GB copies, same arms).
    "context_arms" (USER DECISION 2026-10-05): arms kept for context only, never in an outcome verdict, for example
    stock OpenSearch with memory mapping. They join [arms] only when a label of the session names them (labels), so a
    session that does not ask for them sees no stock arm (no store-type reset of the shared indices to hybridfs).
    """
    cfg = json.load(open(path))
    if indices_override:
        cfg["indices"] = json.load(open(indices_override))["indices"]
    indices_ext.normalize(cfg["indices"])
    context = cfg.get("context_arms") or {}
    clash = sorted(set(context) & set(cfg["arms"]))
    if clash:
        raise ValueError(f"arms {clash} are both in [arms] and in [context_arms]")
    for name in sorted({parse_label(lab)[0] for lab in (labels or [])} & set(context)):
        cfg["arms"][name] = {**context[name], "context": True}
    for name, a in cfg["arms"].items():
        if indices_ext.not_applicable(a):
            continue
        runguards.validate_build(name, a)
        for key in ("node", "index", "open"):
            if key not in a:
                raise ValueError(f"arm {name}: missing [{key}]")
        if a["index"] not in cfg["indices"] or any(k not in cfg["indices"] for k in a["open"]):
            raise ValueError(f"arm {name}: unknown index key")
        if a["index"] not in a["open"]:
            raise ValueError(f"arm {name}: its query index must be in [open]")
    return cfg


def parse_label(label):
    """'S1@a' -> ('S1', 'S1@a'): two labels of one arm are two arms of an A/A comparison."""
    return label.split("@", 1)[0], label


def index_state(node, name):
    rows = node.os.request("GET", f"/_cat/indices/{name}?format=json&h=index,status,uuid,docs.count&expand_wildcards=all")
    if not rows:
        raise RuntimeError(f"index {name} does not exist")
    r = rows[0]
    s = node.os.request("GET", f"/{name}/_settings?expand_wildcards=all")
    st = next(iter(s.values()))["settings"]["index"].get("store", {}).get("type")
    return {"status": r["status"], "uuid": r["uuid"], "store_type": st}


def stock_store_types(cfg):
    """Index name -> store type of the stock (non-bufferpool) arms that open it. Indices no stock arm opens: absent."""
    out = {}
    for a in cfg["arms"].values():
        if a.get("bufferpool") or indices_ext.not_applicable(a):
            continue
        for key in a.get("open", []):
            t = a.get("store_types", {}).get(key)
            if not t:
                continue
            for m in indices_ext.members(cfg["indices"][key]):
                if out.setdefault(m["name"], t) != t:
                    raise ValueError(f"index {m['name']}: stock arms use two store types ({out[m['name']]}, {t})")
    return out


def close_indices(node, cfg, log, reset=True):
    """
    On the RUNNING node, before it stops: close every configured index it has open. Every node therefore starts with
    all benchmark indices closed, and open_indices sets the store type the next arm needs before it opens them, so a
    stock node never opens a bufferpoolfs or split-format index (S0 and POC share a data path per storage). Closing
    and opening is the same for every arm, and the cold clear after it removes what the open read.
    A closed index stays allocated, so the stock binary cannot start next to a closed index whose store type it does
    not know (shard fails with "Unknown store type [bufferpoolfs]", cluster red, and the stock node rejects the
    store-type update too; probe on the big5-100 data node, br-big5-100/closed-index-probe.txt). So a node that knows
    the bufferpool store type gives every closed index that a stock arm opens the stock arms' store type back here.
    """
    stock = stock_store_types(cfg)
    reset_ok = None
    for name in indices_ext.physical_names(cfg["indices"]):
        try:
            st = index_state(node, name)
        except RuntimeError:
            continue
        if st["status"] == "open":
            node.os.request("POST", f"/{name}/_close?wait_for_active_shards=0")
            log(f"  closed {name}")
        want = stock.get(name)
        # no index.store.type = the node default (fs, hybridfs on Linux), which the stock binary reads
        if reset and want and st["store_type"] is not None and st["store_type"] != want:
            if reset_ok is None:
                reset_ok = any(p.get("component") == "store-bufferpool"
                               for p in node.os.request("GET", "/_cat/plugins?format=json"))
            if not reset_ok:
                raise RuntimeError(f"{name}: closed with store type {st['store_type']} on a node without the bufferpool "
                                   f"plugin; start a POC arm to set it back to {want}")
            node.os.request("PUT", f"/{name}/_settings", {"index.store.type": want})
            log(f"  {name}: store type {st['store_type']} -> {want} (closed; the stock binary can start next to it)")


def open_indices(node, cfg, arm, log):
    for key in arm["open"]:
        want_type = arm.get("store_types", {}).get(key)
        for member in indices_ext.members(cfg["indices"][key]):
            name = member["name"]
            st = index_state(node, name)
            if want_type and st["store_type"] != want_type:
                if st["status"] == "open":
                    node.os.request("POST", f"/{name}/_close?wait_for_active_shards=0")
                node.os.request("PUT", f"/{name}/_settings", {"index.store.type": want_type})
            if index_state(node, name)["status"] != "open":
                node.os.request("POST", f"/{name}/_open?wait_for_active_shards=all")
                log(f"  opened {name}")
    node.wait_green()


def verify_indices(node, cfg, arm, agent_arm=None):
    """Store type, docs, shards, segments per shard (vs the expected values), store sizes; and du via the agent."""
    out = {}
    for key in arm["open"]:
        idx = cfg["indices"][key]
        if idx.get("members"):
            # multi-index target: every member verified like a single index, with its own expected values
            sub = {"indices": {f"{key}/{m['name']}": m for m in idx["members"]}}
            want_type = arm.get("store_types", {}).get(key)
            sub_arm = {**arm, "open": list(sub["indices"]), "store_types": {k: want_type for k in sub["indices"]} if want_type else {}}
            infos = list(verify_indices(node, sub, sub_arm, agent_arm).values())
            out[key] = {"name": idx["name"], "members": infos, "uuids": [i["uuid"] for i in infos],
                        "docs": sum(i["docs"] for i in infos)}
            continue
        name = idx["name"]
        st = index_state(node, name)
        segs = node.os.request("GET", f"/_cat/segments/{name}?format=json&h=shard,prirep,segment,docs.count,size,searchable")
        per_shard = {}
        for s in segs:
            if s["prirep"] in ("p", "primary") and str(s.get("searchable")).lower() == "true":
                per_shard[int(s["shard"])] = per_shard.get(int(s["shard"]), 0) + 1
        count = node.os.request("GET", f"/{name}/_count")["count"]
        size = node.os.request("GET", f"/_cat/indices/{name}?format=json&bytes=b&h=pri,rep,pri.store.size,store.size")[0]
        info = {"name": name, "uuid": st["uuid"], "store_type": st["store_type"], "docs": count,
                "segments_per_shard": dict(sorted(per_shard.items())), "primaries": int(size["pri"]),
                "replicas": int(size["rep"]), "pri_store_bytes": int(size["pri.store.size"]),
                "store_bytes": int(size["store.size"])}
        if node.agent and agent_arm:
            info["du"] = node.agent.request("GET", f"/index/du?arm={urllib.parse.quote(agent_arm)}&uuids={st['uuid']}")
        if idx.get("formats") is not None:
            # the per-field format attributes and files of every segment, never the codec name (segformat_check.py)
            if not (node.agent and agent_arm):
                raise RuntimeError(f"{name}: the \"formats\" check reads the segment files and needs --agent")
            res = segformat_check.check_index(node.os, node.agent, agent_arm, name, st["uuid"], idx["formats"])
            info["formats"] = {k: res[k] for k in ("ok", "expected", "shards", "segments", "summary")}
            if not res["ok"]:
                raise RuntimeError(f"{name}: segment formats do not match \"formats\" {json.dumps(idx['formats'])}: "
                                   + "; ".join(res["errors"][:10]) + (" ..." if len(res["errors"]) > 10 else ""))
        if idx.get("min_store_bytes") is not None and info["pri_store_bytes"] < idx["min_store_bytes"]:
            raise RuntimeError(f"{name}: primary store {info['pri_store_bytes']} B < required {idx['min_store_bytes']} B")
        want = idx.get("segments_per_shard")
        if want is not None and any(v != want for v in per_shard.values()):
            raise RuntimeError(f"{name}: segments per shard {per_shard}, expected {want} in every shard")
        if idx.get("shards") is not None and len(per_shard) != idx["shards"]:
            raise RuntimeError(f"{name}: {len(per_shard)} shards with searchable segments, expected {idx['shards']}")
        if idx.get("docs") is not None and count != idx["docs"]:
            raise RuntimeError(f"{name}: {count} docs, expected {idx['docs']}")
        want_type = arm.get("store_types", {}).get(key)
        if want_type and st["store_type"] != want_type:
            raise RuntimeError(f"{name}: store type {st['store_type']}, arm wants {want_type}")
        out[key] = info
    return out


def apply_settings(node, cfg, arm):
    keys = set(cfg.get("cluster_settings_reset", []))
    for a in cfg["arms"].values():
        keys.update(a.get("cluster_settings", {}))
    body = {k: None for k in keys}
    body.update(arm.get("cluster_settings", {}))
    if body:
        node.os.request("PUT", "/_cluster/settings", {"persistent": body})
    got = node.os.request("GET", "/_cluster/settings?flat_settings=true")["persistent"]
    for k, v in arm.get("cluster_settings", {}).items():
        if str(got.get(k)).lower() != str(v).lower():
            raise RuntimeError(f"cluster setting {k}: node has {got.get(k)}, arm wants {v}")
    return got


def apply_switches(node, cfg, arm_name, arm):
    """
    Posts the base switches (bufferpool arms of the proof-of-concept build) and the arm's own; checks the read-back.
    400 = switch not in binary. The baseline build (stock OpenSearch with the plugin) has no experiment endpoint: it gets
    no base switches and its sort_opt state is not read (load_arms refuses switches on it).
    """
    baseline = runguards.arm_build(arm) == "baseline"
    calls = (cfg.get("base_switches", []) if arm.get("bufferpool") and not baseline else []) + arm.get("switches", [])
    applied = []
    for c in calls:
        status, resp = node.os.raw(c.get("method", "POST"), c["path"])
        if status == 400:
            raise ArmUnavailable(f"arm {arm_name}: {c['path']} -> 400 {json.dumps(resp)[:400]} (switch not in this binary)")
        if status // 100 != 2:
            raise RuntimeError(f"arm {arm_name}: {c['path']} -> {status} {json.dumps(resp)[:400]}")
        for k, v in c.get("verify", {}).items():
            if str(resp.get(k)).lower() != str(v).lower():
                raise RuntimeError(f"arm {arm_name}: after {c['path']} node reports {k}={resp.get(k)}, expected {v}")
        applied.append({"path": c["path"], "response": resp})
    state = {}
    if arm.get("bufferpool"):
        if not baseline:
            state["sort_opt"] = node.os.request("GET", "/_bufferpool/sort_opt")
        st = node.bp_stats()
        state["bp"] = {k: v for k, v in st.items() if k != "files"}
    return applied, state


def run_metadata(node, cfg, arm_name, arm):
    md = {"root": node.os.request("GET", "/"),
          "plugins": node.os.request("GET", "/_cat/plugins?format=json"),
          "jvm": node.os.request("GET", "/_nodes/_local/jvm,os?filter_path=nodes.*.jvm.version,nodes.*.jvm.vm_name,"
                                        "nodes.*.jvm.vm_vendor,nodes.*.jvm.vm_version,nodes.*.jvm.input_arguments,"
                                        "nodes.*.jvm.mem,nodes.*.os"),
          "cluster_settings": node.os.request("GET", "/_cluster/settings?include_defaults=true&flat_settings=true"
                                                     "&filter_path=**.bufferpool*,**.search.concurrent*")}
    if node.agent:
        md["host"] = node.agent.request("GET", f"/host?arm={urllib.parse.quote(arm['node'])}")
        md["node_status"] = node.agent.request("GET", "/node/status")
    return md


# ---------------------------------------------------------------- search caches
CACHE_STATS = "/_nodes/_local/stats/indices/fielddata,query_cache,request_cache"


def search_cache_bytes(node):
    """The node's fielddata (including global ordinals, which IndicesFieldDataCache holds), query cache and request
    cache: bytes and entries. A stat the node does not report is None."""
    n = next(iter(node.os.request("GET", CACHE_STATS)["nodes"].values())).get("indices", {})
    fd, qc, rc = n.get("fielddata") or {}, n.get("query_cache") or {}, n.get("request_cache") or {}
    return {"fielddata_bytes": fd.get("memory_size_in_bytes"), "query_cache_bytes": qc.get("memory_size_in_bytes"),
            "query_cache_entries": qc.get("cache_size"), "request_cache_bytes": rc.get("memory_size_in_bytes")}


# POST /_cache/clear only MARKS fielddata entries (global ordinals included) of the index; IndicesFieldDataCache drops
# them in its periodic sweep, every indices.cache.cleanup_interval (default 1m, IndicesService.CacheCleaner). Every
# arm's node therefore runs with indices.cache.cleanup_interval: 1s (S0 and POC alike, a node setting both binaries
# have), and the clear waits up to CACHE_CLEAR_WAIT_S for the sweep. With the default interval the wait times out and
# the iteration fails search_caches_empty (pmc: global ordinals of `name` stayed 1.7-11 MB after the clear).
CACHE_CLEAR_WAIT_S = 5.0


def node_settings(node):
    """The running node's own settings, flat (GET /_nodes/_local/settings)."""
    r = node.os.request("GET", "/_nodes/_local/settings?flat_settings=true")
    return next(iter(r["nodes"].values())).get("settings", {})


def clear_search_caches(node, rounds=3, wait_s=None):
    """
    POST /_cache/clear (query, fielddata, request) and read the node's cache stats back until fielddata, query cache
    and request cache report 0: the clear is re-posted every second (at least `rounds` times) for at most `wait_s`
    seconds (default CACHE_CLEAR_WAIT_S), because the node drops marked fielddata only in its periodic sweep.
    `empty` is False when a stat stays above 0 or is missing; `wait_ms` is the time until empty (or the timeout).
    """
    wait_s = CACHE_CLEAR_WAIT_S if wait_s is None else wait_s
    out = {"rounds": 0}
    t0 = time.monotonic()
    last_post = None
    while True:
        now = time.monotonic()
        if last_post is None or now - last_post >= 1.0 or out["rounds"] < rounds:
            node.os.request("POST", "/_cache/clear?query=true&fielddata=true&request=true")
            out["rounds"] += 1
            last_post = time.monotonic()
        st = search_cache_bytes(node)
        out.update(st)
        if all(v == 0 for v in st.values()) or time.monotonic() - t0 >= wait_s:
            break
        time.sleep(0.05)
    out["wait_ms"] = round((time.monotonic() - t0) * 1e3, 1)
    out["empty"] = all(out.get(k) == 0 for k in ("fielddata_bytes", "query_cache_bytes", "query_cache_entries",
                                                  "request_cache_bytes"))
    return out


def jit_warmup(session, run, index, ops, seed):
    """
    Cold means data-cold on a JIT-warm JVM (common-rules DECISION "cold means data-cold on a JIT-warm JVM"): before the
    cold block of a run, every op runs once, unmeasured, so iteration 0 of a cold op no longer pays the JIT compilation
    of its query shape. The full cold clear before every cold iteration then drops what this warm-up loaded (bufferpool,
    page cache, fielddata and global ordinals, query and request caches).
    """
    t0 = time.monotonic()
    order = op_order(ops, session.ref, seed)
    for op in order:
        session.mem_check()
        execute(session.node.os, index, op)
    rec = {"ops": len(order), "elapsed_ms": (time.monotonic() - t0) * 1e3, "caches_after": search_cache_bytes(session.node)}
    session.record(type="jit_warmup", **run, **rec)
    session.log(f"  JIT warm-up: {len(order)} op executions in {rec['elapsed_ms'] / 1e3:.1f} s (unmeasured)")
    return rec


# ---------------------------------------------------------------- one iteration
class Iteration:
    def __init__(self, node, arm, uuids, residency_every, efs=False, efs_target=0):
        self.node, self.arm, self.uuids, self.residency_every = node, arm, uuids, residency_every
        self.count = 0
        self.q = "arm=" + urllib.parse.quote(arm["node"])
        # EFS arms: efs-proxy's backend connection count is held at efs_target (0 = only recorded)
        self.efs, self.efs_target = bool(efs), int(efs_target or 0)
        self.last_efs_precondition = None

    def snapshot(self):
        return self.node.snapshot(bool(self.arm.get("bufferpool")), self.arm["node"])

    def ensure_efs(self, timeout_s=None):
        """
        Brings the EFS mount to efs_target backend connections before a sample: if the count is lower, the agent reads
        a scratch file on the mount with O_DIRECT (no page cache, no index file) until efs-proxy has scaled up. A count
        above the target cannot be lowered without a remount, so it is refused. Returns None if nothing was needed.
        """
        if not (self.efs and self.efs_target and self.node.agent):
            return None
        timeout_s = EFS_PRECONDITION_TIMEOUT_S if timeout_s is None else timeout_s
        c = self.node.agent.request("GET", f"/efs/connections?{self.q}")["efs_connections"]
        n = (c or {}).get("count")
        if efs_level(n, self.efs_target):
            return None
        if n is not None and self.efs_target == 1 and n > 1:
            raise RuntimeError(f"arm {self.arm['node']}: the EFS mount has {n} backend connections, target {self.efs_target}; "
                               "a count cannot be lowered without a remount (for 1: remount with efs_conn_ctl.sh pin-on)")
        r = self.node.agent.request("POST", f"/efs/precondition?{self.q}&target={self.efs_target}&timeout_s={timeout_s}",
                                    timeout=timeout_s + 30)
        r = {k: r.get(k) for k in ("ok", "count", "count_before", "read_MBps", "elapsed_s", "skipped")}
        self.last_efs_precondition = r
        return r

    def pre_snapshot(self):
        """snapshot() at the start of a measured warm sample, after ensure_efs()."""
        self.ensure_efs()
        return self.snapshot()

    def take_efs_precondition(self):
        r, self.last_efs_precondition = self.last_efs_precondition, None
        return r

    def clear(self):
        n, bp = self.node, bool(self.arm.get("bufferpool"))
        out = {}
        out["idle_wait_ms"], _ = n.wait_prefetch_idle()
        if bp:
            n.os.request("POST", "/_bufferpool/cache/_clear")
        out["caches"] = clear_search_caches(n)
        checked = False
        if n.agent:
            # until_empty: pageout + sync + drop_caches repeated until mincore finds no resident page of the arm's
            # index files (one MADV_PAGEOUT pass can leave a few pages; common-rules "Agent pageout bug" follow-up)
            d = n.agent.request("POST", f"/cache/drop?pageout=1&until_empty={DROP_ROUNDS}&{self.q}&uuids="
                                + ",".join(self.uuids))
            out["nfs"] = d.get("nfs")
            out["drop"] = {k: d[k] for k in ("pageout_ms", "sync_ms", "drop_ms")}
            out["drop_rounds"] = d.get("rounds")
            out["pageout"] = d.get("pageout")
            if "resident_bytes" in d:
                out["resident_bytes"] = d["resident_bytes"]
                out["index_bytes"] = d["bytes"]
                if d["resident_bytes"] > n.residency_tolerance:
                    out["top_resident"] = (d.get("round_detail") or [{}])[-1].get("top_resident")
                checked = True
            else:  # an agent without until_empty: one drop, then the residency check below
                out["cached_after_drop"] = d["meminfo_after"]["Cached"]
        if bp:
            out["bp_cached_blocks"] = n.bp_stats()["cached_blocks"]
        # after the clears: the pre-conditioning reads a scratch file with O_DIRECT, so it adds no page-cache page and
        # reads no index file
        pc = self.ensure_efs()
        if pc is not None:
            out["efs_precondition"] = pc
            self.last_efs_precondition = None
        if not checked and n.agent and self.residency_every and self.count % self.residency_every == 0:
            r = n.agent.request("GET", f"/cache/residency?{self.q}&uuids=" + ",".join(self.uuids))
            out["resident_bytes"] = r["resident_bytes"]
            out["index_bytes"] = r["bytes"]
            out["residency_ms"] = r["elapsed_ms"]
            if r["resident_bytes"] > n.residency_tolerance:
                out["top_resident"] = r["top_resident"]
        self.count += 1
        return out

    def verify(self, pre, io):
        bp = bool(self.arm.get("bufferpool"))
        checks = {}
        if pre.get("caches") is not None:
            # fielddata (with global ordinals), query cache and request cache empty after the clear: the JIT warm-up
            # before the cold block builds them, and a cold iteration must not reuse them
            checks["search_caches_empty"] = pre["caches"]["empty"]
        if bp:
            checks["bp_empty"] = pre.get("bp_cached_blocks") == 0
        if "resident_bytes" in pre:
            checks["page_cache_empty"] = pre["resident_bytes"] <= self.node.residency_tolerance
        demand = io.get("bp", {}).get("loads", 0) + io.get("bp", {}).get("prefetch_loads", 0) if bp else None
        proc = (io.get("proc_io") or {}).get("read_bytes")
        disk = (io.get("disk") or {}).get("reads")
        nfs = io.get("nfs")
        nfs_reads = (nfs.get("READ") or {}).get("ops", 0) if nfs else None
        read_any = (demand or 0) > 0 or (proc or 0) > 0 or (nfs_reads or 0) > 0
        checks["no_io"] = not read_any
        if read_any and nfs is not None:
            # EFS: the reads must reach the server (NFS READ ops), not the client page cache
            checks["reads_reached_nfs_server"] = nfs_reads > 0
        elif read_any and disk is not None:
            checks["reads_reached_device"] = disk > 0
        if bp and (demand or 0) > 0 and proc is not None and nfs is None:
            # block devices account storage reads in /proc/<pid>/io read_bytes; NFS reads are not accounted there
            checks["jvm_read_bytes"] = proc > 0
        eok = efs_connections_ok(io, self.efs_target) if self.efs else None
        if eok is not None:
            checks["efs_connections_ok"] = eok
        ok = all(v for k, v in checks.items() if k != "no_io")
        if self.node.agent is None:
            checks["gap"] = "no agent: OS page cache not dropped or verified"
            ok = False
        elif disk is None and nfs is None and read_any:
            checks["gap"] = "no device or NFS counters for the data path: reads not verified"
            ok = False
        return ok, checks


# ---------------------------------------------------------------- kernel readahead and device read sizes
POC_READ_AHEAD_KB = 0  # bufferpool arms: no kernel readahead (common-rules.md, kernel readahead, REVISED ~20:00)


def readahead_mode(arm, poc_kb=POC_READ_AHEAD_KB):
    """Stock arms keep the as-mounted kernel readahead (mmap; EFS 15360 KiB from efs-utils / the AL2023 udev rule);
    bufferpool arms run with read_ahead_kb 0: the bufferpool announces each miss window with POSIX_FADV_WILLNEED and
    preads it, one device read per window. An arm may set "readahead": "default" | KiB."""
    if "read_ahead_kb" in arm:
        raise ValueError("arm key read_ahead_kb is replaced by \"readahead\": \"default\" | <KiB> (common-rules: "
                         "stock arms as mounted, POC arms 0)")
    mode = arm.get("readahead", "default" if not arm.get("bufferpool") else poc_kb)
    if mode != "default" and not str(mode).isdigit():
        raise ValueError(f"readahead {mode!r}: default or a value in KiB")
    return str(mode)


def set_readahead(node, arm, poc_kb=POC_READ_AHEAD_KB):
    """Sets the arm's readahead on every layer of its data path, reads it back; refuses to measure if it differs."""
    mode = readahead_mode(arm, poc_kb)
    q = f"arm={urllib.parse.quote(arm['node'])}&mode={mode}"
    res = node.agent.request("POST", f"/readahead/mode?{q}")
    if not res.get("ok"):
        raise RuntimeError(f"arm {arm['node']}: readahead is not {mode} after setting it: {json.dumps(res)[:800]}; not measuring")
    return res


def check_readahead(node, arm, poc_kb=POC_READ_AHEAD_KB):
    q = f"arm={urllib.parse.quote(arm['node'])}&mode={readahead_mode(arm, poc_kb)}"
    return node.agent.request("GET", f"/readahead?{q}")


def check_opened_readahead(node, arm, poc_kb=POC_READ_AHEAD_KB):
    """After the node start and the index open: the files were opened with the arm's readahead only if it still holds
    (a mount by the start command would have reset it); else refuse to measure."""
    res = check_readahead(node, arm, poc_kb)
    if not res.get("ok"):
        raise RuntimeError(f"arm {arm['node']}: readahead changed while the node started and opened its indices: "
                           f"{json.dumps(res)[:800]}; the index files may hold another value; not measuring")
    return res


def device_vs_bufferpool(io):
    """
    Device reads of one cold iteration against the bufferpool's storage reads (bufferpool arms): every device read must
    be one bufferpool window (a 32 or 128 KiB pread, clipped at end of file), so the device read count, bytes and size
    histogram (bucketed into the bufferpool's reads_by_size classes) equal the bufferpool's reads + prefetch_reads,
    bytes_read and reads_by_size. Source: the agent trace (NFS READ RPCs on EFS, block read requests on EBS), else
    mountstats (READ ops, server_read_bytes) or diskstats (reads, sectors). ok None = nothing to compare with.
    """
    bp = io.get("bp") or {}
    hist = {int(k): v for k, v in (bp.get("reads_by_size") or {}).items() if v}
    out = {"bufferpool_reads": bp.get("reads", 0) + bp.get("prefetch_reads", 0), "bufferpool_bytes_read": bp.get("bytes_read", 0),
           "bufferpool_reads_by_size": {str(k): v for k, v in sorted(hist.items())}}
    if "bytes_read" not in bp and "reads" not in bp:
        out["ok"] = None
        out["gap"] = "bufferpool stats without reads / bytes_read (binary before the IO-window change)"
        return out
    trace = io.get("nfs_read_sizes") or io.get("block_read_sizes")
    if trace:
        classes = sorted(hist)
        dev = {}
        for size, n in trace.get("bytes_hist", {}).items():
            c = next((k for k in classes if int(size) <= k), None)
            key = str(c) if c is not None else f"unmatched:{size}"
            dev[key] = dev.get(key, 0) + n
        out.update({"source": trace.get("source") or ("nfs:nfs_initiate_read" if "nfs_read_sizes" in io else "block"),
                    "device_reads": trace.get("reads"), "device_bytes": trace.get("total_bytes"),
                    "device_bytes_hist": trace.get("bytes_hist"), "device_reads_by_class": dict(sorted(dev.items()))})
        att = trace.get("attribution")
        if att is not None:
            out["rule"] = "attributed"
            out["ok"] = attributed_ok(out, att)
        else:
            out["rule"] = "strict" + (f" ({trace['attribution_gap']})" if trace.get("attribution_gap") else "")
            out["ok"] = (out["device_reads"] == out["bufferpool_reads"] and out["device_bytes"] == out["bufferpool_bytes_read"]
                         and dev == out["bufferpool_reads_by_size"])
    elif io.get("nfs"):
        r = io["nfs"].get("READ") or {}
        out.update({"source": "mountstats", "device_reads": r.get("ops", 0), "device_bytes": io["nfs"].get("server_read_bytes")})
        out["ok"] = out["device_reads"] == out["bufferpool_reads"] and out["device_bytes"] == out["bufferpool_bytes_read"]
    elif io.get("disk"):
        out.update({"source": "diskstats", "device_reads": io["disk"].get("reads", 0), "device_bytes": io["disk"].get("read_bytes")})
        out["ok"] = out["device_reads"] == out["bufferpool_reads"] and out["device_bytes"] == out["bufferpool_bytes_read"]
    else:
        out["ok"] = None
    return out


MAX_WINDOW = 128 * 1024  # the largest bufferpool window (bufferpool.io.sequential_read_size; checked per run)


def attributed_ok(out, att):
    """
    The device check with the agent's attribution of every device read to index-file data or other reads
    (agent/coldpath_readattr.py; common-rules.md "DECISION ~22:00", device check). Data reads: every one lies inside one
    window (no read crosses a 128 KiB-aligned file block or is larger than the largest window), no more data bytes than
    the bufferpool read (page cache hits make it less), no more 128 KiB file blocks touched than bufferpool reads, and no
    more data reads than bufferpool reads except at extent boundaries of the file (XFS fragmentation splits a window
    into two requests; a kernel split of a window into 4 KiB pages fails). Other reads (file-system metadata after
    drop_caches, journal) are reported by size and count, not checked.
    """
    reads = out["bufferpool_reads"]
    checks = {"no_read_crosses_window": att["cross_window"] == 0,
              "max_data_read_within_window": (att["data_max_bytes"] or 0) <= MAX_WINDOW,
              "data_bytes_le_bufferpool_bytes": att["data_bytes"] <= out["bufferpool_bytes_read"],
              "windows_le_bufferpool_reads": att["windows"] <= reads,
              "data_reads_le_bufferpool_reads_plus_extent_splits": att["data_reads"] <= reads + att["extent_splits"]}
    out.update({"data_reads": att["data_reads"], "data_bytes": att["data_bytes"], "data_hist": att.get("data_hist"),
                "windows": att["windows"], "extent_splits": att["extent_splits"], "cross_window": att["cross_window"],
                "other_reads": att["other_reads"], "other_bytes": att["other_bytes"], "other_hist": att.get("other_hist"),
                "page_cache_bytes": out["bufferpool_bytes_read"] - att["data_bytes"], "attributed_checks": checks})
    return all(checks.values())


def read_size_trace(a, arm, node, nfs):
    """'nfs' (EFS), 'block' (EBS) or None: the agent trace that records every device read of a cold iteration."""
    if node.agent is None or getattr(a, "no_read_size_trace", False):
        return None
    if nfs is None:
        nfs = arm.get("storage") == "EFS"
    return "nfs" if nfs else "block"


# ---------------------------------------------------------------- session
CAP_KEYS = ("cold_iters", "warm_warmup", "warm_iters")


def load_op_caps(path, ops, ref):
    """
    Pre-registered per-operation iteration caps (common-rules "Per-operation iteration caps"): {"ops": {name:
    {"cold_iters": c, "warm_warmup": w, "warm_iters": m}}, "preregistration": "<file>"}. The same caps apply to every
    arm of the session (they are session arguments, not arm keys), so no comparison mixes protocols. A capped op keeps
    at least 1 cold and 1 measured warm iteration (never dropped); the reference op cannot be capped (it brackets
    every block for drift).
    """
    if not path:
        return {}
    spec = json.load(open(path))
    caps = spec.get("ops") or {}
    names = {o["name"] for o in ops}
    for name, c in caps.items():
        if name not in names:
            raise ValueError(f"--op-caps: {name} is not an op of this session")
        if name == ref:
            raise ValueError(f"--op-caps: the reference op {name} cannot be capped")
        if set(c) != set(CAP_KEYS):
            raise ValueError(f"--op-caps {name}: needs exactly {list(CAP_KEYS)}, got {sorted(c)}")
        if c["cold_iters"] < 1 or c["warm_iters"] < 1 or c["warm_warmup"] < 0:
            raise ValueError(f"--op-caps {name}: at least 1 cold and 1 measured warm iteration ({c})")
    return caps


def iters(a, caps, op):
    """(cold_iters, warm_warmup, warm_iters) of one op: the session's, or the op's pre-registered cap."""
    c = caps.get(op["name"])
    return (c["cold_iters"], c["warm_warmup"], c["warm_iters"]) if c else (a.cold_iters, a.warm_warmup, a.warm_iters)


def op_order(ops, ref, seed):
    others = [o for o in ops if o["name"] != ref]
    random.Random(seed).shuffle(others)
    by = {o["name"]: o for o in ops}
    return [by[ref]] + others + [by[ref]]


def _seed(*parts):
    return int(hashlib.sha256("/".join(map(str, parts)).encode()).hexdigest()[:12], 16)


def schedule(labels, rounds, order, seed, offset=0):
    """Rounds offset .. offset+rounds-1: a long session split into several invocations (--round-offset) gets the same
    round orders and distinct run ids (label#r<round>) as one invocation of all rounds."""
    out = []
    for r in range(offset, offset + rounds):
        if order == "abba":
            seq = list(labels) if r % 2 == 0 else list(reversed(labels))
        else:
            seq = list(labels)
            random.Random(_seed(seed, "round", r)).shuffle(seq)
        out += [(r, lab) for lab in seq]
    return out


class Session:
    def __init__(self, a):
        self.a = a
        labels = (a.arm_list.split(",") if getattr(a, "arm_list", None) else
                  [a.arm] if getattr(a, "arm", None) else [])
        self.cfg = load_arms(a.arms, a.indices, labels)
        opsfile = json.load(open(a.ops))
        self.ref = a.reference_op or opsfile["reference_op"]
        ops = opsfile["ops"]
        if a.op_filter:
            keep = set(a.op_filter.split(","))
            ops = [o for o in ops if o["name"] in keep or o["name"] == self.ref]
        if a.families:
            fam = set(a.families.split(","))
            ops = [o for o in ops if fam & set(o["families"]) or o["name"] == self.ref]
        if self.ref not in {o["name"] for o in ops}:
            sys.exit(f"reference op {self.ref} is not in {a.ops}")
        self.ops = ops
        self.op_caps = load_op_caps(getattr(a, "op_caps", None), ops, self.ref)
        if self.op_caps and getattr(a, "executor", "replay") != "replay":
            sys.exit("--op-caps is implemented for the replay executor only")
        token = open(a.token_file).read().strip() if a.token_file else ""
        if getattr(a, "request_timeout", REQUEST_TIMEOUT_DEFAULT_S) <= 0:
            sys.exit("--request-timeout must be > 0 seconds")
        self.node = Node(a.url, a.agent, token, a.residency_tolerance,
                         timeout=float(getattr(a, "request_timeout", REQUEST_TIMEOUT_DEFAULT_S)))
        os.makedirs(a.out, exist_ok=True)
        self.w = JsonlWriter(os.path.join(a.out, "samples.jsonl"))
        self.log_f = open(os.path.join(a.out, "session.log"), "a")
        self.device_mismatches = {}  # run_id -> cold iterations whose device reads were not bufferpool windows
        self.current_arm = None
        runguards.other_policy(self.cfg)  # a bad other_indices value stops the session before any run
        stock_store_types(self.cfg)  # one stock store type per index, else stop before any run
        # a POC-only (split BKD) index must not share a data path with a stock arm's node, else stop before any run
        self.isolation = (runguards.check_format_isolation(self.cfg, self.node.agent.request("GET", "/health").get("storages"))
                          if self.node.agent else None)
        self.other_indices = None
        # C memory allocator of every node of the session (arms file "allocator", --allocator), checked at every node
        # start; every run of a session must also match the first run's allocator exactly (allocator_signature)
        self.want_allocator = runguards.want_allocator(self.cfg, a)
        if self.want_allocator and not self.node.agent:
            sys.exit("the allocator check (arms file \"allocator\" or --allocator) reads the node's /proc through "
                     "--agent; pass --agent or --allocator any")
        self.allocator_signature = None
        self.bp_signatures = {}  # storage -> bufferpool settings of the first run (same_bufferpool_settings)

    def check_allocator(self, arm_name):
        """At node start: the JVM's allocator from the agent (GET /node/memory), against the session's requirement
        and against the first run of the session. Returns the record for the run; raises otherwise."""
        if not self.node.agent:
            return None
        mem = self.node.agent.request("GET", "/node/memory")
        rec = runguards.check_allocator(arm_name, mem, self.want_allocator)
        sig = runguards.allocator_signature(rec["node"])
        rec["signature"] = sig
        if self.allocator_signature is None:
            self.allocator_signature = sig
        elif sig != self.allocator_signature and self.want_allocator is not None:
            raise RuntimeError(f"arm {arm_name}: node allocator {sig} differs from the session's first run "
                               f"{self.allocator_signature}; every node unit needs the same drop-in; not measuring")
        a = rec["node"] or {}
        self.log(f"  allocator {a.get('name')} LD_PRELOAD={a.get('ld_preload')} MALLOC_CONF={a.get('malloc_conf')} "
                 f"MALLOC_ARENA_MAX={a.get('malloc_arena_max')}" + ("" if self.want_allocator else " (record only)"))
        return rec

    def check_same_bufferpool(self, arm_name, arm, bp_stats):
        """same_bufferpool_settings: every bufferpool configuration on one storage runs the same plugin settings."""
        if not (self.cfg.get("same_bufferpool_settings") and arm.get("bufferpool")):
            return None
        sig = runguards.bufferpool_signature(bp_stats)
        storage = arm.get("storage") or arm["node"]
        first = self.bp_signatures.setdefault(storage, {"arm": arm_name, "settings": sig})
        diff = {k: (first["settings"].get(k), v) for k, v in sig.items() if first["settings"].get(k) != v}
        if diff:
            raise RuntimeError(f"arm {arm_name}: bufferpool settings differ from {first['arm']} on {storage}: {diff} "
                               "(same_bufferpool_settings: the bufferpool is not a variable); not measuring")
        return {"storage": storage, "settings": sig, "same_as": first["arm"]}

    def log(self, msg):
        line = f"{time.strftime('%H:%M:%S')} {msg}"
        print(line, flush=True)
        self.log_f.write(line + "\n")
        self.log_f.flush()

    def record(self, **kw):
        self.w.write({"schema": SCHEMA, "t": time.time(), **kw})

    def normalize_stock_paths(self, labels):
        """
        Pre-start check (common-rules "Store type left on the other data path after an aborted session"). A session
        killed while a POC arm ran leaves the stock indices of THAT data path in store type bufferpoolfs; close_indices
        resets them only on the node that is running, so the other storage's path can stay dirty, and the next stock
        start there is red ("Unknown store type", shards past their allocation retries). Before the first run, for
        every data path that a scheduled stock arm uses, start a POC arm of the arms file on the same path, read
        index.store.type of the stock indices, reset any foreign value to the stock arms' store type, retry failed
        allocations, wait for green, and close. Returns one record per path.
        """
        n = self.node
        if not (n.agent and self.a.restart):
            return []
        paths = {k: (v or {}).get("data_path") for k, v in (n.agent.request("GET", "/health").get("storages") or {}).items()}
        stock_nodes = sorted({self.cfg["arms"][parse_label(lab)[0]]["node"] for lab in labels
                              if not self.cfg["arms"][parse_label(lab)[0]].get("bufferpool")
                              and not indices_ext.not_applicable(self.cfg["arms"][parse_label(lab)[0]])})
        poc_nodes = sorted({a["node"] for a in self.cfg["arms"].values()
                            if a.get("bufferpool") and not indices_ext.not_applicable(a) and "node" in a})
        stock = stock_store_types(self.cfg)
        out = []
        for sn in stock_nodes:
            path = paths.get(sn)
            poc = next((pn for pn in poc_nodes if path and paths.get(pn) == path), None)
            rec = {"stock_node": sn, "data_path": path, "poc_node": poc, "reset": []}
            if poc is None:
                rec["skipped"] = "no bufferpool arm of the arms file shares this data path"
                self.log(f"  store types on {path}: not checked ({rec['skipped']})")
                out.append(rec)
                continue
            if n.up():
                # only close here: the running node may be a stock node, which cannot reset a store type; the POC
                # arm started next resets them
                close_indices(n, self.cfg, self.log, reset=False)
            n.agent.request("POST", "/node/restart", {"arm": poc})
            wait_until(n.up, 900, 1.0, "OpenSearch HTTP")
            for name, want in sorted(stock.items()):
                try:
                    st = index_state(n, name)
                except RuntimeError:
                    continue
                if st["store_type"] is not None and st["store_type"] != want:
                    rec["reset"].append({"index": name, "from": st["store_type"], "to": want})
            close_indices(n, self.cfg, self.log)  # closes what is open and resets every foreign store type
            status, _ = n.os.raw("POST", "/_cluster/reroute?retry_failed=true")
            rec["reroute_retry_failed"] = status
            n.wait_green()
            self.log(f"  store types on {path} (checked by {poc}): "
                     + (", ".join(f"{r['index']} {r['from']} -> {r['to']}" for r in rec["reset"]) or "all stock"))
            out.append(rec)
        return out

    def start_arm(self, arm_name, arm):
        """
        Call after set_readahead: every index file is opened here, after the arm's readahead is set (Linux copies the
        bdi readahead into each file at open). The configured indices are closed first, also without --restart, so
        no file of the arm's indices stays open from before.
        """
        n = self.node
        if self.a.restart and not n.agent:
            raise RuntimeError("--restart needs --agent")
        if n.up():
            close_indices(n, self.cfg, self.log)
        for attempt in range(NODE_START_RETRIES + 1):
            try:
                if self.a.restart:
                    res = n.agent.request("POST", "/node/restart", {"arm": arm["node"]})
                    self.log(f"  node restarted as {arm['node']} pid {res['pid']}")
                n.wait_green()
                break
            except Exception as e:  # noqa: BLE001 - a start that fails before any measured iteration is retried
                rec = {"node": arm["node"], "attempt": attempt + 1, "of": NODE_START_RETRIES,
                       "exception": f"{type(e).__name__}: {e}"[:2000], "jvm_gone": n.jvm_gone(),
                       "efs_connections": self.efs_connection_state(arm), "nfs_xprt": self.nfs_transport(arm),
                       "node_status": self.agent_get("/node/status")}
                if attempt == NODE_START_RETRIES or not self.a.restart:
                    self.record(type="node_start_failed", **getattr(self, "current_run", {}), **rec)
                    raise NodeStartFailed(f"node {arm['node']} did not start after {NODE_START_RETRIES} retries: "
                                          f"{rec['exception']}") from e
                self.record(type="node_start_retry", **getattr(self, "current_run", {}), **rec)
                self.log(f"  node start of {arm['node']} failed ({rec['exception'][:200]}); retry {attempt + 1} of "
                         f"{NODE_START_RETRIES} in {NODE_START_RETRY_WAIT_S:.0f} s")
                time.sleep(NODE_START_RETRY_WAIT_S)
        self.other_indices = runguards.enforce_other_indices(n.os, self.cfg, self.log)
        open_indices(n, self.cfg, arm, self.log)
        return verify_indices(n, self.cfg, arm, arm["node"])

    def agent_get(self, path):
        if not self.node.agent:
            return None
        try:
            return self.node.agent.request("GET", path)
        except Exception as e:  # noqa: BLE001 - recorded, never fatal
            return {"error": f"{type(e).__name__}: {e}"[:300]}

    def nfs_transport(self, arm):
        """NFS client transport counters of the arm's mount (mountstats xprt line via agent /host), None on EBS:
        connect_count counts the client's (re)connects to efs-proxy (common-rules: recorded per run)."""
        host = self.agent_get(f"/host?arm={urllib.parse.quote(arm['node'])}") or {}
        return parse_xprt(host.get("nfs_xprt"))

    def efs_connection_state(self, arm):
        """The EFS backend connection count and efs-proxy pid of the arm's data path (agent v2), None on EBS."""
        st = (self.agent_get("/health") or {}).get("storages", {}).get(arm["node"]) or {}
        if not st.get("nfs"):
            return None
        return self.agent_get(f"/efs/connections?arm={urllib.parse.quote(arm['node'])}")

    def cold_sample(self, it, run, op, index, i, mode="cold"):
        pre_state = it.clear()
        # device read sizes of every cold iteration, every arm: NFS READ RPCs on EFS, block read requests on EBS
        trace = read_size_trace(self.a, it.arm, self.node, getattr(self, "storage_nfs", None))
        q = "arm=" + urllib.parse.quote(it.arm["node"])
        if trace:
            self.node.agent.request("POST", f"/trace/{trace}/_start?{q}")
        pre = it.snapshot()
        res = execute(self.node.os, index, op)
        idle_ms, _ = self.node.wait_prefetch_idle()
        post = it.snapshot()
        io = io_delta(pre, post)
        if trace:
            io["nfs_read_sizes" if trace == "nfs" else "block_read_sizes"] = self.node.agent.request(
                "POST", f"/trace/{trace}/_stop?{q}&uuids=" + ",".join(it.uuids))
        ok, checks = it.verify(pre_state, io)
        # IO-size configuration check (separate from cold_ok) of the bufferpool arms: no device read larger than the
        # largest configured IO size. Stock arms keep the kernel's default readahead, so their sizes are only recorded.
        mx = getattr(self.a, "max_read_bytes", None)
        if mx and it.arm.get("bufferpool"):
            sizes = io.get("nfs_read_sizes") or io.get("block_read_sizes")
            avg = (io.get("nfs") or {}).get("read_avg_bytes")
            if sizes and sizes.get("max_bytes") is not None:
                checks["io_size_ok"] = sizes["max_bytes"] <= mx
            elif avg is not None:
                checks["io_size_ok"] = avg <= mx + 512  # mountstats average includes the RPC reply header
        if it.arm.get("bufferpool"):
            # every device read is one bufferpool window (common-rules.md, REVISED ~20:00): else the run is invalid
            dv = device_vs_bufferpool(io)
            io["device_vs_bufferpool"] = dv
            if dv["ok"] is not None:
                checks["device_reads_are_windows"] = dv["ok"]
                if not dv["ok"]:
                    ok = False
                    self.device_mismatches[run["run_id"]] = self.device_mismatches.get(run["run_id"], 0) + 1
        rec = {"type": "sample", "mode": mode, **run, "op": op["name"], "iter": i, "took_ms": res["took_ms"],
               "wall_ms": res["wall_ms"], "requests": res["requests"], "digest": res["canonical"]["digest"],
               "cold_ok": ok, "checks": checks, "clear": pre_state, "io": io, "post_idle_ms": idle_ms}
        if res.get("flags"):
            rec["flags"] = res["flags"]
        self.record(**rec)
        if not ok and self.a.strict:
            raise RuntimeError(f"cold verification failed: {op['name']} iter {i}: {checks} {pre_state}")
        return rec

    def warm_sample(self, it, run, op, index, i, phase):
        pre = it.pre_snapshot() if phase == "measure" else None
        res = execute(self.node.os, index, op)
        if phase != "measure":
            return None
        idle_ms, _ = self.node.wait_prefetch_idle()
        post = it.snapshot()
        io = io_delta(pre, post)
        rec = {"type": "sample", "mode": "warm", **run, "op": op["name"], "iter": i, "took_ms": res["took_ms"],
               "wall_ms": res["wall_ms"], "requests": res["requests"], "digest": res["canonical"]["digest"],
               "io": io, "post_idle_ms": idle_ms}
        rec.update(warm_efs_fields(it, io))
        if res.get("flags"):
            rec["flags"] = res["flags"]
        self.record(**rec)
        return rec

    def concurrent_cold(self, it, run, index, ops):
        a = self.a
        rng = random.Random(_seed(a.seed, run["run_id"].split(".a")[0], "ccold"))
        for b in range(a.concurrent_batches):
            batch = [rng.choice(ops) for _ in range(a.clients)]
            pre_state = it.clear()
            pre = it.snapshot()
            results = [None] * len(batch)
            barrier = threading.Barrier(len(batch))

            def worker(k):
                c = JsonClient(self.node.url)
                barrier.wait()
                try:
                    results[k] = execute(c, index, batch[k])
                except Exception as e:  # noqa: BLE001
                    results[k] = {"error": str(e)}
                finally:
                    c.close()
            th = [threading.Thread(target=worker, args=(k,)) for k in range(len(batch))]
            for t in th:
                t.start()
            for t in th:
                t.join()
            self.node.wait_prefetch_idle()
            post = it.snapshot()
            pool = self.node.prefetch_pool()
            for k, r in enumerate(results):
                self.record(type="sample", mode="ccold", **run, op=batch[k]["name"], iter=b, client=k,
                            took_ms=r.get("took_ms"), wall_ms=r.get("wall_ms"), error=r.get("error"),
                            digest=(r.get("canonical") or {}).get("digest"))
            io = io_delta(pre, post)
            ok, checks = it.verify(pre_state, io)
            self.record(type="batch", mode="ccold", **run, batch=b, clients=a.clients, clear=pre_state, io=io,
                        cold_ok=ok, checks=checks, prefetch_pool=pool)

    def concurrent_warm(self, run, index, ops):
        a = self.a
        stop_at = time.monotonic() + a.concurrent_seconds
        lock = threading.Lock()

        def worker(k):
            c = JsonClient(self.node.url)
            rng = random.Random(_seed(a.seed, run["run_id"].split(".a")[0], "cwarm", k))
            i = 0
            try:
                while time.monotonic() < stop_at:
                    op = rng.choice(ops)
                    try:
                        r = execute(c, index, op)
                        rec = {"took_ms": r["took_ms"], "wall_ms": r["wall_ms"], "digest": r["canonical"]["digest"]}
                    except Exception as e:  # noqa: BLE001
                        rec = {"error": str(e)}
                    with lock:
                        self.record(type="sample", mode="cwarm", **run, op=op["name"], iter=i, client=k, **rec)
                    i += 1
            finally:
                c.close()
        th = [threading.Thread(target=worker, args=(k,)) for k in range(a.clients)]
        for t in th:
            t.start()
        for t in th:
            t.join()
        self.record(type="batch", mode="cwarm", **run, clients=a.clients, prefetch_pool=self.node.prefetch_pool(),
                    jvm=self.node.os.request("GET", "/_nodes/_local/stats/jvm?filter_path=nodes.*.jvm.mem"))

    def run_one(self, rnd, label, attempt=0):
        a = self.a
        arm_name, label = parse_label(label)
        arm = self.cfg["arms"][arm_name]
        # a re-queued run gets its own run id (the discarded attempt's samples never merge with it) and the same seeds
        # (op orders) as the first attempt
        seed_id = f"{label}#r{rnd}"
        run_id = seed_id + (f".a{attempt}" if attempt else "")
        run = {"arm": arm_name, "label": label, "round": rnd, "run_id": run_id}
        if attempt:
            run["attempt"] = attempt
        self.current_run = run
        self.current_run_t0 = time.time()
        if indices_ext.not_applicable(arm):
            self.log(f"run {run_id}: NOT APPLICABLE: {arm['not_applicable']}")
            self.record(type="run", **run, available=False, reason=f"not applicable: {arm['not_applicable']}")
            return
        self.log(f"run {run_id} (node {arm['node']})")
        readahead, readahead_open, self.storage_nfs = None, None, None
        if self.node.agent:
            # before the node restart and the index open: each file keeps the readahead it was opened with
            readahead = set_readahead(self.node, arm, a.poc_read_ahead_kb)
            self.storage_nfs = readahead["nfs"]
            self.log(f"  readahead {readahead['mode']}: " + ", ".join(f"{x['key']}={x['read_ahead_kb']}" for x in readahead["layers"]))
        indices = self.start_arm(arm_name, arm)
        self.run_allocator = self.check_allocator(arm_name)
        self.mem_monitor = None
        if self.node.agent and getattr(a, "mem_limit_pct", 70.0) > 0:
            self.mem_monitor = MemoryMonitor(self.node, a.mem_interval, a.mem_limit_pct).start()
        try:
            self._measure_started_run(a, arm_name, arm, run, run_id, seed_id, indices, readahead)
        finally:
            if self.mem_monitor is not None:
                self.record(type="node_memory", **run, **self.mem_monitor.stop())
                self.mem_monitor = None

    def mem_check(self):
        """Operation boundary: ends the run (MemoryLimitExceeded) once the node passed the memory limit."""
        mm = getattr(self, "mem_monitor", None)
        if mm is not None:
            mm.check()

    def _measure_started_run(self, a, arm_name, arm, run, run_id, seed_id, indices, readahead):
        readahead_open = None
        if self.node.agent:
            readahead_open = check_opened_readahead(self.node, arm, a.poc_read_ahead_kb)
        try:
            settings = apply_settings(self.node, self.cfg, arm)
            switches, state = apply_switches(self.node, self.cfg, arm_name, arm)
        except ArmUnavailable as e:
            self.log(f"  NOT AVAILABLE: {e}")
            self.record(type="run", **run, available=False, reason=str(e), indices=indices)
            return
        io_config = runguards.check_io_config(arm_name, state["bp"], runguards.want_io(a)) if arm.get("bufferpool") else None
        build = runguards.check_build(arm_name, arm, self.cfg, self.node.os.request("GET", "/"), state.get("bp"))
        same_bp = self.check_same_bufferpool(arm_name, arm, state.get("bp"))
        cache_cleanup = None
        if any(m in a.modes.split(",") for m in ("cold", "ccold")):
            cache_cleanup = runguards.check_cache_cleanup(arm_name, node_settings(self.node))
        index = self.cfg["indices"][arm["index"]]["name"]
        uuids = indices_ext.uuids(indices[arm["index"]])
        self.read_trace_kind = read_size_trace(a, arm, self.node, self.storage_nfs)
        it = Iteration(self.node, arm, uuids, a.residency_every, efs=bool(self.storage_nfs), efs_target=a.efs_connections)
        efs_start = None
        if it.efs and it.efs_target:
            # before the JIT warm-up and the measured blocks: the mount at the target count (verified per sample)
            efs_start = it.ensure_efs() or {"ok": True, "count": it.efs_target, "skipped": True}
            it.take_efs_precondition()
            self.log(f"  EFS backend connections: {efs_start['count']} (target {it.efs_target})")
            if not efs_start["ok"]:
                raise RuntimeError(f"run {run_id}: EFS mount not at {it.efs_target} backend connections after "
                                   f"pre-conditioning: {efs_start}; not measuring")
        self.record(type="run", **run, available=True, indices=indices, query_index=index, settings=settings, readahead=readahead,
                    cold_protocol="jit-warm" if a.jit_warmup else "jit-cold",
                    readahead_after_open=readahead_open, io_config=io_config, other_indices=self.other_indices,
                    build=build, allocator=getattr(self, "run_allocator", None), same_bufferpool=same_bp,
                    context_arm=bool(arm.get("context")),
                    cache_cleanup=cache_cleanup, nfs_xprt=self.nfs_transport(arm) if it.efs else None,
                    efs_connections_target=a.efs_connections if it.efs else None, efs_precondition_start=efs_start,
                    switches=switches, state=state, metadata=run_metadata(self.node, self.cfg, arm_name, arm),
                    args=vars(a), op_caps=self.op_caps)
        modes = a.modes.split(",")
        if a.jit_warmup and any(m in modes for m in ("cold", "ccold")):
            jit_warmup(self, run, index, self.ops, _seed(a.seed, seed_id, "jit"))
        if a.executor == "osb":
            import osb_cold
            osb_ops = [o for o in self.ops if o.get("type", "search") in osb_cold.OSB_TYPE]
            if "cold" in modes:
                osb_cold.run_block(self, it, run, index, "cold", op_order(osb_ops, self.ref, _seed(a.seed, seed_id, "cold")),
                                   a.cold_iters)
            if "warm" in modes:
                osb_cold.run_block(self, it, run, index, "warm", op_order(osb_ops, self.ref, _seed(a.seed, seed_id, "warm")),
                                   a.warm_iters, a.warm_warmup)
            modes = [m for m in modes if m not in ("cold", "warm")]
        if "cold" in modes:
            for pos, op in enumerate(op_order(self.ops, self.ref, _seed(a.seed, seed_id, "cold"))):
                for i in range(iters(a, self.op_caps, op)[0]):
                    self.mem_check()
                    r = self.cold_sample(it, {**run, "pos": pos}, op, index, i)
                    if i == 0:
                        self.log(f"  cold {op['name']:<56} took {r['took_ms']:8.1f} ms ok={r['cold_ok']}")
        if "warm" in modes:
            for pos, op in enumerate(op_order(self.ops, self.ref, _seed(a.seed, seed_id, "warm"))):
                _, n_warmup, n_measure = iters(a, self.op_caps, op)
                for i in range(n_warmup):
                    self.mem_check()
                    self.warm_sample(it, run, op, index, i, "warmup")
                for i in range(n_measure):
                    self.mem_check()
                    self.warm_sample(it, {**run, "pos": pos}, op, index, i, "measure")
        if "ccold" in modes:
            self.concurrent_cold(it, run, index, self.ops)
        if "cwarm" in modes:
            for op in self.ops:
                for i in range(a.warm_warmup):
                    execute(self.node.os, index, op)
            self.concurrent_warm(run, index, self.ops)
        if not a.no_results:
            for op in self.ops:
                res = execute(self.node.os, index, op)
                self.record(type="result", **run, op=op["name"], canonical=res["canonical"])
        readahead_end = check_readahead(self.node, arm, a.poc_read_ahead_kb) if self.node.agent else None
        mism = self.device_mismatches.get(run_id, 0)
        if mism:
            self.log(f"  INVALID run {run_id}: {mism} cold iterations with device reads that are not bufferpool windows")
        self.record(type="run_end", **run, prefetch_pool=self.node.prefetch_pool(), readahead=readahead_end,
                    nfs_xprt=self.nfs_transport(arm) if self.storage_nfs else None,
                    device_read_mismatches=mism, valid=not mism)
        self.record_storage_incidents(run)
        if readahead_end is not None and not readahead_end["ok"]:
            raise RuntimeError(f"run {run_id}: kernel readahead changed during the run (a remount?): {readahead_end}; "
                               "this run is invalid")

    def record_storage_incidents(self, run):
        """
        Kernel NFS stall windows ("nfs: server X not responding" until "... OK") during the run, from the agent
        (GET /storage/incidents, agent v3). A hard NFS mount blocks every reader while it stalls, so samples inside a
        window measure the stall: analyze.py excludes them from every verdict and reports them as sensitivity. An agent
        without the endpoint is recorded as unavailable (the run's samples then carry no incident check).
        """
        if not self.node.agent:
            return None
        t0 = getattr(self, "current_run_t0", None) or time.time()
        inc = self.agent_get(f"/storage/incidents?since={t0 - 5:.3f}&until={time.time() + 5:.3f}")
        if inc is None:
            return None
        if inc.get("error") or not inc.get("available"):
            self.record(type="storage_incident_check", **run, available=False, detail=inc)
            return inc
        self.record(type="storage_incident_check", **run, available=True, windows=len(inc.get("windows") or []))
        if inc.get("windows"):
            self.record(type="storage_incident", **run, windows=inc["windows"], lines=inc.get("lines", [])[:50])
            self.log(f"  STORAGE INCIDENT during {run.get('run_id')}: {len(inc['windows'])} NFS stall window(s) "
                     f"{[(w['server'], round(w['end'] - w['start'])) for w in inc['windows']]}; samples inside are excluded")
        return inc

    def measure_run(self, rnd, lab, attempt):
        """run_one; a node failure during the run (the JVM is gone) or a request that outlived --request-timeout
        becomes RunDiscarded (re-queued), anything else propagates."""
        try:
            self.run_one(rnd, lab, attempt)
        except (NodeStartFailed, ArmUnavailable):
            raise
        except MemoryLimitExceeded as e:
            run = getattr(self, "current_run", {})
            self.record(type="run_discarded", **run, reason=f"memory limit: {e}"[:2000], memory_limit=True,
                        node_status=self.agent_get("/node/status"))
            raise RunDiscarded(f"run {run.get('run_id')}: {e}") from e
        except RequestTimeout as e:
            # never a latency sample: cancel what still runs on the node, discard the run, re-queue it
            run = getattr(self, "current_run", {})
            try:
                self.node.os.request("POST", "/_tasks/_cancel?actions=*search*", timeout=120)
            except Exception as e2:  # noqa: BLE001 - the discard stands either way
                self.log(f"  task cancel after the timeout failed: {e2}")
            inc = self.record_storage_incidents(run)
            self.record(type="run_discarded", **run, reason=f"request timeout: {e}"[:2000], request_timeout_s=e.timeout_s,
                        storage_incident=bool(inc and inc.get("windows")),
                        efs_connections=self.efs_connection_state(self.cfg["arms"][run.get("arm", parse_label(lab)[0])]),
                        node_status=self.agent_get("/node/status"))
            raise RunDiscarded(f"run {run.get('run_id')}: {e}") from e
        except Exception as e:  # noqa: BLE001 - classified below: only a node that died discards the run
            if not self.node.jvm_gone():
                raise
            run = getattr(self, "current_run", {})
            self.record_storage_incidents(run)
            self.record(type="run_discarded", **run, reason=f"node failed during the run: {type(e).__name__}: {e}"[:2000],
                        efs_connections=self.efs_connection_state(self.cfg["arms"][run.get("arm", parse_label(lab)[0])]),
                        node_status=self.agent_get("/node/status"))
            raise RunDiscarded(f"run {run.get('run_id')}: node failed during the run ({type(e).__name__}: {e})") from e

    def run_schedule(self, sched):
        """The schedule in order; a run whose node did not start (NodeStartFailed) or failed while measuring
        (RunDiscarded) is re-queued at the end of the session, RUN_REQUEUES times at most, under a new run id."""
        queue = [(rnd, lab, 0) for rnd, lab in sched]
        while queue:
            rnd, lab, attempt = queue.pop(0)
            try:
                self.measure_run(rnd, lab, attempt)
            except (NodeStartFailed, RunDiscarded) as e:
                run = {"label": parse_label(lab)[1], "round": rnd, "run_id": f"{parse_label(lab)[1]}#r{rnd}"}
                if attempt < RUN_REQUEUES:
                    self.record(type="run_requeued", **run, attempt=attempt, reason=str(e)[:2000])
                    self.log(f"  {e}; re-queued at the end of the session")
                    queue.append((rnd, lab, attempt + 1))
                else:
                    self.record(type="run_failed", **run, attempt=attempt, reason=str(e)[:2000])
                    self.log(f"  {e}; re-queued {RUN_REQUEUES} time(s) already: run FAILED, the session continues")
                if self.cfg["arms"][parse_label(lab)[0]].get("bufferpool") and getattr(self.a, "normalize_store_types", True):
                    # a POC node that died leaves its path's stock indices in bufferpoolfs: reset before a stock start
                    self.record(type="store_type_normalize", after=run["run_id"],
                                paths=self.normalize_stock_paths(self.session_labels))

    def run(self):
        labels = self.a.arm_list.split(",")
        self.session_labels = labels
        for lab in labels:
            if parse_label(lab)[0] not in self.cfg["arms"]:
                sys.exit(f"unknown arm {lab}; arms file has {sorted(self.cfg['arms'])}")
        sched = schedule(labels, self.a.rounds, self.a.order, self.a.seed, getattr(self.a, "round_offset", 0))
        self.record(type="session", ops=[o["name"] for o in self.ops], reference_op=self.ref, schedule=sched,
                    args=vars(self.a), ops_file=os.path.abspath(self.a.ops), arms_file=self.cfg,
                    format_isolation=self.isolation, op_caps=self.op_caps,
                    op_caps_file=os.path.abspath(self.a.op_caps) if getattr(self.a, "op_caps", None) else None)
        self.log(f"session: {len(self.ops)} ops, schedule {[l for _, l in sched]}")
        if getattr(self.a, "normalize_store_types", True):
            self.record(type="store_type_normalize", paths=self.normalize_stock_paths(labels))
        try:
            self.run_schedule(sched)
        except BaseException as e:
            # exit trap (KeyboardInterrupt, SIGTERM via cmd_run's handler, any error): the running node may be a POC
            # arm with the stock indices of its data path in store type bufferpoolfs; reset them while it runs, so the
            # next stock start on that path is not red. SIGKILL cannot be trapped: normalize_stock_paths covers it.
            self.log(f"session aborted ({type(e).__name__}: {e}); resetting store types on the running node")
            try:
                if self.node.up():
                    close_indices(self.node, self.cfg, self.log)
            except Exception as e2:  # noqa: BLE001 - keep the original error
                self.log(f"  store-type reset on abort failed: {e2}")
            raise
        if self.node.up():
            # leave the node with every configured index closed and in the stock store type, so a later stock start
            # (another session, another indices file, a manual start) is not red next to a closed bufferpoolfs index
            close_indices(self.node, self.cfg, self.log)
        invalid = sorted(self.device_mismatches)
        self.record(type="session_end", invalid_runs=invalid, device_read_mismatches=self.device_mismatches)
        self.log("session done" + (f"; INVALID runs (device reads not bufferpool windows): {invalid}" if invalid else ""))


# ---------------------------------------------------------------- commands
def add_common(p):
    p.add_argument("--arms", required=True, help="arms JSON (see arms.example.json)")
    p.add_argument("--indices", help="JSON with an [indices] section that replaces the arms file's (e.g. "
                                     "indices.single-shard.example.json)")
    p.add_argument("--ops", required=True, help="op set from queries.py build")
    p.add_argument("--url", default="http://localhost:9200")
    p.add_argument("--agent", help="data-node agent URL, e.g. http://10.0.1.5:9700")
    p.add_argument("--token-file")
    p.add_argument("--residency-tolerance", type=int, default=0, help="bytes of index files allowed resident after a clear "
                   "(only 0 is accepted: a cold iteration needs every index page evicted)")
    p.add_argument("--request-timeout", type=float, default=REQUEST_TIMEOUT_DEFAULT_S,
                   help="client socket timeout in seconds for every OpenSearch request (default 7200); the same "
                        "value applies to every configuration of the session")
    p.add_argument("--mem-limit-pct", type=float, default=70.0,
                   help="discard and re-queue a run at the next operation boundary once the node's VmRSS or RssAnon "
                        "passes this %% of host memory (agent GET /node/memory, polled every --mem-interval s); 0 = off "
                        "(common-rules BLOCKER: native memory growth)")
    p.add_argument("--mem-interval", type=float, default=10.0, help="node memory poll interval in seconds")
    p.add_argument("--allocator", choices=["jemalloc", "glibc", "any"],
                   help="C memory allocator every node must run with, checked at each node start from /proc/<pid>/maps "
                        "and environ (agent v4 GET /node/memory); default: the arms file's \"allocator\" block, else "
                        "record only; any = record only")
    p.add_argument("--residency-every", type=int, default=1, help="mincore check every Nth cold iteration (0 = never)")
    p.add_argument("--efs-connections", type=int, default=EFS_CONNECTIONS_DEFAULT,
                   help="EFS arms: efs-proxy backend TCP connections every sample must have at its start and end (at "
                        "least N for N > 1, exactly 1 for 1); the agent pre-conditions the mount (O_DIRECT reads of a "
                        "scratch file) when it has fewer. 5 = the scaled-up state of a busy node (default); 1 = "
                        "sensitivity, needs a fresh mount pinned to one "
                        "connection (storage/efs_conn_ctl.sh pin-on); 0 = record the count only (no EFS verdict)")
    p.add_argument("--bp-block-size", type=int, default=runguards.IO_DEFAULTS["block_size"],
                   help="bufferpool arms: required cache block size in bytes (from /_bufferpool/stats; else refused)")
    p.add_argument("--bp-random-read-size", type=int, default=runguards.IO_DEFAULTS["random_read_size"],
                   help="bufferpool arms: required random read size in bytes")
    p.add_argument("--bp-sequential-read-size", type=int, default=runguards.IO_DEFAULTS["sequential_read_size"],
                   help="bufferpool arms: required sequential read size in bytes")
    p.add_argument("--reference-op")
    p.add_argument("--op-filter", help="comma list of op names (reference op always kept)")
    p.add_argument("--families", help="comma list of family tags")


def cmd_run(a):
    if a.read_ahead_kb is not None:
        sys.exit("--read-ahead-kb is no longer used: the harness sets kernel readahead per arm (stock arms keep the "
                 "as-mounted default, bufferpool arms run with 0) and verifies it before and after every run "
                 "(common-rules.md, kernel readahead)")
    import signal

    def _term(signum, frame):  # noqa: ARG001 - a kill (pkill, systemd stop) runs the session's exit trap
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, _term)
    Session(a).run()


def cmd_probe(a):
    """
    One verified cold iteration of one op on the node as it runs now (no restart), printed in full. The arm's readahead
    is set first, then the configured indices are closed and the arm's opened again, so the probe reads files that were
    opened with the arm's readahead (as a run does).
    """
    a.out = a.out or "/tmp/coldbench-probe"
    a.restart = False
    s = Session(a)
    arm = s.cfg["arms"][a.arm]
    if s.node.agent:
        s.storage_nfs = set_readahead(s.node, arm, a.poc_read_ahead_kb)["nfs"]
    indices = s.start_arm(a.arm, arm)
    if s.node.agent:
        check_opened_readahead(s.node, arm, a.poc_read_ahead_kb)
    print(json.dumps({"allocator": s.check_allocator(a.arm)}, default=str))
    _, state = apply_switches(s.node, s.cfg, a.arm, arm)
    print(json.dumps({"build": runguards.check_build(a.arm, arm, s.cfg, s.node.os.request("GET", "/"), state.get("bp"))}))
    if arm.get("bufferpool"):
        print(json.dumps({"io_config": runguards.check_io_config(a.arm, state["bp"], runguards.want_io(a))}))
    op = next(o for o in s.ops if o["name"] == a.op)
    # data-cold on a JIT-warm JVM, as in a run: the probed op once, unmeasured, before its cold iteration
    execute(s.node.os, s.cfg["indices"][arm["index"]]["name"], op)
    it = Iteration(s.node, arm, indices_ext.uuids(indices[arm["index"]]), 1, efs=bool(getattr(s, "storage_nfs", None)),
                   efs_target=a.efs_connections)
    run = {"arm": a.arm, "label": a.arm, "round": -1, "run_id": "probe"}
    r = s.cold_sample(it, run, op, s.cfg["indices"][arm["index"]]["name"], 0, mode="probe")
    print(json.dumps(r, indent=1, default=str))


def cmd_plan(a):
    ops = json.load(open(a.ops))["ops"]
    n_ops = len(ops) + 1
    labels = a.arm_list.split(",")
    per_cold = a.cold_iters * n_ops * (a.est_clear_s + a.est_cold_query_s)
    per_warm = n_ops * (a.warm_warmup + a.warm_iters) * a.est_warm_query_s
    per_run = a.est_restart_s + per_cold + per_warm
    total = per_run * len(labels) * a.rounds
    print(f"{n_ops} op slots, {len(labels)} arm labels x {a.rounds} rounds = {len(labels) * a.rounds} JVM runs; "
          f"per run {per_run / 60:.1f} min (restart {a.est_restart_s:.0f}s, cold {per_cold / 60:.1f} min, warm "
          f"{per_warm / 60:.1f} min); session {total / 3600:.2f} h")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    add_common(r)
    r.add_argument("--arm-list", required=True, help="comma list of arm labels; ARM@x labels repeat an arm (A/A)")
    r.add_argument("--rounds", type=int, default=5, help="JVM runs per arm label")
    r.add_argument("--order", choices=["abba", "random"], default="abba")
    r.add_argument("--no-normalize-store-types", dest="normalize_store_types", action="store_false",
                   help="skip the pre-start store-type check of the stock arms' data paths (one POC start per path)")
    r.add_argument("--round-offset", type=int, default=0, help="number of the first round: continue a session in a new "
                   "invocation and output directory (same seed) with the same round orders and distinct run ids")
    r.add_argument("--modes", default="cold,warm", help="cold,warm,ccold,cwarm")
    r.add_argument("--cold-iters", type=int, default=3, help="cold iterations per op per JVM run")
    r.add_argument("--warm-warmup", type=int, default=5)
    r.add_argument("--warm-iters", type=int, default=10)
    r.add_argument("--op-caps", help="JSON of pre-registered per-op iteration caps {\"ops\": {name: {cold_iters, "
                   "warm_warmup, warm_iters}}}, the same for every arm (common-rules: per-operation iteration caps)")
    r.add_argument("--clients", type=int, default=4)
    r.add_argument("--concurrent-batches", type=int, default=20)
    r.add_argument("--concurrent-seconds", type=float, default=120)
    r.add_argument("--seed", default="coldpath")
    r.add_argument("--no-restart", dest="restart", action="store_false")
    r.add_argument("--executor", choices=["replay", "osb"], default="replay",
                   help="replay: this client runs the op bodies; osb: OpenSearch Benchmark runs them through a derived "
                        "workload with the coldpath-search runner (see osb_cold.py)")
    r.add_argument("--osb-bin", default="opensearch-benchmark")
    r.add_argument("--no-results", action="store_true", help="skip the result-equality pass")
    r.add_argument("--no-jit-warmup", dest="jit_warmup", action="store_false",
                   help="old protocol (jit-cold): no unmeasured execution of every op before the cold block")
    r.add_argument("--strict", action="store_true", help="abort on the first cold-verification failure")
    r.add_argument("--read-ahead-kb", type=int, help="REMOVED: readahead is set per arm (stock arms: as mounted; "
                                                     "bufferpool arms: 0), verified before and after every run")
    r.add_argument("--nfs-trace", action="store_true", help="accepted for compatibility: the read-size trace is on by "
                                                            "default for every arm (NFS READ on EFS, block reads on EBS)")
    r.add_argument("--no-read-size-trace", action="store_true", help="do not trace device read sizes per cold iteration "
                                                                     "(the device-read check then uses mountstats / diskstats)")
    r.add_argument("--poc-read-ahead-kb", type=int, default=POC_READ_AHEAD_KB,
                   help="read_ahead_kb of the bufferpool arms' data devices (stock arms keep the as-mounted default)")
    r.add_argument("--max-read-bytes", type=int, default=131072, help="largest configured IO size; check io_size_ok")
    r.add_argument("--out", required=True)
    p = sub.add_parser("probe")
    add_common(p)
    p.add_argument("--arm", required=True)
    p.add_argument("--op", required=True)
    p.add_argument("--out")
    p.add_argument("--strict", action="store_true")
    p.add_argument("--seed", default="coldpath")
    p.add_argument("--nfs-trace", action="store_true")
    p.add_argument("--no-read-size-trace", action="store_true")
    p.add_argument("--poc-read-ahead-kb", type=int, default=POC_READ_AHEAD_KB)
    p.add_argument("--max-read-bytes", type=int, default=131072)
    pl = sub.add_parser("plan")
    pl.add_argument("--ops", required=True)
    pl.add_argument("--arm-list", required=True)
    pl.add_argument("--rounds", type=int, default=5)
    pl.add_argument("--cold-iters", type=int, default=3)
    pl.add_argument("--warm-warmup", type=int, default=5)
    pl.add_argument("--warm-iters", type=int, default=10)
    pl.add_argument("--est-restart-s", type=float, default=45)
    pl.add_argument("--est-clear-s", type=float, default=1.5)
    pl.add_argument("--est-cold-query-s", type=float, default=1.0)
    pl.add_argument("--est-warm-query-s", type=float, default=0.1)
    a = ap.parse_args()
    {"run": cmd_run, "probe": cmd_probe, "plan": cmd_plan}[a.cmd](a)


if __name__ == "__main__":
    main()
