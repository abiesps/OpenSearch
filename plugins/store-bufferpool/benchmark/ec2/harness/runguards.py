#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Checks coldbench makes at the start of every arm run, before it measures (harness-ext review findings 3 and 4).

IO configuration of the bufferpool arms (common-rules.md, IO sizes): cache block 8 KiB, random reads 32 KiB, sequential
reads 128 KiB, buffered IO. The node reports what it runs with in GET /_bufferpool/stats (block_size, random_read_size,
sequential_read_size, bytes). A bufferpool run on a node whose values differ, or that does not report them (a binary
built before the IO-window change reads one block per miss), is refused. The plugin has no O_DIRECT mode: it always
reads through the page cache (FileChannel pread; NativeReadHints only adds POSIX_FADV_WILLNEED), so there is no
direct-IO setting to check.

Other open indices (GENERIC-PLAN section 2, grouped hosts): while one workload is measured every other workload's
indices are closed, so the heap, the bufferpool and the page cache hold only the workload under test. The arms file's
"other_indices" selects what happens to an open, non-system index that is not in its [indices]:
  "record"  (default, unchanged behaviour of the main workflow's arms files) list them in the run record and the log;
  "close"   close them before the arm's indices are opened (arms_generic.py writes this for every generic workload);
  "refuse"  stop the session.
Names starting with "." (system and plugin indices) are never touched.
"""
import json

import indices_ext

IO_DEFAULTS = {"block_size": 8192, "random_read_size": 32768, "sequential_read_size": 131072}
OTHER_POLICIES = ("record", "close", "refuse")


def want_io(a):
    """The IO configuration the session requires (coldbench --bp-block-size / --bp-random-read-size / ...)."""
    return {"block_size": getattr(a, "bp_block_size", IO_DEFAULTS["block_size"]),
            "random_read_size": getattr(a, "bp_random_read_size", IO_DEFAULTS["random_read_size"]),
            "sequential_read_size": getattr(a, "bp_sequential_read_size", IO_DEFAULTS["sequential_read_size"])}


def check_io_config(arm_name, stats, want):
    """stats: GET /_bufferpool/stats of the running node. Returns the record; raises when a value differs."""
    got = {k: stats.get(k) for k in want}
    rec = {"want": dict(want), "node": got, "read_hint": stats.get("read_hint"), "ok": got == want}
    if not rec["ok"]:
        missing = [k for k, v in got.items() if v is None]
        why = (f"the node does not report {missing} (binary before the IO-window change?)" if missing else
               "the node's bufferpool settings differ")
        raise RuntimeError(f"arm {arm_name}: IO configuration {got} != required {want}: {why}; not measuring "
                           "(common-rules.md: 8 KiB block, 32 KiB random, 128 KiB sequential)")
    return rec


CACHE_CLEANUP_MAX_S = 1.0


def _seconds(v):
    """'1s' / '500ms' / '1m' -> seconds; None when not parseable."""
    import re
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h)?\s*", str(v))
    if not m:
        return None
    return float(m.group(1)) * {"ms": 1e-3, "s": 1, "m": 60, "h": 3600, None: 1e-3}[m.group(2)]


def check_cache_cleanup(arm_name, node_settings):
    """
    node_settings: the node's own settings (GET /_nodes/_local/settings, flat). POST /_cache/clear only marks fielddata
    and global ordinals; the node drops them every indices.cache.cleanup_interval (default 1m). A cold iteration needs
    them gone, so every arm's node must run with an interval of at most CACHE_CLEANUP_MAX_S. Returns the record; raises
    otherwise (the run is refused, never measured).
    """
    v = node_settings.get("indices.cache.cleanup_interval")
    s = _seconds(v) if v is not None else None
    rec = {"indices.cache.cleanup_interval": v, "ok": s is not None and s <= CACHE_CLEANUP_MAX_S}
    if not rec["ok"]:
        raise RuntimeError(f"arm {arm_name}: indices.cache.cleanup_interval is {v or 'the default 1m'}; set it to 1s in "
                           "every arm's opensearch.yml (marked fielddata and global ordinals are dropped only by that "
                           "periodic sweep, so a cold iteration would find them cached); not measuring")
    return rec


# ---------------------------------------------------------------- builds (USER DECISION 2026-10-05)
# An arm's "build" names the binary its agent arm (node) runs:
#   "baseline"  stock OpenSearch (stock Lucene) WITH the bufferpool plugin built for it (artifact baseline_bufferpool);
#               GET /_bufferpool/stats reports build_target "stock"; it has no experiment endpoint, so it gets no
#               base_switches and no switches
#   "poc"       the proof-of-concept OpenSearch and Lucene fork with the same plugin (build_target "poc")
#   "stock"     stock OpenSearch without the plugin (memory mapping): a context arm only, no verdict
# An arm without "build" keeps the earlier meaning: bufferpool true = poc, false = stock.
BUILDS = ("baseline", "poc", "stock")
BUILD_TARGET = {"baseline": "stock", "poc": "poc"}


def arm_build(arm):
    b = arm.get("build")
    if b is None:
        return "poc" if arm.get("bufferpool") else "stock"
    return b


def validate_build(arm_name, arm):
    """Arms-file check (load_arms): the build exists and agrees with bufferpool and switches."""
    b = arm.get("build")
    if b is None:
        return
    if b not in BUILDS:
        raise ValueError(f"arm {arm_name}: build {b!r}: one of {list(BUILDS)}")
    if (b == "stock") == bool(arm.get("bufferpool")):
        raise ValueError(f"arm {arm_name}: build {b} needs bufferpool {b != 'stock'}")
    if b == "baseline" and arm.get("switches"):
        raise ValueError(f"arm {arm_name}: the baseline build has no experiment switches")


def stock_binary_paths(cfg, path):
    """Data paths of nodes that run a binary without the split points codec (stock and baseline builds)."""
    return {path.get(a["node"]) for a in cfg["arms"].values()
            if arm_build(a) in ("stock", "baseline") and not indices_ext.not_applicable(a) and "node" in a}


def check_build(arm_name, arm, cfg, root, bp_stats):
    """
    The running node is the arm's build: GET /_bufferpool/stats build_target (baseline -> stock, poc -> poc) and, when
    the arms file's "builds" block names one, GET / version.build_hash starts with it. Only for arms with an explicit
    "build" (older arms files: recorded as unchecked). Returns the record; raises when the node is another build.
    """
    b = arm.get("build")
    got = {"build_target": (bp_stats or {}).get("build_target"),
           "build_hash": ((root or {}).get("version") or {}).get("build_hash"),
           "lucene_version": ((root or {}).get("version") or {}).get("lucene_version")}
    rec = {"build": b, "node": got, "ok": None}
    if b is None or b == "stock":
        return rec
    want = dict((cfg.get("builds") or {}).get(b) or {})
    want.setdefault("build_target", BUILD_TARGET[b])
    rec["want"] = want
    errors = []
    if got["build_target"] != want["build_target"]:
        errors.append(f"build_target {got['build_target']!r}, the {b} build reports {want['build_target']!r}")
    if want.get("build_hash") and not str(got["build_hash"] or "").startswith(want["build_hash"]):
        errors.append(f"build_hash {got['build_hash']!r} does not start with {want['build_hash']!r}")
    rec["ok"] = not errors
    if errors:
        raise RuntimeError(f"arm {arm_name}: the node is not the {b} build ({'; '.join(errors)}); check the agent arm "
                           f"{arm.get('node')} unit; not measuring")
    return rec


# ---------------------------------------------------------------- C memory allocator (jemalloc for every node unit)
ALLOCATOR_KEYS = ("ld_preload", "malloc_conf", "malloc_arena_max")


def want_allocator(cfg, a):
    """
    The allocator the session requires: --allocator (jemalloc | glibc | any) over the arms file's "allocator" block
    {"name": "jemalloc", "ld_preload": ..., "malloc_conf": ..., "jemalloc_sha256": ...}. None = record only (older
    arms files). For jemalloc, MALLOC_ARENA_MAX must be unset (common-rules: removed once jemalloc is in place).
    """
    block = dict(cfg.get("allocator") or {})
    cli = getattr(a, "allocator", None)
    if cli == "any":
        return None
    if cli and cli != block.get("name"):
        block = {"name": cli}
    if not block.get("name"):
        return None
    if block["name"] not in ("jemalloc", "glibc"):
        raise ValueError(f"allocator {block['name']!r}: jemalloc or glibc")
    if block["name"] == "jemalloc":
        block.setdefault("malloc_arena_max", None)
    return block


def allocator_signature(alloc):
    """What must be identical in every run of a session: name, the three variables and the mapped files' sha256."""
    alloc = alloc or {}
    return json.dumps({"name": alloc.get("name"), **{k: alloc.get(k) for k in ALLOCATOR_KEYS},
                       "jemalloc_sha256": sorted((alloc.get("jemalloc_sha256") or {}).values())}, sort_keys=True)


def check_allocator(arm_name, mem, want):
    """
    mem: agent GET /node/memory of the running node (v4 "allocator": read from /proc/<pid>/maps and environ). Returns
    the record {want, node, ok}; raises when the node's allocator differs from `want` (None = record only).
    """
    alloc = (mem or {}).get("allocator")
    rec = {"want": want, "node": alloc, "ok": None}
    if want is None:
        return rec
    errors = []
    if not isinstance(alloc, dict) or alloc.get("error"):
        errors.append(f"the agent reports no allocator ({alloc!r}; agent before v4, or the JVM exited)")
    else:
        if alloc.get("name") != want["name"]:
            errors.append(f"allocator {alloc.get('name')} (libjemalloc mapped: {alloc.get('jemalloc_mapped')}), want "
                          f"{want['name']}")
        for k in ALLOCATOR_KEYS:
            if k in want and alloc.get(k) != want[k]:
                errors.append(f"{k.upper()} {alloc.get(k)!r}, want {want[k]!r}")
        if want["name"] == "glibc" and alloc.get("ld_preload"):
            errors.append(f"LD_PRELOAD {alloc.get('ld_preload')!r} on a glibc node")
        sha = want.get("jemalloc_sha256")
        if sha and any(v != sha for v in (alloc.get("jemalloc_sha256") or {}).values()):
            errors.append(f"libjemalloc sha256 {alloc.get('jemalloc_sha256')}, want {sha}")
    rec["ok"] = not errors
    if errors:
        raise RuntimeError(f"arm {arm_name}: node allocator differs: {'; '.join(errors)}; write the same drop-in to every "
                           "node unit (node_allocator.sh apply) and restart; not measuring")
    return rec


# bufferpool settings that must be identical in every configuration on one storage (USER DECISION: the bufferpool is
# not a variable), from GET /_bufferpool/stats; opt-in with the arms file's "same_bufferpool_settings": true
BP_SAME_KEYS = ("block_size", "random_read_size", "sequential_read_size", "read_hint", "prefetch_node_bytes",
                "prefetch_task_per_window")
BP_SAME_SCHEDULER_KEYS = ("max_in_flight", "budget_scope", "queue_size", "queue_policy")


def bufferpool_signature(stats):
    s = stats or {}
    sched = s.get("prefetch_scheduler") or {}
    return {**{k: s.get(k) for k in BP_SAME_KEYS}, **{f"prefetch_scheduler.{k}": sched.get(k) for k in BP_SAME_SCHEDULER_KEYS}}


def poc_only(idx):
    """An [indices] entry in a format the stock binary cannot read (split BKD), or marked "poc_only": true."""
    return bool(idx.get("poc_only")) or "split" in str(idx.get("format", "")).lower()


def check_format_isolation(cfg, storages):
    """
    A closed index stays allocated: the stock binary cannot start on a data path that holds an index in a format it
    cannot read (shard copy "stale or corrupt": Could not load codec 'Lucene104SplitPoints', cluster red; probe on the
    big5-100 data node). So every node whose arms open a POC-only index must have a data path that no stock arm's node
    uses. storages: the agent's /health "storages" (agent arm -> data_path). Raises before any run; returns the record.
    """
    path = {name: (s or {}).get("data_path") for name, s in (storages or {}).items()}
    # the baseline build (stock OpenSearch and Lucene with the bufferpool plugin) has no split codec either
    stock_paths = stock_binary_paths(cfg, path)
    rec = {"poc_only_indices": [], "ok": True}
    for key, idx in cfg["indices"].items():
        if not poc_only(idx):
            continue
        nodes = sorted({a["node"] for a in cfg["arms"].values() if key in a.get("open", []) and "node" in a})
        rec["poc_only_indices"].append({"key": key, "nodes": nodes, "data_paths": [path.get(n) for n in nodes]})
        for n in nodes:
            if path.get(n) is None:
                raise RuntimeError(f"indices[{key}] ({idx.get('format')}): agent arm {n} has no data path in the agent config")
            if path[n] in stock_paths:
                raise RuntimeError(f"indices[{key}] ({idx.get('format')}) is opened by node {n} whose data path {path[n]} "
                                   "is also the data path of a stock or baseline build's node: that binary cannot start next to it (closed "
                                   "indices stay allocated); give the POC-only index its own agent arm and data path")
    return rec


def other_policy(cfg):
    p = cfg.get("other_indices", "record")
    if p not in OTHER_POLICIES:
        raise ValueError(f"other_indices {p!r}: one of {list(OTHER_POLICIES)}")
    return p


def other_open_indices(client, cfg):
    """Open non-system indices of the node that are not in the arms file's [indices]."""
    rows = client.request("GET", "/_cat/indices?format=json&h=index,status&expand_wildcards=open,hidden")
    mine = set(indices_ext.physical_names(cfg["indices"]))
    return sorted(r["index"] for r in rows
                  if r.get("status") == "open" and not r["index"].startswith(".") and r["index"] not in mine)


def enforce_other_indices(client, cfg, log):
    """Applies the arms file's other_indices policy on the running node; returns the record for the run."""
    policy = other_policy(cfg)
    found = other_open_indices(client, cfg)
    rec = {"policy": policy, "open": found, "closed": []}
    if not found:
        return rec
    if policy == "refuse":
        raise RuntimeError(f"indices outside the arms file are open: {found}; close them first (other_indices=refuse)")
    if policy == "close":
        for name in found:
            client.request("POST", f"/{name}/_close?wait_for_active_shards=0")
            rec["closed"].append(name)
            log(f"  closed {name} (not in the arms file; other_indices=close)")
    else:
        log(f"  WARNING open indices outside the arms file (other_indices=record): {found}")
    return rec
