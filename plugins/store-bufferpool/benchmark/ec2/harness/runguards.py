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
    stock_paths = {path.get(a["node"]) for a in cfg["arms"].values()
                   if not a.get("bufferpool") and not indices_ext.not_applicable(a) and "node" in a}
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
                                   "is also a stock arm's data path: the stock binary cannot start next to it (closed "
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
