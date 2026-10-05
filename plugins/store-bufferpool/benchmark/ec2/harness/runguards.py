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
