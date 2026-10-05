#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Multi-index targets and not-applicable arms for coldbench (generic workloads).

An [indices] key may list `members` (geoshape: osmlinestrings, osmmultilinestrings, osmpolygons). The key's query
target is the comma-joined member names (its `name`; filled in when absent, checked when present). Each member has
its own expected docs / shards / segments_per_shard / min_store_bytes. Closing, store-type switching, opening, the
shard/segment/doc-count verification and the page-cache residency check run per member. A key without `members` is
one index, exactly as before (members() returns the key itself, and verify output keeps its old shape).

An arm with `"not_applicable": "<reason>"` (for example S2-B on a corpus where split BKD cannot apply) is not run: the
session records a run with available=false and the reason, which analyze.py lists under "Not available (gaps, not
zero effects)".
"""


def members(idx):
    """The physical indices of an [indices] entry: its members, or the entry itself."""
    return idx["members"] if idx.get("members") else [idx]


def target(idx):
    return idx["name"]


def normalize(indices):
    """Fills / checks the comma-joined name of multi-index keys; checks member specs. Single-index keys unchanged."""
    for key, idx in indices.items():
        ms = idx.get("members")
        if not ms:
            continue
        for m in ms:
            if not m.get("name") or "," in m["name"] or "*" in m["name"]:
                raise ValueError(f"indices[{key}]: every member needs one concrete index name, got {m.get('name')!r}")
        joined = ",".join(m["name"] for m in ms)
        if idx.get("name") and idx["name"] != joined:
            raise ValueError(f"indices[{key}]: name {idx['name']!r} is not the comma-joined members {joined!r}")
        idx["name"] = joined
    return indices


def physical_names(indices):
    return sorted({m["name"] for idx in indices.values() for m in members(idx)})


def uuids(info):
    """UUIDs of a verify_indices entry (one index or the members of a multi-index key)."""
    return list(info["uuids"]) if "uuids" in info else [info["uuid"]]


def not_applicable(arm):
    return arm.get("not_applicable")
