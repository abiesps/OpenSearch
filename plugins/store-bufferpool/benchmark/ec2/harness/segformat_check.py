#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Post-ingest segment format check of one index, from the load generator: the agent reads the segment files of every
shard (GET /index/formats, agent/coldpath_segformat.py) and this module checks them against what the copy is meant to
have. The proof is the per-field attribute (PerFieldPointsFormat.format, PerFieldPostingsFormat.format) and the
format's files in every segment, never the codec name (every Lucene104 segment of a bufferpoolfs index is named
Lucene104SplitPoints).
An expectation ("formats" of an index in the indices file, or the indexprep.py formats options) is one of:
  {"points": {"FIELD": "Lucene90Split"}, "postings": {"FIELD": "Lucene104Nav"}}   a copy meant to have the formats:
        every segment that holds the field must have the attribute and the files, and at least one segment must hold it
  "mapping"     the same, with the fields taken from the index mapping (meta.points_format / meta.postings_format);
                a mapping without such an entry is an error
  "control"     a control copy: no field and no file of any segment may use Lucene90Split points or Lucene104Nav
                (or Lucene104DualNav) postings
Used by indexprep.py formats (after an ingest or a force-merge) and by coldbench.py run/probe (verify step).
"""
import os
import sys
import urllib.parse

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(here, "agent"))
import coldpath_segformat as sf  # noqa: E402 - harness/agent/coldpath_segformat.py
from common import HttpError  # noqa: E402

POINTS_META = "points_format"
POSTINGS_META = "postings_format"


def expected_from_mapping(mappings):
    """({field: points format}, {field: postings format}) named by the meta entries of a mapping (any nesting)."""
    points, postings = {}, {}

    def walk(props, prefix):
        for name, field in (props or {}).items():
            if not isinstance(field, dict):
                continue
            path = prefix + name
            meta = field.get("meta") or {}
            if POINTS_META in meta:
                points[path] = meta[POINTS_META]
            if POSTINGS_META in meta:
                postings[path] = meta[POSTINGS_META]
            walk(field.get("properties"), path + ".")
            walk(field.get("fields"), path + ".")

    walk((mappings or {}).get("properties"), "")
    return points, postings


def resolve(spec, mapping_of):
    """(points, postings, control) of an expectation; mapping_of() returns the index mapping when spec is "mapping"."""
    if spec == "control" or (isinstance(spec, dict) and spec.get("control")):
        if isinstance(spec, dict) and (spec.get("points") or spec.get("postings")):
            raise ValueError("a control copy takes no expected formats")
        return {}, {}, True
    if spec == "mapping":
        points, postings = expected_from_mapping(mapping_of())
        if not points and not postings:
            raise ValueError("formats \"mapping\": the mapping has no meta.points_format or meta.postings_format entry")
        return points, postings, False
    if isinstance(spec, dict) and (spec.get("points") or spec.get("postings")):
        return dict(spec.get("points") or {}), dict(spec.get("postings") or {}), False
    raise ValueError(f"formats: expected {{\"points\": ..., \"postings\": ...}}, \"mapping\" or \"control\", got {spec!r}")


def check_index(os_client, agent, agent_arm, name, uuid, spec):
    """The agent's segment description of index UUID checked against spec; returns coldpath_segformat.check() + context."""
    points, postings, control = resolve(
        spec, lambda: os_client.request("GET", f"/{name}/_mapping")[name]["mappings"])
    try:
        got = agent.request("GET", f"/index/formats?arm={urllib.parse.quote(agent_arm)}&uuids={uuid}")
    except HttpError as e:
        if e.status == 404:
            raise RuntimeError("the agent has no GET /index/formats: install agent/coldpath_agent.py and "
                               "agent/coldpath_segformat.py of this commit on the data node") from e
        raise
    shards = (got.get("indices") or {}).get(uuid) or {}
    out = sf.check(shards, points, postings, control)
    out.update({"index": name, "uuid": uuid, "expected": {"points": points, "postings": postings, "control": control}})
    return out
