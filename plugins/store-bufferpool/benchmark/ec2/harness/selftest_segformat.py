#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Unit checks of the post-ingest segment format check (stdlib, no root, no AWS, no Java):
  - agent/coldpath_segformat.py on real Lucene shards (testdata/segformat): split = written through the plugin's
    codec service (BufferPoolCodecService, index.codec best_compression, a date field ts_split with
    meta.points_format Lucene90Split and a keyword field kw_nav with meta.postings_format Lucene104Nav); control = the
    stock OpenSearch codec, same fields and documents. Each shard has a compound segment _0 whose field infos were
    rewritten by a soft-deletes doc-values update (_0_1.fnm) and a non-compound segment _1.
    The copy meant to have the formats passes; the control passes as a control; each one checked as the other fails;
    a missing format file, an unknown field-infos header, a field in no segment and a field without points fail.
  - the agent's GET /index/formats (Agent.formats over a data_path tree) and segformat_check.check_index with fake
    clients: explicit expectations, "mapping" (expected fields from the mapping meta), "control", and the error of an
    agent without the endpoint.
  - coldbench.verify_indices with an index "formats" entry: passes, and raises before any result on a mismatch.
  selftest_segformat.py
"""
import json
import os
import shutil
import sys
import tempfile

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
sys.path.insert(0, os.path.join(here, "agent"))
import coldpath_segformat as sf  # noqa: E402
import segformat_check  # noqa: E402
from common import HttpError  # noqa: E402

FIXTURE = os.path.join(here, "testdata", "segformat")
SPLIT = os.path.join(FIXTURE, "split")
CONTROL = os.path.join(FIXTURE, "control")
POINTS = {"ts_split": "Lucene90Split"}
POSTINGS = {"kw_nav": "Lucene104Nav"}
failures = []
checks = [0]


def check(cond, what):
    checks[0] += 1
    print(("ok   " if cond else "FAIL ") + what)
    if not cond:
        failures.append(what)


def errors_contain(res, text):
    return any(text in e for e in res["errors"])


def module_checks(tmp):
    split = sf.describe(SPLIT)
    control = sf.describe(CONTROL)
    check(len(split) == 1 and len(control) == 1, "one shard directory below each index directory")
    shard = next(iter(split.values()))
    segs = {s["name"]: s for s in shard["segments"]}
    check(shard["segments_file"] == "segments_2" and sorted(segs) == ["_0", "_1"], "newest commit segments_2 with _0 and _1")
    check(segs["_0"]["compound"] and not segs["_1"]["compound"], "_0 compound, _1 not")
    check(segs["_0"]["field_infos"] == "_0_1.fnm" and segs["_1"]["field_infos"] == "_1.fnm",
          "field infos: the doc-values update generation for _0, the base file for _1")
    check(all(s["codec"] == "Lucene104SplitPoints" for s in segs.values()), "codec name Lucene104SplitPoints (which proves nothing)")
    fi = segs["_0"]["fields"]
    check(fi["ts_split"]["attributes"].get(sf.POINTS_KEY) == "Lucene90Split" and sf.POINTS_KEY not in fi["ts"]["attributes"],
          "attributes read inside the compound file: ts_split split, ts stock")
    check(fi["kw_nav"]["attributes"].get(sf.POSTINGS_KEY) == "Lucene104Nav"
          and fi["kw"]["attributes"].get(sf.POSTINGS_KEY) == "Lucene104", "postings attributes: kw_nav Nav, kw Lucene104")
    check("_0_Lucene90Split_0.kdm" in segs["_0"]["files"] and "_0_Lucene104Nav_0.nav" in segs["_0"]["files"],
          "format files listed from the .cfe entries of the compound segment")

    res = sf.check(split, POINTS, POSTINGS)
    check(res["ok"], f"split copy with its expected formats passes: {res['errors']}")
    s = res["summary"]
    check(s["points ts_split"]["segments"] == 2 and s["points ts_split"]["files"].get("kdv") == 2
          and s["postings kw_nav"]["files"].get("nav") == 2, "summary: 2 segments, kdv and nav files counted")
    res = sf.check(control, control=True)
    check(res["ok"], f"control copy passes as a control: {res['errors']}")
    res = sf.check(split, control=True)
    check(not res["ok"] and errors_contain(res, "control copy, field [ts_split] has points format attribute Lucene90Split")
          and errors_contain(res, "control copy has the format file _1_Lucene104Nav_0.nav"),
          "split copy checked as a control fails on the attribute and the files")
    res = sf.check(control, POINTS, POSTINGS)
    check(not res["ok"] and errors_contain(res, "field [ts_split] points format attribute PerFieldPointsFormat.format=None")
          and errors_contain(res, "PerFieldPostingsFormat.format=Lucene104, expected Lucene104Nav"),
          "control copy checked as a format copy fails (a copy meant to have the format lacks it)")
    check(not sf.check(split, POINTS, POSTINGS, control=True)["ok"], "a control with expected formats is refused")

    # a missing format file in the non-compound segment
    broken = os.path.join(tmp, "missing_file")
    shutil.copytree(SPLIT, broken)
    os.remove(os.path.join(broken, "0", "index", "_1_Lucene90Split_0.kdi"))
    res = sf.check(sf.describe(broken), POINTS, POSTINGS)
    check(not res["ok"] and errors_contain(res, "_1_Lucene90Split_0.kdi are missing"), f"missing .kdi fails: {res['errors']}")

    # field infos of an unknown format: an error, not a guess
    unknown = os.path.join(tmp, "unknown_header")
    shutil.copytree(SPLIT, unknown)
    p = os.path.join(unknown, "0", "index", "_1.fnm")
    with open(p, "rb") as f:
        b = bytearray(f.read())
    i = b.find(b"Lucene94FieldInfos")
    b[i:i + len("Lucene94FieldInfos")] = b"Lucene99FieldInfos"
    with open(p, "wb") as f:
        f.write(bytes(b))
    res = sf.check(sf.describe(unknown), POINTS, POSTINGS)
    check(not res["ok"] and errors_contain(res, "header Lucene99FieldInfos"), f"unknown field-infos header fails: {res['errors']}")

    res = sf.check(split, {"no_such_field": "Lucene90Split"})
    check(not res["ok"] and errors_contain(res, "field [no_such_field]: in no segment"), "an expected field in no segment fails")
    res = sf.check(split, {"kw": "Lucene90Split"})
    check(not res["ok"] and errors_contain(res, "field [kw] has no points"), "a field without points fails")
    res = sf.check({}, POINTS)
    check(not res["ok"] and errors_contain(res, "no shard directory"), "no shard directory fails")


class FakeClient:
    def __init__(self, routes):
        self.routes = routes
        self.calls = []

    def request(self, method, path, body=None, timeout=None):
        self.calls.append((method, path))
        for prefix, value in self.routes.items():
            if path.startswith(prefix):
                if isinstance(value, Exception):
                    raise value
                return value
        raise HttpError(404, method, path, "no route")


def agent_checks(tmp):
    import coldpath_agent
    data = os.path.join(tmp, "data")
    for uuid, src in (("uuidsplit", SPLIT), ("uuidcontrol", CONTROL)):
        shutil.copytree(src, os.path.join(data, "nodes", "0", "indices", uuid))
    got = coldpath_agent.Agent.formats(coldpath_agent.Agent, {"uuidsplit"}, {"data_path": data})
    check(list(got["indices"]) == ["uuidsplit"] and len(got["indices"]["uuidsplit"]) == 1,
          "agent /index/formats: the asked UUID only, one shard")
    agent = FakeClient({"/index/formats": json.loads(json.dumps(
        coldpath_agent.Agent.formats(coldpath_agent.Agent, {"uuidsplit", "uuidcontrol"}, {"data_path": data})))})
    mapping = {"properties": {"ts": {"type": "date"},
                              "ts_split": {"type": "date", "meta": {"points_format": "Lucene90Split"}},
                              "obj": {"properties": {"kw_nav": {"type": "keyword", "meta": {"postings_format": "Lucene104Nav"}}}}}}
    check(segformat_check.expected_from_mapping(mapping) == ({"ts_split": "Lucene90Split"}, {"obj.kw_nav": "Lucene104Nav"}),
          "expected fields from the mapping meta, nested paths")
    os_client = FakeClient({"/split/_mapping": {"split": {"mappings": {"properties": {
        "ts_split": {"type": "date", "meta": {"points_format": "Lucene90Split"}},
        "kw_nav": {"type": "keyword", "meta": {"postings_format": "Lucene104Nav"}}}}}},
        "/control/_mapping": {"control": {"mappings": {"properties": {"ts": {"type": "date"}}}}}})
    res = segformat_check.check_index(os_client, agent, "POC-EBS", "split", "uuidsplit", "mapping")
    check(res["ok"] and res["expected"]["points"] == POINTS, f"check_index \"mapping\" on the split copy passes: {res['errors']}")
    check(("GET", "/index/formats?arm=POC-EBS&uuids=uuidsplit") in agent.calls, "check_index asks the agent for the index UUID and arm")
    res = segformat_check.check_index(os_client, agent, "POC-EBS", "control", "uuidcontrol", "control")
    check(res["ok"], "check_index \"control\" on the control copy passes")
    res = segformat_check.check_index(os_client, agent, "POC-EBS", "split", "uuidsplit", {"control": True})
    check(not res["ok"], "check_index control on the split copy fails")
    res = segformat_check.check_index(os_client, agent, "POC-EBS", "control", "uuidcontrol", {"points": POINTS, "postings": POSTINGS})
    check(not res["ok"], "check_index explicit formats on the control copy fails")
    try:
        segformat_check.check_index(os_client, agent, "POC-EBS", "control", "uuidcontrol", "mapping")
        check(False, "\"mapping\" without meta entries is an error")
    except ValueError as e:
        check("has no meta.points_format" in str(e), "\"mapping\" without meta entries is an error")
    old_agent = FakeClient({})
    try:
        segformat_check.check_index(os_client, old_agent, "POC-EBS", "split", "uuidsplit", "mapping")
        check(False, "an agent without /index/formats is a clear error")
    except RuntimeError as e:
        check("has no GET /index/formats" in str(e), "an agent without /index/formats is a clear error")
    return agent


def coldbench_checks(agent):
    import coldbench

    class Node:
        pass

    def os_client(name, uuid):
        return FakeClient({
            f"/_cat/segments/{name}": [{"shard": "0", "prirep": "p", "segment": "_0", "searchable": "true"},
                                       {"shard": "0", "prirep": "p", "segment": "_1", "searchable": "true"}],
            f"/{name}/_count": {"count": 40},
            f"/_cat/indices/{name}": [{"pri": "1", "rep": "0", "pri.store.size": "1000", "store.size": "1000"}],
            f"/{name}/_settings": {name: {"settings": {"index": {"store": {"type": "bufferpoolfs"}, "uuid": uuid}}}},
            "/_cat/indices": [{"index": name, "status": "open", "uuid": uuid}],
        })

    def verify(name, uuid, formats):
        node = Node()
        node.os = os_client(name, uuid)
        node.agent = FakeClient({"/index/du": {}, "/index/formats": agent.routes["/index/formats"]})
        orig = coldbench.index_state
        coldbench.index_state = lambda n, x: {"uuid": uuid, "store_type": "bufferpoolfs", "status": "open"}
        try:
            cfg = {"indices": {"k": {"name": name, "formats": formats, "segments_per_shard": 2}}}
            return coldbench.verify_indices(node, cfg, {"open": ["k"]}, "POC-EBS")
        finally:
            coldbench.index_state = orig

    out = verify("split", "uuidsplit", {"points": POINTS, "postings": POSTINGS})
    check(out["k"]["formats"]["ok"] and out["k"]["formats"]["segments"] == 2, "coldbench verify: the split copy's formats pass")
    out = verify("control", "uuidcontrol", "control")
    check(out["k"]["formats"]["ok"], "coldbench verify: the control copy passes as a control")
    for name, uuid, formats, what in (("control", "uuidcontrol", {"points": POINTS}, "a copy meant to have the format lacks it"),
                                      ("split", "uuidsplit", "control", "a control copy has the format")):
        try:
            verify(name, uuid, formats)
            check(False, f"coldbench verify raises when {what}")
        except RuntimeError as e:
            check("segment formats do not match" in str(e), f"coldbench verify raises when {what}")


def main():
    tmp = tempfile.mkdtemp(prefix="segformat-")
    try:
        module_checks(tmp)
        agent = agent_checks(tmp)
        coldbench_checks(agent)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    if failures:
        print(f"SELFTEST-SEGFORMAT FAIL: {len(failures)} of {checks[0]} checks")
        sys.exit(1)
    print(f"SELFTEST-SEGFORMAT PASS: {checks[0]} checks")


if __name__ == "__main__":
    main()
