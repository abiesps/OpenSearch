#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
OpenSearch Benchmark (OSB) operation import for the generic workloads (clickbench, eventdata, geonames, geopoint,
geopointshape, geoshape, nested, noaa, percolator, so). Used by `queries.py build` only for corpus profiles with
`"osb_import": "ext"`; the big5 / nyc_taxis / http_logs / pmc profiles keep queries.render_osb_ops unchanged.

Every operation of the workload's operations/*.json (rendered with jinja2 like OSB, fixed `now`) is classified:
  - `operation-type: search` with a body                 -> a search op (scroll when it has `pages`)
  - `operation-type: search` with a `param-source`        -> materialized from the workload's OWN workload.py: the
    module is imported, its classes are registered with a stub registry, the class is constructed with the op's
    params after `random.seed(SEED)`, and params() is called until INSTANCES distinct bodies exist; each body is its
    own op `osb:<name>#<k>` (k = 1..INSTANCES), so every arm runs byte-identical requests
  - `operation-type: raw-request`, POST /_search or /<index>/_search -> a search op (path recorded, index replaced
    by the arm's target)
  - bulk ops                                              -> listed as ingest ops (ingest_osb.py runs them)
  - anything else (PPL, flush, transforms, ...)           -> listed as not measured, with the reason
An op's own `index` (geoshape: "osm*") is recorded as `osb_index`; the session queries the arm's target instead
(the comma-joined members of the index key, see indices_ext.py). `cache: true` is recorded: every op runs with
request_cache=false (cold protocol), so a cached/uncached pair is the same request. A profile `body_edits` entry
removes top-level body keys identically for every arm (clickbench: `timeout`, so a slow cold query is measured
instead of returning partial results) and is recorded per op.
"""
import copy
import datetime
import glob
import hashlib
import importlib.util
import json
import os
import random
import re
import sys

SEARCH_PATH = re.compile(r"^/(?:([^/_][^/]*)/)?_search/?$")
DEFAULT_SEED = 4
DEFAULT_INSTANCES = 5
MAX_DRAWS = 1000


def jinja_env(workload_dir):
    """The same environment as queries.render_osb_ops, plus OSB's `benchmark.helpers` (collect renders nothing)."""
    import jinja2  # installed with opensearch-benchmark

    helpers = '{% macro collect(parts) %}{% endmacro %}'
    loader = jinja2.ChoiceLoader([jinja2.FileSystemLoader(workload_dir),
                                  jinja2.DictLoader({"benchmark.helpers": helpers})])
    env = jinja2.Environment(loader=loader, undefined=jinja2.ChainableUndefined)

    def days_ago(start_date, end_date, date_format="%d-%m-%Y"):
        start = datetime.datetime.strptime(start_date, date_format)
        end = datetime.datetime.fromtimestamp(end_date) if isinstance(end_date, (int, float)) else end_date
        return (end - start).days

    env.filters["days_ago"] = days_ago
    return env


def render_items(workload_dir, rel, params, distribution_version="3.0.0", now_epoch=1759600000):
    env = jinja_env(workload_dir)
    text = env.get_template(rel).render(distribution_version=distribution_version, now=now_epoch, **params)
    text = text.strip().rstrip(",")
    return json.loads("[" + text + "]")


def ops_files(workload_dir, osb):
    files = osb.get("ops_files") or ["operations/*.json"]
    out = []
    for pattern in files:
        hits = sorted(glob.glob(os.path.join(workload_dir, pattern)))
        if not hits:
            raise FileNotFoundError(f"{workload_dir}: no file matches {pattern}")
        out += [os.path.relpath(h, workload_dir) for h in hits]
    return out


# ---------------------------------------------------------------- param sources
class _Registry:
    def __init__(self):
        self.param_sources, self.runners, self.other = {}, {}, []

    def register_param_source(self, name, cls):
        self.param_sources[name] = cls

    def register_runner(self, name, runner, **kw):
        self.runners[name] = runner

    def __getattr__(self, name):  # register_scheduler, register_track_processor, ...
        if name.startswith("register_"):
            return lambda *a, **k: self.other.append(name)
        raise AttributeError(name)


def load_param_sources(workload_dir):
    path = os.path.join(workload_dir, "workload.py")
    if not os.path.exists(path):
        return {}, None
    digest = hashlib.sha256(open(path, "rb").read()).hexdigest()
    name = "osb_workload_" + re.sub(r"\W", "_", os.path.basename(os.path.abspath(workload_dir))) + "_" + digest[:8]
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    reg = _Registry()
    mod.register(reg)
    return reg.param_sources, digest


def materialize(op, cls, seed, instances):
    """INSTANCES distinct request bodies from the workload's own param source (seeded, deterministic)."""
    params = {k: v for k, v in op.items() if k not in ("name", "operation-type", "param-source")}
    state = random.getstate()
    try:
        random.seed(seed)
        src = cls(None, params)
        if hasattr(src, "partition"):
            src = src.partition(0, 1)
        bodies, seen, draws = [], set(), 0
        while len(bodies) < instances:
            draws += 1
            if draws > MAX_DRAWS:
                raise RuntimeError(f"{op['name']}: only {len(bodies)} distinct bodies after {MAX_DRAWS} draws")
            p = src.params()
            key = json.dumps(p.get("body"), sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            bodies.append((p, draws))
    finally:
        random.setstate(state)
    return bodies


# ---------------------------------------------------------------- the import
def _params(op):
    return {k: str(v).lower() if isinstance(v, bool) else str(v) for k, v in (op.get("request-params") or {}).items()}


def _edit_body(body, edits):
    """Removes the profile's top-level keys; returns (body, [edit notes])."""
    if not edits:
        return body, []
    notes = []
    body = copy.deepcopy(body)
    for k in edits.get("remove_keys", []):
        if k in body:
            notes.append(f"removed top-level key {k}={json.dumps(body[k])} ({edits.get('reason', 'profile body_edits')})")
            del body[k]
    return body, notes


def render(workload_dir, profile, classify, classify_extra):
    """(ops, report). classify/classify_extra: queries.classify and families_ext.classify_extra."""
    osb = profile["osb"]
    seed = osb.get("param_source_seed", DEFAULT_SEED)
    instances = osb.get("param_source_instances", DEFAULT_INSTANCES)
    sources, py_digest = load_param_sources(workload_dir)
    skip_paths = osb.get("skip_paths", {})
    report = {"workload_dir": os.path.basename(os.path.abspath(workload_dir)), "ops_files": [], "measured": [],
              "ingest_ops": [], "not_measured": [], "body_edits": {}, "param_sources": {},
              "workload_py_sha256": py_digest, "param_source_seed": seed, "param_source_instances": instances}
    if osb.get("exclude"):
        raise RuntimeError("osb.exclude is not allowed for generic profiles: every search op is measured; list ops "
                           "that need absent software in osb.skip_paths with the reason")
    out = []

    def emit(o, op=None):
        o["families"] = sorted(set(classify(o, profile)) | set(classify_extra(o)))
        out.append(o)
        report["measured"].append(o["name"])

    for rel in ops_files(workload_dir, osb):
        report["ops_files"].append(rel)
        for op in render_items(workload_dir, rel, osb.get("params", {})):
            name, typ = op["name"], op.get("operation-type")
            src = f"osb:{os.path.basename(os.path.abspath(workload_dir))}/{rel}"
            if typ == "bulk":
                report["ingest_ops"].append({k: v for k, v in op.items()})
                continue
            if typ == "raw-request":
                path = op.get("path", "")
                m = SEARCH_PATH.match(path)
                reason = next((r for p, r in skip_paths.items() if path.startswith(p)), None)
                if reason:
                    report["not_measured"].append({"name": name, "path": path, "reason": reason})
                    continue
                if not m or op.get("method", "GET").upper() not in ("POST", "GET") or "body" not in op:
                    report["not_measured"].append({"name": name, "path": path, "reason": "not a search request "
                                                   "(maintenance or ingest raw-request)"})
                    continue
                body, notes = _edit_body(op["body"], osb.get("body_edits"))
                o = {"name": "osb:" + name, "source": src, "type": "search", "body": body, "params": _params(op),
                     "osb_path": path, "osb_index": m.group(1)}
                if notes:
                    o["body_edits"] = notes
                    report["body_edits"][o["name"]] = notes
                emit(o, op)
                continue
            if typ != "search":
                report["not_measured"].append({"name": name, "operation-type": typ, "reason": "not a search operation"})
                continue
            notes_common = []
            if op.get("cache") is True:
                notes_common.append("workload sets cache: true; measured with request_cache=false like every op "
                                    "(cold protocol), so it is the same request as its uncached twin")
            if "param-source" in op:
                cls = sources.get(op["param-source"])
                if cls is None:
                    raise RuntimeError(f"{name}: param source {op['param-source']} is not registered by workload.py")
                drawn = materialize(op, cls, seed, instances)
                report["param_sources"][name] = {"param_source": op["param-source"], "class": cls.__name__,
                                                 "seed": seed, "draws": [d for _, d in drawn]}
                for k, (p, draws) in enumerate(drawn, 1):
                    body, notes = _edit_body(p["body"], osb.get("body_edits"))
                    o = {"name": f"osb:{name}#{k}", "source": src, "type": "search", "body": body,
                         "params": _params(op), "param_source": {"name": op["param-source"], "class": cls.__name__,
                                                                 "seed": seed, "instance": k, "draw": draws,
                                                                 "op_name": name}}
                    if p.get("index"):
                        o["osb_index"] = p["index"]
                    if p.get("cache") is True or notes_common:
                        o["notes"] = notes_common or ["param source sets cache: true; measured with request_cache=false"]
                    if notes:
                        o["body_edits"] = notes
                        report["body_edits"][o["name"]] = notes
                    emit(o, op)
                continue
            if "body" not in op:
                report["not_measured"].append({"name": name, "reason": "search op without body or param source"})
                continue
            body, notes = _edit_body(op["body"], osb.get("body_edits"))
            o = {"name": "osb:" + name, "source": src, "type": "search", "body": body, "params": _params(op)}
            if "pages" in op:
                o["type"] = "scroll"
                o["pages"] = int(op["pages"])
                o["page_size"] = int(op.get("results-per-page", 1000))
            if op.get("index"):
                o["osb_index"] = op["index"]
            if notes_common:
                o["notes"] = notes_common
            if notes:
                o["body_edits"] = notes
                report["body_edits"][o["name"]] = notes
            emit(o, op)
    return out, report


def body_digest(body):
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


def summarize(doc, max_body_bytes=65536):
    """A reviewable copy of a built op set: bodies above max_body_bytes (geonames' 45,000-term queries) are replaced
    by their sha256, size and term counts. The committed queries/<w>.osb.summary.json is checked against a fresh
    render by selftest_generic.py (deterministic materialization)."""
    doc = copy.deepcopy(doc)
    for o in doc["ops"]:
        text = json.dumps(o["body"], sort_keys=True)
        if len(text) > max_body_bytes:
            terms = [len(v) for v in _lists(o["body"])]
            o["body"] = {"_summary": {"sha256": body_digest(o["body"]), "bytes": len(text), "list_lengths": terms}}
    return doc


def _lists(node):
    if isinstance(node, dict):
        for v in node.values():
            yield from _lists(v)
    elif isinstance(node, list):
        if node and all(isinstance(x, str) for x in node):
            yield node
        for v in node:
            yield from _lists(v)


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "summarize":
        with open(sys.argv[3], "w") as f:
            json.dump(summarize(json.load(open(sys.argv[2]))), f, indent=1, sort_keys=True)
        sys.exit(0)
    sys.exit("osb_import_ext.py is used by queries.py build (profiles with \"osb_import\": \"ext\"); "
             "osb_import_ext.py summarize OPS.json OUT.json writes a reviewable summary")
