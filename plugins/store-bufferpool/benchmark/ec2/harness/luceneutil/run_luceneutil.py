#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Runs luceneutil (wikimediumall) for the coldpath arms on ONE host: stock Lucene (L0) vs POC Lucene (L1, L2-x,
L2-CORE), interleaved JVM runs, on the index copies on EBS and on EFS. luceneutil does the work: this script writes
a driver (one Python file per session, kept next to the results) that uses luceneutil's own API
(competition.Competition / newIndex / competitor, benchUtil.RunAlgs.makeIndex, compile, runSimpleSearchBench and
simpleReport, i.e. what searchBench.run does for two competitors) for N arms:
  - every arm is a luceneutil competitor with the SAME base java command (config java_command) plus its own
    -Dcoldpath.switches (validated against switches.json, read back by ColdpathSwitches in the JVM);
  - JVM iteration it runs every arm once, the arm order rotated by `it` (luceneutil rotates its two competitors the
    same way), with the same per-iteration seed for every arm (same tasks);
  - mode warm: luceneutil's normal run (taskRepeatCount instances of each task in one JVM);
    mode cold-luceneutil: what luceneutil's cold=True does (sync + drop_caches before each JVM), done through the
    agent because luceneutil's dropCaches.sh path is wrong at the pin, plus a residency check of the index files;
  - an arm label ARM:STORAGE@REPEAT reads the index copy on STORAGE (default --storage), so one session can
    interleave the same arm on EBS and EFS; competition.cold_jvm_count (default jvm_count) sets the cold JVM count;
    mode cold-strict: one task at a time (numConcurrentQueries 1) and, before EVERY task, the coldpath agent pages
    out the JVM's index mappings, syncs and drops the page cache, and checks mincore residency (patch 0002);
  - after the runs, luceneutil's simpleReport for each arm against the base arm (its QPS table and p-values, and its
    verifyScores / verifyCounts result comparison: a difference fails the session).
Every JVM run is appended to <out>/manifest.jsonl (arm, iteration, storage, log file, seed, switches, java command),
which analyze_luceneutil.py converts into a coldbench session for analyze.py.

  run_luceneutil.py plan --config arms.luceneutil.json --mode warm --tasks wikimedium.10M.nostopwords.tasks \
      --storage EBS --arms L0@a,L0@b,L1 --out results/lu/warm-ebs-1      (writes the driver, runs nothing)
  run_luceneutil.py run  ... same arguments ...                           (writes and runs the driver)
"""
import argparse
import json
import os
import shlex
import subprocess
import sys

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
import switches as sw  # noqa: E402

MODES = ("warm", "cold-luceneutil", "cold-strict")


def load_config(path):
    cfg = json.load(open(path))
    for k in ("luceneutil", "checkouts", "indices", "storages", "arms", "java_command", "competition"):
        if k not in cfg:
            raise ValueError(f"{path}: missing [{k}]")
    return cfg


def arm_spec(cfg, label, default_storage=None):
    """label = ARM[:STORAGE][@REPEAT]: the arm, the storage whose index copy it reads (default: --storage), and an
    optional repeat tag (A/A). One session may interleave arms on several storages."""
    head = label.split("@", 1)[0]
    arm, _, storage = head.partition(":")
    storage = storage or default_storage
    if arm not in cfg["arms"]:
        raise ValueError(f"unknown arm {label}; arms: {sorted(cfg['arms'])}")
    if storage not in cfg["storages"]:
        raise ValueError(f"arm {label}: storage {storage}: one of {sorted(cfg['storages'])}")
    return arm, cfg["arms"][arm], storage


def plan(cfg, table, mode, tasks, storage, labels, out):
    if mode not in MODES:
        raise ValueError(f"mode {mode}: one of {MODES}")
    if len(set(labels)) != len(labels):
        raise ValueError(f"duplicate arm label in {labels}")
    params = cfg.get("params", {})
    comp = cfg["competition"]
    arms = []
    for label in labels:
        name, a, st_name = arm_spec(cfg, label, storage)
        if a.get("not_applicable"):
            arms.append({"label": label, "arm": name, "storage": st_name, "not_applicable": a["not_applicable"]})
            continue
        checkout = cfg["checkouts"][a["checkout"]]
        idx = cfg["indices"][a["index"]]
        flag, resolved = sw.jvm_flag(table, a.get("switches", {}), params)
        if resolved and a["checkout"] != "poc":
            raise ValueError(f"arm {label}: switches need the POC Lucene (stock Lucene has none)")
        java = cfg["java_command"] + (" " + flag if flag else "")
        arms.append({"label": label, "arm": name, "storage": st_name, "storage_spec": cfg["storages"][st_name],
                     "checkout": checkout["path"], "index": a["index"], "index_spec": idx, "java_command": java,
                     "switches": resolved})
    runnable = [x for x in arms if not x.get("not_applicable")]
    if not runnable:
        raise ValueError("no runnable arm")
    storages = sorted({x["storage"] for x in runnable})
    os.makedirs(out, exist_ok=True)
    session = {"mode": mode, "tasks": tasks, "storage": ",".join(storages),
               "storage_spec": {s: cfg["storages"][s] for s in storages}, "labels": labels, "arms": arms,
               "competition": comp, "luceneutil": cfg["luceneutil"], "params": params,
               "switches_fork_commit": table.get("fork_commit"), "id": os.path.basename(os.path.abspath(out))}
    driver = os.path.join(out, "driver.py")
    with open(driver, "w") as f:
        f.write(DRIVER.replace("@SESSION@", repr(json.dumps(session))).replace("@OUT@", repr(os.path.abspath(out))))
    with open(os.path.join(out, "session.json"), "w") as f:
        json.dump(session, f, indent=1)
    return driver, session


DRIVER = r'''#!/usr/bin/env python3
# Generated by run_luceneutil.py (coldpath harness): luceneutil's API for N interleaved arms. Run from
# <luceneutil>/src/python (constants / localconstants). Do not edit; regenerate.
import json, os, random, sys, time
sys.path.insert(0, os.getcwd())
import benchUtil, competition, constants  # noqa: E402

S = json.loads(@SESSION@)
OUT = @OUT@
comp_cfg = S["competition"]
cold_luceneutil = S["mode"] == "cold-luceneutil"
strict = S["mode"] == "cold-strict"
jvm_count = comp_cfg["jvm_count"] if S["mode"] == "warm" else comp_cfg.get("cold_jvm_count", comp_cfg["jvm_count"])
# cold=False for luceneutil itself: its cold=True runs "sudo BENCH_BASE_DIR/dropCaches.sh", a path that does not exist
# at the pin (the script is in scripts/); the driver does the same thing (sync + drop_caches) through the agent before
# each JVM and checks the residency of the arm's index files afterwards
comp = competition.Competition(cold=False, verifyScores=comp_cfg.get("verify_scores", True),
                               verifyCounts=comp_cfg.get("verify_counts", True), randomSeed=comp_cfg["random_seed"],
                               taskCountPerCat=comp_cfg.get("task_count_per_cat", 1),
                               taskRepeatCount=comp_cfg["cold_task_repeat_count" if strict else "task_repeat_count"],
                               jvmCount=jvm_count)
tasks_file = os.path.join(constants.BENCH_BASE_DIR, "tasks", S["tasks"])
if not os.path.exists(tasks_file):
    raise SystemExit(f"no tasks file {tasks_file}")
import urllib.request


def agent(st, method, path):
    tok = open(st["agent_token_file"]).read().strip()
    req = urllib.request.Request(f"{st['agent_url']}{path}", method=method, headers={"X-Coldpath-Token": tok})
    return json.loads(urllib.request.urlopen(req, timeout=900).read())


# kernel readahead of every layer of each storage's data path, set through the agent, verified and recorded: every
# luceneutil arm reads through MMapDirectory (no bufferpool), so all arms use the same mode, "default" = as mounted
# (common-rules: stock mmap keeps the kernel readahead); a storage whose value differs refuses the session. The JVMs
# start after this, so every index file is opened with the verified value.
ra_all = {}
for name, st in S["storage_spec"].items():
    ra = agent(st, "POST", f"/readahead/mode?arm={st['agent_arm']}&mode={st.get('read_ahead_mode', 'default')}")
    ra_all[name] = ra
    print("readahead", name, ra)
    if not ra.get("ok"):
        raise SystemExit(f"readahead of {name} is not {st.get('read_ahead_mode', 'default')}: {ra}; not measuring")
json.dump(ra_all, open(os.path.join(OUT, "readahead.json"), "w"), indent=1)
indices, comps = {}, {}
for a in S["arms"]:
    if a.get("not_applicable"):
        continue
    spec = a["index_spec"]
    key = a["index"]
    if key not in indices:
        kw = dict(spec.get("luceneutil_index_kwargs", {}))
        if spec.get("facets"):
            kw["facets"] = tuple(tuple(x) for x in spec["facets"])
        indices[key] = comp.newIndex(spec["builder_checkout"], competition.WIKI_MEDIUM_ALL, **kw)
    kwc = {"index": indices[key], "javaCommand": a["java_command"], "directory": "MMapDirectory",
           "searchConcurrency": comp_cfg.get("search_concurrency", 0)}
    nq = 1 if strict else comp_cfg.get("num_concurrent_queries")
    if nq is not None:
        kwc["numConcurrentQueries"] = nq
    c = comp.competitor(a["label"], a["checkout"], **kwc)
    c.tasksFile = tasks_file
    c.coldpath = a
    comps[a["label"]] = c
r = benchUtil.RunAlgs(constants.JAVA_COMMAND, comp.verifyScores, comp.verifyCounts)
for c in comps.values():
    path = os.path.join(c.coldpath["storage_spec"]["index_dir_base"], c.index.getName())
    if not os.path.isdir(os.path.join(path, "index")):
        raise SystemExit(f"arm {c.name}: index {c.coldpath['index']} not at {path}: build it (run_luceneutil.py index) "
                         f"and copy it (run_luceneutil.py copy) first")
print("compile:")
for c in comps.values():
    r.compile(c)
rand = random.Random(comp.randomSeed)
static_seed = rand.randint(-10000000, 1000000)
labels = list(comps)
results = {l: [] for l in labels}
manifest = open(os.path.join(OUT, "manifest.jsonl"), "a")
for it in range(comp.jvmCount):
    seed = rand.randint(-10000000, 1000000)
    order = labels[it % len(labels):] + labels[:it % len(labels)]
    for label in order:
        c = comps[label]
        st = c.coldpath["storage_spec"]
        # the index copy of this arm's storage: luceneutil resolves every index path through constants.INDEX_DIR_BASE
        constants.INDEX_DIR_BASE = st["index_dir_base"]
        base_cmd = c.coldpath["java_command"]
        cold_log = None
        jvm_drop = None
        if strict:
            cold_log = os.path.join(OUT, f"{S['id']}.{label}.{it}.cold.jsonl")
            props = {"coldpath.cold.agent": st["agent_url"], "coldpath.cold.tokenFile": st["agent_token_file"],
                     "coldpath.cold.arm": st["agent_arm"], "coldpath.cold.uuids": c.index.getName(),
                     "coldpath.cold.residencyTolerance": str(st.get("residency_tolerance", 1 << 20)),
                     "coldpath.cold.log": cold_log}
            c.javaCommand = base_cmd + "".join(f" -D{k}={v}" for k, v in props.items())
        else:
            c.javaCommand = base_cmd
        if cold_luceneutil:
            # luceneutil cold=True: sync + drop_caches once before the JVM (no JVM runs, so nothing is mapped)
            drop = agent(st, "POST", f"/cache/drop?pageout=0&arm={st['agent_arm']}")
            res = agent(st, "GET", f"/cache/residency?arm={st['agent_arm']}&uuids={c.index.getName()}")
            ok = res["resident_bytes"] <= st.get("residency_tolerance", 1 << 20) and res["files"] > 0
            jvm_drop = {"cold_ok": ok, "resident_bytes": res["resident_bytes"], "files": res["files"],
                        "bytes": res["bytes"], "drop_ms": drop.get("drop_ms"), "sync_ms": drop.get("sync_ms")}
            if not ok:
                raise SystemExit(f"{label} iteration {it}: index files still resident after the drop: {jvm_drop}")
        t0 = time.time()
        log = r.runSimpleSearchBench(it, S["id"], c, False, seed, static_seed)
        results[label].append(log)
        manifest.write(json.dumps({"iter": it, "label": label, "arm": c.coldpath["arm"], "log": log,
                                   "stdout": log + ".stdout", "cold_log": cold_log, "jvm_drop": jvm_drop, "seed": seed,
                                   "static_seed": static_seed, "mode": S["mode"], "storage": c.coldpath["storage"],
                                   "tasks": S["tasks"], "index": c.index.getName(), "java_command": c.javaCommand,
                                   "switches": c.coldpath["switches"], "wall_s": time.time() - t0}) + "\n")
        manifest.flush()
base = labels[0]
reports = {}
for label in labels[1:]:
    details, diffs, heap = r.simpleReport(results[base], results[label], False, False, baseDesc=base, cmpDesc=label)
    reports[label] = {"diffs": diffs}
    if diffs is not None and (diffs[1] or diffs[2] < 1.0):
        raise SystemExit(f"luceneutil result comparison {base} vs {label} FAILED: {diffs}")
json.dump({"base": base, "reports": reports}, open(os.path.join(OUT, "luceneutil-report.json"), "w"), indent=1, default=str)
print("session done")
'''


def build_index_driver(cfg, index_key, storage, out, build_id, name_suffix=None):
    """Driver that builds one index variant with luceneutil (makeIndex) on the storage's index base."""
    spec = cfg["indices"][index_key]
    st = cfg["storages"][storage]
    os.makedirs(out, exist_ok=True)
    session = {"index": index_key, "spec": spec, "storage": storage, "storage_spec": st, "id": build_id,
               "name_suffix": name_suffix}
    path = os.path.join(out, f"index-{index_key}-{build_id}.py")
    with open(path, "w") as f:
        f.write(INDEX_DRIVER.replace("@SESSION@", repr(json.dumps(session))).replace("@OUT@", repr(os.path.abspath(out))))
    return path


INDEX_DRIVER = r'''#!/usr/bin/env python3
# Generated by run_luceneutil.py index: builds one index variant with luceneutil's makeIndex. Run from
# <luceneutil>/src/python.
import json, os, sys, time
sys.path.insert(0, os.getcwd())
import benchUtil, competition, constants  # noqa: E402
S = json.loads(@SESSION@)
OUT = @OUT@
spec = S["spec"]
constants.INDEX_DIR_BASE = S["storage_spec"]["index_dir_base"]
kw = dict(spec.get("luceneutil_index_kwargs", {}))
if spec.get("facets"):
    kw["facets"] = tuple(tuple(x) for x in spec["facets"])
if spec.get("points_format"):
    kw["javaCommand"] = kw.get("javaCommand", constants.JAVA_COMMAND) + " -Dcoldpath.pointsFormat=" + spec["points_format"]
if S.get("name_suffix"):
    kw["extraNamePart"] = (kw.get("extraNamePart") or "") + S["name_suffix"]
comp = competition.Competition()
idx = comp.newIndex(spec["builder_checkout"], competition.WIKI_MEDIUM_ALL, **kw)
r = benchUtil.RunAlgs(constants.JAVA_COMMAND, True, True)
c = comp.competitor("indexer", spec["builder_checkout"], index=idx)
r.compile(c)
t0 = time.time()
res = r.makeIndex(S["id"], idx)
json.dump({"index": idx.getName(), "path": benchUtil.nameToIndexPath(idx.getName()), "wall_s": time.time() - t0,
           "result": res if isinstance(res, str) else list(res)[:2]},
          open(os.path.join(OUT, f"index-{S['index']}-{S['id']}.json"), "w"), indent=1, default=str)
'''


def _sha256(path):
    import hashlib

    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def copy_index(cfg, name, src_storage, dst_storage, out):
    """Copies one index directory (index/ and facets/) to another storage, then verifies every file's sha256."""
    import shutil

    src = os.path.join(cfg["storages"][src_storage]["index_dir_base"], name)
    dst = os.path.join(cfg["storages"][dst_storage]["index_dir_base"], name)
    if not os.path.isdir(src):
        raise SystemExit(f"no index at {src}")
    if os.path.exists(dst):
        raise SystemExit(f"{dst} exists: refusing to overwrite (remove it yourself if it is a stale copy)")
    shutil.copytree(src, dst)
    files = {}
    for root, _, names in os.walk(src):
        for n in names:
            rel = os.path.relpath(os.path.join(root, n), src)
            a, b = _sha256(os.path.join(src, rel)), _sha256(os.path.join(dst, rel))
            if a != b:
                raise SystemExit(f"checksum differs after copy: {rel}")
            files[rel] = {"sha256": a, "bytes": os.path.getsize(os.path.join(src, rel))}
    rec = {"index": name, "from": src, "to": dst, "files": files, "bytes": sum(f["bytes"] for f in files.values())}
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, f"copy-{name}-{src_storage}-to-{dst_storage}.json"), "w") as f:
        json.dump(rec, f, indent=1)
    print(f"{dst}: {len(files)} files, {rec['bytes']} bytes, checksums equal")
    return rec


def run_driver(cfg, driver):
    pydir = os.path.join(cfg["luceneutil"]["path"], "src", "python")
    cmd = [cfg.get("python", sys.executable), driver]
    print("+", shlex.join(cmd), f"(cwd {pydir})", flush=True)
    return subprocess.run(cmd, cwd=pydir).returncode


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("plan", "run"):
        p = sub.add_parser(name)
        p.add_argument("--config", required=True)
        p.add_argument("--switches", default=os.path.join(here, "switches.json"))
        p.add_argument("--mode", choices=MODES, required=True)
        p.add_argument("--tasks", required=True, help="task file name under luceneutil/tasks")
        p.add_argument("--storage", required=True)
        p.add_argument("--arms", required=True, help="comma list of arm labels; ARM@x repeats an arm (A/A)")
        p.add_argument("--out", required=True)
    ix = sub.add_parser("index")
    ix.add_argument("--config", required=True)
    ix.add_argument("--index", required=True, help="key of [indices]")
    ix.add_argument("--storage", default="EBS")
    ix.add_argument("--build-id", required=True, help="e.g. b1; ingest checks build 3 per Lucene, interleaved")
    ix.add_argument("--out", required=True)
    ix.add_argument("--name-suffix", help="extra index name part, e.g. ib2 for the 2nd..6th ingest-check builds")
    ix.add_argument("--dry-run", action="store_true")
    cp = sub.add_parser("copy")
    cp.add_argument("--config", required=True)
    cp.add_argument("--name", required=True, help="index directory name (printed by the index build)")
    cp.add_argument("--from", dest="src", default="EBS")
    cp.add_argument("--to", dest="dst", default="EFS")
    cp.add_argument("--out", required=True)
    a = ap.parse_args()
    cfg = load_config(a.config)
    if a.cmd == "copy":
        copy_index(cfg, a.name, a.src, a.dst, a.out)
        return
    if a.cmd == "index":
        d = build_index_driver(cfg, a.index, a.storage, a.out, a.build_id, a.name_suffix)
        print(d)
        sys.exit(0 if a.dry_run else run_driver(cfg, d))
    table = sw.load(a.switches)
    driver, session = plan(cfg, table, a.mode, a.tasks, a.storage, a.arms.split(","), a.out)
    print(f"{driver}: {len(session['arms'])} arms, {session['competition']['jvm_count']} JVM iterations, mode {a.mode}")
    for x in session["arms"]:
        print(f"  {x['label']}: " + (f"NOT APPLICABLE {x['not_applicable']}" if x.get("not_applicable") else x["java_command"]))
    if a.cmd == "run":
        sys.exit(run_driver(cfg, driver))


if __name__ == "__main__":
    main()
