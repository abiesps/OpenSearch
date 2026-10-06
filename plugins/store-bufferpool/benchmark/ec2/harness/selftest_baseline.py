#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Self-test of the configuration set of the USER DECISION 2026-10-05 and of jemalloc on every node unit (stdlib only):
  - node_allocator.sh apply under a fake root: the same drop-in in every unit, earlier MALLOC_ARENA_MAX settings
    removed with a backup, the setup record, glibc and glibc-arenas2 drop-ins, refusal of a jemalloc that is not the
    pinned build;
  - the agent's allocator_info on /proc/<pid>/maps and environ text (jemalloc mapped, a preload the loader ignored);
  - arms_baseline.py on the committed templates (USER CORRECTION 2026-10-06): stock OpenSearch with memory mapping is
    first-class on both storages and the outcome reference; the proof-of-concept arms are bufferpool arms with a build;
    stock OpenSearch with the bufferpool (BASE-*) is an optional diagnostic in context_arms, copied from S1-* without
    proof-of-concept-only indices and switches; the superseded bufferpool-baseline set still converts;
  - coldbench end to end against the selftest.py mock: stock memory-mapping, all-off and changes-on runs with build,
    allocator, read hint and plugin checks at every node start, recorded per run and in node_memory; the diagnostic
    runs only when named, without base switches or sort_opt; a node without jemalloc (stock included), an agent before
    v4, a node of the wrong build, another plugin jar, read hint none and readahead drift are refused or discarded.
  python3 selftest_baseline.py
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
sys.path.insert(0, os.path.join(here, "agent"))
import selftest as st  # noqa: E402
PY = sys.executable
RESULTS = []
JE = "/usr/lib64/libjemalloc.so.2"
JE_SHA = "99d2ff028c115cfba4d8e249bca417e03b36e650b257550e687b52ac40be65bf"
PROBE_DROPIN_SHA = "4cfbdf95d6b24114314fb0779c6efe150e41d055e554898222c266391a5e1a57"  # the builder probe's drop-in


def check(ok, what):
    if not ok:
        raise AssertionError(what)
    RESULTS.append(what)


def node_allocator(tmp):
    root = os.path.join(tmp, "root")
    units = os.path.join(root, "etc/systemd/system")
    os.makedirs(units + "/opensearch-s0-ebs.service.d")
    for u in ("opensearch-base-ebs", "opensearch-poc-ebs", "opensearch-s0-ebs"):
        with open(f"{units}/{u}.service", "w") as f:
            f.write("[Service]\nUser=opensearch\n")
            if u == "opensearch-poc-ebs":  # the interim setting written into the unit file (sed, g-so style)
                f.write("Environment=MALLOC_ARENA_MAX=2\n")
            f.write("Environment=OPENSEARCH_JAVA_HOME=/usr/lib/jvm/java\nExecStart=/opt/os/bin/opensearch\n")
    with open(f"{units}/opensearch-s0-ebs.service.d/50-malloc-arena.conf", "w") as f:  # big5-1000 style drop-in
        f.write("[Service]\n# interim\nEnvironment=MALLOC_ARENA_MAX=2\n")
    # a fake systemctl: show reads the unit file and its drop-ins in name order, like systemd
    fake = os.path.join(tmp, "systemctl")
    with open(fake, "w") as f:
        f.write(f"""#!{PY}
import glob, os, sys
units = {units!r}
a = sys.argv[1:]
if a[0] == "daemon-reload":
    sys.exit(0)
prop, unit = a[2], a[-1]
files = [os.path.join(units, unit)] + sorted(glob.glob(os.path.join(units, unit + ".d", "*.conf")))
env, unset = [], []
for p in files:
    for line in open(p):
        line = line.strip()
        if line.startswith("Environment="):
            env.append(line.split("=", 1)[1].strip('"'))
        if line.startswith("UnsetEnvironment="):
            unset += line.split("=", 1)[1].split()
print(" ".join(env if prop == "Environment" else unset))
""")
    os.chmod(fake, 0o755)
    os.makedirs(os.path.join(root, "usr/lib64"))
    sh = os.path.join(here, "node_allocator.sh")
    envb = {**os.environ, "NODE_ALLOCATOR_ROOT": root, "SYSTEMCTL": fake}

    def run(*args):
        return subprocess.run(["bash", sh, *args], capture_output=True, text=True, env=envb)
    with open(os.path.join(root, "usr/lib64/libjemalloc.so.2"), "wb") as f:
        f.write(b"not the pinned build")
    p = run("apply")
    check(p.returncode != 0 and "not the pinned build" in p.stderr, "node_allocator: a jemalloc that is not the pinned "
          "build is refused")
    check(not os.path.exists(f"{units}/opensearch-poc-ebs.service.d"), "node_allocator: nothing written when refused")
    p = run("apply", "--allocator", "glibc-arenas2")
    check(p.returncode == 0 and "all 3 units: same allocator environment" in p.stdout, "node_allocator: glibc-arenas2 on "
          f"every unit ({p.stderr[-300:]})")
    check("MALLOC_ARENA_MAX=2" not in open(f"{units}/opensearch-poc-ebs.service").read() and
          not os.path.exists(f"{units}/opensearch-s0-ebs.service.d/50-malloc-arena.conf"),
          "node_allocator: earlier MALLOC_ARENA_MAX settings removed from the unit file and the drop-in")
    backups = [os.path.join(d, x) for d, _, fs in os.walk(os.path.join(root, "var/lib/coldpath/allocator-backup")) for x in fs]
    check(sorted(os.path.basename(b) for b in backups) == ["50-malloc-arena.conf", "opensearch-poc-ebs.service"],
          f"node_allocator: changed files backed up ({backups})")
    # the real pinned file cannot be shipped: the test points the pinned sha at the fake library through the script copy
    sh2 = os.path.join(tmp, "node_allocator.sh")
    fake_sha = subprocess.run(["shasum", "-a", "256", os.path.join(root, "usr/lib64/libjemalloc.so.2")] if shutil.which("shasum")
                              else ["sha256sum", os.path.join(root, "usr/lib64/libjemalloc.so.2")],
                              capture_output=True, text=True).stdout.split()[0]
    open(sh2, "w").write(open(sh).read().replace(JE_SHA, fake_sha))
    p = subprocess.run(["bash", sh2, "apply"], capture_output=True, text=True, env=envb)
    check(p.returncode == 0 and "all 3 units: same allocator environment" in p.stdout, f"node_allocator: jemalloc on every "
          f"unit ({p.stdout[-300:]} {p.stderr[-300:]})")
    texts = {u: open(f"{units}/{u}.service.d/90-coldpath-allocator.conf").read()
             for u in ("opensearch-base-ebs", "opensearch-poc-ebs", "opensearch-s0-ebs")}
    check(len(set(texts.values())) == 1 and f"Environment=LD_PRELOAD={JE}\n" in texts["opensearch-poc-ebs"] and
          'Environment="MALLOC_CONF=background_thread:true"\n' in texts["opensearch-poc-ebs"] and
          "UnsetEnvironment=MALLOC_ARENA_MAX\n" in texts["opensearch-poc-ebs"], "node_allocator: one identical jemalloc drop-in")
    state = json.load(open(os.path.join(root, "etc/coldpath/allocator.json")))
    check(state["allocator"] == "jemalloc" and state["ld_preload"] == JE and state["malloc_conf"] == "background_thread:true"
          and state["malloc_arena_max"] is None and state["jemalloc_so_sha256"] == fake_sha and len(state["units"]) == 3
          and state["dropin_sha256"] == PROBE_DROPIN_SHA,
          f"node_allocator: setup record; the drop-in is byte-identical to the one the builder probe used ({state})")
    show = subprocess.run(["bash", sh2, "show"], capture_output=True, text=True, env=envb)
    check(show.returncode == 0 and f"LD_PRELOAD={JE} MALLOC_CONF=background_thread:true" in show.stdout,
          "node_allocator: show lists the variables of every unit")
    with open(f"{units}/opensearch-s0-ebs.service.d/99-other.conf", "w") as f:
        f.write("[Service]\nEnvironment=MALLOC_ARENA_MAX=4\n")
    show = subprocess.run(["bash", sh2, "show"], capture_output=True, text=True, env=envb)
    check(show.returncode != 0 and "differ" in show.stderr, "node_allocator: show fails when one unit differs")
    p = subprocess.run(["bash", sh2, "apply", "--malloc-conf", "background_thread:true dirty_decay_ms:1"],
                       capture_output=True, text=True, env=envb)
    check(p.returncode != 0, "node_allocator: a MALLOC_CONF with a space is refused")


def agent_parse():
    import coldpath_agent as ag
    maps = ("7f00-7f10 r--p 00000000 103:01 123 /usr/lib64/libjemalloc.so.2\n"
            "7f10-7f20 r-xp 00010000 103:01 123 /usr/lib64/libjemalloc.so.2\n"
            "7f30-7f40 rw-p 00000000 00:00 0 \n"
            "7f50-7f60 r-xp 00000000 103:01 99 /usr/lib64/libc.so.6\n")
    env = b"PATH=/bin\0LD_PRELOAD=/usr/lib64/libjemalloc.so.2\0MALLOC_CONF=background_thread:true\0"
    a = ag.allocator_info(env, maps)
    check(a == {"ld_preload": JE, "malloc_conf": "background_thread:true", "malloc_arena_max": None,
                "jemalloc_mapped": [JE], "name": "jemalloc"}, f"agent: jemalloc mapped and its variables ({a})")
    b = ag.allocator_info(b"LD_PRELOAD=/opt/missing/libjemalloc.so.2\0MALLOC_ARENA_MAX=2\0", maps.split("\n", 2)[2])
    check(b["name"] == "glibc" and b["jemalloc_mapped"] == [] and b["malloc_arena_max"] == "2",
          "agent: a preload the loader ignored (no mapping) is glibc")


def arms_template():
    import arms_baseline
    import coldbench
    import runguards
    cfg = json.load(open(os.path.join(here, "arms.example.json")))
    check(cfg.get("_baseline", {}).get("set") == "memory-mapping" and cfg["allocator"]["name"] == "jemalloc" and
          cfg["allocator"]["jemalloc_sha256"] == JE_SHA, "template: arms.example.json has the memory-mapping set and "
          "asks for the pinned jemalloc")
    arms = {n: a for n, a in cfg["arms"].items() if not a.get("not_applicable")}
    stock = sorted(n for n, a in arms.items() if a.get("build") == "stock")
    check(stock == ["S0-EBS", "S0-EBS-css", "S0-EFS", "S0-EFS-css"] and
          all(not arms[n]["bufferpool"] and set(arms[n]["store_types"].values()) == {"hybridfs"} and
              coldbench.readahead_mode(arms[n]) == "default" for n in stock),
          "template: stock OpenSearch with memory mapping is first-class on both storages, mounted default readahead")
    check(all(a["build"] == "poc" and a["bufferpool"] and coldbench.readahead_mode(a) == "0"
              for n, a in arms.items() if n not in stock), "template: every other arm is the proof-of-concept build "
          "with the bufferpool, readahead 0")
    check(sorted(cfg["context_arms"]) == ["BASE-EBS", "BASE-EBS-css", "BASE-EFS", "BASE-EFS-css"],
          "template: stock OpenSearch with the bufferpool is only an optional diagnostic (context arms)")
    for s_ in ("EBS", "EFS"):
        b, s1 = cfg["context_arms"][f"BASE-{s_}"], cfg["arms"][f"S1-{s_}"]
        check(b["build"] == "baseline" and b["node"] == f"BASE-{s_}" and b["switches"] == [] and b["index"] == s1["index"]
              and b["cluster_settings"] == s1["cluster_settings"] and
              not any(runguards.poc_only(cfg["indices"][k]) for k in b["open"]) and
              set(b["store_types"].values()) == {"bufferpoolfs"}, f"template: BASE-{s_} is S1-{s_} on the baseline build")
    o = cfg["outcome"]
    check(o["reference"] == "S0-EBS" and o["aa"] == "S0-EBS@a,S0-EBS@b" and
          o["targets"] == ["S2-CORE-EFS", "S2-CORE+PLANNER-EFS"] and
          [x["reference"] for x in o["same_storage"]] == ["S0-EBS", "S0-EFS"] and
          o["attribution_reference"] == {"EBS": "S1-EBS", "EFS": "S1-EFS"},
          "template: outcome against stock OpenSearch with memory mapping on Amazon EBS")
    gen = json.load(open(os.path.join(here, "arms.generic.example.json")))
    check(sorted(n for n, a in gen["arms"].items() if a.get("build") == "stock") == stock and
          sorted(gen["context_arms"]) == sorted(cfg["context_arms"]) and gen["outcome"] == cfg["outcome"],
          "template: the generic template has the same configuration set")
    old, _ = arms_baseline.convert({"indices": {"s": {"name": "x"}}, "arms": {
        "S0-EBS": {"node": "S0-EBS", "storage": "EBS", "bufferpool": False, "index": "s", "open": ["s"]},
        "S1-EBS": {"node": "POC-EBS", "storage": "EBS", "bufferpool": True, "index": "s", "open": ["s"]}}}, {},
        "bufferpool-baseline")
    check(sorted(old["arms"]) == ["BASE-EBS", "S1-EBS"] and list(old["context_arms"]) == ["S0-EBS"] and
          old["outcome"]["reference"] == "BASE-EBS", "converter: the superseded bufferpool-baseline set is still available")
    try:
        arms_baseline.convert({"indices": {}, "arms": {"S0-EBS": {"node": "S0-EBS", "bufferpool": False, "index": "x",
                                                                 "open": ["x"]}}}, {})
        check(False, "converter: the diagnostic arm needs an S1 arm to copy")
    except ValueError:
        check(True, "converter: the diagnostic arm needs an S1 arm to copy")


def e2e(tmp):
    import arms_baseline
    m = st.Mock()
    je = {"ld_preload": JE, "malloc_conf": "background_thread:true", "malloc_arena_max": None, "jemalloc_mapped": [JE],
          "name": "jemalloc", "jemalloc_sha256": {JE: JE_SHA}, "setup": {"allocator": "jemalloc"}}
    glibc = {"ld_preload": None, "malloc_conf": None, "malloc_arena_max": "2", "jemalloc_mapped": [], "name": "glibc",
             "jemalloc_sha256": {}}
    m.allocator = je
    m.binary = "S0-EBS"
    m.indices["big5"]["store_type"] = "hybridfs"
    _, os_url = st.serve(st.make_os_handler(m))
    _, agent_url = st.serve(st.make_agent_handler(m))
    token = os.path.join(tmp, "token")
    open(token, "w").write("selftest-token-0123456789\n")
    vals = os.path.join(tmp, "values.json")
    json.dump(st.VALUES, open(vals, "w"))
    ops = os.path.join(tmp, "ops.json")
    st.run([PY, os.path.join(here, "queries.py"), "build", "--corpus", "big5", "--profile-values", vals, "--out", ops])
    opsd = json.load(open(ops))
    json.dump({**opsd, "ops": [o for o in opsd["ops"] if o["name"] in (opsd["reference_op"], "gen:term_process.name_high",
                                                                      "gen:agg_composite")]}, open(ops, "w"))
    old = {"indices": {"stock_ebs": {"name": "big5", "segments_per_shard": 1, "shards": 2, "storage": "EBS"}},
           "base_switches": [{"method": "POST", "path": "/_bufferpool/sort_opt?bkd_prefetch=false",
                              "verify": {"bkd_prefetch": "false"}}],
           "outcome": {"reference": "S0-EBS", "targets": ["S2-X-EFS"], "aa": "S0-EBS@a,S0-EBS@b", "delta_min": 0.05},
           "arms": {"S0-EBS": {"node": "S0-EBS", "storage": "EBS", "bufferpool": False, "index": "stock_ebs",
                               "open": ["stock_ebs"], "store_types": {"stock_ebs": "hybridfs"}},
                    "S1-EBS": {"node": "POC-EBS", "storage": "EBS", "bufferpool": True, "index": "stock_ebs",
                               "open": ["stock_ebs"], "store_types": {"stock_ebs": "bufferpoolfs"},
                               "cluster_settings": {"search.concurrent_segment_search.mode": "none"}},
                    "S2-X-EBS": {"node": "POC-EBS", "storage": "EBS", "bufferpool": True, "index": "stock_ebs",
                                 "open": ["stock_ebs"], "store_types": {"stock_ebs": "bufferpoolfs"},
                                 "switches": [{"method": "POST", "path": "/_bufferpool/sort_opt?bkd_prefetch=true",
                                               "verify": {"bkd_prefetch": "true"}}]}}}
    cfg, _ = arms_baseline.convert(old, arms_baseline._artifacts(os.path.join(here, "..", "artifacts.json")))
    check(cfg["builds"]["baseline"]["plugin_jar_sha256"].startswith("abd4294a") and
          cfg["builds"]["poc"]["plugin_jar_sha256"].startswith("8086a04c") and
          cfg["builds"]["stock"]["build_hash"] == "b44de786cef", "converter: build hash and plugin jar sha256 per build")
    check(sorted(cfg["arms"]) == ["S0-EBS", "S1-EBS", "S2-X-EBS"] and list(cfg["context_arms"]) == ["BASE-EBS"],
          "converter: S0-EBS stays first-class, BASE-EBS is an optional diagnostic")
    arms_f = os.path.join(tmp, "arms.json")
    json.dump(cfg, open(arms_f, "w"))
    common = ["--arms", arms_f, "--ops", ops, "--url", os_url, "--agent", agent_url, "--token-file", token,
              "--cold-iters", "2", "--warm-warmup", "1", "--warm-iters", "3", "--rounds", "2", "--modes", "cold,warm",
              "--strict", "--no-results"]
    m.calls.clear()
    out = os.path.join(tmp, "s-ok")
    st.run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--arm-list", "S0-EBS,S1-EBS,S2-X-EBS", "--out", out])
    recs = [json.loads(x) for x in open(os.path.join(out, "samples.jsonl"))]
    runs = {r["run_id"]: r for r in recs if r["type"] == "run" and r.get("available")}
    check(len(runs) == 6 and all(r["allocator"]["ok"] and r["allocator"]["node"]["name"] == "jemalloc" for r in runs.values()),
          "e2e: every run, stock memory mapping included, checked jemalloc at node start and records it")
    s0 = [r for r in runs.values() if r["arm"] == "S0-EBS"]
    check(len(s0) == 2 and all(r["build"]["ok"] and r["build"]["node"]["build_hash"].startswith("b44de786cef") and
                               r["readahead"]["mode"] == "default" and not r["context_arm"] and r["plugin"] is None and
                               r["read_hint"] is None for r in s0),
          "e2e: stock OpenSearch with memory mapping runs first-class: build hash checked, mounted default readahead")
    poc = [r for r in runs.values() if r["arm"] != "S0-EBS"]
    check(all(r["build"]["ok"] and r["build"]["node"]["build_target"] == "poc" and "sort_opt" in r["state"] and
              r["readahead"]["mode"] == "0" for r in poc),
          "e2e: the proof-of-concept build keeps its switches, readahead 0, and is checked")
    check(all(r["same_bufferpool"]["storage"] == "EBS" for r in poc), "e2e: same bufferpool settings recorded")
    check(all(r["read_hint"]["ok"] and r["read_hint"]["node"] == "willneed" for r in poc),
          "e2e: every bufferpool run records the effective read hint (willneed)")
    check(all(r["plugin"]["ok"] and r["plugin"]["plugin_source_commit"] == "c83c646b873" and
              r["plugin"]["plugin_jar"][1] == m.plugin_jars["POC"][1] for r in poc),
          "e2e: every bufferpool run records its plugin jar sha256 and the plugin source commit")
    nm = [r for r in recs if r["type"] == "node_memory"]
    check(len(nm) == 6 and all(r["allocator"]["name"] == "jemalloc" for r in nm), "e2e: node_memory records the allocator")
    check(m.indices["big5"]["store_type"] == "hybridfs", "e2e: the session leaves the shared index in the stock store type")
    posts = [c for c in m.calls if c[0] == "os" and c[1] == "POST" and c[2] == "/_bufferpool/sort_opt"]
    check(len(posts) == 2 * 1 + 2 * 2, f"e2e: switches posted only on the proof-of-concept runs ({len(posts)})")
    # the optional diagnostic: stock OpenSearch with the bufferpool, named in the session
    m.calls.clear()
    out = os.path.join(tmp, "s-diag")
    st.run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--rounds", "1", "--arm-list", "S0-EBS,BASE-EBS,S1-EBS",
            "--out", out])
    recs = [json.loads(x) for x in open(os.path.join(out, "samples.jsonl"))]
    base = [r for r in recs if r["type"] == "run" and r.get("arm") == "BASE-EBS"]
    check(len(base) == 1 and base[0]["context_arm"] and base[0]["build"]["ok"] and
          base[0]["build"]["node"]["build_target"] == "stock" and "sort_opt" not in base[0]["state"] and
          base[0]["switches"] == [] and base[0]["readahead"]["mode"] == "0" and
          base[0]["plugin"]["plugin_jar"][1] == m.plugin_jars["BASE"][1],
          "e2e: the named diagnostic runs as stock OpenSearch with the bufferpool: no switches, no sort_opt read, "
          "readahead 0, its own plugin jar")
    posts = [c for c in m.calls if c[0] == "os" and c[1] == "POST" and c[2] == "/_bufferpool/sort_opt"]
    check(len(posts) == 1, f"e2e: the diagnostic gets no base switches ({len(posts)})")
    an = os.path.join(tmp, "analysis")
    st.run([PY, os.path.join(here, "analyze.py"), out, "--base", "S1-EBS", "--boot", "200", "--ni-boot", "100", "--out", an])
    chk = json.load(open(os.path.join(an, "analysis.json"))).get("storage_reads_check") or []
    check(chk and all(r["reference"] == "BASE-EBS" and r["target"] == "S1-EBS" and r["different"] is False and
                      r["ref"]["reads"] == r["ref"]["demand_reads"] + r["ref"]["prefetch_reads"] for r in chk),
          "e2e: analyze compares storage reads and bytes per cold query, diagnostic with the all-off build, demand and "
          "prefetch split")
    an0 = os.path.join(tmp, "analysis-s0")
    st.run([PY, os.path.join(here, "analyze.py"), os.path.join(tmp, "s-ok"), "--base", "S1-EBS", "--boot", "200",
            "--ni-boot", "100", "--out", an0])
    check("storage_reads_check" not in json.load(open(os.path.join(an0, "analysis.json"))),
          "e2e: no storage reads check without the diagnostic in the session")

    def refused(name, arm_list, extra=(), arms=arms_f):
        p = subprocess.run([PY, os.path.join(here, "coldbench.py"), "run", *[c if c != arms_f else arms for c in common],
                            *extra, "--arm-list", arm_list, "--out", os.path.join(tmp, name)], capture_output=True, text=True)
        samples = os.path.join(tmp, name, "samples.jsonl")
        measured = [json.loads(x) for x in open(samples)] if os.path.exists(samples) else []
        return p, [r for r in measured if r["type"] == "sample"]
    m.allocator_by_node = {"POC-EBS": glibc}
    p, smp = refused("s-glibc", "S0-EBS,S1-EBS")
    check(p.returncode != 0 and "node allocator differs" in p.stderr and not any(s["arm"] == "S1-EBS" for s in smp),
          "e2e: a node without jemalloc is refused before it measures")
    m.allocator_by_node = {"S0-EBS": glibc}
    p, smp = refused("s-glibc-s0", "S0-EBS")
    check(p.returncode != 0 and "node allocator differs" in p.stderr and not smp,
          "e2e: a stock node without jemalloc is refused too (the environment is identical in every unit)")
    p, smp = refused("s-glibc-any", "S0-EBS,S1-EBS", ("--allocator", "any", "--rounds", "1"))
    check(p.returncode == 0, f"e2e: --allocator any records only, allocators may differ ({p.stderr[-300:]})")
    m.allocator_by_node = {}
    # readahead back at 15360 at the end of a run (efs-utils watchdog): the run is discarded and re-measured
    m.readahead_get_drift = 2  # the 1st GET /readahead is the check after the index open, the 2nd the end of the run
    out_d = os.path.join(tmp, "s-drift")
    st.run([PY, os.path.join(here, "coldbench.py"), "run", *common, "--rounds", "1", "--arm-list", "S1-EBS", "--out", out_d])
    recs = [json.loads(x) for x in open(os.path.join(out_d, "samples.jsonl"))]
    disc = [r for r in recs if r["type"] == "run_discarded"]
    ends = [r for r in recs if r["type"] == "run_end"]
    check(len(disc) == 1 and disc[0]["readahead_changed"] and [e["valid"] for e in ends] == [False, True] and
          any(r["type"] == "run_requeued" for r in recs), "e2e: a run whose readahead changed is invalid, discarded and re-queued")
    m.read_hint = "none"
    p, smp = refused("s-hint", "S1-EBS")
    check(p.returncode != 0 and "effective read hint is 'none'" in p.stderr and not smp,
          "e2e: a bufferpool node whose read hint is not willneed is refused")
    m.read_hint = "willneed"
    good_jar = m.plugin_jars["POC"]
    m.plugin_jars["POC"] = (good_jar[0], "0" * 64)
    p, smp = refused("s-jar", "S1-EBS")
    check(p.returncode != 0 and "plugin jar" in p.stderr and not smp, "e2e: a node with another plugin jar is refused")
    m.plugin_jars["POC"] = good_jar
    m.allocator = None
    p, smp = refused("s-agent3", "S0-EBS")
    check(p.returncode != 0 and "agent before v4" in p.stderr and not smp, "e2e: an agent without the allocator field is refused")
    m.allocator = je
    wrong = json.loads(json.dumps(cfg))
    wrong["arms"]["S0-EBS"]["node"] = "POC-EBS"  # the stock arm pointed at the proof-of-concept unit
    wrong["context_arms"]["BASE-EBS"]["node"] = "POC-EBS"
    wrong_f = os.path.join(tmp, "arms-wrong.json")
    json.dump(wrong, open(wrong_f, "w"))
    p, smp = refused("s-wrong", "S0-EBS", arms=wrong_f)
    check(p.returncode != 0 and "is not the stock build" in p.stderr and not smp, "e2e: a stock arm on another build is refused")
    p, smp = refused("s-wrong-b", "BASE-EBS", arms=wrong_f)
    check(p.returncode != 0 and "is not the baseline build" in p.stderr and not smp,
          "e2e: the diagnostic on another build is refused")
    bad = json.loads(json.dumps(cfg))
    bad["context_arms"]["BASE-EBS"]["switches"] = old["arms"]["S2-X-EBS"]["switches"]
    bad_f = os.path.join(tmp, "arms-bad.json")
    json.dump(bad, open(bad_f, "w"))
    p, _ = refused("s-bad", "BASE-EBS", arms=bad_f)
    check(p.returncode != 0 and "baseline build has no experiment switches" in p.stderr, "e2e: switches on the baseline refused")
    p, _ = refused("s-unknown", "S0-EFS")
    check(p.returncode != 0 and "unknown arm" in (p.stdout + p.stderr), "e2e: an unknown arm name is refused")


def main():
    tmp = tempfile.mkdtemp(prefix="selftest-baseline-")
    try:
        node_allocator(tmp)
        agent_parse()
        arms_template()
        e2e(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print(f"SELFTEST-BASELINE PASS: {len(RESULTS)} checks")


if __name__ == "__main__":
    main()
