#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Mock-session checks of what coldbench does at the start of every arm run (harness-ext review findings 1, 3, 4), with
selftest.py's mock node and agent (stdlib, no root, no AWS). The mock models Linux per-file readahead: an index file
copies the storage's bdi readahead when it is opened, and keeps it.
  (1) readahead before the node restart and the index open: S0-EFS and S1-EFS share one EFS bdi and alternate (A B B A),
      so a run that sets readahead after the open reads files with the previous arm's value. Checked on the call log
      (POST /readahead/mode with the arm's mode before /node/restart and before every _open of the run), on the files
      (every opened index file holds its arm's value), and with --no-restart and probe (indices re-opened after the
      set).
  (3) IO configuration: a bufferpool run on a node whose block / random / sequential read size differs from
      8 / 32 / 128 KiB, or that does not report them, is refused.
  (4) other open indices: other_indices=close closes them before the arm's indices open, =refuse stops the session,
      default (record) lists them and changes nothing, a bad value is refused.
  selftest_order.py
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
import selftest as st  # noqa: E402

PY = sys.executable
N = [0]


def check(cond, what):
    if not cond:
        raise AssertionError(what)
    N[0] += 1


def mode_of(node):
    return "default" if node.startswith("S0") else "0"


def runs_of(calls):
    """Splits the call log at each POST /readahead/mode: [(mode, arm, [calls until the next set])]."""
    out = []
    for c in calls:
        if c[0] == "agent" and c[2] == "/readahead/mode":
            out.append((c[3], [c]))
        elif out:
            out[-1][1].append(c)
    return out


def coldbench(args, ok=True):
    p = subprocess.run([PY, os.path.join(here, "coldbench.py"), *args], capture_output=True, text=True)
    if ok and p.returncode != 0:
        print(p.stdout[-3000:], p.stderr[-3000:])
        raise SystemExit(f"FAILED: coldbench {' '.join(args)}")
    return p


def main():
    tmp = tempfile.mkdtemp(prefix="selftest-order-")
    try:
        m = st.Mock()
        _, os_url = st.serve(st.make_os_handler(m))
        _, agent_url = st.serve(st.make_agent_handler(m))
        token = os.path.join(tmp, "token")
        open(token, "w").write("selftest-token-0123456789\n")
        vals = os.path.join(tmp, "values.json")
        json.dump(st.VALUES, open(vals, "w"))
        ops = os.path.join(tmp, "ops.json")
        subprocess.run([PY, os.path.join(here, "queries.py"), "build", "--corpus", "big5", "--profile-values", vals,
                        "--out", ops], check=True, capture_output=True)
        d = json.load(open(ops))
        d["ops"] = [o for o in d["ops"] if o["name"] in (d["reference_op"], "gen:term_process.name_high")]
        json.dump(d, open(ops, "w"))

        def arms_file(name, **extra):
            cfg = {"indices": {"stock_efs": {"name": "big5", "segments_per_shard": 1, "shards": 2}},
                   "base_switches": [], "outcome": {"reference": "S0-EFS", "targets": ["S1-EFS"]},
                   "arms": {"S0-EFS": {"node": "S0-EFS", "storage": "EFS", "bufferpool": False, "index": "stock_efs",
                                       "open": ["stock_efs"], "store_types": {"stock_efs": "hybridfs"}},
                            "S1-EFS": {"node": "POC-EFS", "storage": "EFS", "bufferpool": True, "index": "stock_efs",
                                       "open": ["stock_efs"], "store_types": {"stock_efs": "bufferpoolfs"}}}, **extra}
            f = os.path.join(tmp, name)
            json.dump(cfg, open(f, "w"))
            return f
        common = ["--ops", ops, "--url", os_url, "--agent", agent_url, "--token-file", token]
        run_args = ["--modes", "cold", "--cold-iters", "1"]

        # (1) A B B A on one EFS bdi, with other_indices=close (big5_split is open and not in the arms file)
        af = arms_file("arms-close.json", other_indices="close")
        out = os.path.join(tmp, "s-abba")
        coldbench(["run", "--arms", af, *common, *run_args, "--arm-list", "S0-EFS,S1-EFS", "--rounds", "2", "--out", out,
                   "--strict"])
        sets = runs_of(m.calls)
        check([s[0] for s in sets] == ["default", "0", "0", "default"], f"(1) one readahead set per run, A B B A: {sets}")
        for mode, calls in sets:
            restarts = [c for c in calls if c[0] == "agent" and c[2] == "/node/restart"]
            check(len(restarts) == 1 and mode == mode_of(restarts[0][4]),
                  f"(1) /readahead/mode {mode} precedes the /node/restart of its arm ({restarts})")
            opens = [i for i, c in enumerate(calls) if c[0] == "os" and c[2].endswith("/_open")]
            first_restart = next(i for i, c in enumerate(calls) if c[0] == "agent" and c[2] == "/node/restart")
            check(opens and min(opens) > first_restart, "(1) every _open of the run comes after the set and the restart")
        check(len(m.opened) == 4 and all(o["readahead"] == mode_of(o["node"]) for o in m.opened),
              f"(1) every opened index file holds its arm's readahead: {m.opened}")
        recs = [json.loads(x) for x in open(os.path.join(out, "samples.jsonl"))]
        runs = [r for r in recs if r["type"] == "run"]
        check(all(r["readahead"]["ok"] and r["readahead_after_open"]["ok"] for r in runs), "(1) readahead verified after the open")
        check([r["io_config"] is None for r in runs] == [True, False, False, True], "(3) io_config recorded for bufferpool runs")
        check(all(r["io_config"]["ok"] for r in runs if r["io_config"]), "(3) 8/32/128 KiB accepted")
        check(runs[0]["other_indices"] == {"policy": "close", "open": ["big5_split"], "closed": ["big5_split"]} and
              all(r["other_indices"]["open"] == [] for r in runs[1:]) and m.indices["big5_split"]["status"] == "close",
              f"(4) other_indices=close: closed before the first run ({runs[0]['other_indices']})")

        # (1) --no-restart: set, then close and re-open the indices (the node keeps running)
        m.bdi["EFS"] = "0"  # left by a POC arm
        # a session ends with its indices closed; an index opened by hand since then must be closed after the set
        m.indices["big5"]["status"] = "open"
        m.calls.clear()
        n_open = len(m.opened)
        coldbench(["run", "--arms", af, *common, *run_args, "--arm-list", "S0-EFS", "--rounds", "1", "--no-restart",
                   "--out", os.path.join(tmp, "s-norestart"), "--strict"])
        paths = [(c[0], c[2]) for c in m.calls]
        i_set = paths.index(("agent", "/readahead/mode"))
        i_close, i_open = paths.index(("os", "/big5/_close")), paths.index(("os", "/big5/_open"))
        check(i_set < i_close < i_open and ("agent", "/node/restart") not in paths,
              "(1) --no-restart: readahead set, then the index closed and opened again")
        check(len(m.opened) == n_open + 1 and m.opened[-1]["readahead"] == "default", "(1) --no-restart: file opened with default")

        # (1) probe: the same order
        m.bdi["EFS"] = "0"
        m.calls.clear()
        p = coldbench(["probe", "--arms", af, *common, "--arm", "S0-EFS", "--op", "gen:term_process.name_high",
                       "--out", os.path.join(tmp, "s-probe")])
        paths = [(c[0], c[2]) for c in m.calls]
        check(paths.index(("agent", "/readahead/mode")) < paths.index(("os", "/big5/_open")) and
              m.opened[-1]["readahead"] == "default", "(1) probe: readahead set before the index is opened again")
        check('"mode": "probe"' in p.stdout, "(1) probe ran its cold iteration")

        # (3) IO configuration differs / not reported -> refused
        for k, (change, why) in enumerate((({"random_read_size": 131072}, "differ"), ({"block_size": 131072}, "differ"),
                                           ({"sequential_read_size": None}, "does not report"))):
            m.bp_io = {"block_size": 8192, "random_read_size": 32768, "sequential_read_size": 131072}
            m.bp_io.update(change)
            m.bp_io = {k: v for k, v in m.bp_io.items() if v is not None}
            p = coldbench(["run", "--arms", af, *common, *run_args, "--arm-list", "S1-EFS", "--rounds", "1",
                           "--out", os.path.join(tmp, f"s-io-{k}")], ok=False)
            check(p.returncode != 0 and "IO configuration" in p.stderr and why in p.stderr,
                  f"(3) {change}: refused ({p.stderr[-400:]})")
        m.bp_io = {"block_size": 8192, "random_read_size": 32768, "sequential_read_size": 131072}
        p = coldbench(["run", "--arms", af, *common, *run_args, "--arm-list", "S1-EFS", "--rounds", "1",
                       "--bp-random-read-size", "65536", "--out", os.path.join(tmp, "s-io-arg")], ok=False)
        check(p.returncode != 0 and "IO configuration" in p.stderr, "(3) the required sizes come from the arguments")

        # (4) refuse / record (default) / bad value
        m.indices["big5_split"]["status"] = "open"
        p = coldbench(["run", "--arms", arms_file("arms-refuse.json", other_indices="refuse"), *common, *run_args,
                       "--arm-list", "S0-EFS", "--rounds", "1", "--out", os.path.join(tmp, "s-refuse")], ok=False)
        check(p.returncode != 0 and "outside the arms file" in p.stderr and m.indices["big5_split"]["status"] == "open",
              "(4) other_indices=refuse stops the session and closes nothing")
        out = os.path.join(tmp, "s-record")
        coldbench(["run", "--arms", arms_file("arms-record.json"), *common, *run_args, "--arm-list", "S0-EFS",
                   "--rounds", "1", "--out", out, "--strict"])
        rec = [json.loads(x) for x in open(os.path.join(out, "samples.jsonl")) if '"type": "run"' in x][0]
        check(rec["other_indices"] == {"policy": "record", "open": ["big5_split"], "closed": []} and
              m.indices["big5_split"]["status"] == "open", "(4) default: recorded, nothing closed (main workflow unchanged)")
        p = coldbench(["run", "--arms", arms_file("arms-bad.json", other_indices="ignore"), *common, *run_args,
                       "--arm-list", "S0-EFS", "--rounds", "1", "--out", os.path.join(tmp, "s-bad")], ok=False)
        check(p.returncode != 0 and "other_indices" in p.stderr, "(4) a bad other_indices value is refused")
    finally:
        shutil.rmtree(tmp)
    print(f"SELFTEST-ORDER PASS: {N[0]} checks")


if __name__ == "__main__":
    main()
