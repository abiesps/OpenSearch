#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Unit checks of the readahead / device-read rules (stdlib, no root, no AWS):
  - coldbench.readahead_mode: stock arms "default", bufferpool arms 0 (or --poc-read-ahead-kb), explicit "readahead",
    the removed read_ahead_kb arm key refused
  - agent coldpath_readahead: mode targets, Readahead state with a fake sysfs (defaults recorded once per boot before
    any change, set / read back / ok, the last mode applied again after a reboot), the block_rq_issue trace parser
  - coldbench.device_vs_bufferpool: one device read per window passes; split reads, extra bytes or a size outside
    the bufferpool's classes fail; mountstats / diskstats fallback
  selftest_readahead.py
"""
import json
import os
import shutil
import sys
import tempfile

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
sys.path.insert(0, os.path.join(here, "agent"))
import coldbench  # noqa: E402
import coldpath_readahead as cr  # noqa: E402

N = [0]


def check(cond, what):
    if not cond:
        raise AssertionError(what)
    N[0] += 1


def main():
    # modes per arm
    check(coldbench.readahead_mode({"bufferpool": False}) == "default", "stock arm: as mounted")
    check(coldbench.readahead_mode({"bufferpool": True}) == "0", "bufferpool arm: 0 (no kernel readahead)")
    check(coldbench.readahead_mode({"bufferpool": True}, 256) == "256", "bufferpool arm: --poc-read-ahead-kb")
    check(coldbench.readahead_mode({"bufferpool": True, "readahead": "default"}) == "default", "explicit arm mode")
    for bad in ({"read_ahead_kb": 128}, {"readahead": "fast"}):
        try:
            coldbench.readahead_mode(bad)
            check(False, f"refused {bad}")
        except ValueError:
            check(True, f"refused {bad}")
    check(cr.target_kb("default", 15360) == 15360 and cr.target_kb("128", 15360) == 128 and cr.target_kb("zero", 9) == 0,
          "agent mode targets")
    # Readahead state machine on a fake sysfs (one NFS bdi)
    tmp = tempfile.mkdtemp(prefix="selftest-ra-")
    try:
        sysfs = os.path.join(tmp, "read_ahead_kb")
        open(sysfs, "w").write("15360\n")
        boot = ["b1"]
        cr._boot_id = lambda: boot[0]
        st = {"data_path": "/mnt/efs/x", "nfs": True, "bdi": "0:53"}
        ra = cr.Readahead({"readahead_state_file": os.path.join(tmp, "state.json")}, {"POC-EFS": ("/mnt/efs/x", None)},
                          lambda dp, dev: st)
        ra.layers = lambda s: [{"key": "bdi:0:53", "kind": "nfs-bdi", "sysfs": sysfs, "dev": None}]
        r = ra.read(st, "default")
        check(r["ok"] and r["layers"][0]["default_kb"] == 15360, "as-mounted default recorded")
        r = ra.set_mode("POC-EFS", st, "0")
        check(r["ok"] and open(sysfs).read().strip() == "0", "POC mode set and read back")
        check(not ra.read(st, "default")["ok"], "default check fails while 0 is set")
        r = ra.set_mode("POC-EFS", st, "default")
        check(r["ok"] and open(sysfs).read().strip() == "15360", "default restored from the recorded value")
        ra.set_mode("POC-EFS", st, "0")
        # reboot: the kernel / efs-utils set the default again; the agent restarts and applies the last mode again
        open(sysfs, "w").write("15360\n")
        boot[0] = "b2"
        ra2 = cr.Readahead({"readahead_state_file": os.path.join(tmp, "state.json")}, {"POC-EFS": ("/mnt/efs/x", None)},
                           lambda dp, dev: st)
        ra2.layers = ra.layers
        logs = []
        ra2.restore_after_boot(logs.append)
        check(open(sysfs).read().strip() == "0" and ra2.state["defaults"]["bdi:0:53"]["read_ahead_kb"] == 15360,
              f"after a reboot: default re-recorded, last mode applied again ({logs})")
        try:
            ra2.set_mode("POC-EFS", st, "fast")
            check(False, "bad mode refused")
        except KeyError:
            check(True, "bad mode refused")
    finally:
        shutil.rmtree(tmp)
    # block trace line parser
    line = "  java-1234 [003] ..... 100.5: block_rq_issue: 259,1 R 131072 () 2048 + 256 [java]"
    m = cr.RQ.search(line)
    check(m and m.group(1) == "259" and m.group(3) == "R" and int(m.group(6)) * 512 == 131072, "block_rq_issue parsed")
    # device reads vs bufferpool windows
    bp = {"reads": 3, "prefetch_reads": 1, "bytes_read": 3 * 131072 + 32768, "reads_by_size": {"32768": 1, "131072": 3}}
    good = {"bp": bp, "nfs_read_sizes": {"reads": 4, "total_bytes": 3 * 131072 + 32768, "bytes_hist": {"32768": 1, "131072": 3}}}
    check(coldbench.device_vs_bufferpool(good)["ok"] is True, "one device read per window passes")
    split = {"bp": bp, "nfs_read_sizes": {"reads": 128, "total_bytes": 3 * 131072 + 32768, "bytes_hist": {"4096": 104}}}
    check(coldbench.device_vs_bufferpool(split)["ok"] is False, "4 KiB split reads fail")
    big = {"bp": bp, "block_read_sizes": {"reads": 4, "total_bytes": 4 * 131072 + 32768,
                                          "bytes_hist": {"32768": 1, "131072": 2, "262144": 1}}}
    d = coldbench.device_vs_bufferpool(big)
    check(d["ok"] is False and "unmatched:262144" in d["device_reads_by_class"], "a read larger than a window fails")
    eof = {"bp": {"reads": 1, "prefetch_reads": 0, "bytes_read": 70000, "reads_by_size": {"131072": 1}},
           "block_read_sizes": {"reads": 1, "total_bytes": 70000, "bytes_hist": {"70000": 1}}}
    check(coldbench.device_vs_bufferpool(eof)["ok"] is True, "a window clipped at end of file passes")
    ms = {"bp": bp, "nfs": {"READ": {"ops": 4}, "server_read_bytes": 3 * 131072 + 32768}}
    check(coldbench.device_vs_bufferpool(ms)["ok"] is True, "mountstats fallback")
    ds = {"bp": bp, "disk": {"reads": 5, "read_bytes": 3 * 131072 + 32768}}
    check(coldbench.device_vs_bufferpool(ds)["ok"] is False, "diskstats fallback catches an extra read")
    old = {"bp": {"loads": 3}}
    check(coldbench.device_vs_bufferpool(old)["ok"] is None, "old binary without read counters: a gap, not a pass")
    # reads_by_size deltas (dict counters) in io_delta
    pre = {"bp": {"files": {"_0.kdd": {"reads": 1, "reads_by_size": {"131072": 1}}}}}
    post = {"bp": {"files": {"_0.kdd": {"reads": 3, "reads_by_size": {"131072": 2, "32768": 1}}}}}
    io = coldbench.io_delta(pre, post)
    check(io["bp"]["reads"] == 2 and io["bp"]["reads_by_size"] == {"131072": 1, "32768": 1}, json.dumps(io))
    print(f"SELFTEST-READAHEAD PASS: {N[0]} checks")


if __name__ == "__main__":
    main()
