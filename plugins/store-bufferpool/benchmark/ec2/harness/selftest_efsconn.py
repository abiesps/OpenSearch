#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Unit checks of agent/coldpath_efsconn.py (stdlib, no root, no AWS), against a fake /proc:
  - /proc/net/tcp and tcp6 rows decoded (IPv4 little-endian, IPv6 words, ports, state, inode)
  - backend_connections of an efs-utils TLS mount: only ESTABLISHED sockets of the mount's own efs-proxy (found by
    the port in its config file name) to remote port 2049; another mount's proxy, TIME_WAIT rows and other ports are
    not counted; a missing proxy gives count None with the reason
  - a plain NFS mount (no proxy): the kernel's sockets (inode 0) to addr=<ip>:2049
  - precondition: nothing is read when the count is already at the target or the target is 1; otherwise it reads until
    the count is at the target and stable (fake count; the O_DIRECT read itself runs on Linux only)
  selftest_efsconn.py
"""
import os
import shutil
import sys
import tempfile

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(here, "agent"))
import coldpath_efsconn as ec  # noqa: E402

N = [0]


def check(cond, what):
    if not cond:
        raise AssertionError(what)
    N[0] += 1


def v4(ip, port):
    return "".join(f"{int(x):02X}" for x in reversed(ip.split("."))) + f":{port:04X}"


HDR = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"


def row(i, local, remote, state, inode):
    return f"   {i}: {local} {remote} {state} 00000000:00000000 00:00000000 00000000     0        0 {inode} 1 0 20 4 30 10 -1\n"


def fake_proc(root):
    os.makedirs(os.path.join(root, "net"))
    mt = "10.42.3.113"
    rows = [row(0, v4("10.42.8.228", 47368), v4(mt, 2049), "01", 1001),   # proxy A, established
            row(1, v4("10.42.8.228", 47370), v4(mt, 2049), "01", 1002),   # proxy A, established
            row(2, v4("10.42.8.228", 47372), v4(mt, 2049), "06", 1003),   # proxy A, TIME_WAIT: not counted
            row(3, v4("10.42.8.228", 47374), v4(mt, 443), "01", 1004),    # proxy A, other port: not counted
            row(4, v4("10.42.8.228", 47376), v4(mt, 2049), "01", 2001),   # proxy B (another mount)
            row(5, v4("127.0.0.1", 36964), v4("127.0.0.1", 20655), "01", 0),  # NFS client -> proxy A
            row(6, v4("10.42.8.228", 900), v4("10.42.9.9", 2049), "01", 0)]   # kernel NFS socket of a plain mount
    open(os.path.join(root, "net", "tcp"), "w").write(HDR + "".join(rows))
    # IPv6: ::ffff:10.42.3.113 is not used by efs-proxy; one row that must decode and not count
    open(os.path.join(root, "net", "tcp6"), "w").write(
        HDR + row(0, "0000000000000000FFFF0000E4082A0A:B9A0", "0000000000000000FFFF0000710D2A0A:0801", "0A", 3001))
    for pid, port, inodes in ((1234, 20655, (1001, 1002, 1003, 1004)), (5678, 20891, (2001,))):
        d = os.path.join(root, str(pid))
        os.makedirs(os.path.join(d, "fd"))
        open(os.path.join(d, "cmdline"), "wb").write(
            b"/sbin/efs-proxy\0/var/run/efs/stunnel-config.fs-060fb5da13f9a7e5b.mnt.efs.%d\0--tls\0" % port)
        for k, ino in enumerate(inodes):
            os.symlink(f"socket:[{ino}]", os.path.join(d, "fd", str(10 + k)))
        os.symlink("/dev/null", os.path.join(d, "fd", "0"))
    os.makedirs(os.path.join(root, "77", "fd"))
    open(os.path.join(root, "77", "cmdline"), "wb").write(b"/usr/bin/java\0-Xmx1g\0")
    os.makedirs(os.path.join(root, "self"))  # non-numeric entries are skipped


def main():
    tmp = tempfile.mkdtemp(prefix="selftest-efsconn-")
    try:
        proc = os.path.join(tmp, "proc")
        fake_proc(proc)
        rows = ec.parse_net_tcp(open(os.path.join(proc, "net", "tcp")).read())
        check(rows[0] == ("10.42.8.228", 47368, "10.42.3.113", 2049, "01", 1001), rows[0])
        r6 = ec.parse_net_tcp(open(os.path.join(proc, "net", "tcp6")).read())
        check(r6[0][3] == 2049 and r6[0][4] == "0A" and r6[0][5] == 3001, r6)
        check(ec.mount_option("rw,vers=4.1,port=20655,addr=127.0.0.1", "port") == "20655", "mount option port")
        check(ec.mount_option("rw,vers=4.1", "port") is None, "missing option")
        tls = {"mountpoint": "/mnt/efs", "options": "rw,vers=4.1,rsize=1048576,hard,noresvport,proto=tcp,port=20655,"
                                                    "timeo=600,retrans=2,sec=sys,clientaddr=127.0.0.1,addr=127.0.0.1"}
        b = ec.backend_connections(tls, proc)
        check(b["via_proxy"] and b["proxy_pid"] == 1234 and b["mount_port"] == 20655, b)
        check(b["count"] == 2 and b["local_ports"] == [47368, 47370] and b["remotes"] == ["10.42.3.113"], b)
        other = dict(tls, options=tls["options"].replace("port=20655", "port=20891"))
        check(ec.backend_connections(other, proc)["count"] == 1, "the other mount's proxy has its own count")
        gone = dict(tls, options=tls["options"].replace("port=20655", "port=20999"))
        g = ec.backend_connections(gone, proc)
        check(g["count"] is None and "no efs-proxy" in g["error"], g)
        plain = {"mountpoint": "/mnt/nfs", "options": "rw,vers=4.1,proto=tcp,addr=10.42.9.9"}
        p = ec.backend_connections(plain, proc)
        check(not p["via_proxy"] and p["count"] == 1 and p["remotes"] == ["10.42.9.9"], p)
        # precondition: no read when already at the target, or for a target of 1
        r = ec.precondition(tls, 5, count_fn=lambda: 5)
        check(r["ok"] and r["skipped"] and r["read_bytes"] == 0, r)
        r = ec.precondition(tls, 1, count_fn=lambda: 1)
        check(r["ok"] and r["skipped"], r)
        r = ec.precondition(tls, 1, count_fn=lambda: 5)
        check(r["ok"] and r["skipped"], "a target of 1 never reads; coldbench refuses a count above the target")
        if hasattr(os, "O_DIRECT") and sys.platform.startswith("linux"):
            scratch = os.path.join(tmp, "scratch.bin")
            seq = iter([1, 1, 1, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5])
            try:
                r = ec.precondition({"mountpoint": tmp}, 5, timeout_s=20, stable_s=1.0, threads=2, scratch=scratch,
                                    scratch_bytes=16 << 20, count_fn=lambda: next(seq, 5))
                check(r["ok"] and r["count"] == 5 and r["count_before"] == 1 and r["read_bytes"] > 0, r)
                # a lost connection right after the scale-up (back to 1): it reads again and waits for a new stable 5
                seq = iter([1, 1, 5, 5, 1, 1, 1, 5] + [5] * 20)
                r = ec.precondition({"mountpoint": tmp}, 5, timeout_s=20, stable_s=1.5, threads=2, scratch=scratch,
                                    scratch_bytes=16 << 20, count_fn=lambda: next(seq, 5))
                check(r["ok"] and r["count"] == 5 and [c for _, c in r["timeline"]][:8] == [1, 5, 5, 1, 1, 1, 5, 5]
                      and r["timeline"][-1][0] >= 3.5, r["timeline"])
            except OSError as e:  # tmpfs refuses O_DIRECT; the data node runs it on EFS
                print(f"  (O_DIRECT read not available in {tmp}: {e}; checked on the data node)")
        else:
            print("  (O_DIRECT not available here; the pre-conditioning read is checked on the data node)")
    finally:
        shutil.rmtree(tmp)
    print(f"SELFTEST-EFSCONN PASS: {N[0]} checks")


if __name__ == "__main__":
    main()
