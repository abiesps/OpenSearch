#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Amazon EFS backend connection count of an NFS mount, and the pre-conditioning that brings it to a target
(common-rules.md "Amazon EFS connection count is a measured variable"; storage-model.md section 4.2.2).

With the efs-utils TLS mount the NFS client talks to efs-proxy on 127.0.0.1:<port> (mount option port=), and
efs-proxy holds the TCP connections to the mount target on port 2049. efs-proxy 3.3.2 starts with ONE connection and
adds 4 more (5) only after one 3 s stats window at >= 300 MiB/s (src/proxy/src/controller.rs,
DEFAULT_SCALE_UP_CONFIG and should_scale_up); it never reduces them until the proxy incarnation restarts. The count
is not configurable. High-concurrency read latency depends on it (one connection is the "slow" level), so every EFS
sample records it and comparisons hold it fixed.

- backend_connections(mount): established TCP connections of the mount's efs-proxy process to remote port 2049
  (socket inodes of /proc/<pid>/fd matched in /proc/net/tcp and tcp6). A mount without efs-proxy (plain NFS to the
  mount target) counts the kernel's connections to addr=<ip>:2049 (inode 0). stdlib only.
- precondition(mount, ...): reads a scratch file on the same mount with O_DIRECT 1 MiB reads (no page-cache
  footprint, no index file touched) from 16 threads until the count is at least the target, then stops reading and
  waits until the count has held for stable_s (30 s), reading again whenever it drops; or the timeout passes.
  Why 30 s: on the storage host 21 of 69 scale-ups were followed within 7-25 s by a lost backend connection that
  restarted the proxy incarnation (back to 1 connection; storage-model.md section 4.2.2). Creates the scratch file
  (O_DIRECT 1 MiB writes) the first time.
"""
import mmap
import os
import random
import re
import threading
import time

NFS_PORT = 2049
TCP_ESTABLISHED = "01"
SCRATCH_NAME = "coldpath-efs-precondition.bin"
SCRATCH_BYTES = 4 << 30
IO = 1 << 20


def mount_option(options, key):
    m = re.search(r"(?:^|,)" + re.escape(key) + r"=([^,]+)", options or "")
    return m.group(1) if m else None


def _hex_addr(h):
    ip, port = h.split(":")
    if len(ip) == 8:  # IPv4, little-endian
        b = bytes.fromhex(ip)[::-1]
        addr = ".".join(str(x) for x in b)
    else:  # IPv6: four little-endian 32-bit words
        words = [bytes.fromhex(ip[i:i + 8])[::-1].hex() for i in range(0, 32, 8)]
        addr = ":".join("".join(words)[i:i + 4] for i in range(0, 32, 4))
    return addr, int(port, 16)


def parse_net_tcp(text):
    """Rows of /proc/net/tcp or tcp6: (local, lport, remote, rport, state, inode)."""
    rows = []
    for line in text.splitlines()[1:]:
        f = line.split()
        if len(f) < 10:
            continue
        la, lp = _hex_addr(f[1])
        ra, rp = _hex_addr(f[2])
        rows.append((la, lp, ra, rp, f[3], int(f[9])))
    return rows


def proxy_pid(port, proc="/proc"):
    """pid of the efs-proxy that serves the mount on 127.0.0.1:<port> (its config file name ends in .<port>)."""
    for name in os.listdir(proc):
        if not name.isdigit():
            continue
        try:
            with open(os.path.join(proc, name, "cmdline"), "rb") as f:
                argv = [a.decode(errors="replace") for a in f.read().split(b"\0") if a]
        except OSError:
            continue
        if argv and os.path.basename(argv[0]) == "efs-proxy" and any(a.endswith(f".{port}") for a in argv[1:]):
            return int(name)
    return None


def _socket_inodes(pid, proc):
    out = set()
    d = os.path.join(proc, str(pid), "fd")
    for fd in os.listdir(d):
        try:
            link = os.readlink(os.path.join(d, fd))
        except OSError:
            continue
        m = re.match(r"socket:\[(\d+)\]", link)
        if m:
            out.add(int(m.group(1)))
    return out


def _tcp_rows(proc):
    rows = []
    for n in ("tcp", "tcp6"):
        p = os.path.join(proc, "net", n)
        if os.path.exists(p):
            with open(p) as f:
                rows.extend(parse_net_tcp(f.read()))
    return rows


def backend_connections(mount, proc="/proc"):
    """{count, via_proxy, proxy_pid, mount_port, remotes, local_ports} for an NFS mount (dict from /proc/mounts)."""
    opts = mount.get("options", "")
    addr, port = mount_option(opts, "addr"), mount_option(opts, "port")
    via_proxy = addr in ("127.0.0.1", "::1") and port is not None
    rows = [r for r in _tcp_rows(proc) if r[3] == NFS_PORT and r[4] == TCP_ESTABLISHED]
    if via_proxy:
        pid = proxy_pid(int(port), proc)
        if pid is None:
            return {"count": None, "via_proxy": True, "proxy_pid": None, "mount_port": int(port),
                    "error": f"no efs-proxy process for port {port}"}
        inodes = _socket_inodes(pid, proc)
        rows = [r for r in rows if r[5] in inodes]
    else:
        pid = None
        rows = [r for r in rows if r[2] == addr and r[5] == 0]  # kernel-owned NFS client sockets
    return {"count": len(rows), "via_proxy": via_proxy, "proxy_pid": pid, "mount_port": int(port) if port else None,
            "remotes": sorted({r[2] for r in rows}), "local_ports": sorted(r[1] for r in rows)}


def _aligned(n):
    return mmap.mmap(-1, n)  # anonymous mappings are page aligned, as O_DIRECT needs


def ensure_scratch(path, size=SCRATCH_BYTES):
    if os.path.exists(path) and os.path.getsize(path) >= size:
        return {"created": False, "bytes": os.path.getsize(path)}
    t0 = time.monotonic()
    buf = _aligned(IO)
    buf.write(os.urandom(IO))
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_DIRECT, 0o600)
    try:
        for off in range(0, size, IO):
            os.pwritev(fd, [buf], off)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.rename(tmp, path)
    return {"created": True, "bytes": size, "elapsed_s": round(time.monotonic() - t0, 2)}


def precondition(mount, target, timeout_s=60.0, stable_s=30.0, threads=16, scratch=None, proc="/proc", count_fn=None,
                 scratch_bytes=SCRATCH_BYTES):
    """
    Reads the scratch file with O_DIRECT 1 MiB reads from `threads` threads while backend_connections() < target
    (efs-proxy's scale-up needs >= 300 MiB/s for one 3 s window), and returns once the count has been at the target
    for stable_s seconds without reading, or after timeout_s.
    Returns {ok, count, timeline [[s, count]], read_MBps, scratch}. A target of 1 or less reads nothing.
    """
    count_fn = count_fn or (lambda: backend_connections(mount, proc)["count"])
    first = count_fn()
    if target <= 1 or (first is not None and first >= target):
        return {"ok": first is not None and first >= target, "count": first, "timeline": [[0.0, first]],
                "read_bytes": 0, "skipped": True}
    path = scratch or os.path.join(mount["mountpoint"], SCRATCH_NAME)
    made = ensure_scratch(path, scratch_bytes)
    size = os.path.getsize(path)
    stop, heavy = threading.Event(), threading.Event()
    heavy.set()
    done = [0] * threads

    def reader(k):
        buf = _aligned(IO)
        rng = random.Random(k)
        fd = os.open(path, os.O_RDONLY | os.O_DIRECT)
        try:
            while not stop.is_set():
                if not heavy.wait(0.2):
                    continue
                os.preadv(fd, [buf], rng.randrange(size // IO) * IO)
                done[k] += IO
        finally:
            os.close(fd)

    ths = [threading.Thread(target=reader, args=(k,), daemon=True) for k in range(threads)]
    t0 = time.monotonic()
    for t in ths:
        t.start()
    tl, last, since = [], first, time.monotonic()
    try:
        while time.monotonic() - t0 < timeout_s:
            time.sleep(0.5)
            c = count_fn()
            tl.append([round(time.monotonic() - t0, 1), c])
            if c is not None and c >= target:
                heavy.clear()  # scaled up: stop reading, watch whether it holds
            else:
                heavy.set()
            if c != last:
                last, since = c, time.monotonic()
            elif c is not None and c >= target and time.monotonic() - since >= stable_s:
                break
    finally:
        stop.set()
        for t in ths:
            t.join(10)
    el = time.monotonic() - t0
    return {"ok": last is not None and last >= target, "count": last, "count_before": first, "timeline": tl,
            "read_bytes": sum(done), "read_MBps": round(sum(done) / el / 1e6, 1), "elapsed_s": round(el, 1),
            "scratch": {"path": path, **made}, "skipped": False}
