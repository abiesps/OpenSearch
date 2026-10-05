#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Cold-path agent for the OpenSearch DATA NODE. Runs as root (it writes /proc/sys/vm/drop_caches and pages out the
JVM's file mappings), stdlib only (Python >= 3.8), so it runs on a plain Amazon Linux 2023 host.

The load generator calls it over HTTP before and after every cold iteration. It only runs the fixed actions below and
the node start/stop commands that are written in its own config file; a caller can name an arm but never send a
command. Bind it to the private address and allow the port only from the load generator's security group; every
request must carry the shared token (header X-Coldpath-Token) from token_file.

Endpoints (JSON in and out):
  GET  /health                       agent version, config summary, JVM pid
  POST /cache/drop?pageout=1         (1) pageout=1: process_madvise(MADV_PAGEOUT) on every JVM mapping of a file under
                                     <data_path>/.../indices/ (mmap'd Lucene files of stock hybridfs/mmapfs stay in the
                                     page cache after drop_caches while they are mapped, so drop_caches alone is not
                                     cold for S0); (2) sync; (3) echo 3 > /proc/sys/vm/drop_caches. Returns the time of
                                     each step and meminfo Cached before/after.
  Every endpoint takes ?arm=NAME (an arm of the config) to select that arm's data_path / device / mount; without
  it the last started arm's (else the default data_path). An NFS (EFS) data path reports NFS client counters
  (/proc/self/mountstats: READ ops, bytes, RTT, execute time) instead of block-device diskstats.
  GET  /cache/residency?uuids=a,b    page-cache residency (mincore) of every file under the index directories of the
                                     given index UUIDs (all indices if absent): resident bytes total and per extension
  GET  /index/du?uuids=a,b           on-disk size of those index directories (apparent, allocated, Lucene files)
  GET  /snapshot                     JVM /proc/<pid>/io, /proc/diskstats of the data device (EBS) or NFS mountstats of
                                     the data mount (EFS), and a monotonic timestamp
  GET  /host                         kernel, CPU, memory, data mount, device queue settings, EBS volume id
  POST /readahead?kb=N|default       sets read_ahead_kb of the data path's backing device (/sys/class/bdi/<maj:min>,
                                     the NFS mount's bdi on EFS) so the kernel does not enlarge reads; "default"
                                     restores the value recorded the first time (state_file)
  GET  /readahead?mode=default|KIB   kernel readahead of every layer of the data path (NFS bdi; EBS block device and
                                     any dm/LUKS layer below it, read_ahead_kb and blockdev --getra), the recorded
                                     as-mounted default of each layer, and ok = every layer at the mode's target
  POST /readahead/mode?mode=default|KIB  sets every layer (default = as mounted: stock arms; 0 = POC arms, the
                                     bufferpool owns the IO size), reads back, returns the same as GET /readahead; the
                                     last mode per arm storage is applied again after a reboot (coldpath_readahead.py)
  POST /trace/block/_start, _stop    tracefs block:block_rq_issue (private instance): histogram of the size of every
                                     read request issued to the data path's disks (EBS device read sizes)
  POST /trace/nfs/_start, _stop      tracefs nfs:nfs_initiate_read: histogram of the byte count of every READ RPC
                                     sent to EFS between start and stop (the per-read request size)
  _stop?uuids=a,b (both traces)      also attributes every read to index-file data (EBS: FIEMAP extents of the Lucene
                                     files; EFS: READ fileid = inode) or to other reads (metadata), with windows,
                                     reads crossing a 128 KiB window and extent splits (coldpath_readattr.py)
  GET  /node/status                  running JVM pid and command line, last started arm
  POST /node/stop                    runs every configured stop command; waits until no JVM matches
  POST /node/restart  {"arm": "S1"}  stop as above, then the arm's start command; returns the new pid

Config (JSON, path in argv[1], example agent.example.json):
  listen, port, token_file, data_path, device (block device name, e.g. "nvme1n1"; derived from data_path if null),
  jvm_match (substring of the JVM command line), arms: {ARM: {"start": [argv...], "stop": [argv...],
  "data_path": optional, "device": optional}} (one arm per binary x storage, e.g. S0-EBS, S0-EFS, POC-EBS, POC-EFS),
  stop_all: [[argv...], ...], command_timeout_s, stop_timeout_s.
"""
import ctypes
import hmac
import json
import os
import platform
import re
import subprocess
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import coldpath_readahead  # noqa: E402 - installed next to this file
import coldpath_readattr  # noqa: E402 - installed next to this file

VERSION = "1"
PAGE = os.sysconf("SC_PAGE_SIZE")
MADV_PAGEOUT = 21
SYS_PIDFD_OPEN = 434  # same number on x86_64 and aarch64 (generic syscall table)
SYS_PROCESS_MADVISE = 440
IOV_BATCH = 512  # <= UIO_MAXIOV (1024)
PROT_READ, MAP_SHARED = 1, 1
WINDOW = 1 << 30  # mincore window: 1 GiB of a file at a time, so the vector stays at 256 KiB

_libc = ctypes.CDLL(None, use_errno=True)
_libc.syscall.restype = ctypes.c_long
_libc.mmap.restype = ctypes.c_void_p
_libc.mmap.argtypes = (ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long)
_libc.munmap.argtypes = (ctypes.c_void_p, ctypes.c_size_t)
_libc.mincore.argtypes = (ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p)
MAP_FAILED = ctypes.c_void_p(-1).value


class Iovec(ctypes.Structure):
    _fields_ = [("iov_base", ctypes.c_void_p), ("iov_len", ctypes.c_size_t)]


class Agent:
    def __init__(self, config):
        self.cfg = config
        self.jvm_match = config.get("jvm_match", "org.opensearch.bootstrap.OpenSearch")
        # names of the directories whose files the residency check covers: OpenSearch keeps Lucene files in
        # <uuid>/<shard>/index/; luceneutil also keeps a taxonomy index in <name>/facets/
        self.residency_dirs = frozenset(config.get("residency_dirs", ["index"]))
        self.lock = threading.Lock()  # one state-changing action at a time
        self.last_arm = None
        # (data_path, device) per arm; resolved to mounts on every call, so a mount made after agent start is seen
        self.storages = {None: (config["data_path"], config.get("device"))}
        for name, arm in config.get("arms", {}).items():
            self.storages[name] = (arm.get("data_path", config["data_path"]), arm.get("device", config.get("device")))
        self.readahead = coldpath_readahead.Readahead(config, self.storages, self._storage)

    def _storage(self, data_path, device):
        """Where an arm keeps its data: the path, the mount it is on (EBS block device or NFS/EFS), the device."""
        dp = os.path.realpath(data_path)
        mount = self._mount_of(dp)
        nfs = bool(mount and mount["fstype"].startswith("nfs"))
        dev = None if nfs else (device or self._device_of(dp))
        bdi = self._bdi_of(dp)
        ra = None
        if bdi:
            try:
                with open(f"/sys/class/bdi/{bdi}/read_ahead_kb") as f:
                    ra = int(f.read())
            except OSError:
                pass
        return {"data_path": dp, "mount": mount, "nfs": nfs, "device": dev, "bdi": bdi, "read_ahead_kb": ra}

    @staticmethod
    def _bdi_of(path):
        """Backing-device name (major:minor) whose read_ahead_kb governs page-cache readahead for files under path."""
        st = os.stat(path)
        dev = f"{os.major(st.st_dev)}:{os.minor(st.st_dev)}"
        if os.path.exists(f"/sys/class/bdi/{dev}"):
            return dev  # NFS (0:NN) and whole disks
        try:  # a partition: the bdi belongs to its parent disk
            part = os.path.realpath(f"/sys/dev/block/{dev}")
            with open(os.path.join(os.path.dirname(part), "dev")) as f:
                parent = f.read().strip()
            return parent if os.path.exists(f"/sys/class/bdi/{parent}") else None
        except OSError:
            return None

    def set_readahead(self, st, kb):
        """Sets read_ahead_kb of the storage's bdi; 'default' restores the value recorded at first agent start."""
        if not st["bdi"]:
            raise RuntimeError(f"no bdi for {st['data_path']}")
        path = f"/sys/class/bdi/{st['bdi']}/read_ahead_kb"
        state_file = self.cfg.get("state_file", "/var/lib/coldpath-agent/readahead-defaults.json")
        try:
            with open(state_file) as f:
                defaults = json.load(f)
        except (OSError, ValueError):
            defaults = {}
        if st["bdi"] not in defaults:
            defaults[st["bdi"]] = st["read_ahead_kb"]
            os.makedirs(os.path.dirname(state_file), exist_ok=True)
            with open(state_file, "w") as f:
                json.dump(defaults, f)
        value = defaults[st["bdi"]] if str(kb) == "default" else int(kb)
        with open(path, "w") as f:
            f.write(f"{value}\n")
        with open(path) as f:
            got = int(f.read())
        return {"bdi": st["bdi"], "read_ahead_kb": got, "requested": kb, "default": defaults[st["bdi"]]}

    # ---- NFS read-size trace (tracefs nfs:nfs_initiate_read: one event per READ RPC, with its byte count) ----
    TRACEFS = ("/sys/kernel/tracing", "/sys/kernel/debug/tracing")

    def _tracefs(self):
        for t in self.TRACEFS:
            if os.path.exists(f"{t}/events/nfs/nfs_initiate_read/enable"):
                return t
        raise RuntimeError("tracefs event nfs/nfs_initiate_read not available")

    def nfs_trace_start(self):
        t = self._tracefs()
        with open(f"{t}/trace", "w") as f:
            f.write("")
        with open(f"{t}/events/nfs/nfs_initiate_read/enable", "w") as f:
            f.write("1\n")
        return {"tracefs": t}

    NFS_READ = re.compile(r"nfs_initiate_read: fileid=[0-9a-f]+:[0-9a-f]+:(\d+) fhandle=\S+ offset=(\d+) count=(\d+)")

    def nfs_trace_stop(self, index_dirs=None):
        """Byte-count histogram of the READ RPCs since start; with index_dirs, also their attribution to index files
        (fileid = inode number; coldpath_readattr.attribute_nfs)."""
        t = self._tracefs()
        with open(f"{t}/events/nfs/nfs_initiate_read/enable", "w") as f:
            f.write("0\n")
        hist = {}
        n = 0
        reads = []
        with open(f"{t}/trace") as f:
            for line in f:
                m = re.search(r"\bcount=(\d+)", line)
                if m and "nfs_initiate_read" in line:
                    c = int(m.group(1))
                    hist[c] = hist.get(c, 0) + 1
                    n += 1
                    d = self.NFS_READ.search(line)
                    if d:
                        reads.append((int(d.group(1)), int(d.group(2)), int(d.group(3))))
        with open(f"{t}/trace", "w") as f:
            f.write("")
        sizes = sorted(hist)
        out = {"reads": n, "bytes_hist": {str(k): hist[k] for k in sizes}, "max_bytes": sizes[-1] if sizes else None,
               "total_bytes": sum(k * v for k, v in hist.items())}
        if index_dirs is not None:
            if len(reads) != n:
                out["attribution_gap"] = f"{n - len(reads)} READ events without fileid/offset"
            else:
                out["attribution"] = coldpath_readattr.attribute_nfs(reads, index_dirs)
        return out

    def trace_index_dirs(self, q, st):
        """?uuids=a,b on a trace stop: the arm's index directories whose files the device reads are attributed to."""
        uuids = set(x for x in q.get("uuids", "").split(",") if x)
        return self.index_dirs(st["data_path"], uuids) if uuids else None

    def storage(self, arm):
        """The storage of an arm (?arm=NAME), else of the last started arm, else the default data_path."""
        if arm is not None and arm not in self.storages:
            raise KeyError(f"unknown arm [{arm}]; configured: {sorted(k for k in self.storages if k)}")
        return self._storage(*self.storages[arm if arm is not None else self.last_arm])

    @staticmethod
    def _mount_of(path):
        best = None
        with open("/proc/mounts") as f:
            for line in f:
                p = line.split()
                mp = p[1]
                if (path == mp or path.startswith(mp.rstrip("/") + "/")) and (best is None or len(mp) > len(best["mountpoint"])):
                    best = {"source": p[0], "mountpoint": mp, "fstype": p[2], "options": p[3]}
        return best

    # ---- JVM ----
    def jvm_pids(self):
        pids = []
        for name in os.listdir("/proc"):
            if not name.isdigit():
                continue
            try:
                with open(f"/proc/{name}/cmdline", "rb") as f:
                    cmd = f.read().replace(b"\0", b" ").decode("utf-8", "replace")
            except OSError:
                continue
            if self.jvm_match in cmd and "coldpath_agent" not in cmd:
                pids.append(int(name))
        return sorted(pids)

    def jvm_pid(self):
        pids = self.jvm_pids()
        if len(pids) > 1:
            raise RuntimeError(f"more than one JVM matches [{self.jvm_match}]: {pids}")
        return pids[0] if pids else None

    # ---- page cache ----
    def _index_mappings(self, pid, data_path):
        """[(start, length)] of the JVM's mappings of files under <data_path>/**/indices/."""
        out = []
        with open(f"/proc/{pid}/maps") as f:
            for line in f:
                parts = line.split(None, 5)
                if len(parts) < 6:
                    continue
                path = parts[5].strip()
                if not path.startswith(data_path) or "/indices/" not in path:
                    continue
                lo, hi = (int(x, 16) for x in parts[0].split("-"))
                out.append((lo, hi - lo))
        return out

    def pageout(self, pid, data_path):
        """process_madvise(MADV_PAGEOUT) over the JVM's index-file mappings: unmaps and reclaims those pages."""
        maps = self._index_mappings(pid, data_path)
        result = {"mappings": len(maps), "mapped_bytes": sum(n for _, n in maps), "advised_bytes": 0, "errors": []}
        if not maps:
            return result
        pidfd = _libc.syscall(SYS_PIDFD_OPEN, ctypes.c_int(pid), ctypes.c_uint(0))
        if pidfd < 0:
            result["errors"].append(f"pidfd_open: errno {ctypes.get_errno()}")
            return result
        try:
            for i in range(0, len(maps), IOV_BATCH):
                batch = maps[i:i + IOV_BATCH]
                vec = (Iovec * len(batch))(*[Iovec(lo, n) for lo, n in batch])
                rc = _libc.syscall(SYS_PROCESS_MADVISE, ctypes.c_int(pidfd), vec, ctypes.c_size_t(len(batch)),
                                   ctypes.c_int(MADV_PAGEOUT), ctypes.c_uint(0))
                if rc >= 0:
                    result["advised_bytes"] += rc
                    continue
                # a mapping went away (segment closed by a merge); retry one range at a time
                for lo, n in batch:
                    one = (Iovec * 1)(Iovec(lo, n))
                    rc = _libc.syscall(SYS_PROCESS_MADVISE, ctypes.c_int(pidfd), one, ctypes.c_size_t(1),
                                       ctypes.c_int(MADV_PAGEOUT), ctypes.c_uint(0))
                    if rc >= 0:
                        result["advised_bytes"] += rc
                    else:
                        err = ctypes.get_errno()
                        if len(result["errors"]) < 10:
                            result["errors"].append(f"process_madvise {lo:x}+{n}: errno {err}")
        finally:
            os.close(pidfd)
        return result

    @staticmethod
    def meminfo():
        out = {}
        with open("/proc/meminfo") as f:
            for line in f:
                k, v = line.split(":", 1)
                out[k] = int(v.split()[0]) * 1024
        return {k: out.get(k) for k in ("MemTotal", "MemFree", "MemAvailable", "Cached", "Buffers", "Mapped", "Dirty")}

    def drop(self, pageout, st):
        res = {"meminfo_before": self.meminfo(), "data_path": st["data_path"], "nfs": st["nfs"]}
        t0 = time.monotonic()
        if pageout:
            pid = self.jvm_pid()
            res["pageout"] = self.pageout(pid, st["data_path"]) if pid else {"skipped": "no JVM"}
        t1 = time.monotonic()
        os.sync()
        t2 = time.monotonic()
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("3\n")
        t3 = time.monotonic()
        res.update({"pageout_ms": (t1 - t0) * 1e3, "sync_ms": (t2 - t1) * 1e3, "drop_ms": (t3 - t2) * 1e3,
                    "meminfo_after": self.meminfo()})
        return res

    @staticmethod
    def index_dirs(data_path, uuids):
        dirs = []
        for root, subdirs, _ in os.walk(data_path):
            if os.path.basename(root) == "indices":
                for u in subdirs:
                    if not uuids or u in uuids:
                        dirs.append(os.path.join(root, u))
                subdirs[:] = []
        return dirs

    @staticmethod
    def _resident(path):
        """(file bytes, resident bytes) via mincore on a fresh read-only mapping (mapping does not fault pages in)."""
        fd = os.open(path, os.O_RDONLY)
        try:
            size = os.fstat(fd).st_size
            resident = 0
            off = 0
            while off < size:
                n = min(WINDOW, size - off)
                addr = _libc.mmap(None, n, PROT_READ, MAP_SHARED, fd, off)
                if addr in (None, MAP_FAILED):
                    raise OSError(ctypes.get_errno(), f"mmap {path}")
                try:
                    pages = (n + PAGE - 1) // PAGE
                    vec = (ctypes.c_ubyte * pages)()
                    if _libc.mincore(addr, n, vec) != 0:
                        raise OSError(ctypes.get_errno(), f"mincore {path}")
                    resident += (pages - bytes(vec).count(0)) * PAGE
                finally:
                    _libc.munmap(addr, n)
                off += n
            return size, min(resident, size)
        finally:
            os.close(fd)

    def residency(self, uuids, st):
        t0 = time.monotonic()
        total = resident = files = 0
        by_ext = {}
        top = []
        for d in self.index_dirs(st["data_path"], uuids):
            for root, _, names in os.walk(d):
                if os.path.basename(root) not in self.residency_dirs:
                    continue  # Lucene files only (<uuid>/<shard>/index/), not translog or _state
                for name in names:
                    p = os.path.join(root, name)
                    try:
                        size, res = self._resident(p)
                    except (FileNotFoundError, PermissionError):
                        continue
                    except OSError as e:
                        if e.errno == 2:
                            continue
                        raise
                    files += 1
                    total += size
                    resident += res
                    ext = name.rsplit(".", 1)[-1] if "." in name else name
                    e = by_ext.setdefault(ext, {"bytes": 0, "resident_bytes": 0})
                    e["bytes"] += size
                    e["resident_bytes"] += res
                    if res:
                        top.append((res, p))
        top.sort(reverse=True)
        return {"files": files, "bytes": total, "resident_bytes": resident, "by_ext": by_ext,
                "top_resident": [{"path": p, "resident_bytes": r} for r, p in top[:10]],
                "elapsed_ms": (time.monotonic() - t0) * 1e3}

    def du(self, uuids, st):
        """On-disk size of the index directories (apparent bytes and allocated bytes), Lucene files and all files."""
        out = {}
        for d in self.index_dirs(st["data_path"], uuids):
            apparent = allocated = lucene = files = 0
            for root, _, names in os.walk(d):
                for name in names:
                    try:
                        s = os.stat(os.path.join(root, name))
                    except FileNotFoundError:
                        continue
                    files += 1
                    apparent += s.st_size
                    allocated += s.st_blocks * 512
                    if os.path.basename(root) == "index":
                        lucene += s.st_size
            out[os.path.basename(d)] = {"path": d, "files": files, "apparent_bytes": apparent,
                                        "allocated_bytes": allocated, "lucene_bytes": lucene}
        return out

    # ---- IO counters ----
    @staticmethod
    def _device_of(path):
        st = os.stat(path)
        major, minor = os.major(st.st_dev), os.minor(st.st_dev)
        try:
            link = os.path.realpath(f"/sys/dev/block/{major}:{minor}")
        except OSError:
            return None
        name = os.path.basename(link)
        # a partition: report its parent disk for queue settings, keep the partition for diskstats
        return name

    @staticmethod
    def diskstats(device):
        if not device:
            return None
        with open("/proc/diskstats") as f:
            for line in f:
                p = line.split()
                if len(p) >= 14 and p[2] == device:
                    v = [int(x) for x in p[3:]]
                    return {"device": device, "reads": v[0], "reads_merged": v[1], "sectors_read": v[2],
                            "read_ms": v[3], "writes": v[4], "sectors_written": v[6], "in_flight": v[8],
                            "io_ms": v[9], "weighted_io_ms": v[10]}
        return None

    @staticmethod
    def proc_io(pid):
        if pid is None:
            return None
        out = {}
        with open(f"/proc/{pid}/io") as f:
            for line in f:
                k, v = line.split(":", 1)
                out[k.strip()] = int(v)
        return out

    @staticmethod
    def nfsstats(mountpoint):
        """
        NFS client counters of one mount from /proc/self/mountstats (what nfsiostat/mountstat read): the bytes: line
        (normal/direct/server read bytes, read pages) and the READ op line (ops, transmissions, timeouts, bytes sent and
        received, cumulative queue, RTT and execute ms), plus GETATTR/LOOKUP/ACCESS op counts (metadata round trips).
        """
        if not mountpoint:
            return None
        with open("/proc/self/mountstats") as f:
            lines = f.read().splitlines()
        out, inside = None, False
        for line in lines:
            if line.startswith("device "):
                inside = f" mounted on {mountpoint} with fstype nfs" in line
                if inside:
                    out = {"mountpoint": mountpoint, "ops": {}}
                continue
            if not inside:
                continue
            t = line.strip()
            if t.startswith("bytes:"):
                v = [int(x) for x in t.split()[1:]]
                out.update({"normal_read_bytes": v[0], "direct_read_bytes": v[2], "server_read_bytes": v[4],
                            "read_pages": v[6] if len(v) > 6 else None})
            elif t.startswith("xprt:"):
                out["xprt"] = t
            else:
                name = t.split(":", 1)[0]
                if name in ("READ", "GETATTR", "LOOKUP", "ACCESS", "OPEN", "READDIR", "READDIRPLUS"):
                    v = [int(x) for x in t.split(":", 1)[1].split()]
                    if len(v) >= 8:
                        out["ops"][name] = {"ops": v[0], "trans": v[1], "timeouts": v[2], "bytes_sent": v[3],
                                            "bytes_recv": v[4], "queue_ms": v[5], "rtt_ms": v[6], "execute_ms": v[7]}
        return out

    def snapshot(self, st):
        pid = self.jvm_pid()
        return {"t_mono": time.monotonic(), "pid": pid, "proc_io": self.proc_io(pid), "disk": self.diskstats(st["device"]),
                "nfs": self.nfsstats(st["mount"]["mountpoint"]) if st["nfs"] else None}

    def host(self, st):
        def read(p):
            try:
                with open(p) as f:
                    return f.read().strip()
            except OSError:
                return None
        cpu = None
        for line in (read("/proc/cpuinfo") or "").splitlines():
            if line.lower().startswith(("model name", "cpu part")):
                cpu = line.split(":", 1)[1].strip()
                break
        mount = st["mount"]
        dev = st["device"]
        disk = re.sub(r"p\d+$", "", dev) if dev and dev.startswith("nvme") else (re.sub(r"\d+$", "", dev) if dev else None)
        q = f"/sys/block/{disk}/queue" if disk else None
        return {
            "uname": platform.uname()._asdict(), "cpu": cpu, "nproc": os.cpu_count(), "meminfo": self.meminfo(),
            "data_path": st["data_path"], "mount": mount, "nfs": st["nfs"], "device": dev, "disk": disk,
            "bdi": st["bdi"], "bdi_read_ahead_kb": st["read_ahead_kb"],
            "nfs_xprt": (self.nfsstats(mount["mountpoint"]) or {}).get("xprt") if st["nfs"] else None,
            "read_ahead_kb": read(f"{q}/read_ahead_kb") if q else None,
            "scheduler": read(f"{q}/scheduler") if q else None, "nr_requests": read(f"{q}/nr_requests") if q else None,
            "max_sectors_kb": read(f"{q}/max_sectors_kb") if q else None,
            "rotational": read(f"{q}/rotational") if q else None,
            "device_model": read(f"/sys/block/{disk}/device/model") if disk else None,
            "device_serial": read(f"/sys/block/{disk}/device/serial") if disk else None,  # EBS: the volume id
            "thp": read("/sys/kernel/mm/transparent_hugepage/enabled"), "swappiness": read("/proc/sys/vm/swappiness"),
        }

    # ---- node lifecycle ----
    def _run(self, argv):
        t = self.cfg.get("command_timeout_s", 300)
        p = subprocess.run(argv, capture_output=True, text=True, timeout=t)
        return {"argv": argv, "rc": p.returncode, "stdout": p.stdout[-2000:], "stderr": p.stderr[-2000:]}

    def stop(self):
        runs = [self._run(argv) for argv in self.cfg.get("stop_all", [])]
        for arm in self.cfg.get("arms", {}).values():
            if arm.get("stop"):
                runs.append(self._run(arm["stop"]))
        deadline = time.monotonic() + self.cfg.get("stop_timeout_s", 180)
        while self.jvm_pids() and time.monotonic() < deadline:
            time.sleep(0.5)
        left = self.jvm_pids()
        if left:
            raise RuntimeError(f"JVM still running after stop: {left}; commands {runs}")
        return runs

    def restart(self, arm):
        arms = self.cfg.get("arms", {})
        if arm not in arms:
            raise KeyError(f"unknown arm [{arm}]; configured: {sorted(arms)}")
        stopped = self.stop()
        started = self._run(arms[arm]["start"])
        if started["rc"] != 0:
            raise RuntimeError(f"start of {arm} failed: {started}")
        deadline = time.monotonic() + self.cfg.get("start_timeout_s", 180)
        pid = None
        while pid is None and time.monotonic() < deadline:
            pid = self.jvm_pid()
            time.sleep(0.2)
        self.last_arm = arm
        return {"arm": arm, "pid": pid, "stop": stopped, "start": started}


def make_handler(agent, token):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            sys.stderr.write("%s %s\n" % (time.strftime("%H:%M:%S"), fmt % args))

        def _send(self, status, obj):
            data = json.dumps(obj, default=str).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self):
            n = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(n) if n else b""
            return json.loads(raw) if raw else {}

        def _dispatch(self, method):
            if not hmac.compare_digest(self.headers.get("X-Coldpath-Token", ""), token):
                self._send(403, {"error": "bad or missing X-Coldpath-Token"})
                return
            u = urllib.parse.urlsplit(self.path)
            q = dict(urllib.parse.parse_qsl(u.query))
            body = self._body() if method == "POST" else {}
            try:
                route = (method, u.path)
                st = agent.storage(q.get("arm"))
                if route == ("GET", "/health"):
                    out = {"version": VERSION, "storages": {str(k): agent._storage(*v) for k, v in agent.storages.items()},
                           "jvm_pid": agent.jvm_pid(), "arms": sorted(agent.cfg.get("arms", {}))}
                elif route == ("POST", "/cache/drop"):
                    with agent.lock:
                        out = agent.drop(q.get("pageout", "1") not in ("0", "false"), st)
                elif route == ("GET", "/cache/residency"):
                    uuids = set(x for x in q.get("uuids", "").split(",") if x)
                    out = agent.residency(uuids, st)
                elif route == ("GET", "/index/du"):
                    out = agent.du(set(x for x in q.get("uuids", "").split(",") if x), st)
                elif route == ("GET", "/snapshot"):
                    out = agent.snapshot(st)
                elif route == ("GET", "/host"):
                    out = agent.host(st)
                elif route == ("POST", "/readahead"):
                    with agent.lock:
                        out = agent.set_readahead(st, q["kb"])
                elif route == ("GET", "/readahead"):
                    out = agent.readahead.read(st, q.get("mode"))
                elif route == ("POST", "/readahead/mode"):
                    with agent.lock:
                        out = agent.readahead.set_mode(q.get("arm"), st, q["mode"])
                elif route == ("POST", "/trace/block/_start"):
                    out = coldpath_readahead.block_trace_start()
                elif route == ("POST", "/trace/block/_stop"):
                    out = coldpath_readahead.block_trace_stop(agent.readahead.layers(st), agent.trace_index_dirs(q, st))
                elif route == ("POST", "/trace/nfs/_start"):
                    out = agent.nfs_trace_start()
                elif route == ("POST", "/trace/nfs/_stop"):
                    out = agent.nfs_trace_stop(agent.trace_index_dirs(q, st))
                elif route == ("GET", "/node/status"):
                    pid = agent.jvm_pid()
                    cmd = None
                    if pid:
                        with open(f"/proc/{pid}/cmdline", "rb") as f:
                            cmd = f.read().replace(b"\0", b" ").decode("utf-8", "replace")
                    out = {"pid": pid, "cmdline": cmd, "last_arm": agent.last_arm}
                elif route == ("POST", "/node/stop"):
                    with agent.lock:
                        out = {"stop": agent.stop()}
                elif route == ("POST", "/node/restart"):
                    with agent.lock:
                        out = agent.restart(body.get("arm") or q.get("arm"))
                else:
                    self._send(404, {"error": f"no route {method} {u.path}"})
                    return
                self._send(200, out)
            except KeyError as e:
                self._send(400, {"error": str(e)})
            except Exception as e:  # noqa: BLE001 - report every failure to the caller
                self._send(500, {"error": f"{type(e).__name__}: {e}"})

        def do_GET(self):  # noqa: N802
            self._dispatch("GET")

        def do_POST(self):  # noqa: N802
            self._dispatch("POST")

    return Handler


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: coldpath_agent.py CONFIG.json")
    with open(sys.argv[1]) as f:
        cfg = json.load(f)
    with open(cfg["token_file"]) as f:
        token = f.read().strip()
    if len(token) < 16:
        sys.exit("token_file must hold a token of at least 16 characters")
    agent = Agent(cfg)
    agent.readahead.restore_after_boot(lambda m: sys.stderr.write(m + "\n"))
    server = ThreadingHTTPServer((cfg.get("listen", "127.0.0.1"), int(cfg.get("port", 9700))), make_handler(agent, token))
    sys.stderr.write(f"coldpath agent {VERSION} on {server.server_address}, storages {agent.storages}\n")
    server.serve_forever()


if __name__ == "__main__":
    main()
