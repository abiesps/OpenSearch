#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Kernel readahead per arm session, and the device read-size trace of EBS, for the cold-path agent (stdlib, root).
Rule (common-rules.md, "Kernel readahead DISABLED; the bufferpool owns IO size", REVISED ~20:00): stock arms (S0,
mmap/hybridfs) run with the DEFAULT readahead as mounted (EBS device default; EFS bdi 15360 KiB, set by efs-utils and
re-applied on every mount by AL2023's udev rule 53-ec2-read-ahead-kb.rules); POC arms (S1, S2) run with 0 (the
bufferpool announces each window with POSIX_FADV_WILLNEED, so one window is one device read). Readahead is per
device, so it is set for every arm session (after any mount) and verified before and after it. A mode is "default"
or a value in KiB ("zero" = 0).

Layers of a data path (every one is set and verified):
  EFS / NFS   the mount's bdi (/sys/class/bdi/<0:NN>/read_ahead_kb)
  EBS         the filesystem's block device (its parent disk for a partition) and, for a device-mapper / LUKS
              device, every device below it (/sys/block/<dm>/slaves, recursively): /sys/block/<dev>/queue/read_ahead_kb
              and `blockdev --setra` on /dev/<dev> (sectors = 2 x KiB)
Defaults: the value of each layer the first time it is seen in a boot (before the agent changes it) is its as-mounted
default, kept in state_file with the boot id; after a reboot the defaults are recorded again (the kernel and
efs-utils set them again) and the last mode set for each arm storage is applied again, so a 0 setting survives a
reboot (the agent unit runs after local and remote file systems are mounted). A layer first seen after a remount
(a new NFS bdi) gets its default recorded before any change.
"""
import json
import os
import re
import subprocess
import time

def target_kb(mode, default_kb):
    """The value a layer must have in a mode: its as-mounted default, or the given KiB."""
    if mode == "default":
        return default_kb
    if mode == "zero":
        return 0
    if isinstance(mode, int) or str(mode).isdigit():
        return int(mode)
    raise KeyError(f"readahead mode [{mode}]: default, zero or a value in KiB")


def _read_int(path):
    with open(path) as f:
        return int(f.read().strip())


def _boot_id():
    try:
        with open("/proc/sys/kernel/random/boot_id") as f:
            return f.read().strip()
    except OSError:
        return "unknown"


class Readahead:
    def __init__(self, cfg, storages, resolve):
        """cfg: agent config; storages: {arm: (data_path, device)}; resolve(data_path, device) -> storage dict."""
        self.path = cfg.get("readahead_state_file", "/var/lib/coldpath-agent/readahead-state.json")
        self.storages, self.resolve = storages, resolve
        self.state = self._load()

    # ---- state ----
    def _load(self):
        try:
            with open(self.path) as f:
                s = json.load(f)
        except (OSError, ValueError):
            s = {}
        s.setdefault("defaults", {})
        s.setdefault("modes", {})
        return s

    def _save(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.state, f, indent=1, sort_keys=True)
        os.replace(tmp, self.path)

    # ---- layers ----
    @staticmethod
    def _slaves(name):
        out = []
        d = f"/sys/block/{name}/slaves"
        if os.path.isdir(d):
            for s in sorted(os.listdir(d)):
                out.append(s)
                out += Readahead._slaves(s)
        return out

    def layers(self, st):
        """[{key, kind, sysfs, dev}] of the storage; key is stable within a boot (bdi id or block device name)."""
        if st["nfs"]:
            if not st["bdi"]:
                raise RuntimeError(f"no bdi for the NFS mount of {st['data_path']}")
            return [{"key": f"bdi:{st['bdi']}", "kind": "nfs-bdi", "sysfs": f"/sys/class/bdi/{st['bdi']}/read_ahead_kb",
                     "dev": None}]
        if not st["bdi"]:
            raise RuntimeError(f"no block device for {st['data_path']}")
        top = os.path.basename(os.path.realpath(f"/sys/dev/block/{st['bdi']}"))
        names = [top] + [n for n in self._slaves(top) if n != top]
        out = []
        for n in names:
            q = f"/sys/block/{n}/queue/read_ahead_kb"
            if not os.path.exists(q):
                q = f"/sys/class/block/{n}/bdi/read_ahead_kb"  # partitions below a dm device
            if os.path.exists(q):
                out.append({"key": f"blk:{n}", "kind": "dm" if n.startswith("dm-") else "block", "sysfs": q,
                            "dev": f"/dev/{n}"})
        return out

    @staticmethod
    def _blockdev_ra(dev):
        if not dev or not os.path.exists(dev):
            return None
        p = subprocess.run(["blockdev", "--getra", dev], capture_output=True, text=True)
        return int(p.stdout.strip()) if p.returncode == 0 and p.stdout.strip().isdigit() else None

    def _record_defaults(self, layers):
        boot = _boot_id()
        if self.state.get("boot_id") != boot:
            self.state["boot_id"] = boot
            self.state["defaults"] = {}
        changed = False
        for la in layers:
            if la["key"] not in self.state["defaults"]:
                self.state["defaults"][la["key"]] = {"read_ahead_kb": _read_int(la["sysfs"]), "recorded_t": time.time()}
                changed = True
        if changed:
            self._save()

    def read(self, st, mode=None):
        """Every layer's value and its default; ok when every layer has the mode's target value."""
        layers = self.layers(st)
        self._record_defaults(layers)
        rows, ok = [], True
        for la in layers:
            v = _read_int(la["sysfs"])
            d = self.state["defaults"][la["key"]]["read_ahead_kb"]
            row = {"key": la["key"], "kind": la["kind"], "read_ahead_kb": v, "default_kb": d,
                   "blockdev_ra_sectors": self._blockdev_ra(la["dev"])}
            if mode is not None:
                want = target_kb(mode, d)
                row["target_kb"] = want
                row["ok"] = v == want and (row["blockdev_ra_sectors"] is None or row["blockdev_ra_sectors"] == 2 * want)
                ok = ok and row["ok"]
            rows.append(row)
        return {"data_path": st["data_path"], "nfs": st["nfs"], "mode": mode, "layers": rows,
                "ok": ok if mode is not None else None, "boot_id": self.state.get("boot_id")}

    def set_mode(self, arm, st, mode):
        target_kb(mode, 0)  # validates the mode before anything changes
        layers = self.layers(st)
        self._record_defaults(layers)
        for la in layers:
            want = target_kb(mode, self.state["defaults"][la["key"]]["read_ahead_kb"])
            with open(la["sysfs"], "w") as f:
                f.write(f"{want}\n")
            if la["dev"] and os.path.exists(la["dev"]):
                subprocess.run(["blockdev", "--setra", str(2 * want), la["dev"]], check=True, capture_output=True)
        self.state["modes"][str(arm)] = mode
        self._save()
        return self.read(st, mode)

    def restore_after_boot(self, log):
        """At agent start: record this boot's defaults, then apply the last mode of every arm storage again."""
        for arm, mode in sorted(self.state.get("modes", {}).items()):
            key = None if arm == "None" else arm
            if key not in self.storages:
                continue
            try:
                st = self.resolve(*self.storages[key])
                res = self.set_mode(key, st, mode)
                log(f"readahead {arm}: {mode} -> ok={res['ok']} {[(r['key'], r['read_ahead_kb']) for r in res['layers']]}")
            except Exception as e:  # noqa: BLE001 - a storage that is not mounted yet is set by the next session
                log(f"readahead {arm}: not restored ({e})")


# ---- EBS read-size trace: tracefs block:block_rq_issue in a private trace instance ----
TRACEFS = ("/sys/kernel/tracing", "/sys/kernel/debug/tracing")
INSTANCE = "coldpath_block"
RQ = re.compile(r"block_rq_issue: (\d+),(\d+) (\S+) (\d+) \(.*?\) (\d+) \+ (\d+)")


def _instance():
    for t in TRACEFS:
        if os.path.exists(f"{t}/events/block/block_rq_issue"):
            inst = f"{t}/instances/{INSTANCE}"
            if not os.path.isdir(inst):
                os.mkdir(inst)
            return inst
    raise RuntimeError("tracefs event block/block_rq_issue not available")


def _disks(layers):
    """major:minor of the devices that issue requests (the bottom of a dm stack, or the block device)."""
    names = [la["key"][4:] for la in layers if la["kind"] == "block"] or [la["key"][4:] for la in layers]
    out = set()
    for n in names:
        try:
            with open(f"/sys/class/block/{n}/dev") as f:
                out.add(f.read().strip())
        except OSError:
            pass
    return out


def block_trace_start():
    inst = _instance()
    with open(f"{inst}/trace", "w") as f:
        f.write("")
    with open(f"{inst}/events/block/block_rq_issue/enable", "w") as f:
        f.write("1\n")
    return {"instance": inst}


def block_trace_stop(layers):
    inst = _instance()
    with open(f"{inst}/events/block/block_rq_issue/enable", "w") as f:
        f.write("0\n")
    devs = _disks(layers)
    hist, n = {}, 0
    with open(f"{inst}/trace") as f:
        for line in f:
            m = RQ.search(line)
            if not m or "R" not in m.group(3) or f"{m.group(1)}:{m.group(2)}" not in devs:
                continue
            b = int(m.group(6)) * 512
            hist[b] = hist.get(b, 0) + 1
            n += 1
    with open(f"{inst}/trace", "w") as f:
        f.write("")
    sizes = sorted(hist)
    return {"reads": n, "bytes_hist": {str(k): hist[k] for k in sizes}, "max_bytes": sizes[-1] if sizes else None,
            "total_bytes": sum(k * v for k, v in hist.items()), "devices": sorted(devs), "source": "block:block_rq_issue"}
