#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Attribution of the device reads of one cold iteration to index-file data or to other reads (stdlib, root), for the
one-device-read-per-window check of the bufferpool arms (common-rules.md, "DECISION ~22:00", device check).
Why: with readahead 0 and POSIX_FADV_WILLNEED every bufferpool window is one storage read, but a block device still
shows (1) a window split into two requests where the file's extents are not physically contiguous (XFS
fragmentation), and (2) small reads of file-system metadata (inodes, extent btrees) on the first access after
drop_caches. Both are real cold-path IO of every arm, not kernel readahead. So each device read is classified:
  data   inside the extents (EBS: FIEMAP of the Lucene files under the arm's index directories) or the inode (EFS: NFS
         READ fileid = st_ino) of an index file; its file range is clipped at end of file (the device reads whole
         pages, the bufferpool's last window of a file is clipped at end of file);
  other  everything else on the data device (metadata, journal, other files), reported by size and count.
For the data reads it reports, per 128 KiB-aligned file block (the largest window; 32 KiB windows lie inside one):
  windows        distinct (file, block) groups that were read;
  cross_window   data reads that cross a 128 KiB-aligned boundary of the file (larger than or across a window);
  extent_splits  places where one read ends and the next read of the same block starts at an extent boundary of the
                 file (a fragmentation split; NFS has none);
so the harness can require: cross_window == 0, data_max_bytes <= the largest window, data_bytes <= bufferpool
bytes_read, windows <= bufferpool reads, data_reads <= bufferpool reads + extent_splits (a kernel split of a window into
4 KiB pages fails the last rule, a fragmentation split does not).
It also attributes the data reads to the Lucene data structure they read (cause attribution of cold reads):
  by_ext        {extension: {reads, bytes}} per Lucene file extension; a read of a compound file (.cfs) is split by
                the compound entry table (.cfe next to it, Lucene90CompoundFormat) into the sub-files it overlaps and
                counted as "cfs/<extension of the sub-file>" (a read that spans two sub-files counts once for each,
                with the overlapping bytes); bytes of a .cfs read outside every entry (header, footer) count as
                "cfs/(none)"; a .cfs whose .cfe cannot be read counts as "cfs/(unparsed)";
  by_structure  the same reads grouped by data structure in words (postings, terms dictionary, terms index, doc
                values, norms, BKD points, stored fields, ...), compound and non-compound files together.
"""
import array
import bisect
import fcntl
import os
import struct

WINDOW = 128 * 1024
FS_IOC_FIEMAP = 0xC020660B
_HDR = struct.Struct("=QQIIII")  # fm_start, fm_length, fm_flags, fm_mapped_extents, fm_extent_count, fm_reserved
_EXT = struct.Struct("=QQQQQIIII")  # fe_logical, fe_physical, fe_length, 2 x reserved64, fe_flags, 3 x reserved
FIEMAP_EXTENT_LAST = 0x1
FIEMAP_FLAG_SYNC = 0x1
_BATCH = 256
_cache = {}  # path -> (size, mtime_ns, ino, extents)
_cfe_cache = {}  # .cfs path -> (cfe size, cfe mtime_ns, [(offset, end, extension)] sorted) or None (unparsable)
CODEC_MAGIC = 0x3FD76C17
STRUCTURE = {
    "doc": "postings", "pos": "postings positions", "pay": "postings payloads and offsets",
    "psm": "postings metadata", "nav": "postings navigation", "tim": "terms dictionary", "tip": "terms index",
    "tmd": "terms metadata", "dvd": "doc values", "dvm": "doc values metadata", "dvs": "doc values skipper",
    "nvd": "norms", "nvm": "norms metadata", "kdd": "BKD points", "kdi": "BKD points", "kdm": "BKD points",
    "kdv": "BKD points", "fdt": "stored fields", "fdx": "stored fields", "fdm": "stored fields",
    "tvd": "term vectors", "tvx": "term vectors", "tvm": "term vectors", "vec": "vectors", "vem": "vectors",
    "vex": "vectors", "fnm": "field infos", "si": "segment info", "liv": "live documents",
    "cfe": "compound entry table", "(none)": "compound file header or footer", "(unparsed)": "compound file (entry table unreadable)",
}


def fiemap(path):
    """[(logical, physical, length)] of a file, in bytes (FS_IOC_FIEMAP, synced)."""
    out = []
    start = 0
    with open(path, "rb") as f:
        while True:
            buf = array.array("B", _HDR.pack(start, (1 << 64) - 1 - start, FIEMAP_FLAG_SYNC, 0, _BATCH, 0)
                              + bytes(_EXT.size * _BATCH))
            fcntl.ioctl(f.fileno(), FS_IOC_FIEMAP, buf, True)
            mapped = _HDR.unpack_from(buf, 0)[3]
            last = False
            for i in range(mapped):
                lo, ph, ln, _, _, flags, _, _, _ = _EXT.unpack_from(buf, _HDR.size + i * _EXT.size)
                out.append((lo, ph, ln))
                last = bool(flags & FIEMAP_EXTENT_LAST)
            if mapped == 0 or last:
                return out
            start = out[-1][0] + out[-1][2]


def lucene_files(index_dirs):
    """Every file under <index uuid>/<shard>/index/ (the Lucene files), as {path: os.stat}."""
    files = {}
    for d in index_dirs:
        for root, _, names in os.walk(d):
            if os.path.basename(root) != "index":
                continue
            for n in names:
                p = os.path.join(root, n)
                try:
                    files[p] = os.stat(p)
                except OSError:
                    pass
    return files


def _file_info(path, st, extents):
    c = _cache.get(path)
    if c and c[0] == st.st_size and c[1] == st.st_mtime_ns and (c[3] is not None or not extents):
        return c
    c = (st.st_size, st.st_mtime_ns, st.st_ino, fiemap(path) if extents else None)
    _cache[path] = c
    return c


def _vint(b, i):
    v, shift = 0, 0
    while True:
        x = b[i]
        i += 1
        v |= (x & 0x7F) << shift
        if x < 0x80:
            return v, i
        shift += 7


def parse_cfe(data):
    """
    Entries of a Lucene90CompoundFormat entry table (.cfe bytes): [(offset, length, name)] with the name as written
    (segment name stripped, e.g. ".kdd" or "_Lucene104_0.doc"). Layout: index header (big-endian magic, codec name,
    big-endian version, 16-byte segment id, suffix), VInt entry count, then per entry String name, little-endian long
    offset and long length into the .cfs, then the codec footer.
    """
    if len(data) < 4 or struct.unpack_from(">I", data, 0)[0] != CODEC_MAGIC:
        raise ValueError("not a Lucene index header")
    n, i = _vint(data, 4)
    i += n          # codec name
    i += 4 + 16     # version, segment id
    i += 1 + data[i]  # suffix length and suffix
    count, i = _vint(data, i)
    out = []
    for _ in range(count):
        ln, i = _vint(data, i)
        name = data[i:i + ln].decode("utf-8")
        i += ln
        off, length = struct.unpack_from("<qq", data, i)
        i += 16
        out.append((off, length, name))
    return out


def _ext(name):
    return name.rsplit(".", 1)[-1] if "." in name else "(none)"


def compound_entries(cfs_path):
    """[(offset, end, extension)] of the .cfs's sub-files, sorted, from the .cfe next to it; None if unreadable."""
    cfe = cfs_path[:-4] + ".cfe"
    try:
        st = os.stat(cfe)
    except OSError:
        return None
    c = _cfe_cache.get(cfs_path)
    if c and c[0] == st.st_size and c[1] == st.st_mtime_ns:
        return c[2]
    try:
        with open(cfe, "rb") as f:
            ents = sorted((off, off + ln, _ext(name)) for off, ln, name in parse_cfe(f.read()))
    except (OSError, ValueError, IndexError, struct.error, UnicodeDecodeError):
        ents = None
    _cfe_cache[cfs_path] = (st.st_size, st.st_mtime_ns, ents)
    return ents


def by_type(pieces):
    """(by_ext, by_structure) of data pieces [(file, lo, hi)]; compound reads split by sub-file (see module doc)."""
    ext = {}

    def add(k, nbytes):
        e = ext.setdefault(k, {"reads": 0, "bytes": 0})
        e["reads"] += 1
        e["bytes"] += nbytes

    for f, lo, hi in pieces:
        x = _ext(os.path.basename(f))
        if x != "cfs":
            add(x, hi - lo)
            continue
        ents = compound_entries(f)
        if ents is None:
            add("cfs/(unparsed)", hi - lo)
            continue
        covered = 0
        i = max(bisect.bisect_right(ents, (lo, float("inf"), "")) - 1, 0)
        while i < len(ents) and ents[i][0] < hi:
            a, b = max(ents[i][0], lo), min(ents[i][1], hi)
            if b > a:
                add("cfs/" + ents[i][2], b - a)
                covered += b - a
            i += 1
        if covered < hi - lo:
            add("cfs/(none)", hi - lo - covered)
    struct_ = {}
    for k, v in ext.items():
        name = STRUCTURE.get(k.split("/", 1)[-1], k.split("/", 1)[-1] + " files")
        e = struct_.setdefault(name, {"reads": 0, "bytes": 0})
        e["reads"] += v["reads"]
        e["bytes"] += v["bytes"]
    return ({k: ext[k] for k in sorted(ext)}, {k: struct_[k] for k in sorted(struct_)})


def _partition_offset(dev):
    """Byte offset of a partition on its disk (block tracepoints report disk sectors; FIEMAP reports fs offsets)."""
    try:
        with open(f"/sys/dev/block/{dev}/start") as f:
            return int(f.read()) * 512
    except OSError:
        return 0


def summarize(pieces, other, extent_starts):
    """pieces: [(file, lo, hi)] data reads in file coordinates (EOF-clipped); other: [bytes]."""
    out = {"data_reads": len(pieces), "data_bytes": sum(hi - lo for _, lo, hi in pieces),
           "data_max_bytes": max((hi - lo for _, lo, hi in pieces), default=None),
           "cross_window": sum(1 for _, lo, hi in pieces if hi > lo and lo // WINDOW != (hi - 1) // WINDOW),
           "other_reads": len(other), "other_bytes": sum(other)}
    hist = {}
    for _, lo, hi in pieces:
        hist[hi - lo] = hist.get(hi - lo, 0) + 1
    out["data_hist"] = {str(k): hist[k] for k in sorted(hist)}
    oh = {}
    for b in other:
        oh[b] = oh.get(b, 0) + 1
    out["other_hist"] = {str(k): oh[k] for k in sorted(oh)}
    groups = {}
    for f, lo, hi in pieces:
        groups.setdefault((f, lo // WINDOW), []).append((lo, hi))
    out["windows"] = len(groups)
    splits = 0
    for (f, _), rs in groups.items():
        rs.sort()
        starts = extent_starts.get(f, ())
        for (a_lo, a_hi), (b_lo, _) in zip(rs, rs[1:]):
            if a_hi == b_lo and b_lo in starts:
                splits += 1
    out["extent_splits"] = splits
    out["by_ext"], out["by_structure"] = by_type(pieces)
    return out


def attribute_block(requests, index_dirs, dev):
    """requests: [(disk_byte_offset, bytes)] of the data device; index_dirs: the arm's index directories."""
    off = _partition_offset(dev)
    ext = []  # (phys_start, phys_end, path, logical_start, size)
    starts = {}
    files = lucene_files(index_dirs)
    for p, st in files.items():
        size, _, _, extents = _file_info(p, st, True)
        starts[p] = {lo for lo, _, _ in extents}
        for lo, ph, ln in extents:
            ext.append((ph + off, ph + off + ln, p, lo, size))
    ext.sort()
    keys = [e[0] for e in ext]
    pieces, other = [], []
    for pos, n in requests:
        i = max(bisect.bisect_right(keys, pos) - 1, 0)
        hits, covered = [], 0
        while i < len(ext) and ext[i][0] < pos + n:
            ps, pe, p, lo, size = ext[i]
            if pe > pos:
                a, b = max(ps, pos), min(pe, pos + n)
                covered += b - a
                flo, fhi = lo + (a - ps), min(lo + (b - ps), size)
                if hits and hits[-1][0] == p and hits[-1][2] == lo + (a - ps):
                    hits[-1] = (p, hits[-1][1], fhi)  # the read continues in the next extent of the same file
                else:
                    hits.append((p, flo, fhi))
            i += 1
        # a request the block layer merged across two files' adjacent extents counts as one piece per file
        pieces += [h for h in hits if h[2] > h[1]]
        if covered < n:
            other.append(n - covered)  # bytes of the request outside every index file extent
    out = summarize(pieces, other, starts)
    out["files_mapped"] = len(files)
    out["extents_mapped"] = len(ext)
    return out


def attribute_nfs(reads, index_dirs):
    """reads: [(fileid, offset, count)] of NFS READ RPCs; files matched by inode number."""
    by_ino = {}
    for p, st in lucene_files(index_dirs).items():
        by_ino[st.st_ino] = (p, st.st_size)
    pieces, other = [], []
    for ino, offset, count in reads:
        f = by_ino.get(ino)
        if f is None:
            other.append(count)
            continue
        hi = min(offset + count, f[1])
        if hi > offset:
            pieces.append((f[0], offset, hi))
        else:
            other.append(count)
    out = summarize(pieces, other, {})
    out["files_mapped"] = len(by_ino)
    return out
