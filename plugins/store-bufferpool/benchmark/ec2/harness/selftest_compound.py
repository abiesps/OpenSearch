#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Self-test of the compound sub-file attribution in agent/coldpath_readattr.py, on a REAL compound segment written by
stock Lucene 10.5.1 (testdata/compound: _0.cfs, _0.cfe, _0.si and expected.json, made by
testdata/compound/gen/MakeCompound.java, which also wrote every entry's length as Lucene's CompoundDirectory reports it).
Checks: (1) the .cfe parser returns exactly Lucene's entries and lengths; (2) every parsed entry starts with the
Lucene index header magic and ends with the codec footer magic inside the .cfs, entries do not overlap and lie inside
the file; (3) NFS read attribution splits .cfs reads by sub-file (inside one entry, across two entries, the .cfs
header, a non-compound file) and groups them by data structure; (4) a .cfs without its .cfe counts as unparsed.
  python3 selftest_compound.py
"""
import json
import os
import shutil
import struct
import sys
import tempfile

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(here, "agent"))
import coldpath_readattr as ra  # noqa: E402

TD = os.path.join(here, "testdata", "compound")
FOOTER_MAGIC = (~ra.CODEC_MAGIC) & 0xFFFFFFFF
checks = 0


def ok(cond, msg):
    global checks
    if not cond:
        raise SystemExit(f"SELFTEST-COMPOUND FAILED: {msg}")
    checks += 1


def main():
    exp = json.load(open(os.path.join(TD, "expected.json")))
    seg = exp["segment"]
    cfs = open(os.path.join(TD, seg + ".cfs"), "rb").read()
    ents = ra.parse_cfe(open(os.path.join(TD, seg + ".cfe"), "rb").read())
    got = {seg + name: ln for _, ln, name in ents}
    ok(got == exp["entries"], f"parsed entries {got} != Lucene's {exp['entries']}")
    prev_end = 0
    for off, ln, name in sorted(ents):
        ok(off >= prev_end and off + ln <= len(cfs), f"entry {name} [{off}, {off + ln}) overlaps or leaves the file")
        ok(struct.unpack_from(">I", cfs, off)[0] == ra.CODEC_MAGIC, f"entry {name} does not start with a Lucene header")
        ok(struct.unpack_from(">I", cfs, off + ln - 16)[0] == FOOTER_MAGIC, f"entry {name} does not end with a footer")
        prev_end = off + ln
    by_name = {name: (off, ln) for off, ln, name in ents}
    tmp = tempfile.mkdtemp(prefix="selftest-compound-")
    try:
        idx = os.path.join(tmp, "nodes", "0", "indices", "uuid1", "0", "index")
        os.makedirs(idx)
        for f in (seg + ".cfs", seg + ".cfe", seg + ".si"):
            shutil.copy(os.path.join(TD, f), os.path.join(idx, f))
        # a second compound file without its entry table, and a non-compound file
        shutil.copy(os.path.join(TD, seg + ".cfs"), os.path.join(idx, "_1.cfs"))
        with open(os.path.join(idx, "_2_Lucene90_0.dvd"), "wb") as f:
            f.write(b"\0" * 8192)
        ino = {n: os.stat(os.path.join(idx, n)).st_ino for n in os.listdir(idx)}
        kdd_off, kdd_ln = by_name[".kdd"]
        pos_off, _ = by_name["_Lucene104_0.pos"]
        fdt_off, fdt_ln = by_name[".fdt"]
        reads = [
            (ino[seg + ".cfs"], kdd_off + 16, 1024),            # inside the BKD leaf entry
            (ino[seg + ".cfs"], pos_off - 512, 1024),            # across the end of one entry and the start of the next
            (ino[seg + ".cfs"], 0, 32),                          # the compound file's own header
            (ino[seg + ".cfs"], fdt_off, fdt_ln),                # one whole stored-fields entry
            (ino["_1.cfs"], 0, 4096),                            # no entry table next to it
            (ino["_2_Lucene90_0.dvd"], 0, 4096),                 # a non-compound doc-values file
            (999999999, 0, 4096),                                # not an index file (other)
        ]
        out = ra.attribute_nfs(reads, [os.path.join(tmp, "nodes", "0", "indices", "uuid1")])
        be, bs = out["by_ext"], out["by_structure"]
        ok(be.get("cfs/kdd") == {"reads": 1, "bytes": 1024}, f"BKD leaf read: {be}")
        before = [e for e in sorted(ents) if e[0] < pos_off][-1]  # the entry that ends where .pos starts (or before)
        ok(be.get("cfs/pos", {}).get("bytes") == 512, f"read across two entries, .pos part: {be}")
        ok(sum(v["reads"] for k, v in be.items() if k != "cfs/pos" and k.startswith("cfs/") and
               k not in ("cfs/kdd", "cfs/fdt", "cfs/(unparsed)")) >= 2, f"header and the entry before .pos: {be}")
        ok(be.get("cfs/fdt") == {"reads": 1, "bytes": fdt_ln}, f"whole stored-fields entry: {be}")
        ok(be.get("cfs/(unparsed)") == {"reads": 1, "bytes": 4096}, f"compound file without entry table: {be}")
        ok(be.get("dvd") == {"reads": 1, "bytes": 4096}, f"non-compound doc values: {be}")
        ok(out["other_reads"] == 1, f"non-index read: {out}")
        ok(bs.get("BKD points") == {"reads": 1, "bytes": 1024}, f"structure BKD points: {bs}")
        ok(bs.get("stored fields", {}).get("bytes") == fdt_ln, f"structure stored fields: {bs}")
        ok(bs.get("doc values", {}).get("bytes") == 4096, f"structure doc values: {bs}")
        ok(sum(v["bytes"] for v in be.values()) == out["data_bytes"], f"by_ext bytes != data bytes: {be} {out}")
        ok(before[2] != "pos", "the read across entries starts in another entry")
        # a cached entry table is re-read when the .cfe changes
        ra._cfe_cache.clear()
        ok(ra.compound_entries(os.path.join(idx, seg + ".cfs")) is not None, "entry table not found")
        ok(ra.compound_entries(os.path.join(idx, "_1.cfs")) is None, "missing entry table should give None")
    finally:
        shutil.rmtree(tmp)
    print(f"SELFTEST-COMPOUND PASS: {checks} checks ({len(ents)} entries, codec {exp['codec']})")


if __name__ == "__main__":
    main()
