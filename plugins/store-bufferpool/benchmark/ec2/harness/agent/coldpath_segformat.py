#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Post-ingest segment format check (stdlib only, Python >= 3.8): proves from the segment files, never from the codec
name, which points and postings format each field of a shard's last commit uses.
Why: every Lucene104 segment of a bufferpoolfs index has the codec name Lucene104SplitPoints, also when no field asks
for the split format, so the codec name proves nothing. A field's format is proven by two things in each segment:
  1. its per-field attribute in the field infos (.fnm): PerFieldPointsFormat.format = Lucene90Split for the split BKD
     points format, PerFieldPostingsFormat.format = Lucene104Nav (and PerFieldPostingsFormat.suffix = N) for the Nav
     postings format;
  2. the files of that format in the segment: <segment>_Lucene90Split_0.kdm/.kdi/.kdd (.kdv is reported), and
     <segment>_Lucene104Nav_N.nav; inside the compound file (.cfe entries) when the segment is compound.
It reads the newest segments_N, every segment's newest field infos (the generation file after doc-values updates,
such as OpenSearch's soft deletes), and the .cfe entry table. Formats it does not know fail loudly (a header with
another codec name or version is an error, not a guess).
Used by the agent (GET /index/formats), by indexprep.py formats and by coldbench.py (index "formats" entry), and
directly on a host that has the index files:
  coldpath_segformat.py --dir /data/.../indices/UUID [--points ts=Lucene90Split] [--postings kw=Lucene104Nav] [--control]
--dir is a shard index directory, a shard directory or an index UUID directory (every shard below it is checked).
Exit status 1 and a JSON report with "errors" when a copy meant to have a format lacks it, or a control copy has it.
"""
import argparse
import json
import os
import re
import struct
import sys

CODEC_MAGIC = 0x3FD76C17
FOOTER_MAGIC = ~CODEC_MAGIC & 0xFFFFFFFF
SEGMENTS_VERSIONS = (9, 10)  # SegmentInfos VERSION_74 .. VERSION_86 (the current one)
FIELD_INFOS_CODEC = "Lucene94FieldInfos"
FIELD_INFOS_VERSIONS = (0, 1, 2)  # FORMAT_START .. FORMAT_DOCVALUE_SKIPPER (the current one)
COMPOUND_ENTRIES_CODEC = "Lucene90CompoundEntries"
POINTS_KEY = "PerFieldPointsFormat.format"
POINTS_SUFFIX_KEY = "PerFieldPointsFormat.suffix"
POSTINGS_KEY = "PerFieldPostingsFormat.format"
POSTINGS_SUFFIX_KEY = "PerFieldPostingsFormat.suffix"
SPLIT = "Lucene90Split"
NAV = "Lucene104Nav"
# files that prove the format, per format name; the suffix comes from the field attribute
REQUIRED_EXTENSIONS = {SPLIT: ("kdm", "kdi", "kdd"), NAV: ("nav",)}
# formats a control copy must not have, in an attribute or in a file name
EXPERIMENTAL_POINTS = (SPLIT,)
EXPERIMENTAL_POSTINGS = (NAV, "Lucene104DualNav")


class FormatError(ValueError):
    """The bytes are not the format this reader knows."""


class Reader:
    """Lucene DataInput over bytes: little-endian ints and longs, big-endian codec headers and footers."""

    def __init__(self, data, name):
        self.b, self.p, self.name = data, 0, name

    def byte(self):
        if self.p >= len(self.b):
            raise FormatError(f"{self.name}: read past the end")
        v = self.b[self.p]
        self.p += 1
        return v

    def take(self, n):
        if self.p + n > len(self.b):
            raise FormatError(f"{self.name}: read past the end")
        v = self.b[self.p:self.p + n]
        self.p += n
        return v

    def be_int(self):
        return struct.unpack(">I", self.take(4))[0]

    def be_long(self):
        return struct.unpack(">q", self.take(8))[0]

    def le_long(self):
        return struct.unpack("<q", self.take(8))[0]

    def vint(self, max_bytes=5):
        v, shift = 0, 0
        for _ in range(max_bytes):
            x = self.byte()
            v |= (x & 0x7F) << shift
            if x < 0x80:
                return v
            shift += 7
        raise FormatError(f"{self.name}: malformed variable-length integer")

    def vlong(self):
        return self.vint(max_bytes=9)

    def string(self):
        return self.take(self.vint()).decode("utf-8")

    def map_of_strings(self):
        return {self.string(): self.string() for _ in range(self.vint())}

    def set_of_strings(self):
        return [self.string() for _ in range(self.vint())]

    def index_header(self, codec, versions):
        if self.be_int() != CODEC_MAGIC:
            raise FormatError(f"{self.name}: no codec header")
        name = self.string()
        version = self.be_int()
        if name != codec or version not in versions:
            raise FormatError(f"{self.name}: header {name} version {version}, this reader knows {codec} {list(versions)}")
        seg_id = self.take(16)
        suffix = self.take(self.byte()).decode("utf-8")
        return version, seg_id, suffix


def _latest_segments_file(files):
    gens = []
    for f in files:
        if f == "segments":
            gens.append((0, f))
        elif f.startswith("segments_"):
            try:
                gens.append((int(f[len("segments_"):], 36), f))
            except ValueError:
                pass
    if not gens:
        raise FormatError("no segments_N file")
    return max(gens)[1]


def parse_segments(data, name):
    """The segment list of a segments_N file: name, codec, field-infos generation and field-infos files."""
    r = Reader(data, name)
    r.index_header("segments", SEGMENTS_VERSIONS)
    r.vint(), r.vint(), r.vint()  # Lucene version that wrote the commit
    r.vint()  # index created major version
    r.be_long()  # version
    r.vlong()  # counter
    n = r.be_int()
    if n > 0:
        r.vint(), r.vint(), r.vint()  # min segment version
    segments = []
    for _ in range(n):
        seg = {"name": r.string()}
        r.take(16)
        seg["codec"] = r.string()
        seg["del_gen"] = r.be_long()
        seg["del_count"] = r.be_int()
        seg["field_infos_gen"] = r.be_long()
        r.be_long()  # doc values gen
        seg["soft_del_count"] = r.be_int()
        if r.byte() == 1:
            r.take(16)
        seg["field_infos_files"] = r.set_of_strings()
        for _ in range(r.be_int()):
            r.be_int()
            r.set_of_strings()
        segments.append(seg)
    return segments


def parse_compound_entries(data, name):
    """File name (without the segment name) -> (offset, length) in the .cfs file."""
    r = Reader(data, name)
    r.index_header(COMPOUND_ENTRIES_CODEC, (0,))
    out = {}
    for _ in range(r.vint()):
        entry = r.string()
        out[entry] = (r.le_long(), r.le_long())
    return out


def parse_field_infos(data, name):
    """Field name -> {number, index_options, point_dims, attributes} of a .fnm file."""
    r = Reader(data, name)
    version, _, _ = r.index_header(FIELD_INFOS_CODEC, FIELD_INFOS_VERSIONS)
    fields = {}
    for _ in range(r.vint()):
        field = r.string()
        number = r.vint()
        r.byte()  # bits
        index_options = r.byte()
        r.byte()  # doc values type
        if version >= 2:
            r.byte()  # doc values skip index type
        r.le_long()  # doc values gen
        attributes = r.map_of_strings()
        dims = r.vint()
        if dims:
            r.vint(), r.vint()
        r.vint()  # vector dimension
        r.byte(), r.byte()  # vector encoding, similarity
        fields[field] = {"number": number, "indexed": index_options != 0, "point_dims": dims, "attributes": attributes}
    footer = r.take(16)
    if struct.unpack(">I", footer[:4])[0] != FOOTER_MAGIC or r.p != len(r.b):
        raise FormatError(f"{name}: field infos do not end with the codec footer")
    return fields


def read_shard(index_dir):
    """Every segment of the shard's newest commit: codec name, compound, fields with their format attributes, files."""
    files = sorted(os.listdir(index_dir))
    seg_file = _latest_segments_file(files)
    with open(os.path.join(index_dir, seg_file), "rb") as f:
        segments = parse_segments(f.read(), seg_file)
    out = []
    for seg in segments:
        name = seg["name"]
        cfe = name + ".cfe"
        compound = cfe in files
        if compound:
            with open(os.path.join(index_dir, cfe), "rb") as f:
                entries = parse_compound_entries(f.read(), cfe)
            seg_files = sorted({name + e for e in entries} | {f for f in files if _belongs(f, name)})
        else:
            entries = {}
            seg_files = [f for f in files if _belongs(f, name)]
        fnm_candidates = [f for f in seg["field_infos_files"] if f.endswith(".fnm")]
        if seg["field_infos_gen"] != -1 and fnm_candidates:
            fnm = fnm_candidates[0]
            with open(os.path.join(index_dir, fnm), "rb") as f:
                fnm_bytes = f.read()
        elif compound:
            if ".fnm" not in entries:
                raise FormatError(f"{cfe}: no .fnm entry")
            fnm = name + ".cfs:.fnm"
            offset, length = entries[".fnm"]
            with open(os.path.join(index_dir, name + ".cfs"), "rb") as f:
                f.seek(offset)
                fnm_bytes = f.read(length)
        else:
            fnm = name + ".fnm"
            with open(os.path.join(index_dir, fnm), "rb") as f:
                fnm_bytes = f.read()
        fields = parse_field_infos(fnm_bytes, fnm)
        out.append({"name": name, "codec": seg["codec"], "compound": compound, "field_infos": fnm,
                    "files": seg_files, "fields": fields})
    return {"dir": index_dir, "segments_file": seg_file, "segments": out}


def _belongs(file_name, segment):
    return file_name.startswith(segment + ".") or file_name.startswith(segment + "_")


def shard_dirs(path):
    """Shard index directories at or below path (an index directory, a shard directory or an index UUID directory)."""
    if any(f.startswith("segments_") for f in _ls(path)):
        return [path]
    if os.path.isdir(os.path.join(path, "index")):
        return [os.path.join(path, "index")]
    out = []
    for d in sorted(_ls(path)):
        if re.fullmatch(r"\d+", d) and os.path.isdir(os.path.join(path, d, "index")):
            out.append(os.path.join(path, d, "index"))
    return out


def _ls(path):
    try:
        return os.listdir(path)
    except OSError:
        return []


def describe(path):
    """{shard index directory: read_shard(...)} for every shard at or below path; a read failure is reported, not raised."""
    out = {}
    for d in shard_dirs(path):
        try:
            out[d] = read_shard(d)
        except (OSError, FormatError, UnicodeDecodeError, struct.error) as e:
            out[d] = {"dir": d, "error": f"{type(e).__name__}: {e}"}
    return out


def _format_files(seg, fmt, suffix):
    prefix = f"{seg['name']}_{fmt}_{suffix}."
    return sorted(f for f in seg["files"] if f.startswith(prefix))


def check(shards, points=None, postings=None, control=False):
    """
    Checks the output of describe(). points / postings: {field: format name} that every segment holding the field must
    use (attribute and files). control: no field and no file of any segment may use an experimental format
    (Lucene90Split points, Lucene104Nav or Lucene104DualNav postings). Returns {"ok", "errors", "summary"}.
    """
    points, postings = dict(points or {}), dict(postings or {})
    errors, summary = [], {}
    if not shards:
        errors.append("no shard directory found")
    if control and (points or postings):
        errors.append("a control copy takes no expected formats")
    seen = {f: 0 for f in list(points) + list(postings)}

    def field_summary(field, kind, fmt):
        return summary.setdefault(f"{kind} {field}", {"format": fmt, "segments": 0, "files": {}, "segments_without_field": 0})

    for d, shard in sorted(shards.items()):
        if "error" in shard:
            errors.append(f"{d}: {shard['error']}")
            continue
        if not shard["segments"]:
            errors.append(f"{d}: the last commit has no segments")
        for seg in shard["segments"]:
            where = f"{d} segment {seg['name']}"
            fields = seg["fields"]
            if control:
                for field, fi in sorted(fields.items()):
                    pf = fi["attributes"].get(POINTS_KEY)
                    if pf in EXPERIMENTAL_POINTS:
                        errors.append(f"{where}: control copy, field [{field}] has points format attribute {pf}")
                    qf = fi["attributes"].get(POSTINGS_KEY)
                    if qf in EXPERIMENTAL_POSTINGS:
                        errors.append(f"{where}: control copy, field [{field}] has postings format attribute {qf}")
                for f in seg["files"]:
                    if any(f"_{x}_" in f for x in EXPERIMENTAL_POINTS + EXPERIMENTAL_POSTINGS):
                        errors.append(f"{where}: control copy has the format file {f}")
            for kind, wanted, key, suffix_key in (("points", points, POINTS_KEY, POINTS_SUFFIX_KEY),
                                                   ("postings", postings, POSTINGS_KEY, POSTINGS_SUFFIX_KEY)):
                for field, fmt in sorted(wanted.items()):
                    fi = fields.get(field)
                    s = field_summary(field, kind, fmt)
                    if fi is None:
                        # no document of this segment has the field
                        s["segments_without_field"] += 1
                        continue
                    if (kind == "points" and fi["point_dims"] == 0) or (kind == "postings" and not fi["indexed"]):
                        errors.append(f"{where}: field [{field}] has no {kind}")
                        continue
                    seen[field] += 1
                    got = fi["attributes"].get(key)
                    if got != fmt:
                        errors.append(f"{where}: field [{field}] {kind} format attribute {key}={got}, expected {fmt}")
                        continue
                    suffix = fi["attributes"].get(suffix_key)
                    found = _format_files(seg, fmt, suffix)
                    exts = {f.rsplit(".", 1)[-1] for f in found}
                    missing = [e for e in REQUIRED_EXTENSIONS.get(fmt, ()) if e not in exts]
                    if not found or missing:
                        errors.append(f"{where}: field [{field}] has {key}={fmt} but the files "
                                      f"{seg['name']}_{fmt}_{suffix}.{'/'.join(missing) or '*'} are missing")
                    s["segments"] += 1
                    for f in found:
                        ext = f.rsplit(".", 1)[-1]
                        s["files"][ext] = s["files"].get(ext, 0) + 1
    for field, n in sorted(seen.items()):
        if n == 0 and shards:
            errors.append(f"field [{field}]: in no segment of any shard with its {'points' if field in points else 'postings'}")
    return {"ok": not errors, "errors": errors, "summary": summary,
            "segments": sum(len(s.get("segments", [])) for s in shards.values()), "shards": len(shards)}


def parse_pairs(pairs):
    out = {}
    for p in pairs or []:
        if "=" not in p:
            raise SystemExit(f"expected FIELD=FORMAT, got {p!r}")
        k, v = p.split("=", 1)
        out[k] = v
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", required=True, help="shard index directory, shard directory or index UUID directory")
    ap.add_argument("--points", action="append", help="FIELD=FORMAT that every segment with the field must use")
    ap.add_argument("--postings", action="append", help="FIELD=FORMAT that every segment with the field must use")
    ap.add_argument("--control", action="store_true", help="no field and no file may use an experimental format")
    ap.add_argument("--describe", action="store_true", help="also print every segment's fields and files")
    a = ap.parse_args()
    shards = describe(a.dir)
    out = check(shards, parse_pairs(a.points), parse_pairs(a.postings), a.control)
    if a.describe:
        out["shards_detail"] = shards
    print(json.dumps(out, indent=1, sort_keys=True))
    if not out["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
