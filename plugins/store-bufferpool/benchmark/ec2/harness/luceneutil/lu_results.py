#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Parser of one luceneutil SearchPerfTest result log (the `-log` file luceneutil writes per JVM run,
<LOGS_DIR>/<id>.<competitor>.<iter>): every task instance with its category, identity key, latency (msec), start
offset, thread, hit count and result lines (doc ids with score or sort value, facet lines, groups). The identity
key is the TASK line without its results (everything before " hits=", " countOnlyCount=" or " groups="), so the
same task in another arm or JVM has the same key; the results digest is what luceneutil's verifyScores /
verifyCounts compare (doc ids, scores or sort values in order, hit count). Also the run's winddown time, the
elapsed time and the average CPU cores.
"""
import hashlib
import re

LATENCY = re.compile(r"^([\d.]+) msec @ ([\d.]+) msec$")
WINDDOWN = re.compile(r"^Start of tasks winddown: ([0-9.]+) msec$")
ELAPSED = re.compile(r"^Elapsed MS \(excluding warmup and winddown\): ([0-9.\-E]+)$")
CPU = re.compile(r"^Average CPU cores used: (-?[0-9.]+)$")
HITS = re.compile(r" hits=(null|[0-9]+\+?)")
COUNT_ONLY = re.compile(r" countOnlyCount=(\S*)")


class Task:
    __slots__ = ("key", "category", "line", "msec", "start_msec", "thread", "hits", "results")

    def digest(self):
        return hashlib.sha256(("\n".join([str(self.hits)] + self.results)).encode()).hexdigest()[:16]


def task_key(line):
    """(key, category, hits) from a TASK line (without the 'TASK: ' prefix)."""
    if line.startswith("cat="):
        cut = len(line)
        for marker in (" group=", " countOnlyCount="):
            i = line.find(marker)
            if i != -1:
                cut = min(cut, i)
        cat = line[4:line.find(" q=")] if " q=" in line else line[4:cut]
        m = HITS.search(line) or COUNT_ONLY.search(line)
        hits = m.group(1) if m else None
        key = line[:cut]
        f = line.find(" facets=")
        if f != -1 and f >= cut:
            key += line[f:]  # the facet request is part of the task's identity, not a result
        return key, cat, hits
    # respell / PK / PointsPK tasks: the whole line is the identity, results follow
    cat = line.split(None, 1)[0].rstrip(":")
    for prefix in ("PKTS", "PointsPK", "PK", "respell"):
        if line.startswith(prefix):
            cat = prefix
            break
    return line, cat, None


def parse(text):
    """{"tasks": [Task], "winddown_ms", "elapsed_ms", "avg_cpu_cores"} of one result log."""
    lines = text.splitlines()
    out = {"tasks": [], "winddown_ms": None, "elapsed_ms": None, "avg_cpu_cores": None}
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        for rx, key in ((WINDDOWN, "winddown_ms"), (ELAPSED, "elapsed_ms"), (CPU, "avg_cpu_cores")):
            m = rx.match(line)
            if m:
                out[key] = float(m.group(1))
        if line.startswith("TASK: "):
            t = Task()
            t.line = line[6:]
            t.key, t.category, t.hits = task_key(t.line)
            m = LATENCY.match(lines[i + 1].strip())
            if m is None:
                raise ValueError(f"line {i + 2}: expected '<ms> msec @ <ms> msec', got {lines[i + 1]!r}")
            t.msec, t.start_msec = float(m.group(1)), float(m.group(2))
            th = lines[i + 2].strip().split()
            if not th or th[0] != "thread":
                raise ValueError(f"line {i + 3}: expected 'thread N', got {lines[i + 2]!r}")
            t.thread = int(th[1])
            i += 3
            t.results = []
            while i < len(lines) and lines[i].strip() != "" and not lines[i].startswith("TASK: "):
                s = lines[i].strip()
                if s.startswith("HEAP: "):
                    break
                if "hilite time" not in s and "getFacetResults time" not in s:
                    t.results.append(s)
                i += 1
            out["tasks"].append(t)
            continue
        if "\tat " in lines[i] or lines[i].startswith("Exception"):
            raise ValueError(f"result log has an exception at line {i + 1}: {lines[i]!r}")
        i += 1
    if out["winddown_ms"] is None:
        raise ValueError("no 'Start of tasks winddown' line: not a complete luceneutil result log")
    return out
