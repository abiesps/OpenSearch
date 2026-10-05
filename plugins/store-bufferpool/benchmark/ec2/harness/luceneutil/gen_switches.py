#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Writes switches.json, the table of the Lucene fork's process-wide experiment switches, by READING the fork's source
(never hand-typed). The experiment classes are found from git, not listed: every main-source .java file changed in
--base..--commit that adds a `public static void setX(T v)` (discover()); --check fails when the fork has a switch
class the table does not list, or the reverse. For each class, every added setter with its parameter type, the
matching read-back getter (`getX()` or `isX()`), the values of an enum parameter type, and the range checks of the
setter body (as text, for the reader). The fork has no system properties for these switches; luceneutil's
ColdpathSwitches (patches/0001) calls exactly these setters by reflection and reads them back through the getters.

  gen_switches.py --fork /path/to/lucene_experiments --commit origin/bkd-split --out switches.json
  gen_switches.py --fork ... --commit ... --check switches.json     (fails if the table is not what the fork has)
"""
import argparse
import hashlib
import json
import re
import subprocess
import sys

BASE = "releases/lucene/10.5.1"  # the fork's base tag (setup.sh STOCK_TAG)
RAW_SETTER = re.compile(r"public\s+static\s+void\s+set\w+\s*\(")
SETTER = re.compile(r"public\s+static\s+void\s+(set\w+)\s*\(\s*([\w.<>]+)\s+(\w+)\s*\)\s*\{")
GETTER = re.compile(r"public\s+static\s+([\w.<>]+)\s+((?:get|is)\w+)\s*\(\s*\)\s*\{")
ENUM = re.compile(r"public\s+enum\s+(\w+)\s*\{([^}]*)\}", re.S)
PACKAGE = re.compile(r"^package\s+([\w.]+);", re.M)


def git_show(fork, commit, path):
    return subprocess.run(["git", "-C", fork, "show", f"{commit}:{path}"], check=True, capture_output=True,
                          text=True).stdout


def _body(src, start):
    """Text of the method body starting at the '{' before index start."""
    depth, i = 0, start
    while i < len(src):
        if src[i] == "{":
            depth += 1
        elif src[i] == "}":
            depth -= 1
            if depth == 0:
                return src[start + 1:i]
        i += 1
    return src[start + 1:]


def _strip_comments(src):
    src = re.sub(r"/\*.*?\*/", lambda m: " " * len(m.group(0)), src, flags=re.S)
    return re.sub(r"//[^\n]*", "", src)


def parse(src):
    code = _strip_comments(src)
    pkg = PACKAGE.search(code).group(1)
    cls = re.search(r"(?:public\s+)?(?:final\s+)?(?:abstract\s+)?class\s+(\w+)", code).group(1)
    enums = {}
    for m in ENUM.finditer(code):
        vals = [v.strip().split("(")[0].strip() for v in m.group(2).split(";")[0].split(",")]
        enums[m.group(1)] = [v for v in vals if v]
    getters = {m.group(2): m.group(1) for m in GETTER.finditer(code)}
    setters = {}
    for m in SETTER.finditer(code):
        name, typ, _ = m.groups()
        base = name[3:]
        getter = next((g for g in (f"get{base}", f"is{base}") if g in getters), None)
        body = _body(code, m.end() - 1)
        checks = [re.sub(r"\s+", " ", c).strip() for c in re.findall(r"if\s*\((.*?)\)\s*\{\s*throw", body, re.S)]
        e = {"type": typ, "getter": getter, "getter_type": getters.get(getter), "checks": checks}
        short = typ.split(".")[-1]
        if short in enums:
            e["enum"] = enums[short]
            e["type"] = f"{pkg}.{cls}${short}" if "." not in typ else typ
        setters[name] = e
    return f"{pkg}.{cls}", setters


def _git(fork, *args):
    return subprocess.run(["git", "-C", fork, *args], check=True, capture_output=True, text=True).stdout


def _setter_names(src):
    """Parsed setter names of a source, and how many `public static void set...(` the parser did not read."""
    code = _strip_comments(src)
    names = [m.group(1) for m in SETTER.finditer(code)]
    return set(names), len(RAW_SETTER.findall(code)) - len(set(names))


def discover(fork, rev, base=BASE):
    """
    The experiment classes the fork adds, read from git (no hand-typed list): every main-source .java file changed
    in base..rev that has a `public static void setX(T v)` the base does not have. Returns {path: [added setters]}.
    A setter the fork adds whose signature the parser does not read (two parameters, an overload, a type SETTER
    does not match) stops the generation, so a switch is never silently left out.
    """
    names = _git(fork, "diff", "--name-only", "--diff-filter=AMR", f"{base}..{rev}", "--", "*.java").split()
    out = {}
    for path in sorted(p for p in names if "/src/java/" in p):
        new, unparsed = _setter_names(git_show(fork, rev, path))
        try:
            old, unparsed_old = _setter_names(git_show(fork, base, path))
        except subprocess.CalledProcessError:  # file added by the fork
            old, unparsed_old = set(), 0
        if unparsed > unparsed_old:
            sys.exit(f"{path} at {rev}: {unparsed - unparsed_old} added `public static void set...(` that the parser "
                     "does not read; extend SETTER")
        added = sorted(new - old)
        if added:
            out[path] = added
    return out


def build(fork, commit, base=BASE):
    rev = _git(fork, "rev-parse", commit).strip()
    base_rev = _git(fork, "rev-parse", f"{base}^{{commit}}").strip()
    found = discover(fork, rev, base)
    out = {"_doc": "Generated by gen_switches.py from the Lucene fork's source; do not edit by hand. Classes: every "
                   "main-source file changed since the base that adds a public static setter (discovered from git).",
           "fork_commit": rev, "fork_ref": commit, "base_ref": base, "base_commit": base_rev, "classes": {},
           "sources": {}}
    for path, added in found.items():
        src = git_show(fork, rev, path)
        fqcn, setters = parse(src)
        out["classes"][fqcn] = {"source": path, "setters": {k: v for k, v in setters.items() if k in added}}
        out["sources"][path] = hashlib.sha256(src.encode()).hexdigest()
    return out


def compare(old, new):
    """Differences between a committed table and a fresh one; [] when they are the same."""
    diff = []
    for c in sorted(set(new["classes"]) - set(old["classes"])):
        diff.append(f"class {c} ({new['classes'][c]['source']}) has switches in the fork but is not in the table")
    for c in sorted(set(old["classes"]) - set(new["classes"])):
        diff.append(f"class {c} is in the table but adds no switch in the fork")
    for c in sorted(set(old["classes"]) & set(new["classes"])):
        if old["classes"][c] != new["classes"][c]:
            diff.append(f"class {c}: setters differ")
    return diff


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fork", required=True)
    ap.add_argument("--commit", required=True)
    ap.add_argument("--out")
    ap.add_argument("--check", help="compare with an existing switches.json")
    ap.add_argument("--base", default=BASE, help="the fork's base (the stock Lucene of the comparison)")
    a = ap.parse_args()
    table = build(a.fork, a.commit, a.base)
    if a.check:
        diff = compare(json.load(open(a.check)), table)
        if diff:
            sys.exit(f"{a.check} does not match the fork at {table['fork_commit']} (regenerate it):\n  " + "\n  ".join(diff))
        print(f"{a.check} matches the fork at {table['fork_commit']}")
    if a.out:
        with open(a.out, "w") as f:
            json.dump(table, f, indent=1, sort_keys=True)
            f.write("\n")
        n = sum(len(c["setters"]) for c in table["classes"].values())
        print(f"{a.out}: {len(table['classes'])} classes, {n} setters at {table['fork_commit']}")


if __name__ == "__main__":
    main()
