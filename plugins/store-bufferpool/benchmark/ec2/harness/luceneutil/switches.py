#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Python side of the luceneutil switch control: validates an arm's switches against switches.json (the table that
gen_switches.py generated from the Lucene fork's source) and encodes them as the JVM flag that ColdpathSwitches.java
(patches/0001) reads: -Dcoldpath.switches=<fully.qualified.Class>.<setter>=<value>,...
An arm lists switches as {"<fully.qualified.Class>.<setter>": value}. Values may name a parameter as "{name}" (for
example "{io.sequential_bytes}" for node bytes derived from the IO configuration), resolved from the run's params.
Errors (unknown class, unknown setter, wrong type, enum value not in the enum, a setter range check that the value
fails when it can be evaluated here) make the arm invalid before any JVM starts; the Java side checks again and
reads every value back.
"""
import json
import re

PLACEHOLDER = re.compile(r"^\{([\w.\-]+)\}$")


class SwitchError(ValueError):
    pass


def load(path):
    return json.load(open(path))


def resolve(value, params):
    if isinstance(value, str):
        m = PLACEHOLDER.match(value)
        if m:
            if m.group(1) not in params:
                raise SwitchError(f"parameter {m.group(1)} is not defined (params: {sorted(params)})")
            return params[m.group(1)]
    return value


def _typed(entry, value, key):
    typ = entry["type"]
    if typ == "boolean":
        if isinstance(value, bool):
            return "true" if value else "false"
        if str(value) in ("true", "false"):
            return str(value)
        raise SwitchError(f"{key}: boolean expected, got {value!r}")
    if typ in ("int", "long"):
        if isinstance(value, bool):
            raise SwitchError(f"{key}: {typ} expected, got {value!r}")
        try:
            v = int(str(value), 10)
        except ValueError:
            raise SwitchError(f"{key}: {typ} expected, got {value!r}") from None
        lim = (1 << 31) if typ == "int" else (1 << 63)
        if not -lim <= v < lim:
            raise SwitchError(f"{key}: {v} out of {typ} range")
        return str(v)
    if "enum" in entry:
        if str(value) not in entry["enum"]:
            raise SwitchError(f"{key}: {value!r} is not one of {entry['enum']}")
        return str(value)
    raise SwitchError(f"{key}: unsupported parameter type {typ}")


def encode(table, switches, params=None):
    """(flag value, resolved list). switches: {"Class.setter": value} in the order to apply."""
    params = params or {}
    out, resolved = [], []
    for key, raw in switches.items():
        if "." not in key:
            raise SwitchError(f"{key}: expected <fully.qualified.Class>.<setter>")
        cls, setter = key.rsplit(".", 1)
        c = table["classes"].get(cls)
        if c is None:
            raise SwitchError(f"{key}: class {cls} is not in switches.json (fork {table.get('fork_commit')})")
        e = c["setters"].get(setter)
        if e is None:
            raise SwitchError(f"{key}: {cls} has no setter {setter}; setters: {sorted(c['setters'])}")
        if e.get("getter") is None:
            raise SwitchError(f"{key}: no read-back getter in the fork")
        v = _typed(e, resolve(raw, params), key)
        if "," in v or "=" in v:
            raise SwitchError(f"{key}: value {v!r} contains a separator")
        out.append(f"{key}={v}")
        resolved.append({"switch": key, "value": v, "getter": e["getter"], "raw": raw})
    return ",".join(out), resolved


def jvm_flag(table, switches, params=None):
    value, resolved = encode(table, switches, params)
    return (f"-Dcoldpath.switches={value}" if value else ""), resolved


def parse_readbacks(stdout_text):
    """{switch: readback} from the COLDPATH lines a SearchPerfTest JVM prints; raises on NOT AVAILABLE."""
    out = {}
    for line in stdout_text.splitlines():
        if line.startswith("COLDPATH switches NOT AVAILABLE"):
            raise SwitchError(line)
        m = re.match(r"^COLDPATH switch (\S+)=(\S*) readback=(\S*)$", line.strip())
        if m:
            out[m.group(1)] = {"sent": m.group(2), "readback": m.group(3)}
    return out
