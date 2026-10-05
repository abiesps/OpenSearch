#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Checks of the agent's node-JVM matcher (stdlib, no AWS): only a java process whose arguments name the main class is
the node JVM; shells, pgrep, grep and SSM scripts that mention the class name are not (they made /cache/drop fail with
"more than one JVM matches" on the g-clickbench data node). Also runs the matcher over this host's /proc.
  selftest_agentjvm.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "agent"))
from coldpath_agent import Agent  # noqa: E402

N = 0


def check(cond, what):
    global N
    if not cond:
        print(f"SELFTEST-AGENTJVM FAIL: {what}")
        sys.exit(1)
    N += 1


def cl(*argv):
    return b"\0".join(a.encode() for a in argv) + b"\0"


def main():
    m = "org.opensearch.bootstrap.OpenSearch"
    yes = [cl("/usr/lib/jvm/java-21-amazon-corretto.x86_64/bin/java", "-Xms31g", "-cp", "/opt/x/lib/*", m, "-d"),
           cl("java", m),
           cl("/usr/bin/java", "-Dfoo=1", "perf.SearchPerfTest", "-index", "x")]
    no = [cl("pgrep", "-f", m),
          cl("/bin/bash", "-c", f"P=$(pgrep -f {m} | head -1); echo $P"),
          cl("grep", m, "/proc/1/cmdline"),
          cl("/usr/bin/python3", "/opt/coldpath/coldpath_agent.py", m),
          cl("sh", "-c", f"/var/lib/amazon/ssm/i-0/document/orchestration/x/awsrunShellScript/0.awsrunShellScript/_script.sh {m}"),
          cl("/usr/lib/jvm/java/bin/java", "-cp", "x", "org.opensearch.tools.java_version_checker.JavaVersionChecker"),
          cl("javac", m),
          b""]
    for c in yes[:2]:
        check(Agent.is_node_jvm(c, m), f"node JVM not matched: {c!r}")
    check(Agent.is_node_jvm(yes[2], "perf.SearchPerfTest"), "luceneutil JVM not matched")
    check(not Agent.is_node_jvm(yes[2], m), "luceneutil JVM matched as OpenSearch")
    for c in no:
        check(not Agent.is_node_jvm(c, m), f"non-JVM matched: {c!r}")
    # this host: the matcher never selects this python process, even with the class name in its argv
    if os.path.isdir("/proc/self"):
        with open("/proc/self/cmdline", "rb") as f:
            check(not Agent.is_node_jvm(f.read() + m.encode() + b"\0", m), "self matched")
    print(f"SELFTEST-AGENTJVM PASS: {N} checks")


if __name__ == "__main__":
    main()
