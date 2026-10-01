#!/usr/bin/env bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# One step from a Lucene fork change to a node that runs it:
#   1. publish every fork module to ~/.m2 (mavenToLocal)
#   2. stop the node, start it again (the run task rebuilds the distribution and the plugin against ~/.m2)
#   3. check that every lucene-*.jar of the running distribution is byte-identical to its ~/.m2 jar and that its
#      manifest names the fork's HEAD commit
# Step 3 fails the script on any mismatch, so a benchmark never runs on stale jars.
#
# Usage: refresh_lucene.sh [--skip-publish] [--verify-only] [--allow-dirty]
#   --skip-publish  do not publish (the jars in ~/.m2 are already current)
#   --verify-only   only run step 3 against the running node's distribution
#   --allow-dirty   accept uncommitted fork changes (the manifest then cannot prove what was built)
# Environment: FORK (default ~/workspace/lucene_experiments), plus everything start_node.sh accepts.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
FORK="${FORK:-$HOME/workspace/lucene_experiments}"
M2="$HOME/.m2/repository/org/apache/lucene"
PUBLISH=1
RESTART=1
ALLOW_DIRTY=0
for arg in "$@"; do
  case "$arg" in
    --skip-publish) PUBLISH=0 ;;
    --verify-only) PUBLISH=0; RESTART=0 ;;
    --allow-dirty) ALLOW_DIRTY=1 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 21)}"
HEAD="$(git -C "$FORK" rev-parse HEAD)"
if [[ -n "$(git -C "$FORK" status --porcelain --untracked-files=no)" && $ALLOW_DIRTY -eq 0 ]]; then
  echo "fork $FORK has uncommitted changes; commit them or pass --allow-dirty" >&2
  exit 1
fi

if [[ $PUBLISH -eq 1 ]]; then
  echo "publishing all fork modules at ${HEAD:0:10} to ~/.m2 ..."
  log="$(mktemp -t mavenToLocal)"
  if ! (cd "$FORK" && ./gradlew mavenToLocal --console=plain < /dev/null > "$log" 2>&1); then
    tail -30 "$log" >&2
    echo "mavenToLocal failed, full log: $log" >&2
    exit 1
  fi
  echo "  published ($(grep -c 'publishJarsPublicationToMavenLocal' "$log" || true) publish tasks, log: $log)"
fi

if [[ $RESTART -eq 1 ]]; then
  "$HERE/stop_node.sh" || true
  "$HERE/start_node.sh"
fi

DISTRO="$(ls -d "$ROOT"/build/testclusters/runTask-0/distro/*-ARCHIVE | head -1)"
echo "checking lucene jars of $DISTRO against ~/.m2 and fork HEAD ${HEAD:0:10} ..."
python3 - "$DISTRO" "$M2" "$HEAD" <<'EOF'
import hashlib, os, re, sys, zipfile

distro, m2, head = sys.argv[1:]

def sha(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

def impl_version(path):
    with zipfile.ZipFile(path) as z:
        text = z.read("META-INF/MANIFEST.MF").decode()
    text = re.sub(r"\r?\n ", "", text)  # manifest continuation lines
    m = re.search(r"^Implementation-Version: (.*)$", text, re.M)
    return m.group(1) if m else ""

bad, ok = [], 0
for dirpath, _, files in os.walk(distro):
    for name in sorted(files):
        m = re.match(r"(lucene-[a-z0-9-]+?)-(\d+\.\d+\.\d+(?:-SNAPSHOT)?)\.jar$", name)
        if not m:
            continue
        jar = os.path.join(dirpath, name)
        rel = os.path.relpath(jar, distro)
        ref = os.path.join(m2, m.group(1), m.group(2), name)
        if not os.path.exists(ref):
            bad.append(f"{rel}: no {ref}")
        elif sha(jar) != sha(ref):
            bad.append(f"{rel}: differs from ~/.m2")
        elif head not in impl_version(jar):
            bad.append(f"{rel}: built from '{impl_version(jar)}', fork HEAD is {head[:10]}")
        else:
            ok += 1
if ok == 0 and not bad:
    bad.append("no lucene jars found")
for b in bad:
    print("  MISMATCH " + b)
print(f"  {ok} lucene jars match ~/.m2 and fork HEAD {head[:10]}")
sys.exit(1 if bad else 0)
EOF
