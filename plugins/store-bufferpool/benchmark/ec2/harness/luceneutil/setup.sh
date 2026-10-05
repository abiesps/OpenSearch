#!/usr/bin/env bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Sets up luceneutil for the coldpath luceneutil branch (GENERIC-PLAN section 5) on the luceneutil host:
#   1. luceneutil at the pin, with the coldpath patches (patches/0000-0003; 0000 makes luceneutil, which tracks
#      Lucene main, compile against the Lucene 10.5.x line; see its commit message);
#   2. two Lucene checkouts: stock = releases/lucene/10.5.1 (the fork's base) and POC = the fork at POC_COMMIT, each
#      with a PRIVATE luceneutil copy (<checkout>/luceneutil: luceneutil compiles and runs a checkout against that
#      copy), patched the same way; the POC copy also gets patches/poc (the split-BKD codec, POC Lucene only);
#   3. both merge-bases recorded (fork vs apache/lucene main, and fork vs the 10.5.1 tag) with the baseline rationale;
#   4. luceneutil's wikimediumall line file (URL read from luceneutil's own initial_setup.py, not typed here),
#      decompressed, sha256 recorded;
#   5. localconstants.py (BASE_DIR, INDEX_DIR_BASE on EBS, LOGS_DIR, JAVA_COMMAND) and both Lucene builds;
#   6. switches.json checked against the POC commit (gen_switches.py --check): the switch table is the fork's.
# Idempotent where it can be; it never deletes anything. Record its output (setup.log) in the branch results.
#
#   setup.sh BASE_DIR POC_COMMIT [LUCENE_FORK_URL]
#   e.g. setup.sh /data/ebs/lu 59f87aa1af8fe8514d73088d05b4847e4b401549
set -euo pipefail
BASE=${1:?BASE_DIR}
POC_COMMIT=${2:?POC_COMMIT}
FORK_URL=${3:-https://github.com/abiesps/lucene_experiments.git}
UPSTREAM_URL=https://github.com/apache/lucene.git
LU_URL=https://github.com/mikemccand/luceneutil.git
LU_PIN=99217ab641d0dd172a89f22f079aa85227f5b7fe
STOCK_TAG=releases/lucene/10.5.1
HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$BASE" "$BASE/data" "$BASE/logs" "$BASE/indices"
exec > >(tee -a "$BASE/setup.log") 2>&1
echo "== $(date -u +%FT%TZ) setup.sh $*"

apply_patches() {  # $1 = luceneutil working copy, $2.. = patch files
  local dir=$1; shift
  for p in "$@"; do
    if git -C "$dir" apply --reverse --check "$p" 2>/dev/null; then
      echo "  already applied: $(basename "$p")"
    else
      git -C "$dir" apply --check "$p"
      git -C "$dir" apply "$p"
      echo "  applied: $(basename "$p")"
    fi
  done
}

# 1. luceneutil (the driver copy)
if [ ! -d "$BASE/luceneutil/.git" ]; then git clone -q "$LU_URL" "$BASE/luceneutil"; fi
git -C "$BASE/luceneutil" fetch -q origin
git -C "$BASE/luceneutil" checkout -q "$LU_PIN"
test "$(git -C "$BASE/luceneutil" rev-parse HEAD)" = "$LU_PIN"
apply_patches "$BASE/luceneutil" "$HERE"/patches/0*.patch

# 2. Lucene checkouts
if [ ! -d "$BASE/lucene-poc/.git" ]; then git clone -q "$FORK_URL" "$BASE/lucene-poc"; fi
git -C "$BASE/lucene-poc" remote get-url upstream >/dev/null 2>&1 || git -C "$BASE/lucene-poc" remote add upstream "$UPSTREAM_URL"
git -C "$BASE/lucene-poc" fetch -q origin
git -C "$BASE/lucene-poc" fetch -q upstream main --tags
git -C "$BASE/lucene-poc" checkout -q "$POC_COMMIT"
if [ ! -d "$BASE/lucene-stock/.git" ]; then git clone -q "$BASE/lucene-poc" "$BASE/lucene-stock"; fi
git -C "$BASE/lucene-stock" fetch -q "$BASE/lucene-poc" "refs/tags/$STOCK_TAG:refs/tags/$STOCK_TAG"
git -C "$BASE/lucene-stock" checkout -q "$STOCK_TAG"

# private luceneutil copies (luceneutil uses <checkout>/luceneutil when it exists)
for c in lucene-stock lucene-poc; do
  dst="$BASE/$c/luceneutil"
  if [ ! -d "$dst/.git" ]; then git clone -q "$BASE/luceneutil" "$dst"; git -C "$dst" checkout -q "$LU_PIN"; fi
  grep -qx 'luceneutil/' "$BASE/$c/.git/info/exclude" || echo 'luceneutil/' >> "$BASE/$c/.git/info/exclude"
  apply_patches "$dst" "$HERE"/patches/0*.patch
done
apply_patches "$BASE/lucene-poc/luceneutil" "$HERE"/patches/poc/*.patch
cp "$BASE/luceneutil/lib/"*.jar "$BASE/lucene-stock/luceneutil/lib/" 2>/dev/null || true
cp "$BASE/luceneutil/lib/"*.jar "$BASE/lucene-poc/luceneutil/lib/" 2>/dev/null || true

# 3. merge-bases and the baseline rationale
{
  echo "poc_commit $(git -C "$BASE/lucene-poc" rev-parse HEAD)"
  echo "stock_tag $STOCK_TAG $(git -C "$BASE/lucene-stock" rev-parse HEAD)"
  echo "merge_base_vs_upstream_main $(git -C "$BASE/lucene-poc" merge-base HEAD upstream/main)"
  echo "merge_base_vs_stock_tag $(git -C "$BASE/lucene-poc" merge-base HEAD "$STOCK_TAG")"
  echo "commits_tag_to_poc $(git -C "$BASE/lucene-poc" rev-list --count "$STOCK_TAG"..HEAD)"
  echo "rationale: the fork is built on $STOCK_TAG (merge-base with the tag is the tag itself); the merge-base with"
  echo "  upstream main is on the pre-10.x main line, so it is not a usable stock baseline (GENERIC-PLAN section 5)"
} | tee "$BASE/merge-bases.txt"

# 4. data: luceneutil's wikimediumall line file
URL=$(cd "$BASE/luceneutil/src/python" && python3 - <<'EOF'
import ast
src = open("initial_setup.py").read()
tree = ast.parse(src)
for node in ast.walk(tree):
    if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "DATA_FILES" for t in node.targets):
        for e in node.value.elts:
            v = ast.literal_eval(e)
            v = v if isinstance(v, str) else v[0]
            if "lines-1k-fixed-utf8-with-random-label" in v:
                print(v)
                break
EOF
)
test -n "$URL"
F="$BASE/data/$(basename "$URL")"
TXT="${F%.lzma}"
if [ ! -f "$TXT" ]; then
  if [ ! -f "$F" ]; then
    curl -fL --retry 5 -o "$F.part" "$URL"
    mv "$F.part" "$F"
  fi
  sha256sum "$F" | tee "$BASE/data/$(basename "$F").sha256"
  xz -dk "$F"
fi
sha256sum "$TXT" | tee "$BASE/data/$(basename "$TXT").sha256"
echo "line docs: $(wc -l < "$TXT") lines"

# 5. localconstants.py and the Lucene builds
cat > "$BASE/luceneutil/src/python/localconstants.py" <<EOF
# written by coldpath setup.sh
BASE_DIR = "$BASE"
INDEX_DIR_BASE = "$BASE/indices"
LOGS_DIR = "$BASE/logs"
JAVA_COMMAND = "java -server -Xms8g -Xmx8g --add-modules jdk.incubator.vector -XX:+HeapDumpOnOutOfMemoryError -XX:+UseParallelGC --enable-native-access=ALL-UNNAMED"
EOF
for c in lucene-stock lucene-poc; do
  (cd "$BASE/$c" && ./gradlew -q lucene:core:jar && ./gradlew -q compileJava)
done

# 6. the switch table is the fork's
python3 "$HERE/gen_switches.py" --fork "$BASE/lucene-poc" --commit HEAD --check "$HERE/switches.json"
echo "== setup done"
