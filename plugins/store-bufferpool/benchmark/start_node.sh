#!/usr/bin/env bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Starts a single OpenSearch node with the store-bufferpool plugin, built against the Lucene fork in ~/.m2
# (publish it first with `./gradlew mavenToLocal` in the fork).
#
# The data directory is passed with --data-dir, which the run task never wipes, so an index ingested once survives
# node restarts and rebuilds. Only a change to an on-disk format needs a new index.
#
# Environment (all optional):
#   DATA_DIR    data directory             (default: ~/bufferpool-bench/data)
#   HEAP        JVM heap; direct memory is half of it (default: 4g)
#   CACHE_SIZE  bufferpool.cache.size      (default: 1gb)
#   BLOCK_SIZE  bufferpool.cache.block_size (default: 128kb)
#   LOG         node log file              (default: ~/bufferpool-bench/node.log)
#   JVM_ARGS    extra JVM flags            (default: -da -dsa -XX:ReservedCodeCacheSize=256m)
#
# Stop the node with: kill $(cat ~/bufferpool-bench/node.pid)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
BENCH_HOME="${BENCH_HOME:-$HOME/bufferpool-bench}"
DATA_DIR="${DATA_DIR:-$BENCH_HOME/data}"
HEAP="${HEAP:-4g}"
CACHE_SIZE="${CACHE_SIZE:-1gb}"
BLOCK_SIZE="${BLOCK_SIZE:-128kb}"
LOG="${LOG:-$BENCH_HOME/node.log}"
# the run task defaults to C1-only JIT (gradle.properties) and -ea -esa; later flags override both
JVM_ARGS="${JVM_ARGS:--da -dsa -XX:ReservedCodeCacheSize=256m}"
mkdir -p "$DATA_DIR" "$(dirname "$LOG")"

if curl -s -m 2 localhost:9200 > /dev/null; then
  echo "a node is already listening on localhost:9200; stop it first" >&2
  exit 1
fi

export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 21 2>/dev/null || true)}"
cd "$ROOT"
nohup ./gradlew run \
  -PinstalledPlugins='["store-bufferpool"]' \
  -Drepos.mavenLocal=true \
  -Dtests.heap.size="$HEAP" \
  "-Dtests.jvm.argline=$JVM_ARGS" \
  -Dtests.opensearch.bufferpool.cache.size="$CACHE_SIZE" \
  -Dtests.opensearch.bufferpool.cache.block_size="$BLOCK_SIZE" \
  --data-dir "$DATA_DIR" \
  --console=plain < /dev/null > "$LOG" 2>&1 &
echo $! > "$BENCH_HOME/node.pid"

echo "starting node (log: $LOG) ..."
for _ in $(seq 1 90); do
  if curl -s -m 2 localhost:9200 > /dev/null; then
    echo "node is up: block_size=$BLOCK_SIZE cache=$CACHE_SIZE heap=$HEAP data=$DATA_DIR"
    exit 0
  fi
  if grep -q 'BUILD FAILED' "$LOG"; then
    echo "build failed, see $LOG" >&2
    exit 1
  fi
  sleep 5
done
echo "node did not start within 7.5 minutes, see $LOG" >&2
exit 1
