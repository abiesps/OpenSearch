#!/usr/bin/env bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Stops the node started by start_node.sh. The data directory is kept.
set -uo pipefail
BENCH_HOME="${BENCH_HOME:-$HOME/bufferpool-bench}"
if [ -f "$BENCH_HOME/node.pid" ]; then
  kill "$(cat "$BENCH_HOME/node.pid")" 2> /dev/null
  rm -f "$BENCH_HOME/node.pid"
fi
# the gradle run task forks the node; make sure it is gone too
pkill -f 'testclusters/runTask-0' 2> /dev/null
for _ in $(seq 1 30); do
  curl -s -m 2 localhost:9200 > /dev/null || { echo "node stopped"; exit 0; }
  sleep 1
done
echo "node still answers on localhost:9200" >&2
exit 1
