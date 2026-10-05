#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Restore big5 (6 primaries x 10 segments, stock format, from the big5-100 snapshot) into the POC EBS and EFS nodes
# with index.store.type=bufferpoolfs, verify docs / segments / files against the snapshot source, close the index.
set -euo pipefail
ES=http://127.0.0.1:9200
for arm in ebs efs; do
  systemctl stop opensearch-poc-ebs opensearch-poc-efs
  systemctl start opensearch-poc-$arm
  for i in $(seq 1 90); do curl -sf $ES/_cluster/health >/dev/null && break; sleep 2; done
  if ! curl -sf "$ES/_cat/indices/big5?h=index" >/dev/null 2>&1; then
    curl -sf -XPUT $ES/_snapshot/big5-100 -H 'Content-Type: application/json' \
      -d '{"type":"fs","settings":{"location":"/data/repo/big5-100","readonly":true,"max_restore_bytes_per_sec":"2gb"}}'
    curl -sf -XPUT $ES/_cluster/settings -H 'Content-Type: application/json' -d '{"transient":{"indices.recovery.max_bytes_per_sec":"2gb"}}'
    t0=$(date +%s)
    curl -sf -XPOST "$ES/_snapshot/big5-100/big5/_restore?wait_for_completion=true" -H 'Content-Type: application/json' \
      -d '{"indices":"big5","include_global_state":false,"index_settings":{"index.store.type":"bufferpoolfs"}}' | head -c 400; echo
    echo "restore_$arm $(( $(date +%s) - t0 )) s"
    curl -sf -XPUT $ES/_cluster/settings -H 'Content-Type: application/json' -d '{"transient":{"indices.recovery.max_bytes_per_sec":null}}' >/dev/null
  fi
  curl -sf "$ES/_cluster/health/big5?wait_for_status=green&timeout=600s" | head -c 300; echo
  curl -sf "$ES/_cat/indices/big5?v&bytes=b&h=index,uuid,pri,rep,docs.count,pri.store.size"
  echo "segments=$(curl -sf "$ES/_cat/segments/big5?h=shard" | wc -l)"
  curl -sf "$ES/big5/_settings/index.store.type,index.codec?flat_settings=true"; echo
  curl -sf -XPOST "$ES/big5/_close" ; echo
done
systemctl stop opensearch-poc-ebs opensearch-poc-efs
