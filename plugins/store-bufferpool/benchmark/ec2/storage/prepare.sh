#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Build wnread, check the tracepoints, lay out the 64 GiB test files on EBS and EFS (real data, O_DIRECT 1 MiB writes).
set -euo pipefail
cd /opt/coldpath-storage
gcc -O2 -pthread -o wnread wnread.c
ls /sys/kernel/tracing/events/block/block_rq_issue/format /sys/kernel/tracing/events/nfs/nfs_initiate_read/format
grep -E 'print fmt' /sys/kernel/tracing/events/block/block_rq_issue/format /sys/kernel/tracing/events/nfs/nfs_initiate_read/format
for f in /data/fio/f64g /mnt/efs/storage-branch/fio/f64g; do
  if [ "$(stat -c %s $f 2>/dev/null || echo 0)" != "68719476736" ]; then
    t0=$(date +%s)
    fio --name=lay --filename=$f --rw=write --bs=1m --direct=1 --ioengine=libaio --iodepth=8 --numjobs=16 \
        --size=4g --offset_increment=4g --refill_buffers=1 --thread=1 --group_reporting=1 --end_fsync=1 \
        --output-format=terse --terse-version=3 >/tmp/lay.out
    echo "laid $f in $(( $(date +%s) - t0 )) s"
  fi
  ls -l $f
done
sync
