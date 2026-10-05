#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# EFS per-connection blocks (review-a pass 3; preregistration-connections.md), run as root on the data node from
# the directory that holds storbench.py, efs_conn_ctl.sh and plan-conn.json.
#   block conn:     40 forced connections x 2 cycles (cycle 2 with the heavy 1 MiB job first)
#   block conn-pin: 8 forced connections x 2 cycles with at most one backend TCP connection (nftables rule)
set -uo pipefail
OUT=${OUT:-/data/results}
WARM=efs,poc-exact,r128k,32
REF=efs,poc-fio,r8k,64
./efs_conn_ctl.sh log-on
python3 storbench.py --out $OUT/fio-conn --plan plan-conn.json --reps 80 --runtime 4 --ramp 1 --block conn \
  --ref $REF --pre efs,poc-fio,r1m,64 --pre-positions 2 --reconnect remount --reps-per-connection 2 \
  --warmup $WARM --warmup-runtime 5 --xprt-log --seed 20261007 > $OUT/fio-conn.log 2>&1
echo "conn exit $?" >> $OUT/fio-conn.log
./efs_conn_ctl.sh pin-on
python3 storbench.py --out $OUT/fio-conn-pin --plan plan-conn.json --reps 16 --runtime 4 --ramp 1 --block conn-pin \
  --ref $REF --reconnect remount --reps-per-connection 2 --kill-old-proxy \
  --warmup $WARM --warmup-runtime 5 --xprt-log --seed 20261008 > $OUT/fio-conn-pin.log 2>&1
echo "conn-pin exit $?" >> $OUT/fio-conn-pin.log
./efs_conn_ctl.sh pin-off
mkdir -p $OUT/efs-proxy-logs && cp -p /var/log/amazon/efs/*efs-proxy.log* /var/log/amazon/efs/mount.log \
  /var/log/amazon/efs/mount-watchdog.log $OUT/efs-proxy-logs/
./efs_conn_ctl.sh log-off
# leave the mount as before: a fresh mount without proxy logging, mounted readahead restored
umount /mnt/efs && mount /mnt/efs
echo 15360 > /sys/class/bdi/$(mountpoint -d /mnt/efs)/read_ahead_kb
echo 128 > /sys/block/nvme1n1/queue/read_ahead_kb
echo "all done $(date -u +%FT%TZ)" >> $OUT/fio-conn-pin.log
