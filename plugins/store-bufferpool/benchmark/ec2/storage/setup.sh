#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Storage branch data node setup (i-0630745d90f026c6e). Idempotent, runs as root through SSM.
# EBS data volume vol-09e822a28d3ce0670 -> XFS /data (noatime); stack EFS fs-060fb5da13f9a7e5b -> /mnt/efs
# (amazon-efs-utils, TLS, default mount helper options). Installs fio, gcc (for wnread), Corretto 21, sysstat.
set -euo pipefail
DATA_VOL_SERIAL=vol09e822a28d3ce0670
EFS_ID=fs-060fb5da13f9a7e5b
LOG=/var/log/coldpath-setup.log
exec > >(tee -a $LOG) 2>&1
echo "== setup start $(date -u +%FT%TZ)"
dnf -y -q install fio gcc java-21-amazon-corretto-headless amazon-efs-utils nfs-utils xfsprogs sysstat python3 jq >/dev/null
DEV=$(lsblk -dn -o NAME,SERIAL | awk -v s=$DATA_VOL_SERIAL '$2==s{print "/dev/"$1}')
[ -n "$DEV" ] || { echo "data volume $DATA_VOL_SERIAL not found"; lsblk; exit 1; }
if ! blkid "$DEV" >/dev/null 2>&1; then mkfs.xfs -q -L osdata "$DEV"; fi
mkdir -p /data
grep -q 'LABEL=osdata' /etc/fstab || echo 'LABEL=osdata /data xfs defaults,noatime,nofail 0 2' >> /etc/fstab
mountpoint -q /data || mount /data
mkdir -p /mnt/efs
grep -q "$EFS_ID" /etc/fstab || echo "$EFS_ID:/ /mnt/efs efs _netdev,tls,nofail 0 0" >> /etc/fstab
mountpoint -q /mnt/efs || mount /mnt/efs
mkdir -p /data/fio /mnt/efs/storage-branch/fio /data/coldpath
cat > /etc/sysctl.d/90-opensearch.conf <<'EOF'
vm.max_map_count = 262144
EOF
sysctl -q --system
echo "== facts"
uname -r
java -version 2>&1 | head -1
fio --version
echo "DEV=$DEV"; lsblk -o NAME,SIZE,SERIAL,MOUNTPOINT
findmnt -no SOURCE,FSTYPE,OPTIONS /data
B=$(basename $DEV)
echo "ebs_blockdev_ra_sectors=$(blockdev --getra $DEV) read_ahead_kb=$(cat /sys/block/$B/queue/read_ahead_kb) scheduler=$(cat /sys/block/$B/queue/scheduler) max_sectors_kb=$(cat /sys/block/$B/queue/max_sectors_kb) max_hw_sectors_kb=$(cat /sys/block/$B/queue/max_hw_sectors_kb) nr_requests=$(cat /sys/block/$B/queue/nr_requests)"
findmnt -no SOURCE,FSTYPE,OPTIONS /mnt/efs
grep ' /mnt/efs ' /proc/mounts
BDI=$(mountpoint -d /mnt/efs); echo "efs_bdi=$BDI read_ahead_kb=$(cat /sys/class/bdi/$BDI/read_ahead_kb)"
echo "nconnect=$(grep ' /mnt/efs ' /proc/mounts | grep -o 'nconnect=[0-9]*' || echo 'not set (1 TCP connection)')"
nfsstat -m 2>/dev/null || true
cat /etc/amazon/efs/efs-utils.conf | grep -v '^#' | grep -v '^$' | head -40
ls /etc/udev/rules.d/ /usr/lib/udev/rules.d/ | grep -i read-ahead || true
cat /usr/lib/udev/rules.d/*read-ahead* 2>/dev/null || true
rpm -q amazon-efs-utils stunnel java-21-amazon-corretto-headless kernel fio || true
echo "thp=$(cat /sys/kernel/mm/transparent_hugepage/enabled)"
nproc; free -g
echo "== setup done $(date -u +%FT%TZ)"
