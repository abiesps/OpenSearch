#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Install the POC artifact poc_iosize2 and two systemd units opensearch-poc-ebs / opensearch-poc-efs that share the
# binary and differ only in path.data (EBS /data/ebs/opensearch vs EFS /mnt/efs/storage-branch/opensearch).
# IO configuration of common-rules.md: 8 KiB blocks, 32 KiB random, 128 KiB sequential, read_hint auto (willneed).
set -euo pipefail
BUCKET=coldpath-poc-823904838333-usw2
ART=artifacts/2026-10-04/opensearch-poc-3.10.0-SNAPSHOT-oseb26156111a-lucene59f87aa1af-bufferpool-iosize2-no-jdk-linux-x64.tar.gz
SHA=7525e2cb3bd1cee06d76e36cd400fc6f2f13e708949349ae0749bca3fce2365b
JAR_SHA=417b86f6bbbdc662fe301514e89eb083803ec5648921d3da40796ada700fdc37
id opensearch >/dev/null 2>&1 || useradd --system --home-dir /opt/opensearch-poc --shell /sbin/nologin opensearch
if [ ! -f /opt/opensearch-poc/.sha256 ] || [ "$(cat /opt/opensearch-poc/.sha256)" != "$SHA" ]; then
  aws s3 cp --only-show-errors s3://$BUCKET/$ART /tmp/poc.tar.gz
  echo "$SHA  /tmp/poc.tar.gz" | sha256sum -c -
  rm -rf /opt/opensearch-poc && mkdir -p /opt/opensearch-poc
  tar -xzf /tmp/poc.tar.gz -C /opt/opensearch-poc --strip-components=1
  echo "$SHA" > /opt/opensearch-poc/.sha256
fi
J=$(ls /opt/opensearch-poc/plugins/store-bufferpool/store-bufferpool-*.jar)
echo "plugin_jar=$J sha256=$(sha256sum $J | cut -d' ' -f1) expected=$JAR_SHA"
[ "$(sha256sum $J | cut -d' ' -f1)" = "$JAR_SHA" ] || { echo "plugin jar mismatch"; exit 1; }
mkdir -p /data/ebs/opensearch /data/repo /data/logs/poc-ebs /data/logs/poc-efs /mnt/efs/storage-branch/opensearch
chown -R opensearch:opensearch /opt/opensearch-poc /data/ebs /data/repo /data/logs /mnt/efs/storage-branch/opensearch
JAVA_HOME_HOST=$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")
for arm in ebs efs; do
  CONF=/etc/opensearch/poc-$arm
  mkdir -p $CONF/jvm.options.d
  cp -f /opt/opensearch-poc/config/jvm.options $CONF/jvm.options
  cp -f /opt/opensearch-poc/config/log4j2.properties $CONF/log4j2.properties
  if [ $arm = ebs ]; then PDATA=/data/ebs/opensearch; else PDATA=/mnt/efs/storage-branch/opensearch; fi
  cat > $CONF/opensearch.yml <<EOF
cluster.name: coldpath-storage-poc-$arm
node.name: storage-poc-$arm
path.data: $PDATA
path.logs: /data/logs/poc-$arm
path.repo: ["/data/repo"]
network.host: ["127.0.0.1"]
http.port: 9200
discovery.type: single-node
action.destructive_requires_name: true
bufferpool.cache.size: 48gb
bufferpool.cache.block_size: 8kb
bufferpool.io.random_read_size: 32kb
bufferpool.io.sequential_read_size: 128kb
EOF
  cat > $CONF/jvm.options.d/coldpath.options <<'EOF'
-Xms31g
-Xmx31g
-XX:MaxDirectMemorySize=64g
-da
-dsa
EOF
  chown -R opensearch:opensearch $CONF
  cat > /etc/systemd/system/opensearch-poc-$arm.service <<EOF
[Unit]
Description=OpenSearch POC poc_iosize2 (bufferpool) with path.data on $arm
After=network-online.target remote-fs.target
Wants=network-online.target
[Service]
Type=simple
User=opensearch
Group=opensearch
Environment=OPENSEARCH_PATH_CONF=$CONF
Environment=OPENSEARCH_JAVA_HOME=$JAVA_HOME_HOST
ExecStart=/opt/opensearch-poc/bin/opensearch
LimitNOFILE=1048576
LimitNPROC=65535
LimitMEMLOCK=infinity
LimitAS=infinity
LimitFSIZE=infinity
TimeoutStopSec=180
KillSignal=SIGTERM
SuccessExitStatus=143
[Install]
WantedBy=multi-user.target
EOF
done
systemctl daemon-reload
echo "JAVA_HOME_HOST=$JAVA_HOME_HOST"; java -version 2>&1 | head -2
