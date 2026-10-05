#!/usr/bin/env bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Puts the OpenSearch store-bufferpool directory on the luceneutil run classpath of both Lucene checkouts (user
# decision 2026-10-05: the bufferpool is the baseline; stock Lucene on it is the luceneutil baseline, the Lucene fork on
# the same bufferpool is the changes configuration).
#   - the plugin jar and its bundled dependencies, UNCHANGED, from the plugin directory of the given OpenSearch
#     distribution (the same plugin build as the OpenSearch configurations);
#   - the OpenSearch library jars the plugin's directory classes load (opensearch-common, opensearch-secure-sm,
#     log4j-api, log4j-core, jna) from the same distribution's lib/;
#   - org.opensearch.common.io.Channels* class files extracted unchanged from the distribution's server jar (the whole
#     server jar is not put on the classpath: its codec SPI files would add OpenSearch codecs to luceneutil's Lucene);
#   - the coldpath shim (bufferpool/LuceneutilBufferPool.java) compiled against that plugin jar.
# HdrHistogram: luceneutil's own lib/HdrHistogram.jar stays (one copy on the classpath); the shim smoke checks the
# plugin's histogram classes load from it. Every jar is recorded with its sha256 in <BASE>/bufferpool-classpath.txt.
# Never deletes anything except the coldpath-bp-*.jar files this script wrote into luceneutil/lib before.
#
#   setup_bufferpool.sh BASE OPENSEARCH_DIST_DIR
#   e.g. setup_bufferpool.sh /data/ebs/lu /opt/opensearch-baseline
set -euo pipefail
BASE=${1:?BASE}
DIST=${2:?OPENSEARCH_DIST_DIR}
HERE=$(cd "$(dirname "$0")" && pwd)
PLUGIN=$(ls -d "$DIST"/plugins/store-bufferpool)
JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-amazon-corretto.x86_64}
W=$(mktemp -d)
pick() { ls "$DIST"/lib/$1 2>/dev/null | head -1; }
JARS=("$PLUGIN"/store-bufferpool-*.jar)
for j in "$PLUGIN"/*.jar; do case "$(basename "$j")" in store-bufferpool-*) ;; *) JARS+=("$j");; esac; done
for pat in 'opensearch-common-*.jar' 'opensearch-secure-sm-*.jar' 'log4j-api-*.jar' 'log4j-core-*.jar' 'jna-[0-9]*.jar'; do
  j=$(pick "$pat"); test -n "$j" || { echo "no $pat in $DIST/lib"; exit 1; }; JARS+=("$j")
done
SERVER=$(pick 'opensearch-[0-9]*.jar'); test -n "$SERVER"
mkdir -p "$W/channels" && (cd "$W/channels" && unzip -q "$SERVER" 'org/opensearch/common/io/Channels*.class')
(cd "$W/channels" && "$JAVA_HOME/bin/jar" --create --file "$W/coldpath-bp-server-channels.jar" org)
CP=$(IFS=:; echo "${JARS[*]}"):$W/coldpath-bp-server-channels.jar:$(ls "$BASE"/lucene-stock/lucene/core/build/libs/lucene-core-*.jar)
mkdir -p "$W/shim"
"$JAVA_HOME/bin/javac" -proc:none -d "$W/shim" -cp "$CP" "$HERE/bufferpool/LuceneutilBufferPool.java"
(cd "$W/shim" && "$JAVA_HOME/bin/jar" --create --file "$W/coldpath-bp-shim.jar" org)
REC="$BASE/bufferpool-classpath.txt"
{
  echo "# $(date -u +%FT%TZ) setup_bufferpool.sh $*  (harness $(git -C "$HERE" rev-parse --short=11 HEAD 2>/dev/null || echo unknown))"
  echo "distribution $DIST"
  for j in "${JARS[@]}" "$SERVER"; do sha256sum "$j"; done
  sha256sum "$W/coldpath-bp-server-channels.jar" "$W/coldpath-bp-shim.jar"
} > "$REC"
for c in lucene-stock lucene-poc; do
  L="$BASE/$c/luceneutil/lib"
  rm -f "$L"/coldpath-bp-*.jar
  for j in "${JARS[@]}"; do cp "$j" "$L/coldpath-bp-$(basename "$j")"; done
  cp "$W/coldpath-bp-server-channels.jar" "$W/coldpath-bp-shim.jar" "$L/"
  echo "$c: $(ls "$L" | tr '\n' ' ')" >> "$REC"
done
cat "$REC"
rm -rf "$W"
