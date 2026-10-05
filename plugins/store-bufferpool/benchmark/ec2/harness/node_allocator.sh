#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# The C memory allocator of every OpenSearch node unit on a data node (common-rules.md, USER DECISION 2026-10-05:
# "For memory fragmentation use jemalloc"). The bufferpool allocates every 8 KiB cache block with its own
# ByteBuffer.allocateDirect; glibc malloc fragments that memory under cache churn (geoshape host: resident memory
# 73 -> 151 GiB over 5 fills of a 44 GB cache). Every node unit gets the SAME systemd drop-in, so every configuration
# (baseline and proof of concept, Amazon EBS and Amazon EFS) runs with the same allocator and the same environment.
#
#   node_allocator.sh install                     install the pinned jemalloc (dnf, else the S3 copy), check sha256
#   node_allocator.sh apply [--allocator A] [--malloc-conf S] [UNIT ...]
#                                                 write the drop-in to every unit (default: every opensearch-*.service),
#                                                 remove earlier MALLOC_ARENA_MAX settings (backed up), daemon-reload,
#                                                 write /etc/coldpath/allocator.json (the agent reports it per run)
#   node_allocator.sh show [UNIT ...]             the allocator variables of every unit (systemctl show); fails when
#                                                 the units differ
#
# A = jemalloc (default) | glibc (no preload, no MALLOC_* variable) | glibc-arenas2 (MALLOC_ARENA_MAX=2, the interim
# setting; kept for comparisons only). Run as root. Takes effect at the next node start (coldbench restarts the node
# for every run); it never starts or stops a node. The harness verifies it at every node start (agent GET /node/memory
# reads /proc/<pid>/maps for libjemalloc and /proc/<pid>/environ) and refuses a run whose allocator differs from the
# arms file's "allocator" block. Validation, version and MALLOC_CONF choice:
# .agents/tasks/baseline-bufferpool-2026-10-05/jemalloc-validation.md.
#
# Test hooks (selftest_baseline.py only): NODE_ALLOCATOR_ROOT=<dir> puts /etc and /var/lib under <dir> and skips the
# root check; SYSTEMCTL=<program> replaces systemctl.
set -euo pipefail

# ---- pinned jemalloc (Amazon Linux 2023 package 5.2.1-7, the AL2023 repository build)
JEMALLOC_NEVRA="jemalloc-5.2.1-7.amzn2023.x86_64"
JEMALLOC_VERSION="5.2.1"
JEMALLOC_SO_SHA256="99d2ff028c115cfba4d8e249bca417e03b36e650b257550e687b52ac40be65bf"
JEMALLOC_RPM_SHA256="bee4279b939dc46e347cc5b1131557d9cd266398e0236a81426d0add84d64578"
JEMALLOC_RPM_S3="s3://coldpath-poc-823904838333-usw2/artifacts/2026-10-05/jemalloc/$JEMALLOC_NEVRA.rpm"
# background_thread:true: jemalloc returns unused dirty pages to the kernel from its own background thread on a timer,
# not only inside malloc and free calls of the application threads (an idle node also gives memory back). The decay
# times stay the 5.2.1 defaults (dirty_decay_ms 10000, muzzy_decay_ms 0).
DEFAULT_MALLOC_CONF="background_thread:true"
DROPIN_NAME="90-coldpath-allocator.conf"

R="${NODE_ALLOCATOR_ROOT:-}"
SYSTEMCTL="${SYSTEMCTL:-systemctl}"
UNIT_DIR="$R/etc/systemd/system"
STATE="$R/etc/coldpath/allocator.json"
BACKUP_ROOT="$R/var/lib/coldpath/allocator-backup"
JEMALLOC_SO="/usr/lib64/libjemalloc.so.2"
[ -n "$R" ] && JEMALLOC_SO_CHECK="$R$JEMALLOC_SO" || JEMALLOC_SO_CHECK="$JEMALLOC_SO"

die() { echo "node_allocator.sh: $*" >&2; exit 1; }
need_root() { [ -n "$R" ] || [ "$(id -u)" = 0 ] || die "run as root"; }
sha256() { if command -v sha256sum >/dev/null; then sha256sum "$1" | awk '{print $1}'; else shasum -a 256 "$1" | awk '{print $1}'; fi; }
sha256_stdin() { if command -v sha256sum >/dev/null; then sha256sum | awk '{print $1}'; else shasum -a 256 | awk '{print $1}'; fi; }

cmd_install() {
  need_root
  if rpm -q "$JEMALLOC_NEVRA" >/dev/null 2>&1; then
    echo "installed: $JEMALLOC_NEVRA"
  elif rpm -q jemalloc >/dev/null 2>&1; then
    die "another jemalloc is installed ($(rpm -q jemalloc)); the pinned build is $JEMALLOC_NEVRA"
  else
    if ! dnf install -y -q "$JEMALLOC_NEVRA" 2>/dev/null; then
      local rpmf=/tmp/$JEMALLOC_NEVRA.rpm
      aws s3 cp --only-show-errors "$JEMALLOC_RPM_S3" "$rpmf"
      [ "$(sha256 "$rpmf")" = "$JEMALLOC_RPM_SHA256" ] || die "rpm sha256 differs from the pinned value"
      rpm -i "$rpmf"
    fi
    echo "installed: $(rpm -q jemalloc)"
  fi
  [ -r "$JEMALLOC_SO" ] || die "$JEMALLOC_SO missing"
  local got; got=$(sha256 "$JEMALLOC_SO")
  [ "$got" = "$JEMALLOC_SO_SHA256" ] || die "$JEMALLOC_SO sha256 $got differs from the pinned $JEMALLOC_SO_SHA256"
  echo "$JEMALLOC_SO sha256 $got"
}

ALLOC_VARS_RE='^[[:space:]]*Environment="?(MALLOC_ARENA_MAX|LD_PRELOAD|MALLOC_CONF)='

# Remove MALLOC_ARENA_MAX / LD_PRELOAD / MALLOC_CONF assignments that earlier interim setups wrote into the unit file or
# into other drop-ins (sed into the unit file, coldpath-malloc.conf, 50-malloc-arena.conf); every changed file is
# copied to the backup directory first. A drop-in left with nothing but [Service] and comments is removed (backup kept).
scrub_unit() {
  local unit=$1 bdir=$2 f
  for f in "$UNIT_DIR/$unit" "$UNIT_DIR/$unit.d"/*.conf; do
    [ -f "$f" ] || continue
    [ "$(basename "$f")" = "$DROPIN_NAME" ] && continue
    grep -Eq "$ALLOC_VARS_RE" "$f" || continue
    mkdir -p "$bdir$(dirname "$f")"
    cp -p "$f" "$bdir$f"
    grep -Ev "$ALLOC_VARS_RE" "$f" > "$f.tmp" || true
    cat "$f.tmp" > "$f" && rm -f "$f.tmp"
    echo "  $unit: removed earlier allocator variables from $f (backup $bdir$f)"
    if [ "$f" != "$UNIT_DIR/$unit" ] && ! grep -Evq '^[[:space:]]*(#.*)?$|^\[Service\][[:space:]]*$' "$f"; then
      rm -f "$f"
      echo "  $unit: $f had no other setting; removed"
    fi
  done
}

dropin_text() {
  local allocator=$1 conf=$2
  echo "[Service]"
  echo "# coldpath node allocator: $allocator (written by node_allocator.sh; identical in every node unit)"
  case "$allocator" in
    jemalloc)
      echo "UnsetEnvironment=MALLOC_ARENA_MAX"
      echo "Environment=LD_PRELOAD=$JEMALLOC_SO"
      echo "Environment=\"MALLOC_CONF=$conf\""
      ;;
    glibc)
      echo "UnsetEnvironment=MALLOC_ARENA_MAX LD_PRELOAD MALLOC_CONF"
      ;;
    glibc-arenas2)
      echo "UnsetEnvironment=LD_PRELOAD MALLOC_CONF"
      echo "Environment=MALLOC_ARENA_MAX=2"
      ;;
    *) die "allocator $allocator: jemalloc, glibc or glibc-arenas2" ;;
  esac
}

json_str() { if [ -n "$1" ]; then printf '"%s"' "$1"; else printf 'null'; fi; }

list_units() {
  # the units given, else every opensearch-*.service unit file
  local u
  if [ $# -gt 0 ]; then
    for u in "$@"; do case "$u" in *.service) echo "$u" ;; *) echo "$u.service" ;; esac; done
  else
    for u in "$UNIT_DIR"/opensearch-*.service; do [ -f "$u" ] && basename "$u"; done
  fi
  return 0
}

cmd_apply() {
  need_root
  local allocator=jemalloc conf=$DEFAULT_MALLOC_CONF
  while [ $# -gt 0 ]; do
    case "$1" in
      --allocator) allocator=$2; shift 2 ;;
      --malloc-conf) conf=$2; shift 2 ;;
      --) shift; break ;;
      -*) die "unknown option $1" ;;
      *) break ;;
    esac
  done
  case "$conf" in *[[:space:]\"\\]*) die "MALLOC_CONF must not contain spaces, quotes or backslashes" ;; esac
  [ "$allocator" = jemalloc ] || conf=""
  local units; units=$(list_units "$@")
  [ -n "$units" ] || die "no unit given and no $UNIT_DIR/opensearch-*.service"
  local so_sha="" pkg=""
  if [ "$allocator" = jemalloc ]; then
    [ -r "$JEMALLOC_SO_CHECK" ] || die "$JEMALLOC_SO missing: run node_allocator.sh install first"
    so_sha=$(sha256 "$JEMALLOC_SO_CHECK")
    [ "$so_sha" = "$JEMALLOC_SO_SHA256" ] || die "$JEMALLOC_SO sha256 $so_sha is not the pinned build $JEMALLOC_SO_SHA256"
    pkg=$(rpm -q jemalloc 2>/dev/null || echo "$JEMALLOC_NEVRA (rpm not available)")
  fi
  local bdir; bdir=$BACKUP_ROOT/$(date -u +%Y%m%dT%H%M%SZ)
  local text; text=$(dropin_text "$allocator" "$conf")
  local dsha; dsha=$(printf '%s\n' "$text" | sha256_stdin)
  local u ulist=""
  for u in $units; do
    [ -f "$UNIT_DIR/$u" ] || die "unit file $UNIT_DIR/$u not found"
  done
  for u in $units; do
    scrub_unit "$u" "$bdir"
    mkdir -p "$UNIT_DIR/$u.d"
    printf '%s\n' "$text" > "$UNIT_DIR/$u.d/$DROPIN_NAME"
    echo "  $u: $UNIT_DIR/$u.d/$DROPIN_NAME sha256 $dsha"
    ulist="$ulist\"$u\","
  done
  "$SYSTEMCTL" daemon-reload
  mkdir -p "$(dirname "$STATE")"
  local arena=""; [ "$allocator" = glibc-arenas2 ] && arena=2
  local je=""; [ "$allocator" = jemalloc ] && je=$JEMALLOC_SO
  local ver=""; [ "$allocator" = jemalloc ] && ver=$JEMALLOC_VERSION
  cat > "$STATE.tmp" <<EOF
{"allocator": "$allocator", "ld_preload": $(json_str "$je"), "malloc_conf": $(json_str "$conf"),
 "malloc_arena_max": $(json_str "$arena"), "jemalloc_version": $(json_str "$ver"),
 "jemalloc_package": $(json_str "$pkg"), "jemalloc_so_sha256": $(json_str "$so_sha"),
 "dropin": "$DROPIN_NAME", "dropin_sha256": "$dsha", "units": [${ulist%,}],
 "written_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)", "backup_dir": "$bdir"}
EOF
  mv "$STATE.tmp" "$STATE"
  chmod 644 "$STATE"
  cmd_show $units
}

cmd_show() {
  local units; units=$(list_units "$@")
  local u first="" same=1 n=0
  for u in $units; do
    local env unset_v
    env=$("$SYSTEMCTL" show -p Environment --value "$u" | tr ' ' '\n' | grep -E '^(LD_PRELOAD|MALLOC_CONF|MALLOC_ARENA_MAX)=' | sort | tr '\n' ' ' || true)
    unset_v=$("$SYSTEMCTL" show -p UnsetEnvironment --value "$u")
    echo "$u: environment [${env% }] unset [$unset_v]"
    local sig="$env|$unset_v"
    if [ -z "$first" ]; then first=$sig; elif [ "$sig" != "$first" ]; then same=0; fi
    n=$((n + 1))
  done
  [ $same = 1 ] || die "node units differ in their allocator environment"
  echo "all $n units: same allocator environment"
  [ -f "$STATE" ] && cat "$STATE"
  return 0
}

case "${1:-}" in
  install) shift; cmd_install "$@" ;;
  apply) shift; cmd_apply "$@" ;;
  show) shift; cmd_show "$@" ;;
  *) die "usage: node_allocator.sh install | apply [--allocator jemalloc|glibc|glibc-arenas2] [--malloc-conf S] [UNIT ...] | show [UNIT ...]" ;;
esac
