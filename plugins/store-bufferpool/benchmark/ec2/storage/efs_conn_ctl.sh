#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Controls for the EFS per-connection blocks (storage-model.md section 4.2.2), run as root on the data node.
#   efs_conn_ctl.sh log-on     efs-proxy INFO log (stunnel_debug_enabled = true, [proxy] proxy_logging_level = INFO);
#                              keeps the current file as efs-utils.conf.pre-conn; takes effect at the next mount
#   efs_conn_ctl.sh log-off    restores efs-utils.conf.pre-conn
#   efs_conn_ctl.sh pin-on     nftables rule: at most ONE TCP connection from this host to the mount target's
#                              port 2049 (new SYNs above it get a TCP reset), so efs-proxy cannot scale up from
#                              one backend connection (its scale-up then fails and backs off for 300 s)
#   efs_conn_ctl.sh pin-off    removes the rule (table inet coldpath_efs_pin)
#   efs_conn_ctl.sh status     prints the config, the rule and the proxy's backend connections
set -euo pipefail
CONF=/etc/amazon/efs/efs-utils.conf
MT_IP=${MT_IP:-$(getent hosts fs-060fb5da13f9a7e5b.efs.us-west-2.amazonaws.com | awk '{print $1}')}
case "${1:-status}" in
  log-on)
    [ -f $CONF.pre-conn ] || cp -p $CONF $CONF.pre-conn
    python3 - "$CONF" <<'EOF'
import configparser, sys
p = sys.argv[1]
c = configparser.RawConfigParser()
c.read(p)
c.set("mount", "stunnel_debug_enabled", "true")
if not c.has_section("proxy"):
    c.add_section("proxy")
c.set("proxy", "proxy_logging_level", "INFO")
c.set("proxy", "proxy_logging_max_bytes", "268435456")
c.set("proxy", "proxy_logging_file_count", "4")
with open(p, "w") as f:
    c.write(f)
EOF
    ;;
  log-off)
    [ -f $CONF.pre-conn ] && cp -p $CONF.pre-conn $CONF && rm -f $CONF.pre-conn
    ;;
  pin-on)
    [ -n "$MT_IP" ] || { echo "mount target IP not resolved"; exit 2; }
    nft list table inet coldpath_efs_pin >/dev/null 2>&1 && nft delete table inet coldpath_efs_pin
    nft add table inet coldpath_efs_pin
    nft add chain inet coldpath_efs_pin out '{ type filter hook output priority 0; policy accept; }'
    nft add rule inet coldpath_efs_pin out ip daddr "$MT_IP" tcp dport 2049 tcp flags syn ct count over 1 reject with tcp reset
    ;;
  pin-off)
    nft list table inet coldpath_efs_pin >/dev/null 2>&1 && nft delete table inet coldpath_efs_pin || true
    ;;
esac
echo "mount target $MT_IP"
grep -nE "stunnel_debug_enabled|^\[proxy\]|proxy_logging|optimize_readahead" $CONF || true
nft list table inet coldpath_efs_pin 2>/dev/null || echo "no pin rule"
ss -tnpH state established "( dport = :2049 )" || true
