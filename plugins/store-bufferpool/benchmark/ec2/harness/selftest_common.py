#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""
Checks of common.JsonClient (stdlib, local HTTP server, no AWS):
  a per-call timeout applies to that call only: after request(..., timeout=0.2) on a keep-alive connection, a call
  without a timeout to an endpoint that answers after 0.6 s succeeds (the client's own timeout applies again), and a
  call with timeout=0.2 to it still times out.
  selftest_common.py
"""
import os
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import JsonClient  # noqa: E402

N = 0


def check(cond, what):
    global N
    if not cond:
        print(f"SELFTEST-COMMON FAIL: {what}")
        sys.exit(1)
    N += 1


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path == "/slow":
            time.sleep(0.6)
        body = b'{"ok": true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def main():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    c = JsonClient(f"http://127.0.0.1:{srv.server_address[1]}", timeout=30.0)
    check(c.request("GET", "/fast", timeout=0.2) == {"ok": True}, "fast call with a per-call timeout")
    check(c.request("GET", "/slow") == {"ok": True}, "a later call without a timeout uses the client timeout (30 s)")
    try:
        c.request("GET", "/slow", timeout=0.2)
        check(False, "a per-call timeout of 0.2 s on a 0.6 s answer times out")
    except (socket.timeout, TimeoutError):
        c.close()
        check(True, "per-call timeout still applies to its own call")
    check(c.request("GET", "/slow") == {"ok": True}, "after a timed-out call, the next call without a timeout succeeds")
    srv.shutdown()
    print(f"SELFTEST-COMMON PASS: {N} checks")


if __name__ == "__main__":
    main()
