#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""Small shared helpers: a JSON-over-HTTP client (stdlib only, keep-alive) and JSONL writing."""
import http.client
import json
import os
import time
import urllib.parse


class HttpError(RuntimeError):
    def __init__(self, status, method, path, body):
        super().__init__(f"{method} {path} -> HTTP {status}: {body[:2000]}")
        self.status = status
        self.body = body


class JsonClient:
    """
    One keep-alive HTTP connection to a base URL. request() returns the decoded JSON body; non-2xx raises HttpError.
    Reconnects once when the server closed the connection (for example after a node restart).
    """

    def __init__(self, base_url, timeout=7200.0, headers=None):
        u = urllib.parse.urlsplit(base_url)
        if u.scheme not in ("http", "https"):
            raise ValueError(f"unsupported URL {base_url}")
        self.scheme, self.host, self.port = u.scheme, u.hostname, u.port or (443 if u.scheme == "https" else 80)
        self.timeout = timeout
        self.headers = dict(headers or {})
        self.conn = None
        self.last_wall_s = None  # network time of the last request: send to end of response read, no JSON work

    def _connect(self):
        cls = http.client.HTTPSConnection if self.scheme == "https" else http.client.HTTPConnection
        self.conn = cls(self.host, self.port, timeout=self.timeout)

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def raw(self, method, path, body=None, timeout=None):
        """(status, decoded JSON or text). body: dict/list (JSON), str (sent as is, e.g. NDJSON) or None."""
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode()
        elif isinstance(body, str):
            data = body.encode()
        else:
            data = body
        headers = {"Content-Type": "application/json", "Accept": "application/json", **self.headers}
        for attempt in (0, 1):
            if self.conn is None:
                self._connect()
            if timeout is not None:
                self.conn.timeout = timeout
                if self.conn.sock is not None:
                    self.conn.sock.settimeout(timeout)
            try:
                t0 = time.perf_counter()
                self.conn.request(method, path, body=data, headers=headers)
                resp = self.conn.getresponse()
                raw = resp.read()
                self.last_wall_s = time.perf_counter() - t0
                break
            except (http.client.RemoteDisconnected, http.client.CannotSendRequest, BrokenPipeError,
                    ConnectionResetError, ConnectionRefusedError):
                self.close()
                if attempt == 1:
                    raise
        text = raw.decode("utf-8", "replace")
        try:
            value = json.loads(text) if text else {}
        except json.JSONDecodeError:
            value = text
        return resp.status, value

    def request(self, method, path, body=None, timeout=None):
        status, value = self.raw(method, path, body, timeout)
        if status // 100 != 2:
            raise HttpError(status, method, path, value if isinstance(value, str) else json.dumps(value))
        return value


def wait_until(predicate, timeout_s, pause_s=1.0, what="condition"):
    """Polls predicate() until it returns a truthy value (returned) or the timeout expires (raises)."""
    deadline = time.monotonic() + timeout_s
    last_error = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except Exception as e:  # noqa: BLE001 - polling a node that may still be starting
            last_error = e
        time.sleep(pause_s)
    raise TimeoutError(f"timed out after {timeout_s}s waiting for {what}" + (f" (last error: {last_error})" if last_error else ""))


class JsonlWriter:
    """Append-only JSONL file, flushed and fsynced per record so a crash loses at most the record in flight."""

    def __init__(self, path):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.f = open(path, "a", encoding="utf-8")

    def write(self, record):
        self.f.write(json.dumps(record, sort_keys=True, default=str) + "\n")
        self.f.flush()
        os.fsync(self.f.fileno())

    def close(self):
        self.f.close()


def read_jsonl(path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)
