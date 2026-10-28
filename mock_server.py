#!/usr/bin/env python3
"""
Local stand-in for the Harness Runtime REST API, used ONLY to test the benchmark harness
(orchestration, timing brackets, failure accounting, cleanup, statistics).

IMPORTANT: this mock encodes the harness author's *assumptions* about the API's shape
(see PATHS in ma_bench.py). Passing tests against it prove the harness logic is sound; they say
nothing about real DigitalOcean latencies or about whether the assumptions are right.
Use `ma_bench.py probe` against the real API for that.

Latencies are configurable and deterministic so tests can assert the harness measures them.
"""
from __future__ import annotations

import argparse
import json
import re
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TOKEN = "test-token"


class State:
    def __init__(self, ready_delay=0.3, pause_delay=0.1, resume_delay=0.15, exec_delay=0.02,
                 exec_fail_rate=0.0, max_active=0, create_call_delay=0.005):
        self.ready_delay, self.pause_delay = ready_delay, pause_delay
        self.resume_delay, self.exec_delay = resume_delay, exec_delay
        self.exec_fail_rate, self.max_active = exec_fail_rate, max_active
        self.create_call_delay = create_call_delay
        self.lock = threading.Lock()
        self.sessions = {}
        self.created = 0
        self.destroyed = 0
        self.requests = 0
        self.n_exec = 0

    def live(self):
        with self.lock:
            return [s for s in self.sessions.values() if not s.get("destroyed")]

    def status(self, s, now):
        if s["phase"] == "creating":
            return "SESSION_STATUS_READY" if now >= s["until"] else "SESSION_STATUS_PROVISIONING"
        if s["phase"] == "pausing":
            return "SESSION_STATUS_PAUSED" if now >= s["until"] else "SESSION_STATUS_READY"
        if s["phase"] == "resuming":
            return "SESSION_STATUS_READY" if now >= s["until"] else "SESSION_STATUS_PAUSED"
        return "SESSION_STATUS_READY" if s["phase"] == "ready" else "SESSION_STATUS_PAUSED"


def make_handler(state: State):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        disable_nagle_algorithm = True      # avoid Nagle + delayed-ACK 40 ms stalls in the mock

        def log_message(self, *a):
            pass

        def _send(self, code, obj=None):
            body = b"" if obj is None else json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("x-request-id", uuid.uuid4().hex)
            self.send_header("ratelimit-remaining", "4999")
            self.end_headers()
            if body:
                self.wfile.write(body)

        def _body(self):
            n = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(n).decode() if n else ""

        def _auth(self):
            if self.headers.get("Authorization") == "Bearer forbidden-token":
                self._send(403, {"id": "Forbidden", "message": "You are not authorized to perform this operation"})
                return False
            if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                self._send(401, {"id": "unauthorized", "message": "Unable to authenticate you."})
                return False
            return True

        def do_GET(self):
            self._route("GET")

        def do_POST(self):
            self._route("POST")

        def do_DELETE(self):
            self._route("DELETE")

        def _route(self, method):
            with state.lock:
                state.requests += 1
            raw = self._body()
            if not self._auth():
                return
            path = self.path.split("?")[0]
            now = time.monotonic()
            if method == "GET" and path == "/v2/agents/sessions/sandbox/sizes":
                return self._send(200, {"sizes": [{"slug": "mars-2vcpu-4gb"}]})
            if method == "POST" and path == "/v2/agents/sessions":
                time.sleep(state.create_call_delay)
                name = None
                if self.headers.get("Content-Type", "").startswith("application/json"):
                    name = (json.loads(raw or "{}")).get("name")
                else:
                    m = re.search(r"^name:\s*(\S+)", raw, re.M)
                    name = m.group(1) if m else None
                with state.lock:
                    if any(s["name"] == name for s in state.sessions.values() if not s.get("destroyed")):
                        return self._send(409, {"id": "conflict", "message": "name in use"})
                    if state.max_active and len([s for s in state.sessions.values()
                                                 if not s.get("destroyed")]) >= state.max_active:
                        return self._send(429, {"id": "too_many_requests", "message": "session limit"})
                    sid = str(uuid.uuid4())
                    state.sessions[sid] = {"id": sid, "name": name, "phase": "creating",
                                           "until": now + state.ready_delay}
                    state.created += 1
                return self._send(201, {"session": {"session_id": sid, "name": name,
                                                    "status": "SESSION_STATUS_PROVISIONING"}})
            m = re.match(r"^/v2/agents/sessions/([0-9a-f-]+)(/.*)?$", path)
            if not m:
                return self._send(404, {"id": "not_found", "message": "no route"})
            sid, sub = m.group(1), m.group(2) or ""
            with state.lock:
                s = state.sessions.get(sid)
            if not s or s.get("destroyed"):
                return self._send(404, {"id": "not_found", "message": "session not found"})
            if method == "GET" and sub == "":
                return self._send(200, {"session": {"session_id": sid, "name": s["name"],
                                                    "status": state.status(s, now)}})
            if method == "DELETE" and sub == "":
                with state.lock:
                    s["destroyed"] = True
                    state.destroyed += 1
                return self._send(204)
            if method == "POST" and sub == "/pause":
                s["phase"], s["until"] = "pausing", now + state.pause_delay
                return self._send(200, {"session": {"session_id": sid, "status": "SESSION_STATUS_READY"}})
            if method == "POST" and sub == "/resume":
                s["phase"], s["until"] = "resuming", now + state.resume_delay
                return self._send(200, {"session": {"session_id": sid, "status": "SESSION_STATUS_PAUSED"}})
            if method == "POST" and sub == "/sandbox/exec":
                st = state.status(s, now)
                time.sleep(state.exec_delay)
                with state.lock:
                    state.n_exec += 1
                    k = state.n_exec
                if st != "SESSION_STATUS_READY":
                    return self._send(409, {"id": "conflict", "message": "session not ready"})
                if state.exec_fail_rate and (k * 0.6180339887 % 1.0) < state.exec_fail_rate:
                    return self._send(500, {"id": "server_error", "message": "boom"})
                return self._send(200, {"exit_code": 0, "stdout": "TGludXgK", "stderr": ""})
            return self._send(404, {"id": "not_found", "message": "no route"})

    return H


def start_mock(**kw):
    state = State(**kw)
    srv = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(state))
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, state, f"http://127.0.0.1:{srv.server_address[1]}"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--ready-delay", type=float, default=0.9)
    a = ap.parse_args()
    state = State(ready_delay=a.ready_delay)
    srv = ThreadingHTTPServer(("127.0.0.1", a.port), make_handler(state))
    print(f"mock on http://127.0.0.1:{a.port}  token={TOKEN}")
    srv.serve_forever()
