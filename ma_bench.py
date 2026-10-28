#!/usr/bin/env python3
"""
ma_bench: lifecycle latency benchmark for DigitalOcean Managed Agents (Harness Runtime).

Measures, from the client's point of view and over the public REST API:
  cold start      create request -> READY, create request -> first successful exec
  steady exec     one-shot command round trip on a warm session
  pause           pause request -> PAUSED
  resume          resume request -> READY, resume request -> first successful exec
  post-resume     steady exec after resume (does resume leave the session slower?)
  destroy         destroy request round trip

Stdlib only (Python 3.9+). No third-party dependencies.

Subcommands:
  plan      estimate request volume / wall-clock / sample-size adequacy (no API calls)
  probe     run ONE verbose journey and dump raw responses (confirms the API contract)
  run       run the experiment
  report    (re)build the report from one or more run directories
  cleanup   destroy leftover sessions whose name starts with the benchmark prefix

The token is read from $DIGITALOCEAN_TOKEN only (never from argv).
"""
from __future__ import annotations

import argparse
import atexit
import csv
import datetime as dt
import http.client
import itertools
import json
import math
import os
import platform
import random
import socket
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
from collections import Counter, defaultdict, deque, namedtuple
from dataclasses import asdict, dataclass, field
from pathlib import Path

HARNESS_VERSION = "0.1.0"

# --------------------------------------------------------------------------------------
# API contract. Everything the harness assumes about the service lives in this block, so
# a wrong assumption is a one-line fix. `ma_bench.py probe` confirms each one.
#   documented = appears in the public Harness Runtime API reference
#   inferred   = inferred from operation names / sibling APIs; confirm with `probe`
# --------------------------------------------------------------------------------------
PATHS = {
    "sessions": "/v2/agents/sessions",                      # documented: POST create, GET list
    "session": "/v2/agents/sessions/{sid}",                 # documented: GET, DELETE
    "pause": "/v2/agents/sessions/{sid}/pause",             # inferred (PauseSession)
    "resume": "/v2/agents/sessions/{sid}/resume",           # inferred (ResumeSession)
    "exec": "/v2/agents/sessions/{sid}/sandbox/exec",       # documented; body {"argv": [...]}
    "events": "/v2/agents/sessions/{sid}/events",           # documented (SSE)
    "sizes": "/v2/agents/sessions/sandbox/sizes",           # documented
    "configs": "/v2/agents/configs",                        # documented
    "config": "/v2/agents/configs/{cid}",                   # documented
}
BAD_TERMINAL = {"failed", "destroyed"}

# Figures published in the Sept 22, 2026 launch post (p50, RIC1 client, 2 vCPU / 4 GB).
# Used only to print a reconciliation table. Definitions may differ from this harness.
REFERENCE_P50_MS = {
    "create_to_ready_ms": 886,
    "resume_to_ready_ms": 305,
    "exec_steady_ms": 189,
}

# metric -> (stage that produces it, description)
METRICS = {
    "create_call_ms": ("create", "create request returns"),
    "create_to_ready_ms": ("ready", "create request -> READY observed (upper bound)"),
    "create_to_exec_ms": ("first_exec", "create request -> first exec completes (poll-then-exec client)"),
    "exec_steady_ms": ("steady", "exec round trip, warm session, before pause"),
    "pause_call_ms": ("pause", "pause request returns"),
    "pause_to_paused_ms": ("paused", "pause request -> PAUSED observed (upper bound)"),
    "resume_call_ms": ("resume", "resume request returns"),
    "resume_to_ready_ms": ("resume_ready", "resume request -> READY observed (upper bound)"),
    "resume_to_exec_ms": ("resume_exec", "resume request -> first exec completes (after polling)"),
    "resume_to_usable_ms": ("resume_exec", "resume request -> first exec completes, NO polling (exec-retry client)"),
    "resume_usable_exec_ms": ("resume_exec", "duration of that successful exec alone"),
    "exec_post_resume_ms": ("post_steady", "exec round trip, warm session, after resume"),
    "destroy_ms": ("destroy", "destroy request round trip"),
}
STAGE_ORDER = ["create", "ready", "first_exec", "steady", "pause", "paused",
               "resume", "resume_ready", "resume_exec", "post_steady", "destroy"]
LIST_METRICS = {"exec_steady_ms", "exec_post_resume_ms"}
BOUND_PAIRS = {  # metric -> its lower-bound companion (width = measurement resolution)
    "create_to_ready_ms": "create_to_ready_lo_ms",
    "pause_to_paused_ms": "pause_to_paused_lo_ms",
    "resume_to_ready_ms": "resume_to_ready_lo_ms",
}

Cond = namedtuple("Cond", "size dwell_s concurrency")


# --------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------
@dataclass
class Config:
    base_url: str = "https://api.digitalocean.com"
    token: str = ""
    agent: str = "opencode"
    sizes: list = field(default_factory=lambda: ["mars-2vcpu-4gb"])
    dwell_s: list = field(default_factory=lambda: [5.0])
    concurrency: list = field(default_factory=lambda: [1])
    trials: int = 30                 # trials per condition (each trial = `concurrency` journeys)
    warmup: int = 2                  # first N trials per condition are recorded but excluded
    steady_execs: int = 5
    poll_ms: int = 100
    ready_timeout_s: float = 120.0
    request_timeout_s: float = 30.0
    create_mode: str = "yaml"        # yaml (inline manifest) | config (saved Environment Config)
    yaml_content_type: str = "application/yaml"
    exec_argv: list = field(default_factory=lambda: ["uname"])
    idle_timeout: str = "2h"
    max_rps: float = 0.0             # 0 = unlimited
    hourly_budget: int = 0           # 0 = unlimited
    budget_window_s: float = 3600.0  # window for hourly_budget (only tests change this)
    cooldown_s: float = 1.0
    seed: int = 1
    out_dir: str = "results"
    name_prefix: str = "mabench"
    label: str = ""
    max_sessions: int = 0            # hard cap on sessions created in one run (0 = none)
    resume_mode: str = "poll"        # poll: wait for READY then exec | exec: exec immediately, retry until it works
    resume_retry_ms: int = 20        # exec mode: pause between retries
    resume_retry_max: int = 100      # exec mode: give up after this many attempts


# --------------------------------------------------------------------------------------
# HTTP client: keep-alive, warm TLS, rate-limit aware, timing excludes connect + throttle
# --------------------------------------------------------------------------------------
@dataclass
class Resp:
    status: int = 0
    json: object = None
    text: str = ""
    ms: float = 0.0
    t_send: float = 0.0           # time.perf_counter() just before the request was written
    t_end: float = 0.0            # time.perf_counter() when the response body was fully read
    headers: dict = field(default_factory=dict)
    throttle_ms: float = 0.0      # time spent waiting on the client-side limiter (NOT in ms)
    error: object = None

    @property
    def ok(self):
        return self.error is None and 200 <= self.status < 300

    @property
    def request_id(self):
        return self.headers.get("x-request-id")


class Limiter:
    """
    Two independent mechanisms:
      * max_rps          per-request pacing (optional; widens polling brackets, avoid unless needed)
      * hourly_budget    BETWEEN-journey pacing via wait_for_capacity(): journeys run at full speed and
                         the harness waits before starting the next one if it would exceed the budget.
                         Nothing is ever stalled mid-journey.
    The DO API default is 250 requests/minute and 5,000 requests/hour.
    """

    def __init__(self, max_rps=0.0, hourly_budget=0, window_s=3600.0):
        self.gap = 1.0 / max_rps if max_rps > 0 else 0.0
        self.hourly, self.window_s = hourly_budget, window_s
        self.lock = threading.Lock()
        self.next_slot = 0.0
        self.window = deque()
        self.total = 0

    def acquire(self):
        """Per-request pacing. Returns seconds spent deliberately pacing (not lock overhead)."""
        with self.lock:
            now = time.monotonic()
            self._expire(now)
            slot = max(now, self.next_slot)
            self.next_slot = slot + self.gap
            self.window.append(slot)
            self.total += 1
        wait = slot - time.monotonic()
        if wait > 0:
            time.sleep(wait)
            return wait
        return 0.0

    def wait_for_capacity(self, need):
        """Block (between journeys) until `need` more requests fit inside the budget window."""
        if not self.hourly:
            return 0.0
        t0 = time.monotonic()
        need = min(need, self.hourly)
        while True:
            with self.lock:
                now = time.monotonic()
                self._expire(now)
                over = len(self.window) + need - self.hourly
                if over <= 0:
                    return time.monotonic() - t0
                nap = max(0.05, self.window[min(over, len(self.window)) - 1] + self.window_s - now + 0.01)
            time.sleep(min(nap, 5.0))

    def _expire(self, now):
        while self.window and now - self.window[0] > self.window_s:
            self.window.popleft()


class HttpClient:
    def __init__(self, base_url, token, limiter, timeout, idle_reconnect_s=15.0):
        u = urllib.parse.urlparse(base_url)
        self.scheme, self.netloc = u.scheme, u.netloc
        self.token, self.limiter, self.timeout = token, limiter, timeout
        self.idle = idle_reconnect_s
        self.tl = threading.local()
        self.lock = threading.Lock()
        self.n429 = 0
        self._all = []

    def close_all(self):
        with self.lock:
            conns, self._all = self._all, []
        for c in conns:
            try:
                c.close()
            except Exception:
                pass

    def _new_conn(self, timeout=None):
        cls = http.client.HTTPSConnection if self.scheme == "https" else http.client.HTTPConnection
        return cls(self.netloc, timeout=timeout or self.timeout)

    def _conn(self):
        c = getattr(self.tl, "c", None)
        if c is not None and time.monotonic() - getattr(self.tl, "last", 0.0) > self.idle:
            c.close()
            c = None
        if c is None:
            c = self._new_conn()
            c.connect()                      # handshake happens here, outside the timed region
            c.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)   # no client-side Nagle delay
            self.tl.c = c
            with self.lock:
                self._all.append(c)
        return c

    def warm(self):
        """Open (or refresh) this thread's connection now, so no journey timer ever includes a handshake."""
        try:
            self._conn()
            self.tl.last = time.monotonic()
        except OSError:
            self._drop()

    def _drop(self):
        c = getattr(self.tl, "c", None)
        if c is not None:
            try:
                c.close()
            finally:
                self.tl.c = None

    def request(self, method, path, body=None, content_type="application/json"):
        throttle = self.limiter.acquire() if self.limiter else 0.0
        headers = {"Authorization": "Bearer " + self.token, "Accept": "application/json",
                   "User-Agent": f"ma-bench/{HARNESS_VERSION}"}
        data = None
        if body is not None:
            if isinstance(body, (dict, list)):
                data, headers["Content-Type"] = json.dumps(body).encode(), "application/json"
            else:
                data = body.encode() if isinstance(body, str) else body
                headers["Content-Type"] = content_type
        resp = Resp(throttle_ms=throttle * 1000.0)
        t0 = None
        try:
            c = self._conn()
            t0 = time.perf_counter()
            resp.t_send = t0
            c.request(method, path, body=data, headers=headers)
            r = c.getresponse()
            raw = r.read()
            t1 = time.perf_counter()
            self.tl.last = time.monotonic()
            resp.status = r.status
            resp.headers = {k.lower(): v for k, v in r.getheaders()}
            resp.ms, resp.t_end = (t1 - t0) * 1000.0, t1
            resp.text = raw[:4000].decode("utf-8", "replace")
            if raw:
                try:
                    resp.json = json.loads(raw)
                except ValueError:
                    pass
            if r.status >= 400:
                resp.error = f"http_{r.status}"
                if r.status == 429:
                    with self.lock:
                        self.n429 += 1
            if (resp.headers.get("connection") or "").lower() == "close":
                self._drop()
        except (socket.timeout, TimeoutError):
            resp.error = "timeout"
            self._drop()
        except (http.client.HTTPException, OSError) as e:
            resp.error = type(e).__name__
            self._drop()
        if t0 is not None and not resp.t_end:
            resp.t_end = time.perf_counter()
            resp.ms = (resp.t_end - t0) * 1000.0
        return resp

    def capture_sse(self, path, seconds):
        """Read raw SSE lines for `seconds`. Used by `probe` to learn the event schema."""
        c = self._new_conn(timeout=1.0)
        lines = []
        try:
            c.request("GET", path, headers={"Authorization": "Bearer " + self.token,
                                            "Accept": "text/event-stream"})
            r = c.getresponse()
            lines.append(f"# HTTP {r.status}")
            end = time.monotonic() + seconds
            while time.monotonic() < end:
                try:
                    line = r.readline()
                except (socket.timeout, TimeoutError):
                    continue
                if not line:
                    break
                lines.append(line.decode("utf-8", "replace").rstrip())
        except (http.client.HTTPException, OSError) as e:
            lines.append(f"# error {type(e).__name__}: {e}")
        finally:
            c.close()
        return lines


# --------------------------------------------------------------------------------------
# Backend interface. Journeys only talk to this, so other providers can be added later
# by implementing the same six methods plus session_id()/status().
# --------------------------------------------------------------------------------------
def _sobj(j):
    if isinstance(j, dict):
        return j["session"] if isinstance(j.get("session"), dict) else j
    return {}


def norm_status(raw):
    if not isinstance(raw, str) or not raw:
        return "unknown"
    return raw.upper().replace("SESSION_STATUS_", "").lower()


class DOBackend:
    name = "digitalocean-managed-agents"

    def __init__(self, cfg, http_client):
        self.cfg, self.http = cfg, http_client
        self._config_ids = {}
        self._lock = threading.Lock()

    # -- helpers
    @staticmethod
    def session_id(resp):
        if not resp.ok:                      # an error body's "id" is an error code, not a session
            return None
        s = _sobj(resp.json)
        return s.get("session_id") or s.get("id")

    @staticmethod
    def status(resp):
        return norm_status(_sobj(resp.json).get("status"))

    def _path(self, key, **kw):
        return PATHS[key].format(**kw)

    def _manifest(self, name, size):
        return (f"name: {name}\nagent: {self.cfg.agent}\nsize: {size}\n"
                f"idle_timeout: {self.cfg.idle_timeout}\n")

    def ensure_config(self, size, run_id):
        with self._lock:
            if size in self._config_ids:
                return self._config_ids[size]
            name = f"{self.cfg.name_prefix}-{run_id}-{size}"[:64]
            manifest = (f"agent: {self.cfg.agent}\nsize: {size}\n"
                        f"idle_timeout: {self.cfg.idle_timeout}\n")
            r = self.http.request("POST", PATHS["configs"], {"name": name, "manifest_yaml": manifest})
            cid = ((r.json or {}).get("config") or {}).get("id") if r.ok else None
            if not cid:
                raise RuntimeError(f"could not create Environment Config ({r.error}): {r.text[:300]}")
            self._config_ids[size] = cid
            return cid

    # -- lifecycle
    def create(self, name, size, run_id=""):
        if self.cfg.create_mode == "config":
            cid = self.ensure_config(size, run_id)
            return self.http.request("POST", PATHS["sessions"], {"name": name, "config_id": cid})
        return self.http.request("POST", PATHS["sessions"], self._manifest(name, size),
                                 self.cfg.yaml_content_type)

    def get(self, sid):
        return self.http.request("GET", self._path("session", sid=sid))

    def pause(self, sid):
        return self.http.request("POST", self._path("pause", sid=sid))

    def resume(self, sid):
        return self.http.request("POST", self._path("resume", sid=sid))

    def exec(self, sid, argv):
        return self.http.request("POST", self._path("exec", sid=sid), {"argv": argv})

    def destroy(self, sid):
        return self.http.request("DELETE", self._path("session", sid=sid))

    def delete_configs(self):
        for cid in self._config_ids.values():
            self.http.request("DELETE", self._path("config", cid=cid))


def retryable(resp):
    """Exec-mode resume: which failed exec responses mean 'not usable yet, try again'."""
    if resp.ok:
        return False                      # exec ran; a non-zero exit is a real failure
    if resp.error == "timeout" or not resp.status:
        return True                       # connection-level hiccup
    if resp.status in (401, 403, 429):
        return False                      # auth / rate limit: retrying only makes it worse
    return resp.status >= 400             # 4xx 'not ready' style responses and 5xx


def exec_succeeded(resp):
    if not resp.ok:
        return False
    j = resp.json
    if isinstance(j, dict):
        for k in ("exit_code", "exitCode", "exit_status"):
            if k in j:
                return j[k] in (0, "0")
    return True


# --------------------------------------------------------------------------------------
# Journey: the unit of measurement
# --------------------------------------------------------------------------------------
class StageError(Exception):
    def __init__(self, stage, error, resp=None):
        super().__init__(f"{stage}: {error}")
        self.stage, self.error = stage, error
        self.request_id = resp.request_id if resp is not None else None
        self.body = resp.text[:300] if resp is not None else ""
        self.status = resp.status if resp is not None else None
        self.ms = round(resp.ms, 2) if resp is not None else None
        self.headers = dict(resp.headers) if resp is not None else {}


class Registry:
    def __init__(self):
        self.live, self.lock = set(), threading.Lock()

    def add(self, sid):
        with self.lock:
            self.live.add(sid)

    def discard(self, sid):
        with self.lock:
            self.live.discard(sid)

    def snapshot(self):
        with self.lock:
            return set(self.live)


def wait_for(be, cfg, sid, targets, t_start, initial):
    """
    Poll until the session reaches a status in `targets`.
    Returns (hi_ms, lo_ms, polls, throttle_ms), all relative to t_start:
      hi = when the response that first showed the target status was RECEIVED. The status was
           evaluated on the server before that, so the transition happened at or before hi.
      lo = when the last poll that did NOT show the target status was SENT. That poll was
           evaluated after it was sent, so the transition happened after lo.
    The true transition lies in (lo, hi]; hi - lo is the measurement resolution and includes
    one client<->API round trip plus the poll interval.
    """
    thr = 0.0
    polls = 0
    if initial is not None and initial.ok and be.status(initial) in targets:
        return (initial.t_end - t_start) * 1000.0, 0.0, 0, thr
    prev = 0.0   # the transition cannot precede the request that triggers it
    deadline = t_start + cfg.ready_timeout_s
    consecutive_err = 0
    while True:
        if time.perf_counter() > deadline:
            raise StageError("wait", "timeout")
        time.sleep(cfg.poll_ms / 1000.0)
        r = be.get(sid)
        polls += 1
        thr += r.throttle_ms
        if not r.ok:
            consecutive_err += 1
            if consecutive_err >= 3:
                raise StageError("wait", f"poll_failed:{r.error}", r)
            continue
        consecutive_err = 0
        st = be.status(r)
        if st in targets:
            return (r.t_end - t_start) * 1000.0, prev, polls, thr
        if st in BAD_TERMINAL:
            raise StageError("wait", f"status_{st}", r)
        prev = (r.t_send - t_start) * 1000.0


def _check(stage, r):
    if not r.ok:
        raise StageError(stage, r.error, r)


def _check_exec(stage, r):
    if not exec_succeeded(r):
        raise StageError(stage, r.error or "nonzero_exit", r)


def run_journey(be, cfg, cond, name, warmup, registry, run_id=""):
    rec = {"journey": name, "t_start_utc": utcnow(), "size": cond.size, "dwell_s": cond.dwell_s,
           "concurrency": cond.concurrency, "agent": cfg.agent, "create_mode": cfg.create_mode,
           "warmup": warmup, "m": {}, "polls": {}, "throttle_ms": 0.0, "failed_stage": None,
           "error": None, "request_id": None, "session_id": None, "resume_mode": cfg.resume_mode}
    m = rec["m"]
    sid = None
    stage = "create"
    try:
        # ---- cold start
        be.http.warm()
        t_create = time.perf_counter()
        r = be.create(name, cond.size, run_id)
        rec["throttle_ms"] += r.throttle_ms
        m["create_call_ms"] = r.ms
        _check("create", r)
        sid = be.session_id(r)
        if not sid:
            raise StageError("create", "no_session_id_in_response", r)
        rec["session_id"] = sid
        registry.add(sid)

        stage = "ready"
        hi, lo, polls, thr = wait_for(be, cfg, sid, {"ready"}, t_create, r)
        m["create_to_ready_ms"], m["create_to_ready_lo_ms"] = hi, lo
        rec["polls"]["ready"], rec["throttle_ms"] = polls, rec["throttle_ms"] + thr

        stage = "first_exec"
        r = be.exec(sid, cfg.exec_argv)
        rec["throttle_ms"] += r.throttle_ms
        _check_exec("first_exec", r)
        m["create_to_exec_ms"] = (r.t_end - t_create) * 1000.0

        stage = "steady"
        m["exec_steady_ms"] = _steady(be, cfg, sid, rec, "steady")

        # ---- pause
        stage = "pause"
        be.http.warm()
        t_p = time.perf_counter()
        r = be.pause(sid)
        rec["throttle_ms"] += r.throttle_ms
        m["pause_call_ms"] = r.ms
        _check("pause", r)
        rec["pause_resp_status"] = be.status(r)
        stage = "paused"
        hi, lo, polls, thr = wait_for(be, cfg, sid, {"paused"}, t_p, r)
        m["pause_to_paused_ms"], m["pause_to_paused_lo_ms"] = hi, lo
        rec["polls"]["paused"], rec["throttle_ms"] = polls, rec["throttle_ms"] + thr

        if cond.dwell_s > 0:
            time.sleep(cond.dwell_s)

        # ---- resume
        stage = "resume"
        be.http.warm()
        t_r = time.perf_counter()
        r = be.resume(sid)
        rec["throttle_ms"] += r.throttle_ms
        m["resume_call_ms"] = r.ms
        _check("resume", r)
        rec["resume_resp_status"] = be.status(r)

        if cfg.resume_mode == "exec":
            # No polling: run a command immediately, retry until it works. This is what a client
            # that resumes and then sends work actually waits for, with no harness artifact.
            stage = "resume_exec"
            attempts, codes = 0, []
            deadline = t_r + cfg.ready_timeout_s
            while True:
                r = be.exec(sid, cfg.exec_argv)
                attempts += 1
                rec["throttle_ms"] += r.throttle_ms
                if exec_succeeded(r):
                    break
                codes.append(r.status or r.error)
                if not retryable(r):
                    raise StageError("resume_exec", f"non_retryable:{r.error or 'nonzero_exit'}", r)
                if attempts >= cfg.resume_retry_max or time.perf_counter() > deadline:
                    raise StageError("resume_exec", f"not_usable_after_{attempts}_attempts", r)
                if cfg.resume_retry_ms:
                    time.sleep(cfg.resume_retry_ms / 1000.0)
            m["resume_to_usable_ms"] = (r.t_end - t_r) * 1000.0
            m["resume_usable_exec_ms"] = r.ms
            rec["resume_attempts"], rec["resume_retry_codes"] = attempts, codes
        else:
            stage = "resume_ready"
            hi, lo, polls, thr = wait_for(be, cfg, sid, {"ready"}, t_r, r)
            m["resume_to_ready_ms"], m["resume_to_ready_lo_ms"] = hi, lo
            rec["polls"]["resume_ready"], rec["throttle_ms"] = polls, rec["throttle_ms"] + thr

            stage = "resume_exec"
            r = be.exec(sid, cfg.exec_argv)
            rec["throttle_ms"] += r.throttle_ms
            _check_exec("resume_exec", r)
            m["resume_to_exec_ms"] = (r.t_end - t_r) * 1000.0

        stage = "post_steady"
        m["exec_post_resume_ms"] = _steady(be, cfg, sid, rec, "post_steady")
    except StageError as e:
        rec["failed_stage"] = stage
        rec["error"] = e.error
        rec["request_id"] = e.request_id
        rec["error_body"] = e.body
        rec["error_status"], rec["error_ms"], rec["error_headers"] = e.status, e.ms, e.headers
    except Exception as e:  # never let one journey kill the run
        rec["failed_stage"] = stage
        rec["error"] = f"{type(e).__name__}: {e}"
    finally:
        if sid:
            td = be.destroy(sid)
            if td.ok or td.status == 404:
                m["destroy_ms"] = td.ms
                registry.discard(sid)
            else:
                rec["cleanup_error"] = td.error
                rec["cleanup_error_headers"] = dict(td.headers)
    rec["m"] = {k: _round(v) for k, v in m.items()}
    rec["throttle_ms"] = round(rec["throttle_ms"], 1)
    return rec


def _steady(be, cfg, sid, rec, stage):
    out = []
    for _ in range(cfg.steady_execs):
        r = be.exec(sid, cfg.exec_argv)
        rec["throttle_ms"] += r.throttle_ms
        _check_exec(stage, r)
        out.append(r.ms)
    return out


def _round(v):
    if isinstance(v, list):
        return [round(x, 2) for x in v]
    return round(v, 2)


def utcnow():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


# --------------------------------------------------------------------------------------
# Statistics
# --------------------------------------------------------------------------------------
def pct(sorted_vals, p):
    """Percentile with linear interpolation (numpy's default 'linear' method). p in [0, 100]."""
    n = len(sorted_vals)
    if n == 0:
        return None
    if n == 1:
        return sorted_vals[0]
    k = (n - 1) * p / 100.0
    f, c = math.floor(k), math.ceil(k)
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def bootstrap_ci(vals, p, alpha=0.05, seed=12345):
    n = len(vals)
    if n < 5:
        return None, None
    B = 1000 if n <= 1000 else 300
    rng = random.Random(seed)
    stats = sorted(pct(sorted(rng.choices(vals, k=n)), p) for _ in range(B))
    return pct(stats, 100 * alpha / 2), pct(stats, 100 * (1 - alpha / 2))


def summarize(vals):
    s = sorted(vals)
    n = len(s)
    if n == 0:
        return {"n": 0}
    lo, hi = bootstrap_ci(s, 50)
    return {"n": n, "min": s[0], "p50": pct(s, 50), "p50_ci_lo": lo, "p50_ci_hi": hi,
            "p95": pct(s, 95), "p99": pct(s, 99), "max": s[-1], "mean": sum(s) / n,
            "tail95_obs": n * 0.05, "tail99_obs": n * 0.01}


# --------------------------------------------------------------------------------------
# Runner
# --------------------------------------------------------------------------------------
def conditions(cfg):
    return [Cond(s, float(d), int(c)) for s, d, c in
            itertools.product(cfg.sizes, cfg.dwell_s, cfg.concurrency)]


def build_schedule(cfg):
    """Randomised, interleaved order so drift (time of day, backend load) is spread across
    conditions instead of being confounded with one of them."""
    sched = [c for c in conditions(cfg) for _ in range(cfg.trials)]
    random.Random(cfg.seed).shuffle(sched)
    seen = defaultdict(int)
    out = []
    for c in sched:
        out.append((c, seen[c] < cfg.warmup))
        seen[c] += 1
    return out


class JsonlWriter:
    def __init__(self, path):
        self.f, self.lock = open(path, "a", buffering=1), threading.Lock()

    def write(self, obj):
        with self.lock:
            self.f.write(json.dumps(obj) + "\n")

    def close(self):
        self.f.close()


def make_stack(cfg):
    limiter = Limiter(cfg.max_rps, cfg.hourly_budget, cfg.budget_window_s)
    http_client = HttpClient(cfg.base_url, cfg.token, limiter, cfg.request_timeout_s)
    return limiter, http_client, DOBackend(cfg, http_client)


def baseline_rtt(http_client, n=40):
    vals, err, sizes = [], None, None
    for _ in range(n):
        r = http_client.request("GET", PATHS["sizes"])
        if r.ok:
            vals.append(r.ms)
            sizes = sizes or r.json
        else:
            err = f"{r.error}: {r.text[:160]}"
        time.sleep(0.05)
    s = sorted(vals)
    return {"n_ok": len(s), "p25_ms": pct(s, 25), "p50_ms": pct(s, 50), "p95_ms": pct(s, 95),
            "min_ms": s[0] if s else None,
            "last_error": err}, sizes


def client_meta():
    meta = {"python": platform.python_version(), "platform": platform.platform(),
            "hostname": socket.gethostname(), "harness_version": HARNESS_VERSION}
    for key, url in (("do_region", "http://169.254.169.254/metadata/v1/region"),
                     ("do_droplet_id", "http://169.254.169.254/metadata/v1/id")):
        try:
            with urllib.request.urlopen(url, timeout=0.5) as f:
                meta[key] = f.read().decode().strip()
        except Exception:
            meta[key] = None
    return meta


def run_experiment(cfg, quiet=False):
    if not cfg.token:
        raise SystemExit("Set DIGITALOCEAN_TOKEN (needs agent_harness_session read/create/manage/operate).")
    run_id = uuid.uuid4().hex[:6]
    out = Path(cfg.out_dir) / f"run-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}-{run_id}"
    out.mkdir(parents=True, exist_ok=True)
    limiter, http_client, be = make_stack(cfg)
    registry = Registry()
    atexit.register(lambda: _emergency_cleanup(be, registry))

    meta = {"run_id": run_id, "label": cfg.label, "started_utc": utcnow(),
            "config": {k: v for k, v in asdict(cfg).items() if k != "token"},
            "client": client_meta()}
    meta["baseline_before"], meta["size_catalog"] = baseline_rtt(http_client)
    if not meta["baseline_before"]["n_ok"]:
        raise SystemExit(f"Baseline request failed ({meta['baseline_before']['last_error']}).\n"
                         "403 = token is not authorized for Managed Agents (scopes or team access); "
                         "401 = bad token; 429 = quota used up by something else. Run `probe` for detail.")
    writer = JsonlWriter(out / "raw.jsonl")
    sched = build_schedule(cfg)
    total_trials, created, n_journey = len(sched), 0, 0
    obs_req, paced_total = [], 0.0       # observed requests per journey; seconds waited for budget
    ok_count = fail_count = 0
    log = (lambda *a: None) if quiet else (lambda *a: print(*a, file=sys.stderr, flush=True))
    log(f"run {run_id}: {total_trials} trials, output -> {out}")

    try:
        for i, (cond, warm) in enumerate(sched, 1):
            if cfg.max_sessions and created + cond.concurrency > cfg.max_sessions:
                log(f"stopping: max_sessions={cfg.max_sessions} reached")
                break
            est = (sum(obs_req) / len(obs_req)) if obs_req else 45.0
            paced_total += limiter.wait_for_capacity(int(math.ceil(1.5 * est)) * cond.concurrency)
            req_before = limiter.total
            names = []
            for _ in range(cond.concurrency):
                n_journey += 1
                names.append(f"{cfg.name_prefix}-{run_id}-{n_journey:05d}")
            results = [None] * len(names)

            def worker(ix, nm):
                results[ix] = run_journey(be, cfg, cond, nm, warm, registry, run_id)

            if len(names) == 1:
                worker(0, names[0])
            else:
                ts = [threading.Thread(target=worker, args=(ix, nm)) for ix, nm in enumerate(names)]
                [t.start() for t in ts]
                [t.join() for t in ts]
            created += len(names)
            obs_req.append((limiter.total - req_before) / cond.concurrency)
            for rec in results:
                rec["trial"], rec["run_id"] = i, run_id
                writer.write(rec)
                if rec["failed_stage"]:
                    fail_count += 1
                else:
                    ok_count += 1
            log(f"[{i}/{total_trials}] size={cond.size} dwell={cond.dwell_s}s c={cond.concurrency}"
                f"{' (warmup)' if warm else ''}  ok={ok_count} fail={fail_count}"
                f" req={limiter.total} 429s={http_client.n429} budget_wait={paced_total:.0f}s")
            if cfg.cooldown_s:
                time.sleep(cfg.cooldown_s)
    except KeyboardInterrupt:
        log("interrupted; cleaning up and writing partial results")
    finally:
        writer.close()
        _emergency_cleanup(be, registry)
        if cfg.create_mode == "config":
            be.delete_configs()
        meta["baseline_after"], _ = baseline_rtt(http_client)
        meta["finished_utc"] = utcnow()
        meta["requests_sent"], meta["http_429"] = limiter.total, http_client.n429
        http_client.close_all()
        meta["sessions_created"] = created
        meta["budget_wait_s"] = round(paced_total, 1)
        (out / "meta.json").write_text(json.dumps(meta, indent=2))
    build_report([out], out_dir=out)
    return out


def _emergency_cleanup(be, registry):
    for sid in registry.snapshot():
        r = be.destroy(sid)
        if r.ok or r.status == 404:
            registry.discard(sid)
        else:
            print(f"WARNING: could not destroy session {sid} ({r.error}); "
                  f"run `ma_bench.py cleanup`", file=sys.stderr)


# --------------------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------------------
def load_runs(run_dirs):
    recs, metas = [], []
    for d in run_dirs:
        d = Path(d)
        metas.append(json.loads((d / "meta.json").read_text()) if (d / "meta.json").exists() else {})
        with open(d / "raw.jsonl") as f:
            recs += [json.loads(line) for line in f if line.strip()]
    return recs, metas


def fmt(x):
    if x is None:
        return "–"
    return f"{x:,.0f}" if abs(x) >= 100 else f"{x:.1f}"


def reached(rec, stage):
    """Did this journey get as far as `stage`? (Failing AT a stage counts as reaching it.)"""
    if stage == "destroy":
        return bool(rec.get("session_id"))
    fs = rec.get("failed_stage")
    if fs is None:
        return True
    return STAGE_ORDER.index(fs) >= STAGE_ORDER.index(stage)


def stage_failed(rec, stage):
    if stage == "destroy":
        return bool(rec.get("cleanup_error"))
    return rec.get("failed_stage") == stage


def collect(recs, include_warmup):
    groups = defaultdict(list)
    for r in recs:
        if r.get("warmup") and not include_warmup:
            continue
        key = (r["size"], r["dwell_s"], r["concurrency"], r.get("agent"), r.get("create_mode"),
               r.get("resume_mode", "poll"))
        groups[key].append(r)
    return groups


def metric_rows(rs):
    rows = {}
    for metric, (stage, desc) in METRICS.items():
        vals, journeys = [], 0
        for r in rs:
            v = r["m"].get(metric)
            if v is None or stage_failed(r, stage):   # e.g. a failed create call's duration
                continue
            journeys += 1
            vals += v if isinstance(v, list) else [v]
        attempted = sum(1 for r in rs if reached(r, stage))
        failed = sum(1 for r in rs if stage_failed(r, stage))
        row = summarize(vals)
        row.update({"journeys": journeys, "attempted": attempted, "failed": failed, "desc": desc})
        if metric in BOUND_PAIRS:
            widths = [r["m"][metric] - r["m"][BOUND_PAIRS[metric]] for r in rs
                      if metric in r["m"] and BOUND_PAIRS[metric] in r["m"]]
            row["resolution_ms_p50"] = pct(sorted(widths), 50) if widths else None
            lows = [r["m"][BOUND_PAIRS[metric]] for r in rs
                    if BOUND_PAIRS[metric] in r["m"] and not stage_failed(r, stage)]
            row["lo_zero_share"] = (sum(1 for x in lows if x == 0) / len(lows)) if lows else None
        rows[metric] = row
    return rows


def build_report(run_dirs, out_dir=None, include_warmup=False):
    recs, metas = load_runs(run_dirs)
    groups = collect(recs, include_warmup)
    out_dir = Path(out_dir or (run_dirs[0] if len(run_dirs) == 1 else Path("results") / "combined"))
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = {"runs": [m.get("run_id") for m in metas], "conditions": []}
    md = ["# Managed Agents lifecycle benchmark", ""]
    md += header_block(metas, recs, include_warmup)
    warnings = []
    for key, rs in sorted(groups.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2])):
        size, dwell, conc, agent, mode, rmode = key
        rows = metric_rows(rs)
        summary["conditions"].append({"size": size, "dwell_s": dwell, "concurrency": conc, "agent": agent,
                                      "create_mode": mode, "resume_mode": rmode, "journeys": len(rs),
                                      "metrics": rows})
        n_fail = sum(1 for r in rs if r.get("failed_stage"))
        thr = sum(1 for r in rs if r.get("throttle_ms", 0) > 25)
        md += [f"## size `{size}` · dwell {dwell:g}s · concurrency {conc} · agent `{agent}` · "
               f"resume mode `{rmode}`", "",
               f"{len(rs)} journeys ({n_fail} failed; {n_fail / max(len(rs), 1):.1%}). "
               f"Times in ms, client-observed.", "",
               "| metric | n | fail | min | p50 (95% CI) | p95 | p99 | max |",
               "|---|---:|---:|---:|---:|---:|---:|---:|"]
        for metric, row in rows.items():
            if not row["attempted"] or (not row["n"] and not row["failed"]):
                continue
            if row["n"]:
                mark = "†" if row["n"] < 100 else ("‡" if row["n"] < 1000 else "")
                ci = f"{fmt(row['p50'])} ({fmt(row['p50_ci_lo'])}–{fmt(row['p50_ci_hi'])})" \
                    if row.get("p50_ci_lo") is not None else fmt(row["p50"])
                md.append(f"| `{metric}` | {row['n']} | {row['failed']}/{row['attempted']} | {fmt(row['min'])} | "
                          f"{ci} | {fmt(row['p95'])} | {fmt(row['p99'])}{mark} | {fmt(row['max'])} |")
            else:
                md.append(f"| `{metric}` | 0 | {row['failed']}/{row['attempted']} | – | – | – | – | – |")
        rstat = Counter(r.get("resume_resp_status") for r in rs if r.get("resume_resp_status"))
        if rstat:
            md += ["", "Session status reported in the resume response itself: " +
                   ", ".join(f"`{k}` {v}" for k, v in rstat.most_common()) + "."]
        att = [r["resume_attempts"] for r in rs if r.get("resume_attempts")]
        if att:
            codes = Counter(c for r in rs for c in (r.get("resume_retry_codes") or []))
            md += ["", f"First command after resume succeeded on the first attempt in "
                   f"{sum(a == 1 for a in att) / len(att):.0%} of journeys (max attempts {max(att)}). "
                   f"Responses seen before success: " +
                   (", ".join(f"`{k}` x{v}" for k, v in codes.most_common()) if codes else "none") + "."]
        res = [(m, r["resolution_ms_p50"]) for m, r in rows.items()
               if r.get("resolution_ms_p50") is not None and (r.get("lo_zero_share") or 0) <= 0.5]
        if res:
            md += ["", "Measurement resolution (median width of the polling bracket; the true transition "
                   "lies inside it): " + ", ".join(f"`{m}` {fmt(w)} ms" for m, w in res)]
        ub = [(m, r) for m, r in rows.items() if (r.get("lo_zero_share") or 0) > 0.5]
        for m, r in ub:
            sync = m == "create_to_ready_ms" and rows["create_call_ms"].get("p50") and \
                abs(r["p50"] - rows["create_call_ms"]["p50"]) < 0.05 * r["p50"]
            if sync:
                md += ["", f"**`{m}`: create appears synchronous.** In {r['lo_zero_share']:.0%} of journeys the "
                       f"create response itself reported READY, so this equals the create call time."]
            else:
                md += ["", f"**`{m}` is an UPPER BOUND ONLY. Do not quote it as a latency.** In "
                       f"{r['lo_zero_share']:.0%} of journeys the first status check already showed the new state, "
                       f"so the real transition happened somewhere between 0 and this value. Polling cannot "
                       f"resolve transitions faster than one API round trip plus the poll interval."]
        md.append("")
        if n_fail / max(len(rs), 1) > 0.05:
            warnings.append(f"{size}/dwell {dwell:g}/c{conc}: failure rate {n_fail / len(rs):.0%} "
                            f"is above 5%. Percentiles describe successes only.")
        if thr / max(len(rs), 1) > 0.05:
            warnings.append(f"{size}/dwell {dwell:g}/c{conc}: {thr / len(rs):.0%} of journeys waited on the "
                            f"client rate limiter for >25 ms, which widens polling brackets.")
    md += ["† n < 100: p99 is effectively the max; treat as indicative only.",
           "‡ n < 1000: fewer than 10 observations above p99; p99 is noisy.", ""]
    md += reference_block(summary)
    md += quality_block(metas, warnings)
    (out_dir / "summary.md").write_text("\n".join(md) + "\n")
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    write_csv(out_dir / "journeys.csv", recs)
    return out_dir


def header_block(metas, recs, include_warmup):
    m0 = metas[0] if metas else {}
    c, cfg = m0.get("client", {}), m0.get("config", {})
    bb, ba = m0.get("baseline_before", {}), m0.get("baseline_after", {})
    lines = [f"- Runs: {', '.join(str(m.get('run_id')) for m in metas)} · harness v{HARNESS_VERSION}",
             f"- Client: {c.get('hostname')} · DO region `{c.get('do_region')}` · Python {c.get('python')}",
             f"- Journeys: {len(recs)} recorded; warm-up trials {'included' if include_warmup else 'excluded'}",
             f"- Poll interval {cfg.get('poll_ms')} ms · steady execs/phase {cfg.get('steady_execs')} · "
             f"create mode `{cfg.get('create_mode')}` · exec argv `{cfg.get('exec_argv')}`",
             "- Baseline API round trip (GET sizes), p50 before/after, per run: " + "; ".join(
                 f"{m.get('run_id')} {fmt(m.get('baseline_before', {}).get('p50_ms'))} / "
                 f"{fmt(m.get('baseline_after', {}).get('p50_ms'))} ms" for m in metas) +
             ". Every number below includes this client-to-API network cost.", ""]
    return lines


def reference_block(summary):
    rows = []
    for cond in summary["conditions"]:
        if cond["size"] != "mars-2vcpu-4gb" or cond["concurrency"] != 1:
            continue
        for metric, ref in REFERENCE_P50_MS.items():
            r = cond["metrics"].get(metric, {})
            if r.get("n"):
                if (r.get("lo_zero_share") or 0) > 0.5 and metric != "create_to_ready_ms":
                    rows.append(f"| `{metric}` (dwell {cond['dwell_s']:g}s) | {ref} | ≤ {fmt(r['p50'])} "
                                f"(upper bound) | not comparable |")
                else:
                    rows.append(f"| `{metric}` (dwell {cond['dwell_s']:g}s) | {ref} | {fmt(r['p50'])} | "
                                f"{r['p50'] - ref:+,.0f} |")
        r = cond["metrics"].get("resume_to_usable_ms", {})
        if r.get("n"):
            rows.append(f"| `resume_to_usable_ms` (dwell {cond['dwell_s']:g}s) | 305 (resume to ready) | "
                        f"{fmt(r['p50'])} | includes one command round trip; not like-for-like |")
    if not rows:
        return []
    return ["## Reconciliation with the launch-post figures", "",
            "p50, 2 vCPU / 4 GB, concurrency 1. The launch post does not say whether its figures are "
            "client- or server-side, so differences may be definitional rather than regressions.", "",
            "| metric | launch post | this run | delta (ms) |", "|---|---:|---:|---:|", *rows, ""]


def quality_block(metas, warnings):
    out = ["## Data-quality notes", ""]
    for m in metas:
        bb, ba = m.get("baseline_before", {}), m.get("baseline_after", {})
        key = "p25_ms" if bb.get("p25_ms") and ba.get("p25_ms") else "p50_ms"
        if bb.get(key) and ba.get(key):
            drift = abs(ba[key] - bb[key]) / bb[key]
            if drift > 0.25:
                warnings.append(f"run {m.get('run_id')}: baseline RTT ({key[:3]}) drifted {drift:.0%} during "
                                f"the run; network conditions may not have been stable.")
        if m.get("http_429"):
            warnings.append(f"run {m.get('run_id')}: {m['http_429']} HTTP 429 responses. Rate limit hit.")
    out += [f"- {w}" for w in warnings] or ["- No automatic warnings."]
    return out + [""]


def write_csv(path, recs):
    metric_cols = list(METRICS) + list(BOUND_PAIRS.values())
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run_id", "journey", "t_start_utc", "size", "dwell_s", "concurrency", "agent", "warmup",
                    "failed_stage", "error", "request_id", "throttle_ms"] +
                   [c if c not in LIST_METRICS else c + "_median" for c in metric_cols])
        for r in recs:
            row = [r.get(k) for k in ("run_id", "journey", "t_start_utc", "size", "dwell_s", "concurrency",
                                      "agent", "warmup", "failed_stage", "error", "request_id", "throttle_ms")]
            for c in metric_cols:
                v = r["m"].get(c)
                if isinstance(v, list):
                    v = pct(sorted(v), 50) if v else None
                row.append(v)
            w.writerow(row)


# --------------------------------------------------------------------------------------
# plan / probe / cleanup
# --------------------------------------------------------------------------------------
def cmd_plan(cfg):
    k, p = cfg.steady_execs, cfg.poll_ms
    polls = lambda secs: max(1, math.ceil(secs * 1000 / (p + 25)))
    per = 1 + polls(1.0) + 1 + k + 1 + polls(0.5) + 1 + polls(0.5) + 1 + k + 1
    budget = cfg.hourly_budget or 4500
    print(f"Assumed requests per journey: ~{per} (poll {p} ms, {k} steady execs per phase; "
          f"poll counts assume ~1 s ready, ~0.5 s pause/resume)")
    print(f"Throughput under the default 5,000/hour API limit (budget {budget}/h): "
          f"~{budget / per:.0f} journeys/hour\n")
    print(f"{'condition':<44}{'journeys':>9}{'requests':>10}{'hours @budget':>15}")
    tot_j = tot_r = 0
    for c in conditions(cfg):
        j = cfg.trials * c.concurrency
        r = j * per
        tot_j, tot_r = tot_j + j, tot_r + r
        print(f"{f'{c.size} dwell={c.dwell_s:g}s c={c.concurrency}':<44}{j:>9}{r:>10}{r / budget:>15.1f}")
    print(f"{'TOTAL':<44}{tot_j:>9}{tot_r:>10}{tot_r / budget:>15.1f}")
    print("\nPacing: pass --hourly-budget 4500 (or --respect-default-limits). Each journey then runs at full "
          "speed and the harness waits BETWEEN journeys to keep the hourly average under budget, so polling "
          "resolution is unaffected. One journey at a time sends ~125 requests/min, under the 250/min limit; "
          "with high --concurrency (about 7+) bursts may exceed it, so watch the 429 count.")
    print("\nSample size needed per condition (journeys that complete the stage):")
    print("  p50 with a useful CI:                        >= 30")
    print("  p95 with >= 10 observations in the tail:     >= 200")
    print("  p99 with >= 10 observations in the tail:     >= 1000")
    print(f"You are planning {cfg.trials} trials x concurrency per condition "
          f"(minus {cfg.warmup} warm-up trials). Steady-exec metrics have {k}x more observations per journey, "
          "but those observations are correlated within a session.")
    print(f"\nEach journey creates one billable session. This plan creates {tot_j}.")


class ProbeStop(Exception):
    pass


def diagnose(label, r):
    st, rl = r.status, r.headers.get("ratelimit-remaining")
    if st == 401:
        return "401: the token was rejected (invalid, expired, or mistyped)."
    if st == 403:
        return ("403 Forbidden: the token authenticated but is not allowed to do this. That is a token-scope or "
                "team-access problem, NOT a wrong-path problem, so the API contract has not been tested yet. "
                "Use a Full Access token (or a custom-scope token with all four agent_harness_session scopes) "
                "and confirm this team can see Managed Agents in the Control Panel.")
    if st in (404, 405):
        return (f"{st}: not found at step '{label}'. With a valid token this means the path assumption "
                f"for that step is wrong: fix PATHS in ma_bench.py.")
    if st == 429:
        return (f"429: rate limited (ratelimit-remaining={rl}). The probe sends only a handful of requests, so "
                f"something else is consuming this token's quota. Use a dedicated token for the benchmark.")
    return f"{r.error or st} at step '{label}': {r.text[:200]}"


def cmd_probe(cfg, sse_seconds):
    if not cfg.token:
        raise SystemExit("Set DIGITALOCEAN_TOKEN.")
    limiter, hc, be = make_stack(cfg)
    out = Path(cfg.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    trace, sid = [], None

    def rec(label, r):
        trace.append({"step": label, "status": r.status, "error": r.error, "ms": round(r.ms, 1),
                      "request_id": r.request_id, "ratelimit_remaining": r.headers.get("ratelimit-remaining"),
                      "content_type": r.headers.get("content-type"), "body": r.json if r.json is not None else r.text})
        print(f"{label:<18} HTTP {r.status or '-':<4} {r.ms:8.1f} ms  {r.error or ''}", file=sys.stderr)
        return r

    def step(label, r):
        rec(label, r)
        if not r.ok:
            raise ProbeStop(diagnose(label, r))
        return r

    def poll(until):
        seen, deadline = [], time.monotonic() + cfg.ready_timeout_s
        while time.monotonic() < deadline:
            r = be.get(sid)
            if not r.ok:
                rec("get", r)
                raise ProbeStop(diagnose("get", r))
            st = be.status(r)
            if not seen or seen[-1] != st:
                seen.append(st)
                rec(f"get:{st}", r)
            if st == until:
                return
            if st in BAD_TERMINAL:
                raise ProbeStop(f"session entered terminal status '{st}' while waiting for '{until}'")
            time.sleep(cfg.poll_ms / 1000)
        raise ProbeStop(f"timed out after {cfg.ready_timeout_s:.0f}s waiting for status '{until}'")

    stopped = None
    try:
        step("sizes", hc.request("GET", PATHS["sizes"]))
        name = f"{cfg.name_prefix}-probe-{uuid.uuid4().hex[:6]}"
        r = step("create", be.create(name, cfg.sizes[0], "probe"))
        sid = be.session_id(r)
        if not sid:
            raise ProbeStop("create succeeded but no session id was found in the response body; "
                            "see probe.json 'create' body and adjust DOBackend.session_id()")
        poll("ready")
        if sse_seconds:
            trace.append({"step": "events_sse",
                          "lines": hc.capture_sse(PATHS["events"].format(sid=sid), sse_seconds)})
        step("exec", be.exec(sid, cfg.exec_argv))
        step("pause", be.pause(sid))
        poll("paused")
        step("resume", be.resume(sid))
        poll("ready")
        step("exec", be.exec(sid, cfg.exec_argv))
    except ProbeStop as e:
        stopped = str(e)
    finally:
        if sid:
            rec("destroy", be.destroy(sid))
        (out / "probe.json").write_text(json.dumps(trace, indent=2))
        print(f"\nraw responses -> {out / 'probe.json'}", file=sys.stderr)
        print(probe_verdict(trace, stopped), file=sys.stderr)
    return stopped is None


def probe_verdict(trace, stopped=None):
    steps = {}
    for t in trace:
        if "status" in t:
            steps.setdefault(t["step"], t)
    def ok(s):
        return s in steps and 200 <= (steps[s]["status"] or 0) < 300
    if stopped:
        return f"\nSTOPPED: {stopped}\n\nThe API contract was NOT fully checked. Fix the problem above and rerun `probe`."
    ex = steps.get("exec", {}).get("body")
    has_exit = isinstance(ex, dict) and any(k in ex for k in ("exit_code", "exitCode", "exit_status"))
    states = [t["step"].split(":", 1)[1] for t in trace if str(t.get("step", "")).startswith("get:")]
    return "\n".join([
        "\nContract check (all steps succeeded):",
        f"  create (yaml body)           {'OK' if ok('create') else 'FAILED'}",
        f"  exec path                    {'OK' if ok('exec') else 'FAILED'}",
        f"  exec exit-code field         {'OK' if has_exit else 'CHECK: no exit_code field found in exec response'}",
        f"  pause / resume paths         {'OK' if ok('pause') and ok('resume') else 'FAILED'}",
        f"  status values seen           {states}",
        "  'ready' and 'paused' must both appear above; if not, update norm_status()."])


def cmd_cleanup(cfg):
    if not cfg.token:
        raise SystemExit("Set DIGITALOCEAN_TOKEN.")
    _, hc, be = make_stack(cfg)
    n = 0
    token = ""
    while True:
        q = f"?search={urllib.parse.quote(cfg.name_prefix)}&page_size=200" + (f"&page_token={token}" if token else "")
        r = hc.request("GET", PATHS["sessions"] + q)
        if not r.ok:
            raise SystemExit(f"list failed: {r.error} {r.text[:200]}")
        for s in (r.json or {}).get("sessions", []):
            if str(s.get("name", "")).startswith(cfg.name_prefix + "-"):
                d = be.destroy(s["session_id"])
                print(f"destroy {s['name']} -> {d.status}")
                n += d.ok
        token = (r.json or {}).get("next_page_token")
        if not token:
            break
    print(f"destroyed {n} session(s)")


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------
def csv_list(cast):
    return lambda s: [cast(x) for x in s.split(",") if x.strip()]


def add_common(p):
    p.add_argument("--base-url", default=Config.base_url)
    p.add_argument("--agent", default=Config.agent, help="adapter used for the session (default opencode)")
    p.add_argument("--sizes", type=csv_list(str), default=["mars-2vcpu-4gb"],
                   help="comma list of sandbox size slugs (see GET /v2/agents/sessions/sandbox/sizes)")
    p.add_argument("--dwell", type=csv_list(float), default=[5.0], help="comma list: seconds paused before resume")
    p.add_argument("--concurrency", type=csv_list(int), default=[1],
                   help="comma list: sessions created simultaneously per trial")
    p.add_argument("--trials", type=int, default=30, help="trials per condition")
    p.add_argument("--warmup", type=int, default=2, help="warm-up trials per condition (recorded, excluded)")
    p.add_argument("--steady-execs", type=int, default=5)
    p.add_argument("--poll-ms", type=int, default=100)
    p.add_argument("--ready-timeout", type=float, default=120.0)
    p.add_argument("--create-mode", choices=["yaml", "config"], default="yaml")
    p.add_argument("--yaml-content-type", default="application/yaml")
    p.add_argument("--exec-argv", type=csv_list(str), default=["uname"], help="comma list, run without a shell")
    p.add_argument("--max-rps", type=float, default=0.0)
    p.add_argument("--hourly-budget", type=int, default=0,
                   help="max requests per hour; the harness waits between journeys to stay under it")
    p.add_argument("--respect-default-limits", action="store_true",
                   help="shortcut for --hourly-budget 4500 (the DO API default is 5000/hour)")
    p.add_argument("--budget-window-s", type=float, default=3600.0, help=argparse.SUPPRESS)
    p.add_argument("--cooldown", type=float, default=1.0)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--out", default="results")
    p.add_argument("--prefix", default="mabench")
    p.add_argument("--label", default="")
    p.add_argument("--max-sessions", type=int, default=0)
    p.add_argument("--resume-mode", choices=["poll", "exec"], default="poll",
                   help="exec = measure resume without polling: run a command immediately, retry until it works")
    p.add_argument("--resume-retry-ms", type=int, default=20)
    p.add_argument("--resume-retry-max", type=int, default=100)


def cfg_from_args(a):
    cfg = Config(base_url=a.base_url, token=os.environ.get("DIGITALOCEAN_TOKEN", ""), agent=a.agent,
                 sizes=a.sizes, dwell_s=a.dwell, concurrency=a.concurrency, trials=a.trials, warmup=a.warmup,
                 steady_execs=a.steady_execs, poll_ms=a.poll_ms, ready_timeout_s=a.ready_timeout,
                 create_mode=a.create_mode, yaml_content_type=a.yaml_content_type, exec_argv=a.exec_argv,
                 max_rps=a.max_rps, hourly_budget=a.hourly_budget, budget_window_s=a.budget_window_s, cooldown_s=a.cooldown, seed=a.seed,
                 out_dir=a.out, name_prefix=a.prefix, label=a.label, max_sessions=a.max_sessions,
                 resume_mode=a.resume_mode, resume_retry_ms=a.resume_retry_ms,
                 resume_retry_max=a.resume_retry_max)
    if a.respect_default_limits:
        cfg.hourly_budget = 4500
    return cfg


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("plan", "run", "cleanup"):
        add_common(sub.add_parser(name))
    pp = sub.add_parser("probe")
    add_common(pp)
    pp.add_argument("--sse-seconds", type=float, default=0.0, help="also capture raw SSE events for N seconds")
    rp = sub.add_parser("report")
    rp.add_argument("run_dirs", nargs="+")
    rp.add_argument("--include-warmup", action="store_true")
    rp.add_argument("--out")
    a = ap.parse_args(argv)

    if a.cmd == "report":
        d = build_report(a.run_dirs, out_dir=a.out, include_warmup=a.include_warmup)
        print(f"report -> {d / 'summary.md'}")
        return
    cfg = cfg_from_args(a)
    if a.cmd == "plan":
        cmd_plan(cfg)
    elif a.cmd == "probe":
        cmd_probe(cfg, a.sse_seconds)
    elif a.cmd == "cleanup":
        cmd_cleanup(cfg)
    elif a.cmd == "run":
        out = run_experiment(cfg)
        print(f"\nreport -> {out / 'summary.md'}")


if __name__ == "__main__":
    main()
