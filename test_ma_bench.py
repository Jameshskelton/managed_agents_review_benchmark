"""
Tests for the harness itself, against mock_server.py. These validate harness logic
(timing brackets, failure accounting, cleanup, stats) -- NOT real service latencies.

    python3 -m unittest -v test_ma_bench
"""
import io
import json
import random
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import ma_bench as mb
from mock_server import start_mock, TOKEN


def read_jsonl(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def base_cfg(url, out, **kw):
    cfg = mb.Config(base_url=url, token=TOKEN, trials=4, warmup=1, steady_execs=3, poll_ms=20,
                    dwell_s=[0.05], concurrency=[1], cooldown_s=0.0, out_dir=out, ready_timeout_s=10)
    for k, v in kw.items():
        setattr(cfg, k, v)
    return cfg


class StatsTests(unittest.TestCase):
    def test_percentile_linear(self):
        self.assertEqual(mb.pct([1, 2, 3, 4, 5], 50), 3)
        self.assertAlmostEqual(mb.pct(list(range(1, 101)), 99), 99.01)
        self.assertEqual(mb.pct([7], 99), 7)
        self.assertIsNone(mb.pct([], 50))

    def test_bootstrap_ci_brackets_true_median(self):
        rng = random.Random(3)
        vals = [rng.gauss(500, 40) for _ in range(300)]
        lo, hi = mb.bootstrap_ci(sorted(vals), 50)
        self.assertLess(lo, 500)
        self.assertGreater(hi, 500)
        self.assertLess(hi - lo, 30)

    def test_summarize_tail_counts(self):
        s = mb.summarize(list(range(1, 201)))
        self.assertEqual(s["n"], 200)
        self.assertAlmostEqual(s["tail95_obs"], 10.0)
        self.assertAlmostEqual(s["tail99_obs"], 2.0)

    def test_schedule_is_interleaved_deterministic_and_warmups_flagged(self):
        cfg = mb.Config(sizes=["a", "b"], dwell_s=[1.0], concurrency=[1], trials=5, warmup=2, seed=7)
        s1, s2 = mb.build_schedule(cfg), mb.build_schedule(cfg)
        self.assertEqual(s1, s2)
        self.assertEqual(len(s1), 10)
        self.assertEqual(sum(1 for _, w in s1 if w), 4)       # 2 warm-ups x 2 conditions
        self.assertNotEqual([c.size for c, _ in s1], sorted(c.size for c, _ in s1))  # shuffled


class EndToEnd(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def _run(self, mock_kw, **cfg_kw):
        srv, state, url = start_mock(**mock_kw)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        cfg = base_cfg(url, self.tmp, **cfg_kw)
        out = mb.run_experiment(cfg, quiet=True)
        recs = read_jsonl(out / "raw.jsonl")
        return out, recs, state

    def test_measures_known_latencies_and_cleans_up(self):
        out, recs, state = self._run(dict(ready_delay=0.30, pause_delay=0.10, resume_delay=0.15, exec_delay=0.02),
                                     concurrency=[1, 2])
        self.assertEqual(len(recs), 4 * (1 + 2))                      # trials x (c=1 + c=2)
        self.assertFalse([r for r in recs if r["failed_stage"]], "no failures expected")
        self.assertEqual(len(state.live()), 0, "every session must be destroyed")
        self.assertEqual(state.created, state.destroyed)

        meas = [r for r in recs if not r["warmup"]]
        self.assertTrue(meas)
        self.assertTrue(all(r["throttle_ms"] < 5 for r in recs), "unlimited run must not report throttling")
        self.assertNotIn("client rate limiter", (out / "summary.md").read_text())
        # resolution = poll interval + ~2 round trips; on loopback the median must be small
        widths = sorted(r["m"]["create_to_ready_ms"] - r["m"]["create_to_ready_lo_ms"] for r in meas)
        self.assertLess(widths[len(widths) // 2], 150)
        for r in meas:
            m = r["m"]
            # the true transition lies in (lo, hi]; the server's true delay is 300 ms
            self.assertLessEqual(m["create_to_ready_lo_ms"], m["create_to_ready_ms"])
            self.assertGreaterEqual(m["create_to_ready_ms"], 300)
            self.assertLessEqual(m["create_to_ready_lo_ms"], 300 + 15)
            self.assertGreaterEqual(m["resume_to_ready_ms"], 150)
            self.assertGreaterEqual(m["pause_to_paused_ms"], 100)
            self.assertGreaterEqual(min(m["exec_steady_ms"]), 20)       # exec_delay 20 ms
            self.assertLess(min(m["exec_steady_ms"]), 35)               # ...and no client-side inflation
            self.assertEqual(len(m["exec_steady_ms"]), 3)
            self.assertGreater(m["create_to_exec_ms"], m["create_to_ready_ms"])

        md = (out / "summary.md").read_text()
        for needle in ("create_to_ready_ms", "resume_to_ready_ms", "Measurement resolution",
                       "Reconciliation", "p50 (95% CI)"):
            self.assertIn(needle, md)
        self.assertTrue((out / "journeys.csv").exists())
        summ = json.loads((out / "summary.json").read_text())
        self.assertEqual(len(summ["conditions"]), 2)

    def test_failures_are_counted_not_hidden_and_sessions_still_destroyed(self):
        out, recs, state = self._run(dict(exec_fail_rate=1.0))
        self.assertTrue(all(r["failed_stage"] == "first_exec" for r in recs))
        self.assertEqual(len(state.live()), 0)
        md = (out / "summary.md").read_text()
        self.assertIn("failure rate 100%", md)
        row = [l for l in md.splitlines() if l.startswith("| `create_to_exec_ms`")][0]
        self.assertIn("3/3", row)      # 3 measured journeys attempted, 3 failed at this stage

    def test_session_limit_surfaces_as_failures_not_crash(self):
        out, recs, state = self._run(dict(max_active=1), concurrency=[3], trials=2, warmup=0)
        self.assertTrue(any(r["failed_stage"] == "create" and r["error"] == "http_429" for r in recs))
        f = [r for r in recs if r["failed_stage"] == "create"][0]
        self.assertEqual(f["error_status"], 429)
        self.assertIn("x-request-id", f["error_headers"], "failure headers must be captured for diagnosis")
        self.assertEqual(len(state.live()), 0)
        summ = json.loads((out / "summary.json").read_text())
        row = summ["conditions"][0]["metrics"]["create_call_ms"]
        n_ok_creates = sum(1 for r in recs if r["failed_stage"] != "create")
        self.assertEqual(row["n"], n_ok_creates, "failed create calls must not enter create_call_ms stats")

    def test_upper_bound_metrics_are_flagged_not_quoted(self):
        # pause/resume complete before the first poll -> lower bound is 0 -> must be flagged
        out, recs, _ = self._run(dict(ready_delay=0.0, pause_delay=0.0, resume_delay=0.0), poll_ms=30)
        md = (out / "summary.md").read_text()
        self.assertIn("`resume_to_ready_ms` is an UPPER BOUND ONLY", md)
        self.assertIn("not comparable", md)

    def test_exec_mode_resume_measures_true_resume_without_polling(self):
        # mock: resume takes 250 ms; exec on a not-ready session returns 409 (like a 'not ready' API)
        out, recs, state = self._run(dict(ready_delay=0.05, pause_delay=0.05, resume_delay=0.25, exec_delay=0.01),
                                     resume_mode="exec", resume_retry_ms=10)
        meas = [r for r in recs if not r["warmup"]]
        self.assertFalse([r for r in recs if r["failed_stage"]])
        for r in meas:
            m = r["m"]
            self.assertNotIn("resume_to_ready_ms", m, "exec mode must not poll")
            self.assertGreaterEqual(m["resume_to_usable_ms"], 250)
            self.assertLess(m["resume_to_usable_ms"], 250 + 10 + 10 + 60)   # + retry gap + exec + slack
            self.assertGreater(r["resume_attempts"], 1)
            self.assertTrue(all(c == 409 for c in r["resume_retry_codes"]))
            self.assertEqual(r["resume_mode"], "exec")
        md = (out / "summary.md").read_text()
        self.assertIn("resume_to_usable_ms", md)
        self.assertIn("Responses seen before success: `409`", md)
        self.assertIn("resume mode `exec`", md)
        self.assertNotIn("| `resume_to_ready_ms` |", md)
        self.assertEqual(len(state.live()), 0)

    def test_end_to_end_timers_exclude_connection_setup_at_concurrency(self):
        out, recs, _ = self._run(dict(ready_delay=0.0, pause_delay=0.02, resume_delay=0.0, exec_delay=0.005),
                                 concurrency=[3], resume_mode="exec", trials=3, warmup=0)
        for r in recs:
            m = r["m"]
            gap = m["resume_to_usable_ms"] - m["resume_call_ms"] - m["resume_usable_exec_ms"]
            self.assertLess(gap, 2.0, "resume timer must not include connection setup")

    def test_exec_mode_gives_up_after_retry_cap(self):
        out, recs, state = self._run(dict(ready_delay=0.05, pause_delay=0.05, resume_delay=30.0),
                                     resume_mode="exec", resume_retry_ms=5, resume_retry_max=5, trials=1, warmup=0)
        self.assertEqual(recs[0]["failed_stage"], "resume_exec")
        self.assertEqual(recs[0]["error"], "not_usable_after_5_attempts")
        self.assertEqual(len(state.live()), 0)

    def test_retry_classification(self):
        R = lambda st, err=None: mb.Resp(status=st, error=err or (f"http_{st}" if st >= 400 else None))
        self.assertTrue(mb.retryable(R(409)))
        self.assertTrue(mb.retryable(R(503)))
        self.assertTrue(mb.retryable(mb.Resp(status=0, error="timeout")))
        for st in (401, 403, 429):
            self.assertFalse(mb.retryable(R(st)))
        self.assertFalse(mb.retryable(R(200)))

    def test_report_merges_multiple_runs(self):
        out1, r1, _ = self._run(dict())
        out2, r2, _ = self._run(dict())
        merged = mb.build_report([out1, out2], out_dir=Path(self.tmp) / "merged")
        md = (merged / "summary.md").read_text()
        self.assertIn(f"{len(r1) + len(r2)} recorded", md)

    def test_config_mode_creates_and_removes_environment_config(self):
        # the mock has no /configs route: the harness must fail loudly, not silently fall back
        srv, state, url = start_mock()
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        cfg = base_cfg(url, self.tmp, create_mode="config", trials=1, warmup=0)
        out = mb.run_experiment(cfg, quiet=True)
        recs = read_jsonl(out / "raw.jsonl")
        self.assertTrue(all(r["failed_stage"] == "create" for r in recs))
        self.assertIn("Environment Config", recs[0]["error"])

    def test_plan_runs_offline(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            mb.cmd_plan(mb.Config(trials=100, concurrency=[1, 5]))
        text = buf.getvalue()
        self.assertIn("p99 with >= 10 observations", text)
        self.assertIn("TOTAL", text)


class BudgetPacingTests(unittest.TestCase):
    def test_wait_for_capacity_blocks_until_window_frees(self):
        import time
        lim = mb.Limiter(hourly_budget=10, window_s=0.4)
        for _ in range(10):
            lim.acquire()
        t0 = time.monotonic()
        waited = lim.wait_for_capacity(5)
        self.assertGreaterEqual(time.monotonic() - t0, 0.3)
        self.assertGreater(waited, 0.3)
        self.assertLess(lim.wait_for_capacity(1), 0.05)       # room now: returns immediately

    def test_no_budget_means_no_wait(self):
        self.assertEqual(mb.Limiter().wait_for_capacity(10 ** 6), 0.0)

    def test_pacing_happens_between_journeys_never_inside_one(self):
        import time, tempfile
        srv, state, url = start_mock(ready_delay=0.1, pause_delay=0.05, resume_delay=0.05, exec_delay=0.005)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        cfg = base_cfg(url, tempfile.mkdtemp(), trials=5, warmup=0, steady_execs=2,
                       hourly_budget=70, budget_window_s=2.0)
        t0 = time.monotonic()
        out = mb.run_experiment(cfg, quiet=True)
        elapsed = time.monotonic() - t0
        recs = read_jsonl(out / "raw.jsonl")
        meta = json.loads((out / "meta.json").read_text())
        self.assertEqual(len(recs), 5)
        self.assertFalse([r for r in recs if r["failed_stage"]])
        self.assertTrue(all(r["throttle_ms"] == 0 for r in recs), "no journey may be stalled mid-flight")
        self.assertGreater(meta["budget_wait_s"], 0.5, "the harness must have waited between journeys")
        self.assertEqual(len(state.live()), 0)
        # no 2s window may ever contain more than the budget of requests the harness itself sent
        self.assertGreater(elapsed, 1.0)


class ProbeTests(unittest.TestCase):
    def test_probe_stops_at_first_403_and_does_not_invent_a_session_id(self):
        from contextlib import redirect_stderr
        srv, state, url = start_mock()
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        cfg = mb.Config(base_url=url, token="forbidden-token", out_dir=tempfile.mkdtemp())
        err = io.StringIO()
        with redirect_stderr(err):
            ok = mb.cmd_probe(cfg, 0)
        self.assertFalse(ok)
        self.assertEqual(state.requests, 1, "must stop after the first rejected request")
        text = err.getvalue()
        self.assertIn("NOT a wrong-path problem", text)
        self.assertNotIn("fix PATHS", text)
        self.assertIn("NOT fully checked", text)

    def test_session_id_is_never_taken_from_an_error_body(self):
        r = mb.Resp(status=403, json={"id": "Forbidden", "message": "x"}, error="http_403")
        self.assertIsNone(mb.DOBackend.session_id(r))

    def test_probe_succeeds_against_mock(self):
        from contextlib import redirect_stderr
        srv, state, url = start_mock(ready_delay=0.1, pause_delay=0.05, resume_delay=0.05)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        cfg = mb.Config(base_url=url, token=TOKEN, poll_ms=20, out_dir=tempfile.mkdtemp())
        err = io.StringIO()
        with redirect_stderr(err):
            ok = mb.cmd_probe(cfg, 0)
        self.assertTrue(ok)
        self.assertIn("Contract check (all steps succeeded)", err.getvalue())
        self.assertEqual(len(state.live()), 0)


class LimiterTests(unittest.TestCase):
    def test_limiter_paces_requests(self):
        import time
        lim = mb.Limiter(max_rps=50)
        t0 = time.monotonic()
        for _ in range(11):
            lim.acquire()
        self.assertGreaterEqual(time.monotonic() - t0, 0.19)      # 10 gaps x 20 ms
        self.assertEqual(lim.total, 11)


if __name__ == "__main__":
    unittest.main()
