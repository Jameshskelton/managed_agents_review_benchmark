# ma_bench: Managed Agents lifecycle latency benchmark

Measures cold start, pause, resume, and exec latency of DigitalOcean Managed Agents (Harness Runtime), over the public REST API, from the client's point of view. DO products only. Standard library only, Python 3.9+.

## Status: read this first

| | |
|---|---|
| Harness logic (scheduling, timing brackets, failure accounting, cleanup, percentiles, rate-limit pacing, report merging) | **Tested** against a local mock: `python3 -m unittest -v test_ma_bench` (21 tests) |
| Real DigitalOcean latencies | **Not measured yet.** Nothing in this package has been run against the live API. |
| API contract (paths, body shapes, status strings) | Partly documented, partly **inferred**. `probe` confirms it in one run. |

The mock (`mock_server.py`) encodes my assumptions about the API. Passing tests prove the harness measures what it claims to measure; they say nothing about whether the assumptions match the real service. Run `probe` first.

| Assumption | Basis |
|---|---|
| `POST /v2/agents/sessions` with a YAML manifest body creates a session | Documented (doctl behaviour; JSON `{name, config_id}` selects the config path) |
| `POST /v2/agents/sessions/{id}/sandbox/exec`, body `{"argv": [...]}` | Documented |
| `GET`/`DELETE /v2/agents/sessions/{id}`, status strings `SESSION_STATUS_READY` / `_PAUSED` | Documented |
| `POST .../pause` and `POST .../resume` paths | **Inferred** from the PauseSession/ResumeSession operation names |
| Create response shape (`session.session_id`), exec response has `exit_code` | **Inferred**; harness tolerates `session` wrapper or flat, and warns if no exit code field is found |
| `Content-Type: application/yaml` accepted for the manifest body | **Inferred**; override with `--yaml-content-type` |

All paths live in one `PATHS` dict at the top of `ma_bench.py`.

## Quick start

```bash
export DIGITALOCEAN_TOKEN=...     # needs agent_harness_session read/create/manage/operate scopes

python3 ma_bench.py plan  --trials 300 --dwell 5 --respect-default-limits   # request volume and wall-clock (offline)
python3 ma_bench.py probe                                 # ONE verbose journey; confirms the contract
python3 ma_bench.py run --trials 10 --warmup 2            # pilot
python3 ma_bench.py run --trials 300 --dwell 5 --respect-default-limits --label "main-1"
python3 ma_bench.py report results/run-A results/run-B    # merge runs into one report
python3 ma_bench.py cleanup                               # destroy leftovers with the mabench- prefix
```

`probe` writes `results/probe.json` with every raw response and prints a contract check. Add `--sse-seconds 20` to also capture raw session events; that is the input needed for the agent-turn phase below.

Output per run (`results/run-<utc>-<id>/`): `raw.jsonl` (one record per journey), `journeys.csv`, `summary.json`, `summary.md`, `meta.json`.

## What a journey does

One session, one pass through the lifecycle, every stage timed:

```
create ──► READY ──► exec ──► K × exec ──► pause ──► PAUSED ──► (dwell) ──► resume ──► READY ──► exec ──► K × exec ──► destroy
```

| Metric | Definition |
|---|---|
| `create_call_ms` | create request returns |
| `create_to_ready_ms` | create request sent → response that first shows READY received |
| `create_to_exec_ms` | create request sent → first exec completes (a poll-then-exec client; "time to usable") |
| `exec_steady_ms` | exec round trip on a warm session, K per journey |
| `pause_call_ms`, `pause_to_paused_ms` | pause request returns; pause request → PAUSED observed |
| `resume_call_ms`, `resume_to_ready_ms` | resume request returns; resume request → READY observed |
| `resume_to_exec_ms` | resume request → first exec completes |
| `exec_post_resume_ms` | steady exec after resume (is a resumed session slower?) |
| `destroy_ms` | destroy round trip |

Conditions are the product of `--sizes` × `--dwell` × `--concurrency`. Dwell is how long the session stays paused before resume (tests whether resume depends on pause duration). Concurrency creates that many sessions simultaneously per trial (tests cold start under burst).

## Method and validity controls

- **Measurement bracket, not a point.** Readiness is detected by polling. For each transition the harness reports `hi` (when the response that first showed the new state was received; the transition happened at or before this) and `lo` (when the last poll that did not show it was *sent*; the transition happened after this). The headline numbers are `hi`, so they never understate. The report prints the median bracket width as the measurement resolution; it is `poll interval + ~2 round trips`. Lower `--poll-ms` to tighten it, subject to rate limits.
- **Warm connections.** TLS handshake is performed before the timer starts and connections are reused (reconnect after 15 s idle). This matches an SDK with connection pooling. `TCP_NODELAY` is set so client-side buffering cannot inflate results.
- **Rate-limit pacing happens between journeys, never inside one.** With `--hourly-budget 4500` (or `--respect-default-limits`) each journey runs at full speed, and the harness waits before starting the next one if it would push the hourly total over budget. Polling resolution is therefore unaffected. Waiting time is recorded in `meta.json` (`budget_wait_s`). Optional `--max-rps` paces every request instead; avoid it, because it widens the polling brackets. Any journey that did wait on per-request pacing for >25 ms is flagged in the report.
- **Randomised, interleaved schedule** (seeded), so time-of-day and backend-load drift is spread across conditions, not confounded with one.
- **Warm-up trials** (default 2 per condition) are recorded but excluded from statistics. `report --include-warmup` shows them.
- **Failures are reported, never dropped.** Each metric shows `failed/attempted` per stage. Percentiles describe successes only; a failure rate above 5% triggers a warning.
- **Baseline network cost.** 20 `GET sizes` calls before and after the run give the client↔API round-trip baseline, and flag drift above 25%. Every number includes this cost; none is server-side.
- **Cleanup is unconditional.** Each journey destroys its session in `finally`; an `atexit` hook and Ctrl-C handler destroy stragglers; `cleanup` handles anything left after a crash.
- **Percentiles use linear interpolation** (numpy's default), with percentile-bootstrap 95% CIs on p50.

## Sample size

Percentiles need tail observations. At `n` completed journeys per condition: p50 is usable from ~30; p95 needs ≥200 for 10 tail observations; **p99 needs ≥1,000**. The report marks p99 with † (n<100, effectively the max) or ‡ (n<1000, noisy). Steady-exec metrics have K× more observations but they are correlated within a session; cite `n` as journeys when making claims about them.

`plan` prints the request budget. A journey sends ~32 requests (mostly status polling), so under the default **5,000 requests/hour** API limit (budgeted at 4,500) you get about **140 journeys per hour**. You do not need the limit raised; it only changes how long runs take:

| Goal (per condition) | Journeys | Wall-clock at ~140/h |
|---|---:|---:|
| p50 with a confidence interval | 30–50 | under 30 min |
| p95 | 200–300 | ~1.5–2 h |
| p99 | 1,000 | ~7 h (run overnight) |

Run `--respect-default-limits` and let it go. One journey at a time sends ~125 requests/minute, under the 250/minute limit. With high `--concurrency` (about 7+) bursts can exceed the per-minute limit; the 429 count is reported. If the limit is ever raised for the benchmark token, drop the flag and the same run finishes several times faster. Limits may be per token or per account; `probe` records the `ratelimit-remaining` header for every call in `probe.json` so you can check whether a second token has its own counter before relying on that.

Two ways to need fewer requests per journey, in order of preference: (1) replace readiness polling with the session event stream (one request instead of ~16; needs the event schema from `probe --sse-seconds 20`, not built yet); (2) lower `--steady-execs` (saves 2 requests per unit). Raising `--poll-ms` also cuts requests but coarsens resolution; do not do it for resume-focused runs.

## Where to run it

Run from a Droplet in the same region the launch-post benchmark used (RIC1, per its footnote) for comparability. The report records the Droplet's region from the metadata service. For a sensitivity check, repeat from a second region and merge nothing across regions; report them separately. Do not publish numbers from a laptop.

Run at least three batches at different times of day (`--label` them), then `report` across the run directories. Check the baseline-drift note for each.

## Measuring resume without polling (`--resume-mode exec`)

Polling cannot resolve resume: in every poll-mode run the first status check already showed READY, so `resume_to_ready_ms` is only an upper bound. In exec mode the harness skips polling entirely: as soon as the resume request returns, it runs a command, and if the API answers "not usable yet" (any 4xx other than 401/403/429, a 5xx, or a connection hiccup) it retries every `--resume-retry-ms` (default 20 ms), up to `--resume-retry-max` attempts (default 100).

| Metric | Definition |
|---|---|
| `resume_to_usable_ms` | resume request sent → first command completes. What a user who resumes and sends work actually waits for. Defined like `create_to_exec_ms`, so cold start and resume are directly comparable. |
| `resume_usable_exec_ms` | duration of that successful command alone |

The report also shows the session status the resume response itself reported, the share of journeys where the first command worked on the first attempt, and every error code seen before success. Those three facts tell you whether resume is synchronous, and what "not ready yet" looks like on the wire.

Pause is still confirmed by polling before the dwell starts, so every resume begins from a session that is really paused. Exec-mode and poll-mode runs are reported as separate conditions, even when merged.

```bash
python3 ma_bench.py run --trials 300 --warmup 2 --dwell 5 --resume-mode exec --respect-default-limits --label resume-exec
```

## Known threats to validity

- Client-observed latency includes network and TLS-session effects; it is not the service's internal time. The launch-post figures may be server-side; the reconciliation table in the report states this caveat.
- Polling resolution (see above). Resume at ~300 ms with a 100 ms poll and 40 ms round trips has a bracket near 180 ms, which is large relative to the thing measured. Use `--poll-ms 25` for resume-focused runs if the rate limit allows.
- Only the `opencode` adapter with a default environment is exercised (`--agent` changes it). Cold start may differ per adapter and image.
- No agent turn is measured (see next section), so "time to first agent response" is out of scope; this harness measures the sandbox lifecycle only.
- Sessions are created with `idle_timeout: 2h` so auto-pause (15 min default) cannot interfere with long dwells. `keep_warm` must stay unset or pause returns 409.
- Account limits on active sessions will surface as create failures (HTTP 409/429) at high concurrency. They are counted, not retried.

## Not built yet (and why)

1. **Agent-turn latency** (create → first agent response, resume → agent response). The launch benchmark's headline numbers (3.3 s, 2.43 s) include a model turn. The input endpoint and event schema are not in the documentation I could reach, and I will not guess at them. Run `probe --sse-seconds 20` plus one manual message and the turn phase becomes a small addition.
2. **Checkpoint → fork latency.** `CreateCheckpoint` and `ForkSession` exist; request/response shapes were not confirmed.
3. **Other providers.** `run_journey` talks only to the backend interface (`create/get/pause/resume/exec/destroy` plus `session_id()` and `status()`). Adding E2B, Modal, Daytona, Fly.io Sprites, and others means writing one class per provider, with the same journey, schedule, statistics, and report. Provider differences in what "ready" and "resume" mean must be documented per provider before any cross-provider table is published.

## Files

| File | Purpose |
|---|---|
| `ma_bench.py` | The harness and CLI |
| `mock_server.py` | Local stand-in API used by the tests; can also run standalone for dry runs: `python3 mock_server.py --port 8765` then `--base-url http://127.0.0.1:8765` with `DIGITALOCEAN_TOKEN=test-token` |
| `test_ma_bench.py` | Harness tests |
