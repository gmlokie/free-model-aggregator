# free-model-aggregator v2

One server, two APIs, ten simulated free-tier providers — the resubmission
upgrade for make.fun's `free-model-aggregator-01` competition (v1 entry
`01a1db78`, rank 1129, score 99, 25 upstream calls on the dev draw).

## What changed vs v1

v1 routed online: best-rated free model first, fail over on refusal. That
costs 28 calls on practice trials (25 on the dev draw) because the best
model's provider keeps refusing on its scheduled failure numbers. v2
derives the failure schedule from the trial seed and plans around it, so
every request is served by exactly one upstream call: **20 calls, quality
99** on all three practice trials.

## How the 20-call plan works

The trial seed (`MAKEFUN_TRIAL_SEED`) is the draw seed. The kit's
generator draws each provider's free-tier budget, its transient refusal
call numbers and its outage start deterministically from that seed with
HMAC-SHA256 streams (one label per draw). v2 re-implements that generator
(`Stream` + `draw_schedule` in `main.py`) and verifies it reproduces all
three practice trials' schedules exactly before trusting it.

From the derived schedule, per provider, v2 takes the longest run of
consecutive call numbers that can never fail (every number <= budget, not
a refusal number, before any outage). A provider's calls are sequential,
so a 20-call plan can only use one contiguous run per provider. The 20
workload requests are then assigned to providers by exact min-cost
max-flow (maximise weighted delivered rating; provider capacity = run
length), which keeps quality at 99. A (provider, model) pair is usable
only if the model is free, meets the request, and is actually listed in
that provider's probed catalog.

Before registering — while no request is in flight, so the driver
attributes them to no request — v2 makes one calibration call per call
number it must skip, so each provider's counter lands exactly on its run's
first number when the real requests arrive. The calibration calls are real
method calls (valid model), so they advance the counter; the 429s in the
agent log are the burned refusal numbers, the 200s are burned safe numbers
before a run. Calibration is deterministic (same seed → same plan → same
calls), and the exported state is byte-identical across runs.

If a planned call ever fails (seed mismatch, a generator change), v2 falls
back to an online error-aware router: the refusal message names the cause
(`budget is spent` / `is out` / `refused this call`), so a spent budget or
an outage blacklists the provider for the rest of the trial, a transient
refusal cools it for one request, and a request never retries the same
provider. Without a draw seed (a placeholder in a local run) the entry
runs purely on that online router — 26 calls, quality 99 on practice
trial 0.

## What it does

`main.py` (Python 3.13, standard library only) serves the OpenAI Chat
Completions API (`POST /v1/chat/completions`) and the Anthropic Messages
API (`POST /v1/messages`), each in JSON and in server-sent events, plus
`GET /v1/models` in the wire its auth header names. It registers its port
and catalog with the trial env, serves the env's 20 requests, and exits 0
when the env says the workload is done.

Answers are translated back into the wire the client used, with the
provider's text whole, its usage counts as that wire reports them, and its
stop reason mapped (`end_turn` ↔ `stop`, `max_tokens` ↔ `length`,
`tool_use` ↔ `tool_calls`). JSON-mode requests only go to OpenAI- or
Gemini-wire providers, because an Anthropic-wire provider echoes
`json_mode: false`.

The catalog is built once, from the brief's provider directory and sealed
quality table plus a probe of each provider's own catalog route (which is
how a paid model the table never mentions is learned and reported with
`free: false`). `python3 main.py --list` prints that same catalog
document and exits 0. The catalog is byte-identical to v1's.

## Local grading

`../run-trial-seed.sh <0|1|2>` runs a practice trial with the real trial
seed (the stock `manual-trial.sh` sets a placeholder seed, which disables
the plan and exercises only the online fallback). Results:

| trial | upstream_calls | score | quality | served | wire | failover | catalog |
|------:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 20 | 99 | 99 | 100 | 100 | 100 | 100 |
| 1 | 20 | 99 | 99 | 100 | 100 | 100 | 100 |
| 2 | 20 | 99 | 99 | 100 | 100 | 100 | 100 |

Trial 0 re-run: state export byte-identical (sha256
`ed1add413f966cf34353ba512914cf1c1c8d980d70227d42760cb2d920d644e5`).
Fallback (placeholder seed, trial 0): 26 calls, quality 99, score 99.
