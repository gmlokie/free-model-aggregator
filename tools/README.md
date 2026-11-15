# tools — verification for the seed-planned entry

These scripts verify the two load-bearing claims of the v2 plan; they are
not part of the graded entry (only `main.py` runs in a trial).

- `derive-draw.py` — re-implements the kit generator's HMAC-SHA256 stream
  PRNG and `drawInstance`, and checks it reproduces all three practice
  trials' `providers.json` exactly (budgets, refusal call numbers, outage
  starts). Run: `python3 tools/derive-draw.py` (expects the kit unpacked
  at `~/workspace/bounty-tools/makefun/kit-free-model-aggregator-01`).
- `plan-test.py` — runs the planner (longest safe run per provider +
  min-cost max-flow assignment) against the three practice trials and
  prints the expected quality (99 on all three). Run:
  `python3 tools/plan-test.py`.
- `run-trial-seed.sh` — runs a practice trial with the REAL trial seed
  from `rules/instance.yaml` (the stock `manual-trial.sh` sets a
  placeholder seed, which disables the plan and exercises only the online
  fallback). Usage: `run-trial-seed.sh <0|1|2> [agent-dir]`.

Trial evidence (scores, state exports, agent/env logs) is kept out of the
repository in `manual-trials/` (git-ignored); the grades are recorded in
`BUILD_LOG.md`.
