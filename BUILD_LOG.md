# free-model-aggregator-01 — build log

Entry: `free-model-aggregator` (repo `gmlokie/free-model-aggregator`, not yet
created — see blocker below). Competition deadline 2026-11-16T02:30:00Z
(16 Nov 09:30 WIB). Prize $500 total (1st $100).

## What was built

- `main.py` — Python 3.13, stdlib only. Serves OpenAI Chat Completions
  (`POST /v1/chat/completions`) and Anthropic Messages
  (`POST /v1/messages`), JSON + SSE, plus `GET /v1/models` in the wire its
  auth header names. Registers port + catalog with the env, polls
  `GET /work` every 100 ms, exits 0 when done.
- Routing: best-rated free model meeting the request's requirements
  (tools/vision/JSON/min_context 200000); JSON requests only to
  OpenAI/Gemini-wire providers (Anthropic wire echoes `json_mode: false`);
  fail over on 402/429/5xx; a provider failing 3 calls in a row is tried
  last; two passes over the candidates. No wall clock, no unseeded
  randomness.
- Catalog: brief's directory + sealed ratings table + a probe of each
  provider's own catalog route (GET `/v1/models`, `/v1beta/models` for
  Gemini) — the probe is how the paid model the ratings table never
  mentions (openrouter `anthropic/claude-opus-5.5`, 1520, `free: false`)
  is learned and reported. `--list` prints the same document.
- `LICENSE` — MIT (starter's, SPDX-License-Identifier: MIT).
- `README.md` — design + run notes.
- `.makefun/owner.json` — `{"wallet": "5Uk2qe3bZBiDEzgZUiKgR3vyKZNptPEyZ427hgbvTPfE"}`.
- Local commit `b754527` (branch master, not pushed — no remote yet).

## Grading (manual trials — grade-local --driver process refuses Python:
"the process sandbox runs node22 only (python3.13 runs only inside
Docker)", and this VM has no Docker)

Driver: `~/workspace/bounty-tools/makefun/manual-trial.sh` (starts the
kit's own `program/env/server.mjs` + `program/scorer/score.mjs` with each
practice trial's inputs). Results:

| trial | score | quality | served | wire | failover | catalog | upstream calls |
|-------|-------|---------|--------|------|----------|---------|----------------|
| 0     | 99    | 99      | 100    | 100  | 100      | 100     | 28 |
| 1     | 99    | 99      | 100    | 100  | 100      | 100     | 31 |
| 2     | 99    | 99      | 100    | 100  | 100      | 100     | 31 |

20/20 valid in every trial. Quality 99 is the ceiling on these draws:
openrouter's budget (10) is smaller than the requests whose best free
model is its 1475 `z-ai/glm-5.3-flash:free`, so the last ones must fall
back to 1470/1465; the floored quality cannot reach 100.

Replay check: trial 0 run twice → `state.json` byte-identical
(`STATE-IDENTICAL-ACROSS-RUNS`). Deterministic by construction.

Trial artefacts: `~/workspace/bounty-tools/makefun/manual-trials-free-model-aggregator/`.

## Blockers (need the parent / Dhika)

1. **Repo does not exist.** `gmlokie/free-model-aggregator` returns 404.
   `gh` is not authed (`gh auth status`: not logged into any host) and the
   GitHub App cannot create repos (403, verified earlier). Dhika (or the
   parent via a browser task) must create the PUBLIC repo
   `gmlokie/free-model-aggregator`, then:
   `git remote add origin https://github.com/gmlokie/free-model-aggregator.git && git push -u origin master`
   (commit `b754527` is push-ready).
2. **Transcript bundle must be made by the top-level agent, not this
   subagent.** This session's usage rows are written only once the run
   completes (bundle dry-run warning), and the bundle must also include
   the parent agent's session (agent_id
   `94524929-139e-4c37-881a-d46f38a7ccf1`). Steps for the parent, after
   the repo exists and the commit is pushed:
   - Run the muse.db query with both agent ids
     (`e58dae3f-80ac-46c2-850a-57103329cafe` and
     `94524929-139e-4c37-881a-d46f38a7ccf1`):
     `SELECT u.agent_id, u.message_id, u.created_at, u.input_tokens, u.output_tokens, a.model FROM agent.agent_message_token_usage u JOIN agent.agents a ON a.agent_id = u.agent_id WHERE u.agent_id IN ('...','...') ORDER BY u.created_at DESC LIMIT 500`
     and write the rows to a file, one JSON object per line.
   - `node cli/makefun.mjs bundle --repo <entry dir> --out transcripts.tar.gz --muse-usage <rows file>` (run from inside the unpacked kit).
   - Submit from inside the unpacked kit:
     `node cli/makefun.mjs submit --competition free-model-aggregator-01 --repo-url https://github.com/gmlokie/free-model-aggregator --commit <SHA> --title "Best free AI model aggregator" --bundle transcripts.tar.gz --keypair /home/hatch/.makefun/keypairs/https_make.fun.json`
   - Session file: `~/agents/agent-e58dae3f-80ac-46c2-850a-57103329cafe/sessions/e58dae3f-80ac-46c2-850a-57103329cafe.jsonl` (started 2026-11-14T18:48Z, in-window).

## Notes

- No model calls, no spending: the entry only calls the trial's simulated
  providers; no provider key was used or sent anywhere.
- Never print/log/send the make.fun API key or keypair contents.
