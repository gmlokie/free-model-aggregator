# free-model-aggregator

One server, two APIs, ten simulated free-tier providers — the entry for
make.fun's `free-model-aggregator-01` competition.

## What it does

`main.py` (Python 3.13, standard library only) serves the OpenAI Chat
Completions API (`POST /v1/chat/completions`) and the Anthropic Messages
API (`POST /v1/messages`), each in JSON and in server-sent events, plus
`GET /v1/models` in the wire its auth header names. It registers its port
and catalog with the trial env, serves the env's 20 requests, and exits 0
when the env says the workload is done.

For every request it picks the best-rated free model that meets the
request's requirements (tools, vision, JSON mode, minimum context), speaks
that provider's wire (OpenAI, Anthropic or Gemini), and fails over to the
next-best free model when a provider refuses (402 budget spent, 429 rate
limit / scheduled failure / outage, 5xx). Answers are translated back
into the wire the client used, with the provider's text whole, its usage
counts as that wire reports them, and its stop reason mapped
(`end_turn` ↔ `stop`, `max_tokens` ↔ `length`, `tool_use` ↔
`tool_calls`).

The catalog is built once, from the brief's provider directory and sealed
quality table plus a probe of each provider's own catalog route (which is
how a paid model the table never mentions is learned and reported with
`free: false`). `python3 main.py --list` prints that same catalog
document and exits 0.

## Routing

- Candidates are the free models meeting the request's requirements;
  JSON-mode requests only go to OpenAI- or Gemini-wire providers, because
  an Anthropic-wire provider echoes `json_mode: false`.
- Candidates rank by rating descending (ties: provider id, model id).
- A provider that fails 3 calls in a row is tried last — the drawn
  failure schedules run at most 2 in a row, so 3 in a row means its
  budget is spent or it is in outage.
- Two passes over the candidates: every candidate once, then each once
  more. A scheduled failure on the first pass can succeed on the second;
  a spent budget fails both.
- No wall clock, no unseeded randomness: every choice is a function of
  the brief, the requests and the providers' answers, so a replayed
  trial leaves the same state export.

## Run

```sh
python3 main.py --list   # print the catalog, exit 0
python3 main.py          # run the trial server (needs MAKEFUN_ENV_URL,
                         # MAKEFUN_PROMPT_FILE, MAKEFUN_TRIAL_SEED)
```

## License

MIT — see `LICENSE` (SPDX-License-Identifier: MIT).
