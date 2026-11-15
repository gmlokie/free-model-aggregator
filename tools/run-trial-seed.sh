#!/bin/bash
# Practice-trial driver for free-model-aggregator-v2 with the REAL trial seed.
# The stock manual-trial.sh sets MAKEFUN_TRIAL_SEED="practice-trial-N" (a
# placeholder); the platform hands the entry the real 64-hex draw seed, so
# this runner injects the practice seed from rules/instance.yaml instead.
# Usage: run-trial-seed.sh <trial-num> [agent-dir]
#   trial-num: 0, 1 or 2
set -u
KIT="$HOME/workspace/bounty-tools/makefun/kit-free-model-aggregator-01/free-model-aggregator-01-kit"
AGENT="${2:-$HOME/workspace/bounty-tools/makefun/entries/free-model-aggregator-v2}"
TRIAL="$1"
case "$TRIAL" in
  0) SEED="4f751e962e1eb1d5b461859662944e82c2ce329459e7bc6842a5ab35170c3f81" ;;
  1) SEED="01fc82338daa99de8447d0258d87cb2fc1f5eb386cc8b6237c3c86259b73e448" ;;
  2) SEED="21086a502f48ccc50a24c68de145b40cbb8cb4877b2c8c7d3cb2159c13248d55" ;;
  *) echo "unknown trial $TRIAL"; exit 1 ;;
esac
TRIALDIR="$KIT/practice/trial-$TRIAL"
OUT="$AGENT/manual-trials/trial-$TRIAL"
PORT=$((18200 + TRIAL))
rm -rf "$OUT"
mkdir -p "$OUT/state" "$OUT/scores"
MAKEFUN_STATE_DIR="$OUT/state" \
MAKEFUN_ENV_DATA_DIR="$TRIALDIR/input/env" \
MAKEFUN_ENV_PORT="$PORT" \
  node "$KIT/program/env/server.mjs" > "$OUT/env.log" 2>&1 &
ENV_PID=$!
for i in $(seq 1 50); do [ -f "$OUT/state/ready" ] && break; sleep 0.1; done
if [ ! -f "$OUT/state/ready" ]; then echo "env did not become ready"; cat "$OUT/env.log"; kill $ENV_PID 2>/dev/null; exit 1; fi
echo "env ready: $(cat "$OUT/state/ready")"
cd "$AGENT"
MAKEFUN_ENV_URL="http://127.0.0.1:$PORT" \
MAKEFUN_PROMPT_FILE="$TRIALDIR/input/prompt.txt" \
MAKEFUN_TRIAL_SEED="$SEED" \
  timeout 120 python3 main.py > "$OUT/agent.log" 2>&1
AGENT_RC=$?
echo "agent exit: $AGENT_RC"
sleep 0.5
kill -TERM $ENV_PID 2>/dev/null
wait $ENV_PID 2>/dev/null
if [ ! -f "$OUT/state/export/state.json" ]; then echo "no state export"; cat "$OUT/env.log"; exit 1; fi
MAKEFUN_REFERENCE_DIR="$TRIALDIR/reference" \
MAKEFUN_OUTPUT_DIR="$OUT/state/export" \
MAKEFUN_SCORES_DIR="$OUT/scores" \
  node "$KIT/program/scorer/score.mjs"
echo "--- agent log ---"
cat "$OUT/agent.log"
echo "--- scores ---"
cat "$OUT/scores/scores.json"
