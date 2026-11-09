#!/usr/bin/env bash
# Seeded, reproducible chaos scenario against a running local cluster
# (`kvctl cluster start ...`). The seed is printed; re-run with the same
# SEED to replay the exact same fault sequence.
#
#   SEED=42 ROUNDS=5 bash scripts/chaos.sh
set -euo pipefail
cd "$(dirname "$0")/.."
EXE=""; case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) EXE=".exe";; esac
K() { "bin/kvctl$EXE" "$@"; }
SEED="${SEED:-$RANDOM}"; ROUNDS="${ROUNDS:-3}"
echo "chaos seed: $SEED (replay with SEED=$SEED)"
RANDOM=$SEED
NODES=($(K cluster status --output json | sed -n 's/.*"node_id": "\(node-[0-9]*\)".*/\1/p' | sort -u))
ACKED=()
for r in $(seq 1 "$ROUNDS"); do
  victim=${NODES[$((RANDOM % ${#NODES[@]}))]}
  echo "round $r: kill $victim"; K node kill "$victim"
  for i in $(seq 1 20); do K put "chaos-$r-$i" "v$i" >/dev/null && ACKED+=("chaos-$r-$i=v$i") || true; done
  echo "round $r: restart $victim"; K node restart "$victim"
  iso=${NODES[$((RANDOM % ${#NODES[@]}))]}
  echo "round $r: isolate $iso"; K partition create --side-a "$iso"
  for i in $(seq 21 40); do K put "chaos-$r-$i" "v$i" >/dev/null && ACKED+=("chaos-$r-$i=v$i") || true; done
  K partition heal
done
echo "verifying ${#ACKED[@]} acknowledged writes..."
for kv in "${ACKED[@]}"; do
  k=${kv%%=*}; want=${kv#*=}
  v=$(K get "$k" || true)
  [ "$v" = "$want" ] || { echo "LOST $k (got '$v', want '$want')"; exit 1; }
done
echo "all writes present — seed $SEED passed"
