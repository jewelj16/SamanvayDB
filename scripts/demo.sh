#!/usr/bin/env bash
# SamanvayDB end-to-end demo — a single narratable sequence exercising every
# major capability, against a local multi-process cluster (no Docker).
#
#   make build && bash scripts/demo.sh
#
# Runs unattended and exits 0 on success (CI-friendly: DEMO_PAUSE=0).
set -euo pipefail
cd "$(dirname "$0")/.."

EXE=""; case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) EXE=".exe";; esac
KVCTL="bin/kvctl$EXE"
DIR="${DEMO_DIR:-.samanvay-demo}"
PAUSE="${DEMO_PAUSE:-1}"
K() { "$KVCTL" --local-dir "$DIR" "$@"; }
step() { echo; echo "=== $* ==="; sleep "$PAUSE"; }
json_field() { sed -n "s/.*\"$1\": *\"\([^\"]*\)\".*/\1/p" | head -1; }

cleanup() { "$KVCTL" --local-dir "$DIR" cluster stop --purge >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

step "1. Multi-Raft: 9 nodes, 3 shards, RF=3 — three independent leaders"
K cluster start --nodes 9 --shards 3 --replication-factor 3 --binary "bin/samanvaydb-server$EXE"

step "2. Sharding: keys land on different shards (note shard= in each response)"
for k in alice bob carol dave erin frank; do
  echo -n "put $k -> "; K put "$k" "hello-$k"
done

step "3. Failure isolation: kill shard 1's leader"
L1=$(K shard status --shard 1 --output json | json_field leader)
echo "shard 1 leader is $L1"
K shard status --shard 0 > /tmp/demo-s0-before.txt
K node kill "$L1"
sleep 1
K shard status --shard 1
echo "--- reads still correct via the new leader:"
for k in alice bob carol; do echo -n "$k = "; K get "$k"; done
echo "--- shard 0 unaffected:"
K shard status --shard 0

step "4. Recovery: restart the killed node — it rejoins as a follower and catches up"
K node restart "$L1"
sleep 2
K shard status --shard 1

step "5. Partition: isolate shard 2's leader from its majority"
L2=$(K shard status --shard 2 --output json | json_field leader)
ADDR2=$(K node status --node "$L2" --output json | json_field addr)
K partition create --side-a "$L2"
sleep 1
KEY2=""
for k in k1 k2 k3 k4 k5 k6 k7 k8 k9 k10 k11 k12; do
  if K put "$k" probe --output json | grep -q "\"shard\": 2"; then KEY2=$k; break; fi
done
echo "a write sent ONLY to the isolated old leader $L2 fails (no quorum):"
if K --server "$ADDR2" --timeout 3s put --direct "$KEY2" minority 2>&1; then
  echo "unexpected: minority write succeeded"; exit 1
fi
echo "...while the majority side has elected a new leader and serves writes:"
K put "$KEY2" majority
K shard status --shard 2
K partition heal
sleep 1
echo "after healing, the old leader has stepped down and reconciled:"
K shard status --shard 2
echo -n "$KEY2 = "; K get "$KEY2"

step "6. Shard migration under load: move shard 0 to three other nodes"
REPS=$(K shard status --shard 0 --output json | tr -d '\n ' | sed -n 's/.*"replicas":\[\([^]]*\)\].*/\1/p' | tr -d '"')
echo "shard 0 replicas: $REPS"
TARGETS=$(for n in node-1 node-2 node-3 node-4 node-5 node-6 node-7 node-8 node-9; do
  case ",$REPS," in *",$n,"*) ;; *) echo "$n";; esac; done | head -3 | paste -sd, -)
echo "migrating to: $TARGETS"
( for i in $(seq 1 60); do K put "load-$i" "v$i" >/dev/null 2>&1 || true; done ) &
LOADER=$!
K shard migrate --shard 0 --target "$TARGETS"
wait $LOADER
K shard status --shard 0
echo -n "load-60 = "; K get load-60

step "7. Observability: cluster status + metrics"
K cluster status
K cluster metrics --node node-1 | grep -E "samanvaydb_raft_(term|is_leader|elections_total|leader_changes_total)" | head -12

echo
echo "Demo complete."
