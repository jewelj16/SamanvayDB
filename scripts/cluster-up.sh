#!/usr/bin/env bash
# Start the 9-node / 3-shard / RF=3 Docker Compose cluster and wait until
# every shard has a leader. Verified with `kvctl cluster status` run inside
# the compose network (the shard map advertises node-N:7000 addresses).
set -euo pipefail
cd "$(dirname "$0")/.."
COMPOSE="docker compose -f docker/docker-compose.yml"

$COMPOSE up -d --build
echo "waiting for every shard to elect a leader..."
for i in $(seq 1 60); do
  if $COMPOSE exec -T node-1 kvctl --server node-1:7000 cluster status --output json 2>/dev/null \
      | grep -q '"leader": "node'; then
    if ! $COMPOSE exec -T node-1 kvctl --server node-1:7000 cluster status --output json \
        | grep -q '"leader": ""'; then
      $COMPOSE exec -T node-1 kvctl --server node-1:7000 cluster status
      echo
      echo "cluster is up. Run kvctl inside the network, e.g.:"
      echo "  $COMPOSE exec node-1 kvctl --server node-1:7000 put hello world"
      exit 0
    fi
  fi
  sleep 1
done
echo "cluster did not become ready in time" >&2
$COMPOSE ps
exit 1
