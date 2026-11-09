#!/usr/bin/env bash
# Stop the Docker Compose cluster and delete its volumes.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker/docker-compose.yml down -v
