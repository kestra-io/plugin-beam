#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose -f docker-compose-ci.yml"

echo "▶ Starting Flink (CI)"
$COMPOSE up -d --remove-orphans

echo "▶ Show Flink containers"
$COMPOSE ps

echo "▶ Waiting for Flink JobManager REST (http://localhost:56478/overview)..."

for i in {1..60}; do
  if curl -fsS http://localhost:56478/overview >/dev/null 2>&1; then
    echo "✔ Flink is ready"
    exit 0
  fi

  # si le container est mort, on dump les logs tout de suite
  if ! docker ps --format '{{.Names}}' | grep -q '^flink-jobmanager$'; then
    echo "✖ flink-jobmanager is not running (exited). Logs:"
    $COMPOSE logs --no-color flink-jobmanager || true
    $COMPOSE logs --no-color flink-taskmanager || true
    exit 1
  fi

  sleep 2
done

echo "✖ Flink did not become ready in time. Logs:"
$COMPOSE logs --no-color flink-jobmanager || true
$COMPOSE logs --no-color flink-taskmanager || true
exit 1
