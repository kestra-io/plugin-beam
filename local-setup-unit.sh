################################################################################
# Description:
#   Bootstrap the environment by spinning up Flink and Spark clusters using
#   Docker Compose. This script ensures services are fully operational before
#   returning control.
#
# Key Features:
#   - Deploys services using 'docker-compose-ci.yml'.
#   - Polling Mechanism: Waits up to 120s for Flink (port 56478) and Spark
#     (port 56479) REST endpoints to be reachable.
#   - Failure Handling: Detects premature container exits and dumps logs to
#     stdout for debugging purposes.
#
# Usage:
#   ./local-setup-unit.sh
################################################################################

#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose -f docker-compose-ci.yml"

wait_for_flink() {
  echo "▶ Waiting for Flink JobManager REST (http://localhost:56478/overview)..."

  for _ in {1..60}; do
    if curl -fsS http://localhost:56478/overview >/dev/null 2>&1; then
      echo "✔ Flink is ready"
      return 0
    fi

    # if the container is dead, we dump logs
    if ! docker ps --format '{{.Names}}' | grep -q '^flink-jobmanager$'; then
      echo "✖ flink-jobmanager is not running (exited). Logs:"
      $COMPOSE logs --no-color flink-jobmanager || true
      $COMPOSE logs --no-color flink-taskmanager || true
      return 1
    fi

    sleep 2
  done

  echo "✖ Flink did not become ready in time. Logs:"
  $COMPOSE logs --no-color flink-jobmanager || true
  $COMPOSE logs --no-color flink-taskmanager || true
  return 1
}

wait_for_spark() {
  echo "▶ Waiting for Spark Master UI (http://localhost:56479)..."

  for _ in {1..60}; do
    if curl -fsS http://localhost:56479/ >/dev/null 2>&1 \
      && docker ps --format '{{.Names}}' | grep -q '^spark-worker$'; then
      echo "✔ Spark is ready"
      return 0
    fi

    if ! docker ps --format '{{.Names}}' | grep -q '^spark-master$'; then
      echo "✖ spark-master is not running (exited). Logs:"
      $COMPOSE logs --no-color spark-master || true
      $COMPOSE logs --no-color spark-worker || true
      return 1
    fi

    if ! docker ps --format '{{.Names}}' | grep -q '^spark-worker$'; then
      echo "✖ spark-worker is not running (exited). Logs:"
      $COMPOSE logs --no-color spark-master || true
      $COMPOSE logs --no-color spark-worker || true
      return 1
    fi

    sleep 2
  done

  echo "✖ Spark did not become ready in time. Logs:"
  $COMPOSE logs --no-color spark-master || true
  $COMPOSE logs --no-color spark-worker || true
  return 1
}

echo "▶ Starting Flink & Spark"
$COMPOSE up -d --remove-orphans

echo "▶ Show containers"
$COMPOSE ps

wait_for_flink
wait_for_spark

echo "✔ Services are ready"
