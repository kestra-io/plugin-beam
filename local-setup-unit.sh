set -euo pipefail

COMPOSE="docker compose -f docker-compose-ci.yml"

# Unlock the Docker socket for the TaskManager
echo "▶ Securing Docker socket permissions..."
sudo chmod 666 /var/run/docker.sock || echo "⚠ Could not chmod socket, skipping..."

wait_for_flink() {
  echo "▶ Waiting for Flink JobManager (http://localhost:8082)..."
  for _ in {1..60}; do
    if curl -fsS http://localhost:8082/overview >/dev/null 2>&1; then
      local slots=$(curl -s http://localhost:8082/overview | grep -o '"slots_total":[0-9]*' | cut -d: -f2 || echo 0)
      if [ "$slots" -gt 0 ]; then
        echo "✔ Flink is ready (Slots: $slots)"
        return 0
      fi
    fi
    sleep 2
  done
  return 1
}

wait_for_jobserver() {
  echo "▶ Waiting for Beam Flink JobServer ports (8097/8098/8099)..."

  for _ in {1..60}; do
    if bash -lc 'for port in 8097 8098 8099; do exec 3<>/dev/tcp/127.0.0.1/$port || exit 1; exec 3<&-; exec 3>&-; done' >/dev/null 2>&1; then
      echo "✔ JobServer is ready"
      return 0
    fi
    sleep 2
  done
  return 1
}

wait_for_spark() {
  echo "▶ Waiting for Spark Master UI (http://localhost:56479)..."

  for _ in {1..60}; do
    if curl -fsS http://localhost:56479/ >/dev/null 2>&1; then
      echo "✔ Spark is ready"
      return 0
    fi
    sleep 2
  done
  return 1
}

echo "▶ Cleaning up old environment..."
$COMPOSE down -v --remove-orphans
docker network prune -f

echo "▶ Starting services (Flink, Spark, Beam Pool)"
$COMPOSE up -d --remove-orphans

echo "▶ Show containers"
$COMPOSE ps

# Check order, important for the network dependency
wait_for_flink
wait_for_jobserver
wait_for_spark

echo "✔ ALL SERVICES ARE READY"
