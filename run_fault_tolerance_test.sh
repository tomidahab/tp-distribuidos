#!/usr/bin/env bash
set -euo pipefail

# run_fault_tolerance_test.sh
# Starts the docker-compose stack, waits for a filter_by_amount worker to start,
# then waits an additional N seconds (default 90), kills the worker, leaves it
# down for D seconds (default 10), and brings it back up. Useful to validate
# ACK-based fault tolerance.

# Usage:
#   ./run_fault_tolerance_test.sh [worker_name] [post_start_wait] [down_time]
# Examples:
#   ./run_fault_tolerance_test.sh                             # uses defaults
#   ./run_fault_tolerance_test.sh filter_by_amount_worker_1 60 15

WORKER_NAME="${1:-filter_by_amount_worker_0}"
POST_START_WAIT="${2:-30}"   # seconds to wait after worker logs startup before killing
DOWN_TIME="${3:-5}"          # seconds the container will stay down
LOG_WAIT_TIMEOUT=180           # maximum seconds to wait for worker startup log
START_LOG_PATTERN="connecting to RabbitMQ"  # substring to detect worker ready

echo "[ft-test] Worker: $WORKER_NAME"
echo "[ft-test] post-start wait: ${POST_START_WAIT}s, down time: ${DOWN_TIME}s"

# Bring up the stack (build if needed)
echo "[ft-test] Bringing up docker compose stack..."
docker compose up -d --build

# Wait for the worker to appear and emit the startup log
echo "[ft-test] Waiting for $WORKER_NAME to show startup log: '${START_LOG_PATTERN}' (timeout ${LOG_WAIT_TIMEOUT}s)"
start_ts=$SECONDS
while true; do
  if docker ps --format '{{.Names}}' | grep -q "^${WORKER_NAME}$"; then
    # check logs for pattern in recent lines
    if docker logs "${WORKER_NAME}" 2>&1 | tail -n 200 | grep -q "${START_LOG_PATTERN}"; then
      echo "[ft-test] Detected startup log for ${WORKER_NAME}"
      break
    fi
  fi
  if (( SECONDS - start_ts > LOG_WAIT_TIMEOUT )); then
    echo "[ft-test] Timeout waiting for ${WORKER_NAME} startup log" >&2
    docker ps --format "table {{.Names}}	{{.Status}}" || true
    exit 1
  fi
  sleep 1
done

# Wait additional configured seconds before killing
echo "[ft-test] Waiting additional ${POST_START_WAIT}s before killing ${WORKER_NAME}"
sleep "${POST_START_WAIT}"

# Kill the worker
echo "[ft-test] Killing container ${WORKER_NAME}"
docker kill "${WORKER_NAME}" || ( echo "[ft-test] docker kill failed (maybe already down)" >&2 )

# Let it stay down
echo "[ft-test] Sleeping ${DOWN_TIME}s while container is down"
sleep "${DOWN_TIME}"

# Bring it back up
echo "[ft-test] Bringing ${WORKER_NAME} back up"
# Use --no-deps to avoid restarting dependent services unnecessarily
if docker compose up -d --no-deps "${WORKER_NAME}" 2>/dev/null; then
  echo "[ft-test] docker-compose up -d --no-deps succeeded for ${WORKER_NAME}"
else
  echo "[ft-test] Fallback: docker-compose up -d ${WORKER_NAME}"
  docker compose up -d "${WORKER_NAME}"
fi

# Wait for reconnect (same log pattern)
echo "[ft-test] Waiting for ${WORKER_NAME} to reconnect and emit startup log"
start_ts=$SECONDS
while true; do
  if docker ps --format '{{.Names}}' | grep -q "^${WORKER_NAME}$"; then
    if docker logs "${WORKER_NAME}" 2>&1 | tail -n 200 | grep -q "${START_LOG_PATTERN}"; then
      echo "[ft-test] ${WORKER_NAME} reconnected and emitted startup log"
      break
    fi
  fi
  if (( SECONDS - start_ts > LOG_WAIT_TIMEOUT )); then
    echo "[ft-test] Timeout waiting for ${WORKER_NAME} to reconnect" >&2
    docker ps --format "table {{.Names}}	{{.Status}}" || true
    exit 1
  fi
  sleep 1
done

# Done
echo "[ft-test] Fault-tolerance cycle completed for ${WORKER_NAME}"

echo "⏰[ft-test] Esperando a que terminen los servicios..."
sleep 150

# Ejecutar comparación
echo "[ft-test] Ejecutando comparación CSV..."
python3 compare_csv_multi.py

echo "✅[ft-test] Completado!"