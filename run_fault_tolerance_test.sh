#!/usr/bin/env bash
set -euo pipefail

# run_fault_tolerance_test.sh
# Starts the docker-compose stack, waits for a filter_by_amount worker to start,
# then waits an additional N seconds (default 90), kills the worker, leaves it
# down for D seconds (default 10), and brings it back up. Useful to validate
# ACK-based fault tolerance.

# Usage:
#   ./run_fault_tolerance_test.sh [worker_name] [post_start_wait] [down_time] [worker_name_2]
# Examples:
#   ./run_fault_tolerance_test.sh                             # uses defaults (kills both filter_by_amount_worker_0 and filter_by_hour_worker_0)
#   ./run_fault_tolerance_test.sh filter_by_amount_worker_1 60 15 filter_by_hour_worker_1

WORKER_NAME="${1:-filter_by_amount_worker_0}"
WORKER_NAME_2="${4:-filter_by_hour_worker_0}"  # second worker to kill
POST_START_WAIT="${2:-30}"   # seconds to wait after worker logs startup before killing
DOWN_TIME="${3:-30}"         # seconds the containers will stay down
LOG_WAIT_TIMEOUT=180           # maximum seconds to wait for worker startup log

# Different log patterns for different workers
if [[ "$WORKER_NAME" == *"filter_by_amount"* ]]; then
    START_LOG_PATTERN="connecting to RabbitMQ"
elif [[ "$WORKER_NAME" == *"filter_by_hour"* ]]; then
    START_LOG_PATTERN="connecting to exchanges"
else
    START_LOG_PATTERN="connecting to"  # generic fallback
fi

if [[ "$WORKER_NAME_2" == *"filter_by_amount"* ]]; then
    START_LOG_PATTERN_2="connecting to RabbitMQ"
elif [[ "$WORKER_NAME_2" == *"filter_by_hour"* ]]; then
    START_LOG_PATTERN_2="connecting to exchanges"
else
    START_LOG_PATTERN_2="connecting to"  # generic fallback
fi

echo "[ft-test] Workers: $WORKER_NAME and $WORKER_NAME_2"
echo "[ft-test] post-start wait: ${POST_START_WAIT}s, down time: ${DOWN_TIME}s"

# Bring up the stack (build if needed)
echo "[ft-test] Bringing up docker compose stack..."
docker compose up -d --build

# Wait for the workers to appear and emit the startup log
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

echo "[ft-test] Waiting for $WORKER_NAME_2 to show startup log: '${START_LOG_PATTERN_2}' (timeout ${LOG_WAIT_TIMEOUT}s)"
start_ts=$SECONDS
while true; do
  if docker ps --format '{{.Names}}' | grep -q "^${WORKER_NAME_2}$"; then
    # check logs for pattern in recent lines
    if docker logs "${WORKER_NAME_2}" 2>&1 | tail -n 200 | grep -q "${START_LOG_PATTERN_2}"; then
      echo "[ft-test] Detected startup log for ${WORKER_NAME_2}"
      break
    fi
  fi
  if (( SECONDS - start_ts > LOG_WAIT_TIMEOUT )); then
    echo "[ft-test] Timeout waiting for ${WORKER_NAME_2} startup log" >&2
    docker ps --format "table {{.Names}}	{{.Status}}" || true
    exit 1
  fi
  sleep 1
done

# Wait additional configured seconds before killing
echo "[ft-test] Waiting additional ${POST_START_WAIT}s before killing ${WORKER_NAME} and ${WORKER_NAME_2}"
sleep "${POST_START_WAIT}"

# Kill the workers
echo "[ft-test] Killing containers ${WORKER_NAME} and ${WORKER_NAME_2}"
docker kill "${WORKER_NAME}" || ( echo "[ft-test] docker kill failed for ${WORKER_NAME} (maybe already down)" >&2 )
docker kill "${WORKER_NAME_2}" || ( echo "[ft-test] docker kill failed for ${WORKER_NAME_2} (maybe already down)" >&2 )

# Let them stay down
echo "[ft-test] Sleeping ${DOWN_TIME}s while containers are down"
sleep "${DOWN_TIME}"

# Bring them back up
echo "[ft-test] Bringing ${WORKER_NAME} and ${WORKER_NAME_2} back up"
# Use --no-deps to avoid restarting dependent services unnecessarily
if docker compose up -d --no-deps "${WORKER_NAME}" "${WORKER_NAME_2}" 2>/dev/null; then
  echo "[ft-test] docker-compose up -d --no-deps succeeded for both workers"
else
  echo "[ft-test] Fallback: docker-compose up -d for both workers"
  docker compose up -d "${WORKER_NAME}" "${WORKER_NAME_2}"
fi

# Done
echo "[ft-test] Fault-tolerance cycle completed for ${WORKER_NAME} and ${WORKER_NAME_2}"

echo "⏰[ft-test] Esperando a que terminen los servicios..."
sleep 150

# Ejecutar comparación
echo "[ft-test] Ejecutando comparación CSV..."
python3 compare_csv_multi.py

echo "✅[ft-test] Completado!"