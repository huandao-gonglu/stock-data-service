#!/usr/bin/env bash
set -euo pipefail

HOST_ADDRESS="127.0.0.1"
PORT="8000"

if command -v lsof >/dev/null 2>&1; then
  pids="$(lsof -tiTCP:"${PORT}" -sTCP:LISTEN || true)"
else
  echo "lsof is required to find the existing service on port ${PORT}." >&2
  exit 1
fi

if [ -n "${pids}" ]; then
  echo "Stopping existing service on ${HOST_ADDRESS}:${PORT} (PID: ${pids})..."
  kill ${pids}
  sleep 1

  remaining_pids="$(lsof -tiTCP:"${PORT}" -sTCP:LISTEN || true)"
  if [ -n "${remaining_pids}" ]; then
    echo "Force stopping existing service on ${HOST_ADDRESS}:${PORT} (PID: ${remaining_pids})..."
    kill -9 ${remaining_pids}
  fi
fi

echo "Starting stock data service on http://${HOST_ADDRESS}:${PORT} ..."
exec ./.venv/bin/python -m uvicorn stock_data_service.main:app --host "${HOST_ADDRESS}" --port "${PORT}"
