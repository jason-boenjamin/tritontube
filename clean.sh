#!/usr/bin/env bash
set -euo pipefail

echo "=== TritonTube clean slate ==="

WEB_PORT=8080
ADMIN_PORT=4000
NODE_PORTS=(5001 5002 5003)

kill_port() {
  local port="$1"
  local pids
  pids=$(netstat -ano 2>/dev/null \
    | awk -v p=":$port" 'tolower($0) ~ /listening/ && $0 ~ p {print $5}' \
    | sort -u || true)

  for pid in $pids; do
    taskkill //F //PID "$pid" >/dev/null 2>&1 || true
  done
}

echo ">>> Killing running services..."
kill_port "$WEB_PORT"
kill_port "$ADMIN_PORT"
for p in "${NODE_PORTS[@]}"; do
  kill_port "$p"
done

echo ">>> Removing runtime state..."
rm -rf data
rm -rf bin
rm -f metadata.db
rm -f *.log

echo ">>> Clean slate ready"