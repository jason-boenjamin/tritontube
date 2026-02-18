#!/usr/bin/env bash
set -euo pipefail

echo "=== TritonTube clean start ==="

WEB_PORT=8080
ADMIN_PORT=4000
NODE1_PORT=5001
NODE1_DIR="data/node1"

# Windows-safe port killer (works in Git Bash)
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

# Stop anything already using our ports
kill_port "$WEB_PORT"
kill_port "$ADMIN_PORT"
kill_port "$NODE1_PORT"

# Clean previous data
rm -rf data
rm -f metadata.db web.log storage.log admin.log

mkdir -p "$NODE1_DIR"

echo ">>> Building binaries..."
rm -rf ./bin
mkdir -p ./bin
go build -o ./bin/storage.exe ./cmd/storage
go build -o ./bin/web.exe     ./cmd/web
go build -o ./bin/admin.exe   ./cmd/admin

echo "Starting storage node on :${NODE1_PORT}"
./bin/storage.exe --port "$NODE1_PORT" "$NODE1_DIR" >storage.log 2>&1 &
STORAGE_PID=$!

sleep 1

echo "Starting web server + admin on :${WEB_PORT} / :${ADMIN_PORT}"
./bin/web.exe \
  -port "$WEB_PORT" \
  sqlite ./metadata.db \
  nw "localhost:${ADMIN_PORT},localhost:${NODE1_PORT}" \
  >web.log 2>&1 &
WEB_PID=$!

echo "Servers running"
echo "Storage PID: $STORAGE_PID"
echo "Web PID: $WEB_PID"
echo "Visit http://localhost:${WEB_PORT}"

wait