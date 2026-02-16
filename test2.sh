#!/usr/bin/env bash
set -euo pipefail

# -------------------------
# CONFIG
# -------------------------
WEB_PORT=8080
ADMIN_PORT=4000

NODE1_HOST=localhost
NODE1_PORT=5001
NODE1_DIR="./data/node1"

NODE2_HOST=localhost
NODE2_PORT=5002
NODE2_DIR="./data/node2"

# Optional third node (enabled if NODES=3)
NODE3_HOST=localhost
NODE3_PORT=5003
NODE3_DIR="./data/node3"

# How many storage nodes to run: 2 (default) or 3
NODES="${NODES:-2}"

VIDEO="${VIDEO:-./samples/Everything_Goes_On.mp4}"

META_DB="./metadata.db"
WEB_LOG="./web.log"
NODE1_LOG="./storage1.log"
NODE2_LOG="./storage2.log"
NODE3_LOG="./storage3.log"

# -------------------------
# HELPERS
# -------------------------
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

wait_http() {
  local url="$1"
  local tries=60
  for _ in $(seq 1 $tries); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

start_storage() {
  local port="$1"
  local dir="$2"
  local log="$3"
  echo ">>> Starting storage node on localhost:${port} (dir: ${dir})..."
  mkdir -p "$dir"
  go run ./cmd/storage/main.go --port "$port" "$dir" >"$log" 2>&1 &
  sleep 0.5
}

count_files() {
  local dir="$1"
  if [ -d "$dir" ]; then
    # counts all files under node dir
    find "$dir" -type f 2>/dev/null | wc -l | tr -d ' '
  else
    echo "0"
  fi
}

# -------------------------
# PRE-TEST CLEANUP
# -------------------------
echo ">>> Cleaning previous state..."

# stop anything already using our ports
kill_port "$WEB_PORT"
kill_port "$ADMIN_PORT"
kill_port "$NODE1_PORT"
kill_port "$NODE2_PORT"
kill_port "$NODE3_PORT"

# wipe state
rm -rf ./data
rm -f "$META_DB"
rm -f "$WEB_LOG" "$NODE1_LOG" "$NODE2_LOG" "$NODE3_LOG"

# -------------------------
# START STORAGE NODES
# -------------------------
start_storage "$NODE1_PORT" "$NODE1_DIR" "$NODE1_LOG"
start_storage "$NODE2_PORT" "$NODE2_DIR" "$NODE2_LOG"

if [ "$NODES" -ge 3 ]; then
  start_storage "$NODE3_PORT" "$NODE3_DIR" "$NODE3_LOG"
fi

# -------------------------
# START WEB SERVER
# -------------------------
echo ">>> Starting web server on localhost:${WEB_PORT} (admin ${ADMIN_PORT})..."

# IMPORTANT:
# Your NewNetworkVideoContentService() currently expects:
# - first entry is ADMIN address
# - and at least ONE storage node present in the string
# We'll pass admin + node1 here; then we register the rest via admin RPC.
go run ./cmd/web/main.go \
  -port "$WEB_PORT" \
  sqlite "$META_DB" \
  nw "localhost:${ADMIN_PORT},${NODE1_HOST}:${NODE1_PORT}" \
  >"$WEB_LOG" 2>&1 &

wait_http "http://localhost:${WEB_PORT}/" || {
  echo "Web server never came up. Tail web.log:"
  tail -n 80 "$WEB_LOG" || true
  exit 1
}

# -------------------------
# REGISTER NODES WITH ADMIN
# -------------------------
echo ">>> Registering storage nodes via admin service..."
go run ./cmd/admin/main.go add "localhost:${ADMIN_PORT}" "${NODE1_HOST}:${NODE1_PORT}" >/dev/null || true
go run ./cmd/admin/main.go add "localhost:${ADMIN_PORT}" "${NODE2_HOST}:${NODE2_PORT}" >/dev/null

if [ "$NODES" -ge 3 ]; then
  go run ./cmd/admin/main.go add "localhost:${ADMIN_PORT}" "${NODE3_HOST}:${NODE3_PORT}" >/dev/null
fi

echo ">>> Admin list:"
go run ./cmd/admin/main.go list "localhost:${ADMIN_PORT}"

# -------------------------
# UPLOAD + VERIFY
# -------------------------
echo ">>> Uploading video: $VIDEO"
curl -fsS -F "file=@${VIDEO}" "http://localhost:${WEB_PORT}/upload" >/dev/null

VIDEO_ID="$(basename "$VIDEO" .mp4)"

echo ">>> Checking manifest..."
curl -fsS "http://localhost:${WEB_PORT}/content/${VIDEO_ID}/manifest.mpd" >/dev/null

echo ">>> Checking init segment..."
curl -fsS "http://localhost:${WEB_PORT}/content/${VIDEO_ID}/init-stream0.m4s" >/dev/null

echo ">>> Checking first chunk..."
curl -fsS "http://localhost:${WEB_PORT}/content/${VIDEO_ID}/chunk-stream0-00001.m4s" >/dev/null

# -------------------------
# DISTRIBUTION CHECK
# -------------------------
echo ">>> Distribution check (file counts per node dir):"
echo "    node1: $(count_files "$NODE1_DIR") files"
echo "    node2: $(count_files "$NODE2_DIR") files"
if [ "$NODES" -ge 3 ]; then
  echo "    node3: $(count_files "$NODE3_DIR") files"
fi

echo ">>> SUCCESS: pipeline verified"
echo ">>> Open: http://localhost:${WEB_PORT}/videos/${VIDEO_ID}"

# -------------------------
# OPTIONAL SHUTDOWN
# -------------------------
# Uncomment if you want it to stop services after verification:
# echo ">>> Shutting down..."
# kill_port "$WEB_PORT"
# kill_port "$ADMIN_PORT"
# kill_port "$NODE1_PORT"
# kill_port "$NODE2_PORT"
# kill_port "$NODE3_PORT"