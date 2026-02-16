#!/usr/bin/env bash
set -e

echo "=== TritonTube clean start ==="

# Kill old servers if rerunning
pkill -f "cmd/storage" || true
pkill -f "cmd/web" || true
pkill -f "cmd/admin" || true

# Clean previous data
rm -rf data
rm -f metadata.db

mkdir -p data/node1

echo "Starting storage node on :5001"
go run ./cmd/storage/main.go --port 5001 data/node1 &
STORAGE_PID=$!

sleep 1

echo "Starting web server + admin on :8080 / :4000"
go run ./cmd/web/main.go \
  -port 8080 \
  sqlite ./metadata.db \
  nw "localhost:4000,localhost:5001" &
WEB_PID=$!

echo "Servers running"
echo "Storage PID: $STORAGE_PID"
echo "Web PID: $WEB_PID"
echo "Visit http://localhost:8080/upload"

wait