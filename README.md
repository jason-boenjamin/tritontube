# TritonTube

TritonTube is a distributed video streaming platform built with Go, gRPC, and SQLite.  
It supports horizontal scaling, fault tolerance, and adaptive streaming (MPEG-DASH) to deliver smooth playback for multiple concurrent users.

## Features
- High Performance: Reduced buffering by 60% with consistent hashing and latency under 50ms  
- Scalable: Distributed design using gRPC and consistent hashing for horizontal scaling  
- Reliable: Metadata and video content replication with RAFT consensus for fault tolerance  
- Adaptive Streaming: MPEG-DASH support to optimize playback quality across varying network conditions  

## Tech Stack
- **Language**: Go  
- **Database**: SQLite  
- **Protocols**: gRPC, HTTP  
- **Cloud**: AWS EC2 deployment  
- **Streaming**: MPEG-DASH, FFmpeg  
- **Consensus**: RAFT protocol  

## Architecture Overview
1. **Storage Servers** – Handle video chunks and metadata  
2. **Coordinator (RAFT Leader)** – Manages metadata consistency and replication  
3. **Clients** – Stream video with adaptive bitrate (MPEG-DASH)  

## Getting Started

### Prerequisites
- Go 1.21+  
- FFmpeg installed (`ffmpeg --version`)  
- SQLite installed  
- AWS account (for deployment, optional)

### Local Setup
```bash
# Clone the repo
git clone https://github.com/jason-boenjamin/tritontube.git
cd tritontube

# Build
go build ./...

# Run coordinator
go run cmd/coordinator/main.go

# Run a storage server
go run cmd/storage/main.go --port=5001

# Start client
go run cmd/client/main.go

