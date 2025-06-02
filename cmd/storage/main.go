package main

import (
	"flag"
	"fmt"
	// added
	"log"
	"net"

	"google.golang.org/grpc"
	"tritontube/internal/proto"
	"tritontube/internal/storage"
)

func main() {
	host := flag.String("host", "localhost", "Host address for the server")
	port := flag.Int("port", 8090, "Port number for the server")
	flag.Parse()

	// Validate arguments
	if *port <= 0 {
		panic("Error: Port number must be positive")
	}

	if flag.NArg() < 1 {
		fmt.Println("Usage: storage.proto [OPTIONS] <baseDir>")
		fmt.Println("Error: Base directory argument is required")
		return
	}
	baseDir := flag.Arg(0)

	fmt.Println("Starting storage.proto server...")
	fmt.Printf("Host: %s\n", *host)
	fmt.Printf("Port: %d\n", *port)
	fmt.Printf("Base Directory: %s\n", baseDir)

	//panic("Lab 8: not implemented")
	// LAB 8 added

	// Construct address and start listener
	addr := fmt.Sprintf("%s:%d", *host, *port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", addr, err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Register the storage service
	proto.RegisterVideoContentStorageServer(grpcServer, storage.NewStorageServer(baseDir))

	log.Printf("Storage server listening on %s", addr)

	// Serve
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
