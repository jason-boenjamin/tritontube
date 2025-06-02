package main

import (
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"net"
	"strings"
	"tritontube/internal/proto"
	"tritontube/internal/web"
)

// printUsage prints the usage information for the application
func printUsage() {
	fmt.Println("Usage: ./program [OPTIONS] METADATA_TYPE METADATA_OPTIONS CONTENT_TYPE CONTENT_OPTIONS")
	fmt.Println()
	fmt.Println("Arguments:")
	fmt.Println("  METADATA_TYPE         Metadata service type (sqlite, etcd)")
	fmt.Println("  METADATA_OPTIONS      Options for metadata service (e.g., db path)")
	fmt.Println("  CONTENT_TYPE          Content service type (fs, nw)")
	fmt.Println("  CONTENT_OPTIONS       Options for content service (e.g., base dir, network addresses)")
	fmt.Println()
	fmt.Println("Options:")
	flag.PrintDefaults()
	fmt.Println()
	fmt.Println("Example: ./program sqlite db.db fs /path/to/videos")
}

func main() {
	// Define flags
	port := flag.Int("port", 8080, "Port number for the web server")
	host := flag.String("host", "localhost", "Host address for the web server")

	// Set custom usage message
	flag.Usage = printUsage

	// Parse flags
	flag.Parse()

	// Check if the correct number of positional arguments is provided
	if len(flag.Args()) != 4 {
		fmt.Println("Error: Incorrect number of arguments")
		printUsage()
		return
	}

	// Parse positional arguments
	metadataServiceType := flag.Arg(0)
	metadataServiceOptions := flag.Arg(1)
	contentServiceType := flag.Arg(2)
	contentServiceOptions := flag.Arg(3)

	// Validate port number (already an int from flag, check if positive)
	if *port <= 0 {
		fmt.Println("Error: Invalid port number:", *port)
		printUsage()
		return
	}

	// Construct metadata service
	var metadataService web.VideoMetadataService
	fmt.Println("Creating metadata service of type", metadataServiceType, "with options", metadataServiceOptions)
	// TODO: Implement metadata service creation logic
	if metadataServiceType == "sqlite" {
		svc, err := web.NewSQLiteVideoMetadataService(metadataServiceOptions)
		if err != nil {
			fmt.Println(err)
			return
		}
		metadataService = svc
	}

	// Construct content service
	var contentService web.VideoContentService
	fmt.Println("Creating content service of type", contentServiceType, "with options", contentServiceOptions)
	// TODO: Implement content service creation logic
	if contentServiceType == "fs" {
		contentService = web.NewFSVideoContentService(contentServiceOptions)
	} else if contentServiceType == "nw" {
		svc, err := web.NewNetworkVideoContentService(contentServiceOptions)
		if err != nil {
			fmt.Println("Failed to initialize NetworkVideoContentService:", err)
			return
		}
		contentService = svc
	} else {
		fmt.Println("Error: Unsupported content service type:", contentServiceType)
		printUsage()
		return
	}

	// Start the server
	server := web.NewServer(metadataService, contentService)
	listenAddr := fmt.Sprintf("%s:%d", *host, *port)
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		fmt.Println("Error starting listener:", err)
		return
	}
	defer lis.Close()

	fmt.Println("Starting web server on", listenAddr)

	if nwService, ok := contentService.(*web.NetworkVideoContentService); ok {
		go func() {
			adminAddr := strings.Split(contentServiceOptions, ",")[0] // e.g. "localhost:8081"
			lis, err := net.Listen("tcp", adminAddr)
			if err != nil {
				fmt.Println("Failed to listen on admin gRPC port:", err)
				return
			}

			grpcServer := grpc.NewServer()
			proto.RegisterVideoContentAdminServiceServer(grpcServer, nwService)
			fmt.Println("Admin gRPC listening on", adminAddr)

			if err := grpcServer.Serve(lis); err != nil {
				fmt.Println("Admin gRPC server error:", err)
			}
		}()
	}

	err = server.Start(lis)
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
}
