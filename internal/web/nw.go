// Lab 8: Implement a network video content service (client using consistent hashing)

package web

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"tritontube/internal/proto"
)

// NetworkVideoContentService implements VideoContentService using a network of nodes.
type NetworkVideoContentService struct {
	proto.UnimplementedVideoContentAdminServiceServer

	clients map[string]proto.VideoContentStorageClient
	ring    *ConsistentHashRing
	mu      sync.RWMutex
}

// Uncomment the following line to ensure NetworkVideoContentService implements VideoContentService
var _ VideoContentService = (*NetworkVideoContentService)(nil)

var _ proto.VideoContentAdminServiceServer = (*NetworkVideoContentService)(nil)

// NewNetworkVideoContentService creates a new distributed content service.
// contentOption format: "adminhost:adminport,node1:port1,node2:port2,..."
func NewNetworkVideoContentService(contentOption string) (*NetworkVideoContentService, error) {
	nodes := parseNodeAddresses(contentOption)
	if len(nodes) < 2 {
		return nil, fmt.Errorf("expected at least one admin + one storage node")
	}

	storageAddrs := nodes[1:]
	clients := make(map[string]proto.VideoContentStorageClient)
	ring := NewConsistentHashRing()

	for _, addr := range storageAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, fmt.Errorf("failed to connect to node %s: %v", addr, err)
		}
		clients[addr] = proto.NewVideoContentStorageClient(conn)
		ring.AddNode(addr)
	}

	return &NetworkVideoContentService{
		clients: clients,
		ring:    ring,
	}, nil
}

// Write sends the file to the correct node
func (n *NetworkVideoContentService) Write(videoId string, filename string, data []byte) error {
	key := fmt.Sprintf("%s/%s", videoId, filename)
	node := n.ring.GetNode(key)

	n.mu.RLock()
	client, ok := n.clients[node]
	n.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no client for node %s", node)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &proto.WriteRequest{
		VideoId:  videoId,
		Filename: filename,
		Data:     data,
	}

	resp, err := client.Write(ctx, req)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("write failed: %s", resp.Error)
	}
	return nil
}

// Read retrieves the file from the correct node
func (n *NetworkVideoContentService) Read(videoId string, filename string) ([]byte, error) {
	key := fmt.Sprintf("%s/%s", videoId, filename)
	node := n.ring.GetNode(key)

	n.mu.RLock()
	client, ok := n.clients[node]
	n.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no client for node %s", node)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &proto.ReadRequest{
		VideoId:  videoId,
		Filename: filename,
	}

	resp, err := client.Read(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("read failed: %s", resp.Error)
	}
	return resp.Data, nil
}

// parseNodeAddresses splits comma-separated list into []string
func parseNodeAddresses(option string) []string {
	parts := strings.Split(option, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// Admin gRPC server section

func (n *NetworkVideoContentService) FileMigration(from string) (int, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	clientFrom, ok := n.clients[from]
	if !ok {
		return 0, fmt.Errorf("source client %s not found", from)
	}

	var migrated int

	// Absolute path to the from-node's local storage folder
	basePath := filepath.Join("storage", extractPort(from))

	// Walk all files under the basePath
	err := filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}

		relPath, _ := filepath.Rel(basePath, path)
		parts := strings.SplitN(relPath, string(filepath.Separator), 2)
		if len(parts) != 2 {
			return nil
		}
		videoID, filename := parts[0], parts[1]

		key := fmt.Sprintf("%s/%s", videoID, filename)
		target := n.ring.GetNode(key)

		if target != from {
			// Read from source
			resp, err := clientFrom.Read(context.Background(), &proto.ReadRequest{
				VideoId:  videoID,
				Filename: filename,
			})
			if err != nil || resp.Error != "" {
				return nil
			}

			clientTo := n.clients[target]
			_, err = clientTo.Write(context.Background(), &proto.WriteRequest{
				VideoId:  videoID,
				Filename: filename,
				Data:     resp.Data,
			})
			if err == nil {
				migrated++
			}
		}

		return nil
	})

	return migrated, err
}

func extractPort(addr string) string {
	parts := strings.Split(addr, ":")
	return parts[len(parts)-1]
}

func (n *NetworkVideoContentService) ListNodes(ctx context.Context, req *proto.ListNodesRequest) (*proto.ListNodesResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	var addrs []string
	for addr := range n.clients {
		addrs = append(addrs, addr)
	}
	return &proto.ListNodesResponse{Nodes: addrs}, nil

}

func (n *NetworkVideoContentService) AddNode(ctx context.Context, req *proto.AddNodeRequest) (*proto.AddNodeResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	addr := req.NodeAddress
	if _, exists := n.clients[addr]; exists {
		return &proto.AddNodeResponse{}, nil
	}

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	n.clients[addr] = proto.NewVideoContentStorageClient(conn)
	n.ring.AddNode(addr)

	// Migrate files to new node
	//return &proto.AddNodeResponse{}, nil
	migrated, err := n.FileMigration(addr)
	if err != nil {
		return nil, err
	}

	return &proto.AddNodeResponse{MigratedFileCount: int32(migrated)}, nil
}

func (n *NetworkVideoContentService) RemoveNode(ctx context.Context, req *proto.RemoveNodeRequest) (*proto.RemoveNodeResponse, error) {
	n.mu.Lock()
	addr := req.NodeAddress
	if _, exists := n.clients[addr]; !exists {
		n.mu.Unlock()
		return &proto.RemoveNodeResponse{}, nil
	}

	n.ring.RemoveNode(addr)
	n.mu.Unlock()

	migrated, _ := n.FileMigration(addr)

	n.mu.Lock()
	delete(n.clients, addr)
	n.mu.Unlock()

	return &proto.RemoveNodeResponse{MigratedFileCount: int32(migrated)}, nil
}

// Consistent hshing section

// ConsistentHashRing maps keys to nodes using consistent hashing.
type ConsistentHashRing struct {
	nodes   []uint64
	nodeMap map[uint64]string
}

// NewConsistentHashRing initializes an empty ring.
func NewConsistentHashRing() *ConsistentHashRing {
	return &ConsistentHashRing{
		nodes:   []uint64{},
		nodeMap: make(map[uint64]string),
	}
}

// AddNode adds a node to the ring.
func (r *ConsistentHashRing) AddNode(address string) {
	hash := hashStringToUint64(address)
	r.nodes = append(r.nodes, hash)
	r.nodeMap[hash] = address
	sort.Slice(r.nodes, func(i, j int) bool {
		return r.nodes[i] < r.nodes[j]
	})
}

func (r *ConsistentHashRing) RemoveNode(address string) {
	hash := hashStringToUint64(address)
	delete(r.nodeMap, hash)
	for i, h := range r.nodes {
		if h == hash {
			r.nodes = append(r.nodes[:i], r.nodes[i+1:]...)
			break
		}
	}
}

// GetNode returns the node responsible for the given key.
func (r *ConsistentHashRing) GetNode(key string) string {
	if len(r.nodes) == 0 {
		return ""
	}
	keyHash := hashStringToUint64(key)

	for _, nodeHash := range r.nodes {
		if keyHash <= nodeHash {
			return r.nodeMap[nodeHash]
		}
	}
	return r.nodeMap[r.nodes[0]]
}

// hashStringToUint64 hashes a string using SHA-256 and takes the first 8 bytes.
func hashStringToUint64(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}
