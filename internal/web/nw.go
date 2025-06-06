// Lab 8: Implement a network video content service (client using consistent hashing)

package web

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"google.golang.org/grpc/credentials/insecure"
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

func NewNetworkVideoContentService(contentOption string) (*NetworkVideoContentService, error) {
	nodes := parseNodeAddresses(contentOption)
	if len(nodes) < 2 {
		return nil, fmt.Errorf("expected at least ADMIN + one storage node")
	}

	storageAddrs := nodes[1:]
	clients := make(map[string]proto.VideoContentStorageClient)
	ring := NewConsistentHashRing()

	for _, addr := range storageAddrs {
		conn, err := grpc.Dial(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to storage node %s: %v", addr, err)
		}
		clients[addr] = proto.NewVideoContentStorageClient(conn)
		ring.AddNode(addr)
	}

	return &NetworkVideoContentService{
		clients: clients,
		ring:    ring,
	}, nil
}

func (n *NetworkVideoContentService) Write(videoID, filename string, data []byte) error {
	key := fmt.Sprintf("%s/%s", videoID, filename)
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
		VideoId:  videoID,
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

func (n *NetworkVideoContentService) Read(videoID, filename string) ([]byte, error) {
	key := fmt.Sprintf("%s/%s", videoID, filename)
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
		VideoId:  videoID,
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

func (n *NetworkVideoContentService) ListNodes(_ context.Context, _ *proto.ListNodesRequest) (*proto.ListNodesResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	var sortedAddrs []string
	for _, h := range n.ring.nodes {
		sortedAddrs = append(sortedAddrs, n.ring.nodeMap[h])
	}
	return &proto.ListNodesResponse{Nodes: sortedAddrs}, nil
}

func (n *NetworkVideoContentService) AddNode(
	_ context.Context,
	req *proto.AddNodeRequest,
) (*proto.AddNodeResponse, error) {
	newAddr := req.NodeAddress

	n.mu.Lock()
	if _, exists := n.clients[newAddr]; exists {
		n.mu.Unlock()
		return &proto.AddNodeResponse{}, nil
	}

	ctxDial, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(
		ctxDial,
		newAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		n.mu.Unlock()
		return nil, fmt.Errorf("failed to connect to new node %s: %v", newAddr, err)
	}

	n.clients[newAddr] = proto.NewVideoContentStorageClient(conn)
	n.ring.AddNode(newAddr)
	n.mu.Unlock()

	migrated := 0
	for oldAddr, clientOld := range n.clients {
		ctxList, cancelList := context.WithTimeout(context.Background(), 5*time.Second)
		respList, err := clientOld.ListFiles(ctxList, &proto.ListFilesRequest{})
		cancelList()
		if err != nil {
			continue
		}

		for _, fullKey := range respList.Keys {
			videoID, filename := splitKey(fullKey)
			key := fmt.Sprintf("%s/%s", videoID, filename)
			target := n.ring.GetNode(key)
			if target == oldAddr {
				continue
			}

			ctxRead, cancelRead := context.WithTimeout(context.Background(), 5*time.Second)
			readResp, readErr := clientOld.Read(ctxRead, &proto.ReadRequest{
				VideoId:  videoID,
				Filename: filename,
			})
			cancelRead()
			if readErr != nil || readResp.Error != "" {
				continue
			}

			n.mu.RLock()
			clientNew, ok := n.clients[target]
			n.mu.RUnlock()
			if !ok {
				continue
			}

			ctxWrite, cancelWrite := context.WithTimeout(context.Background(), 5*time.Second)
			_, writeErr := clientNew.Write(ctxWrite, &proto.WriteRequest{
				VideoId:  videoID,
				Filename: filename,
				Data:     readResp.Data,
			})
			cancelWrite()
			if writeErr != nil {
				continue
			}

			ctxDel, cancelDel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = clientOld.Delete(ctxDel, &proto.DeleteRequest{
				VideoId:  videoID,
				Filename: filename,
			})
			cancelDel()

			migrated++
		}
	}

	return &proto.AddNodeResponse{MigratedFileCount: int32(migrated)}, nil
}
func (n *NetworkVideoContentService) RemoveNode(
	_ context.Context,
	req *proto.RemoveNodeRequest,
) (*proto.RemoveNodeResponse, error) {
	addr := req.NodeAddress

	n.mu.RLock()
	clientOld, exists := n.clients[addr]
	n.mu.RUnlock()
	if !exists {
		return &proto.RemoveNodeResponse{}, nil
	}

	ctxList, cancelList := context.WithTimeout(context.Background(), 5*time.Second)
	respList, err := clientOld.ListFiles(ctxList, &proto.ListFilesRequest{})
	cancelList()
	if err != nil {
		return nil, fmt.Errorf("failed to list files on node %s: %v", addr, err)
	}

	n.mu.Lock()
	n.ring.RemoveNode(addr)
	n.mu.Unlock()

	migrated := 0
	for _, fullKey := range respList.Keys {
		videoID, filename := splitKey(fullKey)
		key := fmt.Sprintf("%s/%s", videoID, filename)
		newOwner := n.ring.GetNode(key)
		if newOwner == "" || newOwner == addr {
			continue
		}

		ctxRead, cancelRead := context.WithTimeout(context.Background(), 5*time.Second)
		readResp, readErr := clientOld.Read(ctxRead, &proto.ReadRequest{
			VideoId:  videoID,
			Filename: filename,
		})
		cancelRead()
		if readErr != nil || readResp.Error != "" {
			continue
		}

		n.mu.RLock()
		clientNew, ok := n.clients[newOwner]
		n.mu.RUnlock()
		if !ok {
			continue
		}

		ctxWrite, cancelWrite := context.WithTimeout(context.Background(), 5*time.Second)
		_, writeErr := clientNew.Write(ctxWrite, &proto.WriteRequest{
			VideoId:  videoID,
			Filename: filename,
			Data:     readResp.Data,
		})
		cancelWrite()
		if writeErr != nil {
			continue
		}

		ctxDel, cancelDel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = clientOld.Delete(ctxDel, &proto.DeleteRequest{
			VideoId:  videoID,
			Filename: filename,
		})
		cancelDel()

		migrated++
	}
	n.mu.Lock()
	delete(n.clients, addr)
	n.mu.Unlock()

	return &proto.RemoveNodeResponse{MigratedFileCount: int32(migrated)}, nil
}

func parseNodeAddresses(option string) []string {
	parts := strings.Split(option, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func splitKey(fullKey string) (string, string) {
	parts := strings.SplitN(fullKey, "/", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

type ConsistentHashRing struct {
	nodes   []uint64
	nodeMap map[uint64]string
}

func NewConsistentHashRing() *ConsistentHashRing {
	return &ConsistentHashRing{
		nodes:   []uint64{},
		nodeMap: make(map[uint64]string),
	}
}

func (r *ConsistentHashRing) AddNode(address string) {
	hash := hashStringToUint64(address)

	for _, h := range r.nodes {
		if h == hash {
			return
		}
	}

	r.nodes = append(r.nodes, hash)
	r.nodeMap[hash] = address
	sort.Slice(r.nodes, func(i, j int) bool { return r.nodes[i] < r.nodes[j] })
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

func hashStringToUint64(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}
