// Lab 8: Implement a network video content service (server)

package storage

// Implement a network video content service (server)
import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"tritontube/internal/proto"
)

type StorageServer struct {
	proto.UnimplementedVideoContentStorageServer
	BaseDir string
}

func NewStorageServer(baseDir string) *StorageServer {
	return &StorageServer{
		BaseDir: baseDir,
	}
}

func (s *StorageServer) Write(ctx context.Context, req *proto.WriteRequest) (*proto.WriteResponse, error) {
	dirPath := filepath.Join(s.BaseDir, req.VideoId)
	filePath := filepath.Join(dirPath, req.Filename)

	if err := os.MkdirAll(dirPath, 0777); err != nil {
		return &proto.WriteResponse{Success: false, Error: err.Error()}, nil
	}

	if err := ioutil.WriteFile(filePath, req.Data, 0644); err != nil {
		return &proto.WriteResponse{Success: false, Error: err.Error()}, nil
	}

	return &proto.WriteResponse{Success: true}, nil
}

func (s *StorageServer) Read(ctx context.Context, req *proto.ReadRequest) (*proto.ReadResponse, error) {
	filePath := filepath.Join(s.BaseDir, req.VideoId, req.Filename)

	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return &proto.ReadResponse{Error: err.Error()}, nil
	}

	return &proto.ReadResponse{Data: data}, nil
}

func (s *StorageServer) ListFiles(_ context.Context, _ *proto.ListFilesRequest) (*proto.ListFilesResponse, error) {
	var keys []string
	filepath.Walk(s.BaseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(s.BaseDir, path) // "videoID/filename"
		parts := strings.SplitN(rel, string(os.PathSeparator), 2)
		if len(parts) == 2 {
			keys = append(keys, parts[0]+"/"+parts[1])
		}
		return nil
	})
	return &proto.ListFilesResponse{Keys: keys}, nil
}
