// Lab 7: Implement a local filesystem video content service

package web

import (
	"os"
	"path/filepath"
)

// FSVideoContentService implements VideoContentService using the local filesystem.
type FSVideoContentService struct {
	baseDir string
}

// Constructor
func NewFSVideoContentService(baseDir string) *FSVideoContentService {
	return &FSVideoContentService{baseDir: baseDir}
}

// WRITE
func (fs *FSVideoContentService) Write(videoId string, filename string, data []byte) error {
	dirPath := filepath.Join(fs.baseDir, videoId)
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		return err
	}
	filePath := filepath.Join(dirPath, filename)
	return os.WriteFile(filePath, data, 0644)
}

// READ
func (fs *FSVideoContentService) Read(videoId string, filename string) ([]byte, error) {
	filePath := filepath.Join(fs.baseDir, videoId, filename)
	return os.ReadFile(filePath)
}

// Uncomment the following line to ensure FSVideoContentService implements VideoContentService
var _ VideoContentService = (*FSVideoContentService)(nil)
