// Lab 7: Implement a SQLite video metadata service

package web

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"time"
)

type SQLiteVideoMetadataService struct {
	//	added
	db *sql.DB
}

func NewSQLiteVideoMetadataService(dbPath string) (*SQLiteVideoMetadataService, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	createTable := `CREATE TABLE IF NOT EXISTS video_metadata (
    	id TEXT PRIMARY KEY,
		uploaded_at DATETIME
	);`
	_, err = db.Exec(createTable)
	if err != nil {
		return nil, err
	}
	return &SQLiteVideoMetadataService{db: db}, nil
}

//CREATE

func (s *SQLiteVideoMetadataService) Create(videoId string, uploadedAt time.Time) error {
	_, err := s.db.Exec(`INSERT INTO video_metadata (id, uploaded_at) VALUES (?, ?)`, videoId, uploadedAt)
	return err
}

// READ
func (s *SQLiteVideoMetadataService) Read(id string) (*VideoMetadata, error) {
	row := s.db.QueryRow(`SELECT id, uploaded_at FROM video_metadata WHERE id = ?`, id)

	var metadata VideoMetadata
	err := row.Scan(&metadata.Id, &metadata.UploadedAt)
	if err == sql.ErrNoRows {
		return nil, nil // video not found
	} else if err != nil {
		return nil, err
	}
	return &metadata, nil
}

// LIST
func (s *SQLiteVideoMetadataService) List() ([]VideoMetadata, error) {
	rows, err := s.db.Query(`SELECT id, uploaded_at FROM video_metadata`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var videos []VideoMetadata
	for rows.Next() {
		var m VideoMetadata
		if err := rows.Scan(&m.Id, &m.UploadedAt); err != nil {
			return nil, err
		}
		videos = append(videos, m)
	}
	return videos, nil
}

// Uncomment the following line to ensure SQLiteVideoMetadataService implements VideoMetadataService
var _ VideoMetadataService = (*SQLiteVideoMetadataService)(nil)
