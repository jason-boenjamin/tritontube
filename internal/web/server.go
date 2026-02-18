// Lab 7: Implement a web server
package web

import (
	"io"
	// "log"
	"bytes"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type server struct {
	Addr string
	Port int

	metadataService VideoMetadataService
	contentService  VideoContentService

	mux *http.ServeMux

	// templates loaded from internal/web/templates/
	tpls *templates
}

func NewServer(
	metadataService VideoMetadataService,
	contentService VideoContentService,
) *server {
	return &server{
		metadataService: metadataService,
		contentService:  contentService,
	}
}

func (s *server) Start(lis net.Listener) error {
	s.mux = http.NewServeMux()

	tpls, err := loadTemplates()
	if err != nil {
		return err
	}
	s.tpls = tpls

	// Serve /static/* from internal/web/static/
	// NOTE: This assumes you run `go run ...` from repo root.
	staticDir := http.Dir(filepath.Join("internal", "web", "static"))
	s.mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(staticDir)))

	s.mux.HandleFunc("/upload", s.handleUpload)
	s.mux.HandleFunc("/videos/", s.handleVideo)
	s.mux.HandleFunc("/content/", s.handleVideoContent)
	s.mux.HandleFunc("/", s.handleIndex)

	return http.Serve(lis, s.mux)
}

// -------- Views (match what templates expect) --------

type VideoListItemView struct {
	Id         string
	EscapedId  string
	UploadTime string
}

type IndexView struct {
	Videos []VideoListItemView
}

type VideoPageView struct {
	Id         string
	EscapedId  string
	UploadedAt string
}

// -------- Handlers --------

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	// Only serve index on exact "/"
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	videos, err := s.metadataService.List()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	view := IndexView{Videos: make([]VideoListItemView, 0, len(videos))}
	for _, v := range videos {
		view.Videos = append(view.Videos, VideoListItemView{
			Id:         v.Id,
			EscapedId:  url.PathEscape(v.Id),
			UploadTime: v.UploadedAt.Format("2006-01-02 15:04:05"),
		})
	}

	if err := s.tpls.Index.Execute(w, view); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Keep as-is for now. If you hit the gRPC 4MB limit again, we’ll switch to streaming/chunked upload.
	if err := r.ParseMultipartForm(32 << 20); err != nil { // 32MB
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	filename := header.Filename
	if !strings.HasSuffix(strings.ToLower(filename), ".mp4") {
		http.Error(w, "only .mp4 files are allowed", http.StatusBadRequest)
		return
	}
	videoId := strings.TrimSuffix(filename, filepath.Ext(filename))

	existing, _ := s.metadataService.Read(videoId)
	if existing != nil {
		http.Error(w, "video already exists", http.StatusBadRequest)
		return
	}

	tempDir, err := os.MkdirTemp("", "upload-")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer os.RemoveAll(tempDir)

	inputPath := filepath.Join(tempDir, "input.mp4")
	outputDir := filepath.Join(tempDir, "out")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Save uploaded file
	data, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := os.WriteFile(inputPath, data, 0644); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Run ffmpeg (force outputs into outputDir)
	cmd := exec.Command("ffmpeg",
		"-i", inputPath,
		"-map", "0",
		"-f", "dash",
		"-use_template", "1",
		"-use_timeline", "1",
		"-init_seg_name", "init-stream$RepresentationID$.m4s",
		"-media_seg_name", "chunk-stream$RepresentationID$-$Number%05d$.m4s",
		filepath.Join(outputDir, "manifest.mpd"),
	)
	cmd.Dir = outputDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Store DASH outputs into content service
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(outputDir, entry.Name()))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := s.contentService.Write(videoId, entry.Name(), b); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Save metadata
	if err := s.metadataService.Create(videoId, time.Now()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/videos/"+url.PathEscape(videoId), http.StatusSeeOther)
}

func (s *server) handleVideo(w http.ResponseWriter, r *http.Request) {
	raw := strings.TrimPrefix(r.URL.Path, "/videos/")
	if raw == "" || strings.Contains(raw, "/") {
		http.NotFound(w, r)
		return
	}

	videoId, err := url.PathUnescape(raw)
	if err != nil {
		http.Error(w, "bad video id", http.StatusBadRequest)
		return
	}

	video, err := s.metadataService.Read(videoId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if video == nil {
		http.Error(w, "video not found", http.StatusNotFound)
		return
	}

	view := VideoPageView{
		Id:         videoId,
		EscapedId:  url.PathEscape(videoId),
		UploadedAt: video.UploadedAt.Format("2006-01-02 15:04:05"),
	}

	if err := s.tpls.Video.Execute(w, view); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) handleVideoContent(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/content/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 {
		http.Error(w, "Invalid content path", http.StatusBadRequest)
		return
	}

	rawId := parts[0]
	filename := parts[1]

	videoId, err := url.PathUnescape(rawId)
	if err != nil {
		http.Error(w, "bad video id", http.StatusBadRequest)
		return
	}

	// If you ever allow spaces etc in IDs, uncomment:
	// if v, err := url.PathUnescape(videoId); err == nil { videoId = v }

	b, err := s.contentService.Read(videoId, filename)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Dash.js is happiest if these are correct:
	if strings.HasSuffix(filename, ".mpd") {
    	w.Header().Set("Content-Type", "application/dash+xml")
	} else if strings.HasSuffix(filename, ".m4s") {
		w.Header().Set("Content-Type", "video/iso.segment")
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	// KEY FIX: ServeContent supports Range requests automatically.
	reader := bytes.NewReader(b)
	http.ServeContent(w, r, filename, time.Now(), reader)
}

// // Lab 7: Implement a web server

// package web

// import (
// 	"io"
// 	"log"
// 	"net"
// 	"net/http"
// 	"net/url"
// 	"os"
// 	"os/exec"
// 	"path/filepath"
// 	"strings"
// 	"time"

// 	"html/template"
// )

// type server struct {
// 	// Addr string
// 	// Port int

// 	metadataService VideoMetadataService
// 	contentService  VideoContentService

// 	mux *http.ServeMux

// 	tpls *templates
// }

// func NewServer(
// 	metadataService VideoMetadataService,
// 	contentService VideoContentService,
// ) *server {
// 	return &server{
// 		metadataService: metadataService,
// 		contentService:  contentService,
// 	}
// }

// // func (s *server) Start(lis net.Listener) error {
// // 	s.mux = http.NewServeMux()
// // 	s.mux.HandleFunc("/upload", s.handleUpload)
// // 	s.mux.HandleFunc("/videos/", s.handleVideo)
// // 	s.mux.HandleFunc("/content/", s.handleVideoContent)
// // 	s.mux.HandleFunc("/", s.handleIndex)

// // 	return http.Serve(lis, s.mux)
// // }

// func (s *server) Start(lis net.Listener) error {
//     s.mux = http.NewServeMux()

//     tpls, err := loadTemplates()
//     if err != nil {
//         return err
//     }
//     s.tpls = tpls

//     // Serve /static/* from internal/web/static/
//     staticDir := http.Dir(filepath.Join("internal", "web", "static"))
//     s.mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(staticDir)))

//     s.mux.HandleFunc("/upload", s.handleUpload)
//     s.mux.HandleFunc("/videos/", s.handleVideo)
//     s.mux.HandleFunc("/content/", s.handleVideoContent)
//     s.mux.HandleFunc("/", s.handleIndex)

//     return http.Serve(lis, s.mux)
// }

// //	func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
// //		panic("Lab 7: not implemented")
// //	}
// //
// // potentially check this
// func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
// 	videos, err := s.metadataService.List()
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}

// 	//	 ask in OH about this

// 	type EscapedVideo struct {
// 		Id         string
// 		EscapedId  string
// 		UploadTime string
// 	}

// 	var escaped []EscapedVideo
// 	for _, video := range videos {
// 		escaped = append(escaped, EscapedVideo{
// 			Id:         video.Id,
// 			EscapedId:  url.PathEscape(video.Id),
// 			UploadTime: video.UploadedAt.Format("2006-01-02 15:04:05"),
// 		})
// 	}

// 	tmpl := template.Must(template.New("index").Parse(indexHTML))
// 	err = tmpl.Execute(w, escaped)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 	}
// }

// //	func (s *server) handleUpload(w http.ResponseWriter, r *http.Request) {
// //		panic("Lab 7: not implemented")
// //	}
// func (s *server) handleUpload(w http.ResponseWriter, r *http.Request) {

// 	if r.Method != http.MethodPost {
// 		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
// 		return
// 	}

// 	err := r.ParseMultipartForm(32 << 20)
// 	//	32 mb
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}

// 	file, header, err := r.FormFile("file")
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	defer file.Close()

// 	filename := header.Filename
// 	if !strings.HasSuffix(filename, ".mp4") {
// 		http.Error(w, "only .mp4 files are allowed", http.StatusBadRequest)
// 		return
// 	}
// 	videoId := strings.TrimSuffix(filename, ".mp4")

// 	isExisting, _ := s.metadataService.Read(videoId)
// 	if isExisting != nil {
// 		http.Error(w, "video already exists", http.StatusBadRequest)
// 		return
// 	}

// 	tempDir, err := os.MkdirTemp("", "upload-")
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	defer os.RemoveAll(tempDir)

// 	inputPath := filepath.Join(tempDir, "input.mp4")
// 	outputDir := filepath.Join(tempDir, "out")
// 	err = os.MkdirAll(outputDir, 0755)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}

// 	//save input file
// 	data, err := io.ReadAll(file)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	err = os.WriteFile(inputPath, data, 0644)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}

// 	// //	run ffmpeg
// 	// cmd := exec.Command("ffmpeg",
// 	// 	"-i", inputPath,
// 	// 	"-map", "0",
// 	// 	"-f", "dash",
// 	// 	filepath.Join(outputDir, "manifest.mpd"),
// 	// )
// 	// cmd.Stdout = os.Stdout
// 	// cmd.Stderr = os.Stderr
// 	// if err := cmd.Run(); err != nil {
// 	// 	http.Error(w, err.Error(), http.StatusInternalServerError)
// 	// 	return
// 	// }
// // run ffmpeg (force all DASH outputs into outputDir)
// 	cmd := exec.Command("ffmpeg",
// 		"-i", inputPath,
// 		"-map", "0",
// 		"-f", "dash",
// 		"-use_template", "1",
// 		"-use_timeline", "1",
// 		"-init_seg_name", "init-stream$RepresentationID$.m4s",
// 		"-media_seg_name", "chunk-stream$RepresentationID$-$Number%05d$.m4s",
// 		filepath.Join(outputDir, "manifest.mpd"),
// 	)

// 	// critical: make relative segment outputs land in outputDir (not repo root)
// 	cmd.Dir = outputDir

// 	cmd.Stdout = os.Stdout
// 	cmd.Stderr = os.Stderr
// 	if err := cmd.Run(); err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	// store output files
// 	entries, err := os.ReadDir(outputDir)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	for _, entry := range entries {
// 		if entry.IsDir() {
// 			continue
// 		}
// 		content, err := os.ReadFile(filepath.Join(outputDir, entry.Name()))
// 		if err != nil {
// 			http.Error(w, err.Error(), http.StatusInternalServerError)
// 			return
// 		}
// 		err = s.contentService.Write(videoId, entry.Name(), content)
// 		if err != nil {
// 			http.Error(w, err.Error(), http.StatusInternalServerError)
// 			return
// 		}
// 	}
// 	// save the metadata
// 	err = s.metadataService.Create(videoId, time.Now())
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}

// 	http.Redirect(w, r, "/videos/"+videoId, http.StatusSeeOther)
// }

// func (s *server) handleVideo(w http.ResponseWriter, r *http.Request) {
// 	videoId := r.URL.Path[len("/videos/"):]
// 	log.Println("Video ID:", videoId)

// 	//panic("Lab 7: not implemented")

// 	//check if vid exists
// 	video, err := s.metadataService.Read(videoId)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	if video == nil {
// 		http.Error(w, "video not found (404)", http.StatusNotFound)
// 		return
// 	}

// 	// prep the data
// 	data := struct {
// 		Id         string
// 		UploadedAt string
// 	}{
// 		Id:         videoId,
// 		UploadedAt: video.UploadedAt.Format("2006-01-02 15:04:05"),
// 	}

// 	tmpl := template.Must(template.New("video").Parse(videoHTML))
// 	err = tmpl.Execute(w, data)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 	}
// }

// func (s *server) handleVideoContent(w http.ResponseWriter, r *http.Request) {
// 	// parse /content/<videoId>/<filename>
// 	videoId := r.URL.Path[len("/content/"):]
// 	parts := strings.Split(videoId, "/")
// 	if len(parts) != 2 {
// 		http.Error(w, "Invalid content path", http.StatusBadRequest)
// 		return
// 	}
// 	videoId = parts[0]
// 	filename := parts[1]
// 	log.Println("Video ID:", videoId, "Filename:", filename)

// 	// my added
// 	data, err := s.contentService.Read(videoId, filename)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}

// 	//set the right content type for DASH files
// 	if strings.HasSuffix(filename, ".mpd") {
// 		w.Header().Set("Content-Type", "application/dash+xml")
// 	} else if strings.HasSuffix(filename, ".m4s") {
// 		w.Header().Set("Content-Type", "video/iso.segment")
// 	} else {
// 		w.Header().Set("Content-Type", "application/octet-stream")
// 	}

// 	w.WriteHeader(http.StatusOK)
// 	w.Write(data)
// }
