// // Lab 7, 8, 9: Use these templates to render the web pages

// package web

// const indexHTML = `
// <!DOCTYPE html>
// <html>
//   <head>
//     <meta charset="UTF-8" />
//     <title>TritonTube</title>
//   </head>
//   <body>
//     <h1>Welcome to TritonTube</h1>
//     <h2>Upload an MP4 Video</h2>
//     <form action="/upload" method="post" enctype="multipart/form-data">
//       <input type="file" name="file" accept="video/mp4" required />
//       <input type="submit" value="Upload" />
//     </form>
//     <h2>Watchlist</h2>
//     <ul>
//       {{range .}}
//       <li>
//         <a href="/videos/{{.EscapedId}}">{{.Id}} ({{.UploadTime}})</a>
//       </li>
//       {{else}}
//       <li>No videos uploaded yet.</li>
//       {{end}}
//     </ul>
//   </body>
// </html>
// `

// const videoHTML = `
// <!DOCTYPE html>
// <html>
//   <head>
//     <meta charset="UTF-8" />
//     <title>{{.Id}} - TritonTube</title>
//     <script src="https://cdn.dashjs.org/latest/dash.all.min.js"></script>
//   </head>
//   <body>
//     <h1>{{.Id}}</h1>
// 	  <p>Uploaded at: {{.UploadedAt}}</p>

//     <video id="dashPlayer" controls style="width: 640px; height: 360px"></video>
//     <script>
//       var url = "/content/{{.Id}}/manifest.mpd";
//       var player = dashjs.MediaPlayer().create();
//       player.initialize(document.querySelector("#dashPlayer"), url, false);
//     </script>

//     <p><a href="/">Back to Home</a></p>
//   </body>
// </html>
// `
package web

import (
	"html/template"
	"path/filepath"
)

type templates struct {
	Index *template.Template
	Video *template.Template
}

func loadTemplates() (*templates, error) {
	// Parse each file separately to keep things simple
	indexT, err := template.ParseFiles(filepath.Join("internal", "web", "templates", "index.html"))
	if err != nil {
		return nil, err
	}
	videoT, err := template.ParseFiles(filepath.Join("internal", "web", "templates", "video.html"))
	if err != nil {
		return nil, err
	}
	return &templates{Index: indexT, Video: videoT}, nil
}