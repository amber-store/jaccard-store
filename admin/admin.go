// Package admin is the server's admin page: a read-only JSON API over the
// database and a small single-page application that shows it, both served
// from one handler.
//
// The handler has no authentication. The server binds it to loopback unless
// told otherwise, and whoever exposes it further is expected to put
// something in front of it.
//
// Everything the page shows was computed by the server from packs it
// verified (package verify); nothing is a client's word.
package admin

import (
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"strconv"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/db"
)

//go:embed web
var web embed.FS

const (
	defaultLimit = 100
	maxLimit     = 1000
)

// Handler returns the admin page over d: the API under /api/ and the page
// everywhere else.
func Handler(d *db.DB) http.Handler {
	a := &api{db: d}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/stats", a.stats)
	mux.HandleFunc("GET /api/refs", a.refs)
	mux.HandleFunc("GET /api/packs", a.packs)
	mux.HandleFunc("GET /api/packs/{root}", a.pack)
	mux.HandleFunc("GET /api/uploads", a.uploads)
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		fail(w, http.StatusNotFound, "no such resource")
	})
	page, err := fs.Sub(web, "web")
	if err != nil {
		panic(err) // the directory is embedded above
	}
	files := http.FileServerFS(page)
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "the admin page is read-only", http.StatusMethodNotAllowed)
			return
		}
		// The page loads nothing from elsewhere and runs no inline script.
		w.Header().Set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		files.ServeHTTP(w, r)
	}))
	return mux
}

type api struct {
	db *db.DB
}

type statsJSON struct {
	Refs         int64   `json:"refs"`
	BasePacks    int64   `json:"base_packs"`
	PatchPacks   int64   `json:"patch_packs"`
	Uploads      int64   `json:"uploads"`
	Deletions    int64   `json:"deletions"`
	S3Bytes      int64   `json:"s3_bytes"`
	DataBytes    int64   `json:"data_bytes"`
	StoredBytes  int64   `json:"stored_bytes"`
	LogicalBytes int64   `json:"logical_bytes"`
	DedupRate    float64 `json:"dedup_rate"`
	Compression  float64 `json:"compression"`
}

type refJSON struct {
	Name                     string  `json:"name"`
	Root                     string  `json:"root"`
	UpdatedBy                string  `json:"updated_by"`
	UpdatedAt                string  `json:"updated_at"`
	Kind                     string  `json:"kind"`
	RefObjects               int64   `json:"ref_objects"`
	RefBytes                 int64   `json:"ref_bytes"`
	SharedObjects            int64   `json:"shared_objects"`
	SharedBytes              int64   `json:"shared_bytes"`
	Dedup                    float64 `json:"dedup"`
	ParentRoot               *string `json:"parent_root"`
	ParentUnreachableObjects int64   `json:"parent_unreachable_objects"`
	ParentUnreachableBytes   int64   `json:"parent_unreachable_bytes"`
}

type packJSON struct {
	ID            int64   `json:"id"`
	Root          string  `json:"root"`
	Kind          string  `json:"kind"`
	ParentRoot    *string `json:"parent_root"`
	Objects       int64   `json:"objects"`
	Bytes         int64   `json:"bytes"`
	DataSize      int64   `json:"data_size"`
	IndexSize     int64   `json:"index_size"`
	LinksSize     int64   `json:"links_size"`
	SharedObjects int64   `json:"shared_objects"`
	SharedBytes   int64   `json:"shared_bytes"`
	Uploader      string  `json:"uploader"`
	UploadedAt    string  `json:"uploaded_at"`
	Refs          int64   `json:"refs"`
	Children      int64   `json:"children"`
}

type packDetailJSON struct {
	Pack     packJSON  `json:"pack"`
	Parent   *packJSON `json:"parent"`
	Refs     []string  `json:"refs"`
	Children []string  `json:"children"`
}

type uploadJSON struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	Root       string  `json:"root"`
	ParentRoot *string `json:"parent_root"`
	Uploader   string  `json:"uploader"`
	DataSize   int64   `json:"data_size"`
	Objects    int64   `json:"objects"`
	Multipart  bool    `json:"multipart"`
	State      string  `json:"state"`
	IssuedAt   string  `json:"issued_at"`
	Deadline   string  `json:"deadline"`
}

func (a *api) stats(w http.ResponseWriter, r *http.Request) {
	st, err := a.db.Stats(r.Context())
	if err != nil {
		failed(w, err)
		return
	}
	out := statsJSON{
		Refs:         st.Refs,
		BasePacks:    st.BasePacks,
		PatchPacks:   st.PatchPacks,
		Uploads:      st.Uploads,
		Deletions:    st.Deletions,
		S3Bytes:      st.S3Bytes,
		DataBytes:    st.DataBytes,
		StoredBytes:  st.StoredBytes,
		LogicalBytes: st.LogicalBytes,
	}
	if st.LogicalBytes > 0 {
		out.DedupRate = 1 - float64(st.StoredBytes)/float64(st.LogicalBytes)
	}
	if st.StoredBytes > 0 {
		out.Compression = float64(st.DataBytes) / float64(st.StoredBytes)
	}
	reply(w, out)
}

func (a *api) refs(w http.ResponseWriter, r *http.Request) {
	limit, ok := limitOf(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	infos, err := a.db.ListRefInfo(r.Context(), q.Get("prefix"), q.Get("after"), limit+1)
	if err != nil {
		failed(w, err)
		return
	}
	more := len(infos) > limit
	if more {
		infos = infos[:limit]
	}
	refs := make([]refJSON, 0, len(infos))
	for _, in := range infos {
		p := in.Pack
		out := refJSON{
			Name:          in.Ref.Name,
			Root:          in.Ref.Root.String(),
			UpdatedBy:     in.Ref.UpdatedBy,
			UpdatedAt:     stamp(in.Ref.UpdatedAt),
			Kind:          kind(p),
			RefObjects:    p.Objects + p.SharedObjects,
			RefBytes:      p.Bytes + p.SharedBytes,
			SharedObjects: p.SharedObjects,
			SharedBytes:   p.SharedBytes,
		}
		if out.RefBytes > 0 {
			out.Dedup = float64(p.SharedBytes) / float64(out.RefBytes)
		}
		if in.Parent != nil {
			root := in.Parent.Root.String()
			out.ParentRoot = &root
			out.ParentUnreachableObjects = in.Parent.Objects - p.SharedObjects
			out.ParentUnreachableBytes = in.Parent.Bytes - p.SharedBytes
		}
		refs = append(refs, out)
	}
	reply(w, map[string]any{"refs": refs, "more": more})
}

func (a *api) packs(w http.ResponseWriter, r *http.Request) {
	limit, ok := limitOf(w, r)
	if !ok {
		return
	}
	var after int64
	if s := r.URL.Query().Get("after"); s != "" {
		var err error
		if after, err = strconv.ParseInt(s, 10, 64); err != nil || after < 0 {
			fail(w, http.StatusBadRequest, "after: want the id of a pack")
			return
		}
	}
	infos, err := a.db.ListPacks(r.Context(), after, limit+1)
	if err != nil {
		failed(w, err)
		return
	}
	more := len(infos) > limit
	if more {
		infos = infos[:limit]
	}
	packs := make([]packJSON, 0, len(infos))
	for _, in := range infos {
		packs = append(packs, packOf(in))
	}
	reply(w, map[string]any{"packs": packs, "more": more})
}

func (a *api) pack(w http.ResponseWriter, r *http.Request) {
	raw, err := hex.DecodeString(r.PathValue("root"))
	if err != nil {
		fail(w, http.StatusBadRequest, "root: want 64 hex characters")
		return
	}
	root, err := key.Parse(raw)
	if err != nil {
		fail(w, http.StatusBadRequest, "root: not a key")
		return
	}
	d, err := a.db.PackDetail(r.Context(), root)
	if errors.Is(err, db.ErrNotFound) {
		fail(w, http.StatusNotFound, "no pack for that root")
		return
	}
	if err != nil {
		failed(w, err)
		return
	}
	out := packDetailJSON{
		Pack:     packOf(d.PackInfo),
		Refs:     d.RefNames,
		Children: make([]string, 0, len(d.Children)),
	}
	if out.Refs == nil {
		out.Refs = []string{}
	}
	for _, c := range d.Children {
		out.Children = append(out.Children, c.String())
	}
	if d.Parent != nil {
		parent := packOf(db.PackInfo{Pack: *d.Parent})
		out.Parent = &parent
	}
	reply(w, out)
}

func (a *api) uploads(w http.ResponseWriter, r *http.Request) {
	ups, err := a.db.ListUploads(r.Context())
	if err != nil {
		failed(w, err)
		return
	}
	uploads := make([]uploadJSON, 0, len(ups))
	for _, u := range ups {
		out := uploadJSON{
			ID:        u.ID,
			Name:      u.Name,
			Root:      u.Root.String(),
			Uploader:  u.Uploader,
			DataSize:  u.DataSize,
			Objects:   u.Objects,
			Multipart: u.MultipartID != "",
			State:     u.State,
			IssuedAt:  stamp(u.IssuedAt),
			Deadline:  stamp(u.Deadline),
		}
		if u.ParentID != 0 {
			parent, err := a.db.PackByID(r.Context(), u.ParentID)
			if err != nil {
				failed(w, err)
				return
			}
			root := parent.Root.String()
			out.ParentRoot = &root
		}
		uploads = append(uploads, out)
	}
	reply(w, map[string]any{"uploads": uploads})
}

func packOf(in db.PackInfo) packJSON {
	p := in.Pack
	out := packJSON{
		ID:            p.ID,
		Root:          p.Root.String(),
		Kind:          kind(p),
		Objects:       p.Objects,
		Bytes:         p.Bytes,
		DataSize:      p.DataSize,
		IndexSize:     p.IndexSize,
		LinksSize:     p.LinksSize,
		SharedObjects: p.SharedObjects,
		SharedBytes:   p.SharedBytes,
		Uploader:      p.Uploader,
		UploadedAt:    stamp(p.UploadedAt),
		Refs:          in.Refs,
		Children:      in.Children,
	}
	if in.ParentRoot != nil {
		root := in.ParentRoot.String()
		out.ParentRoot = &root
	}
	return out
}

func kind(p db.Pack) string {
	if p.IsBase() {
		return "base"
	}
	return "patch"
}

func stamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// limitOf reads the limit parameter; it answers the request itself, and
// returns false, when the parameter is not a count.
func limitOf(w http.ResponseWriter, r *http.Request) (int, bool) {
	s := r.URL.Query().Get("limit")
	if s == "" {
		return defaultLimit, true
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		fail(w, http.StatusBadRequest, "limit: want a count above zero")
		return 0, false
	}
	return min(n, maxLimit), true
}

func reply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

// failed answers for an error of the database, which is not shown.
func failed(w http.ResponseWriter, _ error) {
	fail(w, http.StatusInternalServerError, "the database could not be read")
}
