package admin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/admin"
	"github.com/amber-store/jaccard-store/db"
	"github.com/amber-store/jaccard-store/sketch"
)

var (
	uploaded = time.Unix(1_800_000_000, 0).UTC()
	later    = uploaded.Add(time.Hour)
)

func rootOf(t *testing.T, content string) key.Key {
	t.Helper()
	k, err := key.New(key.Blob, uint64(len(content)), []byte(content))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// store is a database holding a base pack under "base", a patch pack of it
// under "patch" and "alias", and one open upload against the base pack.
type store struct {
	db                *db.DB
	base, patch, open key.Key
	srv               *httptest.Server
}

func newStore(t *testing.T) *store {
	t.Helper()
	d, err := db.Open(filepath.Join(t.TempDir(), "store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	s := &store{db: d, base: rootOf(t, "base"), patch: rootOf(t, "patch"), open: rootOf(t, "open")}
	ctx := context.Background()

	commit := func(id, name, uploader string, root key.Key, parent *key.Key, dataSize int64, v db.Verified) {
		t.Helper()
		u := db.Upload{
			ID: id, Name: name, Root: root, Uploader: uploader,
			DataKey: "packs/" + id + ".data", IndexKey: "packs/" + id + ".idx", LinksKey: "packs/" + id + ".links",
			DataSize: dataSize, Objects: v.Objects, State: db.StatePending, IssuedAt: uploaded, Deadline: later,
		}
		if _, err := d.CreateUpload(ctx, u, parent); err != nil {
			t.Fatal(err)
		}
		if _, err := d.BeginVerify(ctx, id, uploader); err != nil {
			t.Fatal(err)
		}
		if _, err := d.CommitUpload(ctx, id, v, uploaded, later); err != nil {
			t.Fatal(err)
		}
	}
	commit("u-base", "base", "alice", s.base, nil, 400, db.Verified{
		IndexSize: 456, LinksSize: 100, Objects: 10, Bytes: 1000, Sketch: sketch.Sketch{s.base},
	})
	commit("u-patch", "patch", "bob", s.patch, &s.base, 120, db.Verified{
		IndexSize: 148, Objects: 3, Bytes: 300, SharedObjects: 6, SharedBytes: 700,
	})
	if ok, err := d.PointRef(ctx, "alias", s.patch, "carol", later, later); err != nil || !ok {
		t.Fatalf("PointRef = %v, %v", ok, err)
	}
	if _, err := d.CreateUpload(ctx, db.Upload{
		ID: "u-open", Name: "pending/ref", Root: s.open, Uploader: "dave",
		DataKey: "packs/u-open.data", IndexKey: "packs/u-open.idx", LinksKey: "packs/u-open.links",
		MultipartID: "mp-1", DataSize: 5000, Objects: 7, State: db.StatePending, IssuedAt: uploaded, Deadline: later,
	}, &s.base); err != nil {
		t.Fatal(err)
	}
	s.srv = httptest.NewServer(admin.Handler(d))
	t.Cleanup(s.srv.Close)
	return s
}

// get fetches path and decodes the JSON of the answer into out.
func (s *store) get(t *testing.T, path string, status int, out any) http.Header {
	t.Helper()
	res, err := http.Get(s.srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != status {
		t.Fatalf("GET %s answered %s, want %d: %s", path, res.Status, status, body)
	}
	if out != nil {
		if err := json.Unmarshal(body, out); err != nil {
			t.Fatalf("GET %s: %v in %s", path, err, body)
		}
	}
	return res.Header
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestStats(t *testing.T) {
	s := newStore(t)
	var got map[string]float64
	header := s.get(t, "/api/stats", http.StatusOK, &got)
	want := map[string]float64{
		// One deletion: the links key of the patch pack's upload, which a
		// patch pack never fills and the database clears to be sure.
		"refs": 3, "base_packs": 1, "patch_packs": 1, "uploads": 1, "deletions": 1,
		"s3_bytes":      400 + 456 + 100 + 120 + 148,
		"data_bytes":    400 + 120,
		"stored_bytes":  1000 + 300,
		"logical_bytes": 1000 + 1000 + 1000, // base; patch and alias, each the patch pack plus what its parent holds
		"dedup_rate":    1 - 1300.0/3000.0,
		"compression":   520.0 / 1300.0,
	}
	for name, w := range want {
		if g, ok := got[name]; !ok || !near(g, w) {
			t.Errorf("%s = %v, want %v", name, g, w)
		}
	}
	if len(got) != len(want) {
		t.Errorf("stats has %d fields, want %d: %v", len(got), len(want), got)
	}
	if header.Get("Content-Type") != "application/json" || header.Get("Cache-Control") != "no-store" {
		t.Errorf("headers %v", header)
	}
}

func TestStatsOfAnEmptyStore(t *testing.T) {
	d, err := db.Open(filepath.Join(t.TempDir(), "store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	srv := httptest.NewServer(admin.Handler(d))
	defer srv.Close()
	res, err := http.Get(srv.URL + "/api/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var got map[string]float64
	if err := json.NewDecoder(res.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	for name, v := range got {
		if v != 0 {
			t.Errorf("%s = %v in an empty store", name, v)
		}
	}
}

type refsPage struct {
	Refs []struct {
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
	} `json:"refs"`
	More bool `json:"more"`
}

func TestRefs(t *testing.T) {
	s := newStore(t)
	var page refsPage
	s.get(t, "/api/refs", http.StatusOK, &page)
	if len(page.Refs) != 3 || page.More {
		t.Fatalf("got %d refs, more %v", len(page.Refs), page.More)
	}
	alias, base, patch := page.Refs[0], page.Refs[1], page.Refs[2]
	if alias.Name != "alias" || base.Name != "base" || patch.Name != "patch" {
		t.Fatalf("refs in order %s, %s, %s", alias.Name, base.Name, patch.Name)
	}

	if base.Kind != "base" || base.Root != s.base.String() || base.ParentRoot != nil ||
		base.RefObjects != 10 || base.RefBytes != 1000 || base.SharedBytes != 0 || base.Dedup != 0 ||
		base.ParentUnreachableObjects != 0 || base.ParentUnreachableBytes != 0 {
		t.Errorf("base ref: %+v", base)
	}
	if base.UpdatedBy != "alice" || base.UpdatedAt != "2027-01-15T08:00:00Z" {
		t.Errorf("base ref updated by %q at %q", base.UpdatedBy, base.UpdatedAt)
	}

	if patch.Kind != "patch" || patch.Root != s.patch.String() || patch.ParentRoot == nil || *patch.ParentRoot != s.base.String() {
		t.Fatalf("patch ref: %+v", patch)
	}
	if patch.RefObjects != 9 || patch.RefBytes != 1000 || patch.SharedObjects != 6 || patch.SharedBytes != 700 || !near(patch.Dedup, 0.7) {
		t.Errorf("patch ref measures: %+v", patch)
	}
	// The base pack holds 10 objects and 1000 bytes; the ref reaches 6 and 700 of them.
	if patch.ParentUnreachableObjects != 4 || patch.ParentUnreachableBytes != 300 {
		t.Errorf("patch ref leaves %d objects, %d bytes of its parent out of reach; want 4 and 300",
			patch.ParentUnreachableObjects, patch.ParentUnreachableBytes)
	}
	if alias.Root != s.patch.String() || alias.UpdatedBy != "carol" || alias.SharedBytes != 700 {
		t.Errorf("alias ref: %+v", alias)
	}
}

func TestRefsPagingAndPrefix(t *testing.T) {
	s := newStore(t)
	var page refsPage
	s.get(t, "/api/refs?limit=2", http.StatusOK, &page)
	if len(page.Refs) != 2 || !page.More || page.Refs[1].Name != "base" {
		t.Fatalf("first page: %d refs, more %v", len(page.Refs), page.More)
	}
	s.get(t, "/api/refs?limit=2&after=base", http.StatusOK, &page)
	if len(page.Refs) != 1 || page.More || page.Refs[0].Name != "patch" {
		t.Fatalf("second page: %+v", page)
	}
	s.get(t, "/api/refs?prefix=a", http.StatusOK, &page)
	if len(page.Refs) != 1 || page.Refs[0].Name != "alias" {
		t.Fatalf("prefix a: %+v", page)
	}
	s.get(t, "/api/refs?prefix=%25", http.StatusOK, &page)
	if len(page.Refs) != 0 || page.Refs == nil {
		t.Fatalf("prefix %%: got %+v, want an empty list", page)
	}
	for _, q := range []string{"limit=0", "limit=-3", "limit=many"} {
		s.get(t, "/api/refs?"+q, http.StatusBadRequest, nil)
	}
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

type packsPage struct {
	Packs []packJSON `json:"packs"`
	More  bool       `json:"more"`
}

func TestPacks(t *testing.T) {
	s := newStore(t)
	var page packsPage
	s.get(t, "/api/packs", http.StatusOK, &page)
	if len(page.Packs) != 2 || page.More {
		t.Fatalf("got %d packs, more %v", len(page.Packs), page.More)
	}
	base, patch := page.Packs[0], page.Packs[1]
	want := packJSON{
		ID: base.ID, Root: s.base.String(), Kind: "base", Objects: 10, Bytes: 1000, DataSize: 400, IndexSize: 456,
		LinksSize: 100, Uploader: "alice", UploadedAt: "2027-01-15T08:00:00Z", Refs: 1, Children: 1,
	}
	if base != want {
		t.Errorf("base pack:\n got %+v\nwant %+v", base, want)
	}
	if patch.Kind != "patch" || patch.ParentRoot == nil || *patch.ParentRoot != s.base.String() ||
		patch.Uploader != "bob" || patch.Refs != 2 || patch.Children != 0 || patch.SharedObjects != 6 || patch.LinksSize != 0 {
		t.Errorf("patch pack: %+v", patch)
	}

	s.get(t, "/api/packs?limit=1", http.StatusOK, &page)
	if len(page.Packs) != 1 || !page.More || page.Packs[0].Root != s.base.String() {
		t.Fatalf("first page: %+v", page)
	}
	s.get(t, fmt.Sprintf("/api/packs?limit=1&after=%d", page.Packs[0].ID), http.StatusOK, &page)
	if len(page.Packs) != 1 || page.More || page.Packs[0].Root != s.patch.String() {
		t.Fatalf("second page: %+v", page)
	}
	s.get(t, "/api/packs?after=x", http.StatusBadRequest, nil)
}

type detail struct {
	Pack     packJSON  `json:"pack"`
	Parent   *packJSON `json:"parent"`
	Refs     []string  `json:"refs"`
	Children []string  `json:"children"`
}

func TestPackDetail(t *testing.T) {
	s := newStore(t)
	var d detail
	s.get(t, "/api/packs/"+s.base.String(), http.StatusOK, &d)
	if d.Pack.Root != s.base.String() || d.Pack.Uploader != "alice" || d.Pack.UploadedAt != "2027-01-15T08:00:00Z" || d.Parent != nil {
		t.Errorf("base detail: %+v", d)
	}
	if len(d.Refs) != 1 || d.Refs[0] != "base" || len(d.Children) != 1 || d.Children[0] != s.patch.String() {
		t.Errorf("base pack has refs %v and children %v", d.Refs, d.Children)
	}

	s.get(t, "/api/packs/"+s.patch.String(), http.StatusOK, &d)
	if d.Parent == nil || d.Parent.Root != s.base.String() || d.Parent.Objects != 10 || d.Parent.Bytes != 1000 {
		t.Fatalf("patch detail's parent: %+v", d.Parent)
	}
	if strings.Join(d.Refs, " ") != "alias patch" || d.Children == nil || len(d.Children) != 0 {
		t.Errorf("patch pack has refs %v and children %v", d.Refs, d.Children)
	}

	s.get(t, "/api/packs/"+s.open.String(), http.StatusNotFound, nil)
	s.get(t, "/api/packs/not-hex", http.StatusBadRequest, nil)
	s.get(t, "/api/packs/abcd", http.StatusBadRequest, nil)
}

func TestUploads(t *testing.T) {
	s := newStore(t)
	var got struct {
		Uploads []struct {
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
		} `json:"uploads"`
	}
	s.get(t, "/api/uploads", http.StatusOK, &got)
	if len(got.Uploads) != 1 {
		t.Fatalf("got %d uploads", len(got.Uploads))
	}
	u := got.Uploads[0]
	if u.ID != "u-open" || u.Name != "pending/ref" || u.Root != s.open.String() || u.Uploader != "dave" ||
		u.ParentRoot == nil || *u.ParentRoot != s.base.String() || u.DataSize != 5000 || u.Objects != 7 ||
		!u.Multipart || u.State != "pending" || u.IssuedAt != "2027-01-15T08:00:00Z" || u.Deadline != "2027-01-15T09:00:00Z" {
		t.Errorf("upload: %+v", u)
	}
}

func TestThePageIsServed(t *testing.T) {
	s := newStore(t)
	for path, want := range map[string]string{
		"/":          "<title>jaccard-store</title>",
		"/app.js":    "api/stats",
		"/style.css": "--accent",
	} {
		res, err := http.Get(s.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !strings.Contains(string(body), want) {
			t.Errorf("GET %s answered %s without %q", path, res.Status, want)
		}
		if csp := res.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'self'") {
			t.Errorf("GET %s: Content-Security-Policy %q", path, csp)
		}
	}
	// The page keeps its promise to the policy: nothing inline, nothing from elsewhere.
	res, err := http.Get(s.srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(res.Body)
	res.Body.Close()
	for _, banned := range []string{"<style", "style=", "onclick=", "http://", "https://", "<script>"} {
		if strings.Contains(string(page), banned) {
			t.Errorf("the page contains %q", banned)
		}
	}
}

func TestTheAPIIsReadOnlyAndBounded(t *testing.T) {
	s := newStore(t)
	s.get(t, "/api/nothing", http.StatusNotFound, nil)
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req, _ := http.NewRequest(method, s.srv.URL+"/api/stats", nil)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode < 400 {
			t.Errorf("%s /api/stats answered %s", method, res.Status)
		}
	}
	var stats map[string]float64
	s.get(t, "/api/stats", http.StatusOK, &stats)
	if stats["refs"] != 3 {
		t.Errorf("the store changed: %v refs", stats["refs"])
	}
}
