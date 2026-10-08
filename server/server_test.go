package server_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/bucket"
	"github.com/amber-store/jaccard-store/bucket/buckettest"
	"github.com/amber-store/jaccard-store/db"
	"github.com/amber-store/jaccard-store/keyset"
	"github.com/amber-store/jaccard-store/packfile"
	"github.com/amber-store/jaccard-store/server"
	"github.com/amber-store/jaccard-store/sketch"
	"github.com/amber-store/jaccard-store/wire"
)

const (
	alice = "endpoint-alice"
	bob   = "endpoint-bob"
)

// clock is a clock a test moves by hand.
type clock struct{ t time.Time }

func (c *clock) Now() time.Time          { return c.t }
func (c *clock) Advance(d time.Duration) { c.t = c.t.Add(d) }

// flaky is a bucket whose reads or deletes fail on demand.
type flaky struct {
	*bucket.Bucket
	failGet    bool
	failDelete bool
}

func (f *flaky) Get(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	if f.failGet {
		return nil, errors.New("the bucket is away")
	}
	return f.Bucket.Get(ctx, objectKey)
}

func (f *flaky) Delete(ctx context.Context, objectKey string) error {
	if f.failDelete {
		return errors.New("the bucket is away")
	}
	return f.Bucket.Delete(ctx, objectKey)
}

type harness struct {
	t       *testing.T
	srv     *server.Server
	db      *db.DB
	bucket  *flaky
	clock   *clock
	scratch string
	dbPath  string
}

func newHarness(t *testing.T, mutate func(*server.Config)) *harness {
	t.Helper()
	dir := t.TempDir()
	h := &harness{
		t:       t,
		bucket:  &flaky{Bucket: buckettest.New(t)},
		clock:   &clock{t: time.Unix(1_800_000_000, 0)},
		scratch: filepath.Join(dir, "scratch"),
		dbPath:  filepath.Join(dir, "store.sqlite"),
	}
	var err error
	if h.db, err = db.Open(h.dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.db.Close() })
	h.srv = h.newServer(mutate)
	return h
}

func (h *harness) newServer(mutate func(*server.Config)) *server.Server {
	h.t.Helper()
	cfg := server.Config{DB: h.db, Bucket: h.bucket, Scratch: h.scratch, Now: h.clock.Now, Log: slog.New(slog.DiscardHandler)}
	if mutate != nil {
		mutate(&cfg)
	}
	srv, err := server.New(cfg)
	if err != nil {
		h.t.Fatal(err)
	}
	return srv
}

func (h *harness) handle(remote string, req wire.Request) wire.Response {
	return h.srv.Handle(context.Background(), remote, req)
}

// ok fails the test if resp is a refusal.
func (h *harness) ok(resp wire.Response) wire.Response {
	h.t.Helper()
	if resp.Error != nil {
		h.t.Fatalf("refused: %v", resp.Error)
	}
	return resp
}

// refused fails the test unless resp is a refusal with code.
func (h *harness) refused(resp wire.Response, code string) *wire.Error {
	h.t.Helper()
	if resp.Error == nil {
		h.t.Fatalf("answered %+v, want a refusal with %s", resp, code)
	}
	if resp.Error.Code != code {
		h.t.Fatalf("refused with %v, want %s", resp.Error, code)
	}
	return resp.Error
}

func (h *harness) sweep() {
	h.t.Helper()
	if err := h.srv.Sweep(context.Background()); err != nil {
		h.t.Fatalf("Sweep: %v", err)
	}
}

// present reports whether the bucket holds objectKey.
func (h *harness) present(objectKey string) bool {
	h.t.Helper()
	_, err := h.bucket.Size(context.Background(), objectKey)
	if errors.Is(err, bucket.ErrNotFound) {
		return false
	}
	if err != nil {
		h.t.Fatal(err)
	}
	return true
}

// tree is the objects of one ingested directory.
type tree struct {
	root    key.Key
	objects map[key.Key][]byte
	keys    []key.Key // ascending
}

// newTree ingests a directory holding files and returns its objects.
func newTree(t *testing.T, files map[string]string) tree {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	seq, root, err := ingest.Objects(dir, ingest.Opts{NoIgnore: true})
	if err != nil {
		t.Fatal(err)
	}
	tr := tree{objects: map[key.Key][]byte{}}
	for o, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		tr.objects[o.Key] = bytes.Clone(o.Bytes)
	}
	tr.root = *root
	for k := range tr.objects {
		tr.keys = append(tr.keys, k)
	}
	tr.keys = keyset.Normalize(tr.keys)
	return tr
}

func (tr tree) sketch() [][]byte {
	var out [][]byte
	for _, k := range sketch.Of(tr.keys) {
		out = append(out, k[:])
	}
	return out
}

// pack builds a pack of the tree's objects that skip does not hold.
func (tr tree) pack(t *testing.T, skip *packfile.Index) (*packfile.Index, []byte) {
	t.Helper()
	var data bytes.Buffer
	w, err := packfile.NewWriter(&data)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range tr.keys {
		if skip != nil && skip.Has(k) {
			continue
		}
		if err := w.Add(k, tr.objects[k]); err != nil {
			t.Fatal(err)
		}
	}
	index, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return index, data.Bytes()
}

func httpDo(t *testing.T, method, url string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func httpPut(t *testing.T, url string, body []byte) string {
	t.Helper()
	res := httpDo(t, http.MethodPut, url, body)
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("PUT answered %s: %s", res.Status, b)
	}
	return res.Header.Get("ETag")
}

func httpGet(t *testing.T, url string) []byte {
	t.Helper()
	res := httpDo(t, http.MethodGet, url, nil)
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("GET answered %s (%v): %s", res.Status, err, b)
	}
	return b
}

// upload opens an upload for the pack and puts its index and data.
func (h *harness) upload(remote, name string, root key.Key, parent *key.Key, index *packfile.Index, data []byte) wire.Response {
	h.t.Helper()
	req := wire.Request{Op: wire.OpPushUpload, Name: name, Root: root[:], DataSize: uint64(len(data)), Objects: uint64(index.Len())}
	if parent != nil {
		req.Parent = parent[:]
	}
	resp := h.ok(h.handle(remote, req))
	httpPut(h.t, resp.IndexURL, index.Encode())
	if resp.Parts == nil {
		httpPut(h.t, resp.DataURL, data)
		return resp
	}
	var done strings.Builder
	done.WriteString("<CompleteMultipartUpload>")
	for i, url := range resp.Parts.URLs {
		off := uint64(i) * resp.Parts.PartSize
		end := min(off+resp.Parts.PartSize, uint64(len(data)))
		etag := httpPut(h.t, url, data[off:end])
		fmt.Fprintf(&done, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", i+1, etag)
	}
	done.WriteString("</CompleteMultipartUpload>")
	res := httpDo(h.t, http.MethodPost, resp.Parts.CompleteURL, []byte(done.String()))
	defer res.Body.Close()
	if b, _ := io.ReadAll(res.Body); res.StatusCode != http.StatusOK || bytes.Contains(b, []byte("<Error>")) {
		h.t.Fatalf("completing the multipart upload answered %s: %s", res.Status, b)
	}
	return resp
}

func (h *harness) commit(remote, uploadID string) wire.Response {
	return h.handle(remote, wire.Request{Op: wire.OpPushCommit, UploadID: uploadID})
}

// pushBase pushes the whole tree as a base pack under name.
func (h *harness) pushBase(name string, tr tree) *packfile.Index {
	h.t.Helper()
	index, data := tr.pack(h.t, nil)
	up := h.upload(alice, name, tr.root, nil, index, data)
	h.ok(h.commit(alice, up.UploadID))
	return index
}

func (h *harness) packOf(root key.Key) db.Pack {
	h.t.Helper()
	p, err := h.db.PackByRoot(context.Background(), root)
	if err != nil {
		h.t.Fatalf("pack of %s: %v", root, err)
	}
	return p
}

var (
	filesV1 = map[string]string{
		"README":       "the first version\n",
		"src/main.go":  "package main\n\nfunc main() {}\n",
		"src/util.go":  "package main\n\nfunc util() {}\n",
		"docs/guide":   strings.Repeat("a guide line\n", 200),
		"docs/license": strings.Repeat("a license line\n", 200),
	}
	// filesV2 changes one file of filesV1 and adds another.
	filesV2 = map[string]string{
		"README":       "the second version\n",
		"src/main.go":  "package main\n\nfunc main() {}\n",
		"src/util.go":  "package main\n\nfunc util() {}\n",
		"src/new.go":   "package main\n\nfunc added() {}\n",
		"docs/guide":   strings.Repeat("a guide line\n", 200),
		"docs/license": strings.Repeat("a license line\n", 200),
	}
)

func TestPushStartOffersNearbyBasePacks(t *testing.T) {
	h := newHarness(t, nil)
	v1, v2 := newTree(t, filesV1), newTree(t, filesV2)

	resp := h.ok(h.handle(alice, wire.Request{Op: wire.OpPushStart, Name: "v1", Root: v1.root[:], Sketch: v1.sketch()}))
	if resp.Stored || len(resp.Candidates) != 0 {
		t.Fatalf("an empty store answered %+v", resp)
	}
	index := h.pushBase("v1", v1)

	resp = h.ok(h.handle(alice, wire.Request{Op: wire.OpPushStart, Name: "v2", Root: v2.root[:], Sketch: v2.sketch()}))
	if resp.Stored || len(resp.Candidates) != 1 {
		t.Fatalf("got %+v, want one candidate", resp)
	}
	c := resp.Candidates[0]
	want := 1 - sketch.Jaccard(sketch.Of(v2.keys), sketch.Of(v1.keys))
	if !bytes.Equal(c.Root, v1.root[:]) || c.Distance != want || c.Distance <= 0 || c.Distance >= 1 {
		t.Fatalf("candidate %x at distance %v, want %s at %v", c.Root, c.Distance, v1.root, want)
	}
	if c.Objects != uint64(index.Len()) || c.Bytes != index.DataSize() {
		t.Fatalf("candidate of %d objects and %d bytes, the pack has %d and %d", c.Objects, c.Bytes, index.Len(), index.DataSize())
	}
	if got := httpGet(t, c.IndexURL); !bytes.Equal(got, index.Encode()) {
		t.Fatal("the candidate's index URL does not return the pack's index")
	}
}

func TestPushStartOfAStoredRootPointsTheRef(t *testing.T) {
	h := newHarness(t, nil)
	v1 := newTree(t, filesV1)
	h.pushBase("v1", v1)

	resp := h.ok(h.handle(bob, wire.Request{Op: wire.OpPushStart, Name: "alias", Root: v1.root[:], Sketch: v1.sketch()}))
	if !resp.Stored {
		t.Fatalf("got %+v, want stored", resp)
	}
	pulled := h.ok(h.handle(bob, wire.Request{Op: wire.OpPull, Name: "alias"}))
	if !bytes.Equal(pulled.Root, v1.root[:]) {
		t.Fatalf("alias points at %x, want %s", pulled.Root, v1.root)
	}
}

func TestPushStartRefusals(t *testing.T) {
	h := newHarness(t, nil)
	v1 := newTree(t, filesV1)
	good := wire.Request{Op: wire.OpPushStart, Name: "v1", Root: v1.root[:], Sketch: v1.sketch()}

	unsorted := good
	unsorted.Sketch = [][]byte{v1.keys[1][:], v1.keys[0][:]}
	repeated := good
	repeated.Sketch = [][]byte{v1.keys[0][:], v1.keys[0][:]}
	long := good
	long.Sketch = make([][]byte, 5000)
	for i := range long.Sketch {
		long.Sketch[i] = v1.keys[0][:]
	}
	short := good
	short.Sketch = [][]byte{v1.keys[0][:8]}
	empty := good
	empty.Sketch = nil
	noName := good
	noName.Name = ""
	badRoot := good
	badRoot.Root = v1.root[:5]

	for name, req := range map[string]wire.Request{
		"unsorted sketch": unsorted, "repeated key": repeated, "5000 keys": long, "a short key": short,
		"empty sketch": empty, "no name": noName, "a short root": badRoot,
	} {
		resp := h.handle(alice, req)
		if resp.Error == nil || resp.Error.Code != wire.CodeBadRequest {
			t.Errorf("%s: answered %+v, want bad_request", name, resp)
		}
	}
	if resp := h.handle(alice, wire.Request{Op: "frobnicate"}); resp.Error == nil || resp.Error.Code != wire.CodeBadRequest {
		t.Errorf("an unknown operation answered %+v", resp)
	}
}

func TestPushUploadLaysOutThePutOrTheParts(t *testing.T) {
	h := newHarness(t, func(c *server.Config) { c.PartSize = 1000 })
	v1 := newTree(t, filesV1)

	one := h.ok(h.handle(alice, wire.Request{Op: wire.OpPushUpload, Name: "small", Root: v1.root[:], DataSize: 1000, Objects: 3}))
	if one.DataURL == "" || one.Parts != nil || one.IndexURL == "" || one.UploadID == "" {
		t.Fatalf("data of one part size got %+v, want a single PUT", one)
	}
	if want := h.clock.Now().Add(time.Hour).Unix(); one.Deadline != want {
		t.Fatalf("deadline %d, want %d", one.Deadline, want)
	}
	u, err := h.db.UploadByID(context.Background(), one.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	for ext, objectKey := range map[string]string{".idx": u.IndexKey, ".data": u.DataKey, ".links": u.LinksKey} {
		if !strings.HasSuffix(objectKey, one.UploadID+ext) || !strings.Contains(objectKey, v1.root.String()) {
			t.Errorf("the upload's %s key is %q", ext, objectKey)
		}
	}
	if u.Uploader != alice || u.State != db.StatePending || u.MultipartID != "" {
		t.Errorf("upload recorded as %+v", u)
	}

	many := h.ok(h.handle(alice, wire.Request{Op: wire.OpPushUpload, Name: "large", Root: v1.root[:], DataSize: 2500, Objects: 3}))
	if many.DataURL != "" || many.Parts == nil {
		t.Fatalf("data above the part size got %+v, want parts", many)
	}
	if many.Parts.PartSize != 1000 || len(many.Parts.URLs) != 3 || many.Parts.CompleteURL == "" {
		t.Fatalf("2500 bytes in parts of 1000 laid out as %+v", many.Parts)
	}
}

func TestPushUploadRefusals(t *testing.T) {
	h := newHarness(t, nil)
	v1, v2 := newTree(t, filesV1), newTree(t, filesV2)
	base := h.pushBase("v1", v1)

	req := wire.Request{Op: wire.OpPushUpload, Name: "v2", Root: v2.root[:], DataSize: 10, Objects: 1}
	unknown := req
	unknown.Parent = v2.keys[0][:]
	h.refused(h.handle(alice, unknown), wire.CodeParentGone)

	// A patch pack is nobody's parent.
	index, data := v2.pack(t, base)
	up := h.upload(alice, "v2", v2.root, &v1.root, index, data)
	h.ok(h.commit(alice, up.UploadID))
	v3 := newTree(t, map[string]string{"README": "the third version\n"})
	onPatch := wire.Request{Op: wire.OpPushUpload, Name: "v3", Root: v3.root[:], Parent: v2.root[:], DataSize: 10, Objects: 1}
	h.refused(h.handle(alice, onPatch), wire.CodeParentGone)

	self := req
	self.Root, self.Parent = v3.root[:], v3.root[:]
	h.refused(h.handle(alice, self), wire.CodeBadRequest)
	huge := req
	huge.Root, huge.Objects = v3.root[:], packfile.MaxEntries+1
	h.refused(h.handle(alice, huge), wire.CodeBadRequest)

	// A root that is stored needs no upload.
	again := h.ok(h.handle(bob, wire.Request{Op: wire.OpPushUpload, Name: "again", Root: v1.root[:], DataSize: 10, Objects: 1}))
	if !again.Stored || again.UploadID != "" {
		t.Fatalf("an upload for a stored root got %+v", again)
	}
}

func TestCommitOfABasePack(t *testing.T) {
	h := newHarness(t, nil)
	v1 := newTree(t, filesV1)
	index, data := v1.pack(t, nil)
	up := h.upload(alice, "v1", v1.root, nil, index, data)
	resp := h.ok(h.commit(alice, up.UploadID))
	if !bytes.Equal(resp.Root, v1.root[:]) {
		t.Fatalf("commit answered root %x", resp.Root)
	}

	p := h.packOf(v1.root)
	if !p.IsBase() || p.Uploader != alice || !p.UploadedAt.Equal(h.clock.Now()) {
		t.Fatalf("pack recorded as %+v", p)
	}
	if p.Objects != int64(index.Len()) || p.Bytes != int64(index.DataSize()) || p.DataSize != int64(len(data)) ||
		p.IndexSize != int64(len(index.Encode())) || p.SharedObjects != 0 || p.SharedBytes != 0 {
		t.Fatalf("pack measured as %+v", p)
	}
	raw := h.read(p.LinksKey)
	if p.LinksSize != int64(len(raw)) {
		t.Fatalf("links of %d bytes recorded as %d", len(raw), p.LinksSize)
	}
	links, err := packfile.ParseLinks(raw, index.Len())
	if err != nil {
		t.Fatal(err)
	}
	if rootAt, _ := index.Find(v1.root); len(links.Children(rootAt)) == 0 {
		t.Fatal("the links give the root no children")
	}
	if _, err := h.db.UploadByID(context.Background(), up.UploadID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("the upload is still there: %v", err)
	}
	near, err := h.db.Nearest(context.Background(), sketch.Of(v1.keys), 3)
	if err != nil || len(near) != 1 || near[0].Similarity != 1 {
		t.Fatalf("the pack's own sketch finds %+v, %v", near, err)
	}
}

func (h *harness) read(objectKey string) []byte {
	h.t.Helper()
	body, err := h.bucket.Bucket.Get(context.Background(), objectKey)
	if err != nil {
		h.t.Fatal(err)
	}
	defer body.Close()
	b, err := io.ReadAll(body)
	if err != nil {
		h.t.Fatal(err)
	}
	return b
}

func TestCommitOfAPatchPackMeasuresWhatTheParentHolds(t *testing.T) {
	h := newHarness(t, nil)
	v1, v2 := newTree(t, filesV1), newTree(t, filesV2)
	base := h.pushBase("v1", v1)

	index, data := v2.pack(t, base)
	if index.Len() == 0 || index.Len() == len(v2.keys) {
		t.Fatalf("the test trees do not overlap partly: %d of %d objects are new", index.Len(), len(v2.keys))
	}
	up := h.upload(bob, "v2", v2.root, &v1.root, index, data)
	h.ok(h.commit(bob, up.UploadID))

	var sharedObjects, sharedBytes int64
	for _, k := range v2.keys {
		if base.Has(k) {
			sharedObjects++
			sharedBytes += int64(len(v2.objects[k]))
		}
	}
	p := h.packOf(v2.root)
	if p.IsBase() || p.ParentID != h.packOf(v1.root).ID || p.LinksKey != "" || p.LinksSize != 0 || p.Uploader != bob {
		t.Fatalf("patch pack recorded as %+v", p)
	}
	if p.SharedObjects != sharedObjects || p.SharedBytes != sharedBytes {
		t.Fatalf("shared %d objects, %d bytes; the key sets share %d and %d", p.SharedObjects, p.SharedBytes, sharedObjects, sharedBytes)
	}
	if p.Objects+p.SharedObjects != int64(len(v2.keys)) {
		t.Fatalf("pack %d + shared %d objects, the ref has %d", p.Objects, p.SharedObjects, len(v2.keys))
	}

	pulled := h.ok(h.handle(alice, wire.Request{Op: wire.OpPull, Name: "v2"}))
	if len(pulled.Packs) != 2 || !bytes.Equal(pulled.Packs[0].Root, v2.root[:]) || !bytes.Equal(pulled.Packs[1].Root, v1.root[:]) {
		t.Fatalf("pull of a patch pack names %+v", pulled.Packs)
	}
	if !bytes.Equal(httpGet(t, pulled.Packs[0].DataURL), data) || !bytes.Equal(httpGet(t, pulled.Packs[1].IndexURL), base.Encode()) {
		t.Fatal("the pull's URLs do not return the packs")
	}
}

func TestCommitThroughParts(t *testing.T) {
	h := newHarness(t, func(c *server.Config) { c.PartSize = 100 })
	v1 := newTree(t, filesV1)
	index, data := v1.pack(t, nil)
	if len(data) <= 200 {
		t.Fatalf("the test pack is %d bytes, too small for three parts", len(data))
	}
	up := h.upload(alice, "v1", v1.root, nil, index, data)
	if up.Parts == nil {
		t.Fatal("the upload was not laid out in parts")
	}
	h.ok(h.commit(alice, up.UploadID))
	if p := h.packOf(v1.root); !bytes.Equal(h.read(p.DataKey), data) {
		t.Fatal("the parts did not join to the pack's data")
	}
}

func TestMalformedUploadsAreRefusedAndRemoved(t *testing.T) {
	v1, v2 := newTree(t, filesV1), newTree(t, filesV2)
	index, data := v1.pack(t, nil)

	// missing returns a pack of v1 without its last object.
	missing := func(t *testing.T) (*packfile.Index, []byte) {
		short := v1
		short.keys = v1.keys[:len(v1.keys)-1]
		if v1.keys[len(v1.keys)-1] == v1.root {
			short.keys = v1.keys[1:]
		}
		return short.pack(t, nil)
	}

	cases := map[string]func(t *testing.T, h *harness) wire.Response{
		"data shorter than announced": func(t *testing.T, h *harness) wire.Response {
			resp := h.ok(h.handle(alice, wire.Request{Op: wire.OpPushUpload, Name: "v1", Root: v1.root[:], DataSize: uint64(len(data)) + 1, Objects: uint64(index.Len())}))
			httpPut(t, resp.IndexURL, index.Encode())
			httpPut(t, resp.DataURL, data)
			return resp
		},
		"index never uploaded": func(t *testing.T, h *harness) wire.Response {
			resp := h.ok(h.handle(alice, wire.Request{Op: wire.OpPushUpload, Name: "v1", Root: v1.root[:], DataSize: uint64(len(data)), Objects: uint64(index.Len())}))
			httpPut(t, resp.DataURL, data)
			return resp
		},
		"index that is not an index": func(t *testing.T, h *harness) wire.Response {
			resp := h.ok(h.handle(alice, wire.Request{Op: wire.OpPushUpload, Name: "v1", Root: v1.root[:], DataSize: uint64(len(data)), Objects: uint64(index.Len())}))
			httpPut(t, resp.IndexURL, bytes.Repeat([]byte{0xAB}, len(index.Encode())))
			httpPut(t, resp.DataURL, data)
			return resp
		},
		"data that is not zstd": func(t *testing.T, h *harness) wire.Response {
			resp := h.ok(h.handle(alice, wire.Request{Op: wire.OpPushUpload, Name: "v1", Root: v1.root[:], DataSize: uint64(len(data)), Objects: uint64(index.Len())}))
			httpPut(t, resp.IndexURL, index.Encode())
			httpPut(t, resp.DataURL, bytes.Repeat([]byte{0xAB}, len(data)))
			return resp
		},
		"an object missing": func(t *testing.T, h *harness) wire.Response {
			index, data := missing(t)
			return h.upload(alice, "v1", v1.root, nil, index, data)
		},
		"another tree's objects": func(t *testing.T, h *harness) wire.Response {
			index, data := v2.pack(t, nil)
			return h.upload(alice, "v1", v1.root, nil, index, data)
		},
	}
	for name, upload := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, nil)
			up := upload(t, h)
			u, err := h.db.UploadByID(context.Background(), up.UploadID)
			if err != nil {
				t.Fatal(err)
			}
			refusal := h.refused(h.commit(alice, up.UploadID), wire.CodeMalformedPack)
			if refusal.Message == "" {
				t.Error("the refusal gives no reason")
			}
			if _, err := h.db.PackByRoot(context.Background(), v1.root); !errors.Is(err, db.ErrNotFound) {
				t.Fatalf("a malformed pack was recorded: %v", err)
			}
			h.refused(h.handle(alice, wire.Request{Op: wire.OpPull, Name: "v1"}), wire.CodeNotFound)
			h.sweep()
			for _, objectKey := range []string{u.IndexKey, u.DataKey, u.LinksKey} {
				if h.present(objectKey) {
					t.Errorf("%s is still in the bucket", objectKey)
				}
			}
			if entries, _ := os.ReadDir(h.scratch); len(entries) != 0 {
				t.Errorf("%d files were left in the scratch directory", len(entries))
			}
			h.refused(h.commit(alice, up.UploadID), wire.CodeUnknownUpload)
		})
	}
}

func TestCommitIsTheUploadersAlone(t *testing.T) {
	h := newHarness(t, nil)
	v1 := newTree(t, filesV1)
	index, data := v1.pack(t, nil)
	up := h.upload(alice, "v1", v1.root, nil, index, data)

	h.refused(h.commit(bob, up.UploadID), wire.CodeUnknownUpload)
	h.refused(h.commit(alice, "no-such-upload"), wire.CodeUnknownUpload)
	h.refused(h.commit(alice, ""), wire.CodeBadRequest)
	h.ok(h.commit(alice, up.UploadID))
}

func TestCommitWhenTheBucketFailsLeavesTheUploadOpen(t *testing.T) {
	h := newHarness(t, nil)
	v1 := newTree(t, filesV1)
	index, data := v1.pack(t, nil)
	up := h.upload(alice, "v1", v1.root, nil, index, data)

	h.bucket.failGet = true
	h.refused(h.commit(alice, up.UploadID), wire.CodeInternal)
	u, err := h.db.UploadByID(context.Background(), up.UploadID)
	if err != nil || u.State != db.StatePending {
		t.Fatalf("after a failed verification the upload is %+v, %v; want it pending", u, err)
	}
	if entries, _ := os.ReadDir(h.scratch); len(entries) != 0 {
		t.Errorf("%d files were left in the scratch directory", len(entries))
	}
	h.bucket.failGet = false
	h.ok(h.commit(alice, up.UploadID))
}

func TestCommitAfterTheRootGotAPack(t *testing.T) {
	h := newHarness(t, nil)
	v1 := newTree(t, filesV1)
	index, data := v1.pack(t, nil)
	first := h.upload(alice, "from-alice", v1.root, nil, index, data)
	second := h.upload(bob, "from-bob", v1.root, nil, index, data)
	late, err := h.db.UploadByID(context.Background(), second.UploadID)
	if err != nil {
		t.Fatal(err)
	}

	h.ok(h.commit(alice, first.UploadID))
	h.ok(h.commit(bob, second.UploadID))

	p := h.packOf(v1.root)
	if p.Uploader != alice {
		t.Fatalf("the pack is %s's, want the first committer's", p.Uploader)
	}
	for _, name := range []string{"from-alice", "from-bob"} {
		if pulled := h.ok(h.handle(bob, wire.Request{Op: wire.OpPull, Name: name})); !bytes.Equal(pulled.Root, v1.root[:]) {
			t.Fatalf("%s points at %x", name, pulled.Root)
		}
	}
	h.sweep()
	if h.present(late.DataKey) || h.present(late.IndexKey) {
		t.Fatal("the redundant upload's objects are still in the bucket")
	}
	if !h.present(p.DataKey) || !h.present(p.IndexKey) || !h.present(p.LinksKey) {
		t.Fatal("the recorded pack's objects are gone")
	}
}

func TestListAndDelete(t *testing.T) {
	h := newHarness(t, nil)
	v1 := newTree(t, filesV1)
	h.pushBase("a/one", v1)
	for _, name := range []string{"a/two", "b/one"} {
		h.ok(h.handle(alice, wire.Request{Op: wire.OpPushStart, Name: name, Root: v1.root[:], Sketch: v1.sketch()}))
	}

	names := func(resp wire.Response) []string {
		var out []string
		for _, r := range resp.Refs {
			if !bytes.Equal(r.Root, v1.root[:]) {
				t.Errorf("%s listed with root %x", r.Name, r.Root)
			}
			out = append(out, r.Name)
		}
		return out
	}
	all := h.ok(h.handle(alice, wire.Request{Op: wire.OpList}))
	if got := names(all); strings.Join(got, " ") != "a/one a/two b/one" || all.More {
		t.Fatalf("list = %v, more %v", got, all.More)
	}
	page := h.ok(h.handle(alice, wire.Request{Op: wire.OpList, Prefix: "a/", Limit: 1}))
	if got := names(page); strings.Join(got, " ") != "a/one" || !page.More {
		t.Fatalf("first page of a/ = %v, more %v", got, page.More)
	}
	page = h.ok(h.handle(alice, wire.Request{Op: wire.OpList, Prefix: "a/", After: "a/one", Limit: 1}))
	if got := names(page); strings.Join(got, " ") != "a/two" || page.More {
		t.Fatalf("second page of a/ = %v, more %v", got, page.More)
	}

	h.ok(h.handle(bob, wire.Request{Op: wire.OpDelete, Name: "a/two"}))
	h.refused(h.handle(bob, wire.Request{Op: wire.OpDelete, Name: "a/two"}), wire.CodeNotFound)
	h.refused(h.handle(bob, wire.Request{Op: wire.OpPull, Name: "a/two"}), wire.CodeNotFound)
	h.ok(h.handle(bob, wire.Request{Op: wire.OpPull, Name: "a/one"}))
}

func TestCollectedPacksLeaveTheBucketAfterTheURLLifetime(t *testing.T) {
	h := newHarness(t, nil)
	v1, v2 := newTree(t, filesV1), newTree(t, filesV2)
	base := h.pushBase("v1", v1)
	index, data := v2.pack(t, base)
	up := h.upload(alice, "v2", v2.root, &v1.root, index, data)
	h.ok(h.commit(alice, up.UploadID))
	basePack, patchPack := h.packOf(v1.root), h.packOf(v2.root)

	// The base pack loses its ref and stays for the patch pack's sake.
	h.ok(h.handle(alice, wire.Request{Op: wire.OpDelete, Name: "v1"}))
	h.clock.Advance(2 * time.Hour)
	h.sweep()
	if !h.present(basePack.DataKey) || !h.present(basePack.LinksKey) {
		t.Fatal("a base pack a patch pack leans on was removed")
	}

	// With the patch pack's ref both go, but not before the URLs handed
	// out for them have expired.
	h.ok(h.handle(alice, wire.Request{Op: wire.OpDelete, Name: "v2"}))
	h.sweep()
	if !h.present(basePack.DataKey) || !h.present(patchPack.DataKey) {
		t.Fatal("collected packs left the bucket while their URLs were still valid")
	}
	h.clock.Advance(time.Hour + time.Second)
	h.sweep()
	for _, objectKey := range []string{basePack.DataKey, basePack.IndexKey, basePack.LinksKey, patchPack.DataKey, patchPack.IndexKey} {
		if h.present(objectKey) {
			t.Errorf("%s is still in the bucket", objectKey)
		}
	}
	st, err := h.db.Stats(context.Background())
	if err != nil || st.BasePacks != 0 || st.PatchPacks != 0 || st.Deletions != 0 {
		t.Fatalf("after the sweep the store counts %+v, %v", st, err)
	}
}

func TestExpiredUploadsAreSweptAway(t *testing.T) {
	h := newHarness(t, func(c *server.Config) { c.PartSize = 100 })
	v1, v2 := newTree(t, filesV1), newTree(t, filesV2)

	// One upload of a single PUT, complete but never committed.
	index, data := v1.pack(t, nil)
	whole := h.ok(h.handle(alice, wire.Request{Op: wire.OpPushUpload, Name: "v1", Root: v1.root[:], DataSize: 50, Objects: uint64(index.Len())}))
	httpPut(t, whole.IndexURL, index.Encode())
	httpPut(t, whole.DataURL, data[:50])
	single, err := h.db.UploadByID(context.Background(), whole.UploadID)
	if err != nil {
		t.Fatal(err)
	}

	// One in parts, abandoned after the first.
	parts := h.ok(h.handle(alice, wire.Request{Op: wire.OpPushUpload, Name: "v2", Root: v2.root[:], DataSize: 250, Objects: 1}))
	httpPut(t, parts.Parts.URLs[0], bytes.Repeat([]byte{1}, 100))

	h.clock.Advance(59 * time.Minute)
	h.sweep()
	if !h.present(single.IndexKey) || !h.present(single.DataKey) {
		t.Fatal("an upload was swept before its deadline")
	}

	h.clock.Advance(2 * time.Minute)
	h.sweep()
	if h.present(single.IndexKey) || h.present(single.DataKey) {
		t.Fatal("an expired upload's objects are still in the bucket")
	}
	h.refused(h.commit(alice, whole.UploadID), wire.CodeUnknownUpload)
	res := httpDo(t, http.MethodPut, parts.Parts.URLs[1], bytes.Repeat([]byte{2}, 100))
	res.Body.Close()
	if res.StatusCode == http.StatusOK {
		t.Fatal("the multipart upload of an expired upload still takes parts")
	}

	// A PUT that began before the deadline can land after it: the keys
	// are deleted a second time an hour on.
	httpPut(t, whole.DataURL, data[:50])
	h.sweep()
	if !h.present(single.DataKey) {
		t.Fatal("the straggler was removed before the second deletion was due")
	}
	h.clock.Advance(time.Hour)
	h.sweep()
	if h.present(single.DataKey) {
		t.Fatal("an object that landed after the deadline was never removed")
	}
}

func TestCommitAfterTheDeadlineIsRefused(t *testing.T) {
	h := newHarness(t, nil)
	v1 := newTree(t, filesV1)
	index, data := v1.pack(t, nil)
	up := h.upload(alice, "v1", v1.root, nil, index, data)
	h.clock.Advance(time.Hour + time.Second)
	// The sweeper has not run: the server must not take the upload anyway.
	h.refused(h.commit(alice, up.UploadID), wire.CodeUnknownUpload)
	h.sweep()
	if _, err := h.db.UploadByID(context.Background(), up.UploadID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("the expired upload was not swept: %v", err)
	}
}

func TestSweepLeavesAnUploadThatIsBeingVerified(t *testing.T) {
	h := newHarness(t, nil)
	v1 := newTree(t, filesV1)
	index, data := v1.pack(t, nil)
	up := h.upload(alice, "v1", v1.root, nil, index, data)
	u, err := h.db.BeginVerify(context.Background(), up.UploadID, alice)
	if err != nil {
		t.Fatal(err)
	}
	h.clock.Advance(3 * time.Hour)
	h.sweep()
	if _, err := h.db.UploadByID(context.Background(), up.UploadID); err != nil {
		t.Fatalf("an upload under verification was expired: %v", err)
	}
	if !h.present(u.DataKey) {
		t.Fatal("the data of an upload under verification was removed")
	}
}

func TestAFailedDeletionStaysQueued(t *testing.T) {
	h := newHarness(t, nil)
	v1 := newTree(t, filesV1)
	h.pushBase("v1", v1)
	p := h.packOf(v1.root)
	h.ok(h.handle(alice, wire.Request{Op: wire.OpDelete, Name: "v1"}))
	h.clock.Advance(2 * time.Hour)

	h.bucket.failDelete = true
	if err := h.srv.Sweep(context.Background()); err == nil {
		t.Fatal("Sweep reported nothing although the bucket refused every delete")
	}
	if !h.present(p.DataKey) {
		t.Fatal("the object is gone although its deletion failed")
	}
	if st, _ := h.db.Stats(context.Background()); st.Deletions != 3 {
		t.Fatalf("%d deletions are queued, want the pack's 3", st.Deletions)
	}
	h.bucket.failDelete = false
	h.sweep()
	if st, _ := h.db.Stats(context.Background()); st.Deletions != 0 || h.present(p.DataKey) {
		t.Fatalf("after the bucket came back %d deletions are queued", st.Deletions)
	}
}

func TestNewPicksUpWhereACrashedServerLeftOff(t *testing.T) {
	h := newHarness(t, nil)
	v1 := newTree(t, filesV1)
	index, data := v1.pack(t, nil)
	up := h.upload(alice, "v1", v1.root, nil, index, data)
	if _, err := h.db.BeginVerify(context.Background(), up.UploadID, alice); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.scratch, "pack-left-behind"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	// The process dies and another starts on the same database file.
	h.db.Close()
	var err error
	if h.db, err = db.Open(h.dbPath); err != nil {
		t.Fatal(err)
	}
	h.srv = h.newServer(nil)

	if entries, _ := os.ReadDir(h.scratch); len(entries) != 0 {
		t.Errorf("%d files survived in the scratch directory", len(entries))
	}
	h.ok(h.commit(alice, up.UploadID))
	h.packOf(v1.root)
}

func TestNewNeedsItsParts(t *testing.T) {
	if _, err := server.New(server.Config{}); err == nil {
		t.Fatal("New accepted a configuration without a database, a bucket and a scratch directory")
	}
}
