package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/jaccard-store/bucket"
	"github.com/amber-store/jaccard-store/bucket/buckettest"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/db"
	"github.com/amber-store/jaccard-store/node"
	"github.com/amber-store/jaccard-store/packfile"
	"github.com/amber-store/jaccard-store/server"
	"github.com/amber-store/jaccard-store/wire"
	"github.com/tmc/go-iroh/iroh"
)

const testTimeout = 2 * time.Minute

// written is the modification time of every file the tests ingest.
var written = time.Unix(1_700_000_000, 0)

// quiet drops what the server and the endpoints log.
var quiet = slog.New(slog.DiscardHandler)

// clock is a clock a test moves by hand while the server reads it.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// world is a server with its database and its bucket, reachable over iroh
// on loopback.
type world struct {
	t      *testing.T
	ctx    context.Context
	bucket *bucket.Bucket
	db     *db.DB
	srv    *server.Server
	ep     *iroh.Endpoint
	clock  *clock
}

func newWorld(t *testing.T, mutate func(*server.Config)) *world {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	dir := t.TempDir()
	w := &world{t: t, ctx: ctx, bucket: buckettest.New(t), clock: &clock{t: time.Unix(1_800_000_000, 0)}}

	var err error
	if w.db, err = db.Open(filepath.Join(dir, "store.sqlite")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.db.Close() })
	cfg := server.Config{DB: w.db, Bucket: w.bucket, Scratch: filepath.Join(dir, "scratch"), Now: w.clock.Now, Log: quiet}
	if mutate != nil {
		mutate(&cfg)
	}
	if w.srv, err = server.New(cfg); err != nil {
		t.Fatal(err)
	}
	sk, err := node.LoadOrCreateKey(filepath.Join(dir, "server.key"))
	if err != nil {
		t.Fatal(err)
	}
	if w.ep, err = node.Bind(ctx, node.ServerConfig{Key: sk, ALPN: wire.ALPN, Local: true, Log: quiet}); err != nil {
		t.Fatal(err)
	}
	serveCtx, stop := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() {
		defer close(served)
		w.srv.Serve(serveCtx, w.ep)
	}()
	t.Cleanup(func() {
		stop()
		<-served
		w.ep.Shutdown(context.Background())
	})
	return w
}

// peer is a client with a key of its own and a local store.
type peer struct {
	t       *testing.T
	w       *world
	id      string
	conn    *node.Conn
	client  *client.Client
	objects *packstore.Store
}

func (w *world) peer(name string) *peer {
	w.t.Helper()
	dir := w.t.TempDir()
	sk, err := node.LoadOrCreateKey(filepath.Join(dir, name+".key"))
	if err != nil {
		w.t.Fatal(err)
	}
	conn, err := node.Dial(w.ctx, node.DialConfig{
		Key: sk, ALPN: wire.ALPN, Server: w.ep.ID(), Addrs: []netip.AddrPort{w.ep.LocalAddr()}, Log: quiet,
	})
	if err != nil {
		w.t.Fatalf("dialing the server: %v", err)
	}
	w.t.Cleanup(func() { conn.Close() })
	objects, err := packstore.Open(filepath.Join(dir, "packstore"), packstore.WithSync(false))
	if err != nil {
		w.t.Fatal(err)
	}
	w.t.Cleanup(func() { objects.Close() })
	return &peer{
		t: w.t, w: w, id: sk.Public().EndpointID().String(),
		conn: conn, client: client.New(conn, nil), objects: objects,
	}
}

// ingest writes files to a directory and ingests it into the peer's store.
func (p *peer) ingest(files map[string][]byte) key.Key {
	p.t.Helper()
	dir := p.t.TempDir()
	for name, content := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			p.t.Fatal(err)
		}
		if err := os.WriteFile(path, content, 0o644); err != nil {
			p.t.Fatal(err)
		}
	}
	// A directory entry records when its file was last written. The same
	// files are the same tree only if that is the same too.
	err := filepath.WalkDir(dir, func(path string, _ os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return os.Chtimes(path, written, written)
	})
	if err != nil {
		p.t.Fatal(err)
	}
	root, _, err := ingest.Dir(p.objects, dir, ingest.Opts{NoIgnore: true})
	if err != nil {
		p.t.Fatal(err)
	}
	return root
}

func (p *peer) push(name string, root key.Key) client.PushResult {
	p.t.Helper()
	res, err := p.client.Push(p.w.ctx, p.objects, name, root, client.PushOptions{MinDedup: 0.5, TempDir: p.t.TempDir()})
	if err != nil {
		p.t.Fatalf("push %s: %v", name, err)
	}
	return res
}

func (p *peer) pull(name string) client.PullResult {
	p.t.Helper()
	res, err := p.client.Pull(p.w.ctx, p.objects, name)
	if err != nil {
		p.t.Fatalf("pull %s: %v", name, err)
	}
	return res
}

func (p *peer) keys(root key.Key) []key.Key {
	p.t.Helper()
	keys, err := fstree.ReachableKeys(root, p.objects.Get)
	if err != nil {
		p.t.Fatalf("the store does not hold all of %s: %v", root, err)
	}
	return keys
}

// call sends one request the way the client does, for the tests that play
// a client that misbehaves.
func (p *peer) call(req wire.Request) wire.Response {
	p.t.Helper()
	stream, err := p.conn.OpenStreamConn(p.w.ctx)
	if err != nil {
		p.t.Fatal(err)
	}
	defer stream.Close()
	if err := wire.WriteFrame(stream, req); err != nil {
		p.t.Fatal(err)
	}
	var resp wire.Response
	if err := wire.ReadFrame(stream, &resp); err != nil {
		p.t.Fatal(err)
	}
	return resp
}

// sameTree checks that dst holds every object of root as src does.
func sameTree(t *testing.T, root key.Key, src, dst *peer) {
	t.Helper()
	want := src.keys(root)
	got := dst.keys(root)
	if len(got) != len(want) {
		t.Fatalf("the copy of %s has %d objects, the source %d", root, len(got), len(want))
	}
	for _, k := range want {
		a, err := src.objects.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		b, err := dst.objects.Get(k)
		if err != nil {
			t.Fatalf("object %s of the copy: %v", k, err)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("object %s differs in the copy", k)
		}
	}
}

func (w *world) pack(root key.Key) db.Pack {
	w.t.Helper()
	p, err := w.db.PackByRoot(w.ctx, root)
	if err != nil {
		w.t.Fatalf("the server's pack of %s: %v", root, err)
	}
	return p
}

func (w *world) present(objectKey string) bool {
	w.t.Helper()
	_, err := w.bucket.Size(w.ctx, objectKey)
	if errors.Is(err, bucket.ErrNotFound) {
		return false
	}
	if err != nil {
		w.t.Fatal(err)
	}
	return true
}

func (w *world) sweep() {
	w.t.Helper()
	if err := w.srv.Sweep(w.ctx); err != nil {
		w.t.Fatalf("Sweep: %v", err)
	}
}

func (w *world) stats() db.Stats {
	w.t.Helper()
	st, err := w.db.Stats(w.ctx)
	if err != nil {
		w.t.Fatal(err)
	}
	return st
}

// text returns n lines that differ from each other and from those of any
// other seed, so that files neither compress to nothing nor chunk alike.
func text(seed string, n int) []byte {
	var b bytes.Buffer
	for i := range n {
		fmt.Fprintf(&b, "%s: line %d of %d, %x\n", seed, i, n, i*2654435761)
	}
	return b.Bytes()
}

// noise returns n bytes that do not compress.
func noise(seed int64, n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func version1() map[string][]byte {
	return map[string][]byte{
		"README":          []byte("the first version\n"),
		"src/main.go":     text("main", 40),
		"src/util.go":     text("util", 40),
		"docs/guide.txt":  text("guide", 2000),
		"docs/manual.txt": text("manual", 2000),
	}
}

// version2 changes a small file of version1 and adds one: most of its bytes
// are version1's.
func version2() map[string][]byte {
	files := version1()
	files["README"] = []byte("the second version\n")
	files["src/added.go"] = text("added", 40)
	return files
}

func unrelated() map[string][]byte {
	return map[string][]byte{
		"other/a.txt": text("something else", 500),
		"other/b.txt": text("entirely", 500),
	}
}

func TestBaseThenPatchRoundTrip(t *testing.T) {
	w := newWorld(t, nil)
	alice, bob := w.peer("alice"), w.peer("bob")

	v1 := alice.ingest(version1())
	first := alice.push("project/v1", v1)
	if first.Stored || first.Parent != nil || first.Root != v1 || first.Objects != uint64(len(alice.keys(v1))) {
		t.Fatalf("first push: %+v, want a base pack of the whole key set", first)
	}
	base := w.pack(v1)
	if !base.IsBase() || base.Uploader != alice.id || base.Objects != int64(first.Objects) || base.DataSize != int64(first.DataSize) {
		t.Fatalf("the server recorded the base pack as %+v", base)
	}

	v2 := alice.ingest(version2())
	second := alice.push("project/v2", v2)
	if second.Stored || second.Parent == nil || *second.Parent != v1 {
		t.Fatalf("second push: %+v, want a patch pack of %s", second, v1)
	}
	if second.Objects == 0 || second.Objects >= uint64(len(alice.keys(v2))) {
		t.Fatalf("the patch pack holds %d of the reference's %d objects", second.Objects, len(alice.keys(v2)))
	}

	// The server's figures against the two key sets, computed here.
	inV1 := map[key.Key]bool{}
	for _, k := range alice.keys(v1) {
		inV1[k] = true
	}
	var sharedObjects, sharedBytes, ownObjects int64
	for _, k := range alice.keys(v2) {
		data, err := alice.objects.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		if inV1[k] {
			sharedObjects++
			sharedBytes += int64(len(data))
		} else {
			ownObjects++
		}
	}
	patch := w.pack(v2)
	if patch.IsBase() || patch.ParentID != base.ID || patch.Objects != ownObjects {
		t.Fatalf("the server recorded the patch pack as %+v", patch)
	}
	if patch.SharedObjects != sharedObjects || patch.SharedBytes != sharedBytes {
		t.Fatalf("the server counts %d objects and %d bytes held by the parent; the key sets share %d and %d",
			patch.SharedObjects, patch.SharedBytes, sharedObjects, sharedBytes)
	}

	// Into an empty store, the patch pack needs its parent.
	got := bob.pull("project/v2")
	if got.Root != v2 || got.Packs != 2 || got.Objects != len(alice.keys(v2))+int(base.Objects-sharedObjects) {
		t.Fatalf("pull into an empty store: %+v", got)
	}
	sameTree(t, v2, alice, bob)

	// Pulled again, there is nothing to fetch.
	if again := bob.pull("project/v2"); again.Packs != 0 || again.Objects != 0 {
		t.Fatalf("a second pull fetched %+v", again)
	}

	refs, err := bob.client.List(w.ctx, "project/")
	if err != nil || len(refs) != 2 || refs[0] != (client.Ref{Name: "project/v1", Root: v1}) || refs[1] != (client.Ref{Name: "project/v2", Root: v2}) {
		t.Fatalf("list = %+v, %v", refs, err)
	}
}

func TestPullSkipsTheParentTheStoreAlreadyHolds(t *testing.T) {
	w := newWorld(t, nil)
	alice, bob := w.peer("alice"), w.peer("bob")
	v1 := alice.ingest(version1())
	v2 := alice.ingest(version2())
	alice.push("v1", v1)
	if res := alice.push("v2", v2); res.Parent == nil {
		t.Fatalf("v2 was not pushed as a patch pack: %+v", res)
	}

	// Bob holds version 1 from somewhere else.
	if got := bob.ingest(version1()); got != v1 {
		t.Fatalf("the same files ingested to %s and %s", v1, got)
	}
	got := bob.pull("v2")
	if got.Packs != 1 || got.Objects != int(w.pack(v2).Objects) {
		t.Fatalf("pull fetched %+v, want the patch pack alone", got)
	}
	sameTree(t, v2, alice, bob)
}

func TestARootAlreadyStoredIsNotUploadedAgain(t *testing.T) {
	w := newWorld(t, nil)
	alice, bob := w.peer("alice"), w.peer("bob")
	v1 := alice.ingest(version1())
	alice.push("v1", v1)

	if res := alice.push("v1", v1); !res.Stored {
		t.Fatalf("the same push again: %+v, want stored", res)
	}
	if got := bob.ingest(version1()); got != v1 {
		t.Fatal("the two peers ingested the same files differently")
	}
	if res := bob.push("bobs/copy", v1); !res.Stored {
		t.Fatalf("a second name for a stored root: %+v, want stored", res)
	}
	if p := w.pack(v1); p.Uploader != alice.id {
		t.Fatalf("the pack is recorded as %s's", p.Uploader)
	}
	if st := w.stats(); st.Refs != 2 || st.BasePacks != 1 || st.PatchPacks != 0 {
		t.Fatalf("the store counts %+v", st)
	}
}

func TestAMovedRefLeavesItsOldPackToBeCollected(t *testing.T) {
	w := newWorld(t, nil)
	alice, bob := w.peer("alice"), w.peer("bob")
	old := alice.ingest(version1())
	alice.push("moving", old)
	oldPack := w.pack(old)

	replacement := alice.ingest(unrelated())
	if res := alice.push("moving", replacement); res.Stored || res.Parent != nil {
		t.Fatalf("unrelated content was pushed as %+v, want a base pack", res)
	}
	if _, err := w.db.PackByRoot(w.ctx, old); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("the pack nothing refers to is still recorded: %v", err)
	}
	w.sweep()
	if !w.present(oldPack.DataKey) {
		t.Fatal("the old pack left the bucket while its URLs could still be in use")
	}
	w.clock.Advance(time.Hour + time.Second)
	w.sweep()
	for _, objectKey := range []string{oldPack.DataKey, oldPack.IndexKey, oldPack.LinksKey} {
		if w.present(objectKey) {
			t.Errorf("%s is still in the bucket", objectKey)
		}
	}

	if got := bob.pull("moving"); got.Root != replacement {
		t.Fatalf("the moved ref pulls %s, want %s", got.Root, replacement)
	}
	sameTree(t, replacement, alice, bob)

	if err := alice.client.Delete(w.ctx, "moving"); err != nil {
		t.Fatal(err)
	}
	if err := alice.client.Delete(w.ctx, "moving"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("deleting a ref that is gone: %v, want ErrNotFound", err)
	}
	if _, err := bob.client.Pull(w.ctx, bob.objects, "moving"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("pulling a ref that is gone: %v, want ErrNotFound", err)
	}
	if st := w.stats(); st.Refs != 0 || st.BasePacks != 0 {
		t.Fatalf("after the delete the store counts %+v", st)
	}
}

func TestALargePackGoesUpInParts(t *testing.T) {
	const partSize = 64 << 10
	w := newWorld(t, func(c *server.Config) { c.PartSize = partSize })
	alice, bob := w.peer("alice"), w.peer("bob")
	root := alice.ingest(map[string][]byte{
		"blob.bin":  noise(1, 300<<10),
		"small.txt": []byte("beside it\n"),
	})
	res := alice.push("large", root)
	if res.DataSize <= 4*partSize {
		t.Fatalf("the pack is %d bytes, too small for the parts this test is about", res.DataSize)
	}
	p := w.pack(root)
	if size, err := w.bucket.Size(w.ctx, p.DataKey); err != nil || size != int64(res.DataSize) {
		t.Fatalf("the bucket holds %d bytes of data (%v), the client uploaded %d", size, err, res.DataSize)
	}
	if got := bob.pull("large"); got.Packs != 1 {
		t.Fatalf("pull: %+v", got)
	}
	sameTree(t, root, alice, bob)
}

func TestTwoClientsPushingOneRootAtOnce(t *testing.T) {
	w := newWorld(t, nil)
	alice, bob := w.peer("alice"), w.peer("bob")
	root := alice.ingest(version1())
	if got := bob.ingest(version1()); got != root {
		t.Fatal("the two peers ingested the same files differently")
	}

	var wg sync.WaitGroup
	results := make([]client.PushResult, 2)
	errs := make([]error, 2)
	for i, p := range []*peer{alice, bob} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = p.client.Push(w.ctx, p.objects, "from/"+[]string{"alice", "bob"}[i], root,
				client.PushOptions{MinDedup: 0.5, TempDir: t.TempDir()})
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
		if results[i].Root != root {
			t.Fatalf("push %d: %+v", i, results[i])
		}
	}
	if st := w.stats(); st.Refs != 2 || st.BasePacks != 1 || st.PatchPacks != 0 || st.Uploads != 0 {
		t.Fatalf("the store counts %+v, want two refs on one pack", st)
	}
	for _, name := range []string{"from/alice", "from/bob"} {
		ref, err := w.db.Ref(w.ctx, name)
		if err != nil || ref.Root != root {
			t.Fatalf("%s points at %s, %v", name, ref.Root, err)
		}
	}
	w.sweep()
	p := w.pack(root)
	if !w.present(p.DataKey) || !w.present(p.IndexKey) || !w.present(p.LinksKey) {
		t.Fatal("the pack that won lost an object to the sweep")
	}
	carol := w.peer("carol")
	carol.pull("from/bob")
	sameTree(t, root, alice, carol)
}

func TestARefOfOneObject(t *testing.T) {
	w := newWorld(t, nil)
	alice, bob := w.peer("alice"), w.peer("bob")
	blob, err := fstree.EncodeBlob([]byte("a reference that is one blob"))
	if err != nil {
		t.Fatal(err)
	}
	if err := alice.objects.Put(blob.Key, blob.Bytes); err != nil {
		t.Fatal(err)
	}
	res := alice.push("one", blob.Key)
	if res.Stored || res.Parent != nil || res.Objects != 1 {
		t.Fatalf("push: %+v, want a base pack of one object", res)
	}
	if got := bob.pull("one"); got.Root != blob.Key || got.Objects != 1 {
		t.Fatalf("pull: %+v", got)
	}
	data, err := bob.objects.Get(blob.Key)
	if err != nil || string(data) != "a reference that is one blob" {
		t.Fatalf("the blob arrived as %q, %v", data, err)
	}
}

func TestARefWhoseRootSitsInABasePack(t *testing.T) {
	w := newWorld(t, nil)
	alice, bob := w.peer("alice"), w.peer("bob")
	whole := alice.ingest(version1())
	alice.push("whole", whole)

	docs, err := fstree.ResolvePath(whole, "docs", alice.objects.Get)
	if err != nil {
		t.Fatal(err)
	}
	res := alice.push("docs", docs)
	if res.Stored || res.Parent == nil || *res.Parent != whole || res.Objects != 0 {
		t.Fatalf("push of a subtree: %+v, want an empty patch pack of %s", res, whole)
	}
	p := w.pack(docs)
	if p.IsBase() || p.Objects != 0 || p.Bytes != 0 || p.SharedObjects != int64(len(alice.keys(docs))) {
		t.Fatalf("the server recorded %+v; the subtree has %d objects", p, len(alice.keys(docs)))
	}

	if got := bob.pull("docs"); got.Root != docs || got.Packs != 2 {
		t.Fatalf("pull: %+v", got)
	}
	sameTree(t, docs, alice, bob)
}

func TestAMalformedUploadIsRefusedAndRemoved(t *testing.T) {
	w := newWorld(t, nil)
	mallory := w.peer("mallory")
	root := mallory.ingest(version1())
	keys := mallory.keys(root)

	// A pack of everything but one object, announced honestly otherwise.
	var data bytes.Buffer
	pw, err := packfile.NewWriter(&data)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys[:len(keys)-1] { // ReachableKeys puts the root first
		object, err := mallory.objects.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		if err := pw.Add(k, object); err != nil {
			t.Fatal(err)
		}
	}
	index, err := pw.Finish()
	if err != nil {
		t.Fatal(err)
	}

	up := mallory.call(wire.Request{Op: wire.OpPushUpload, Name: "bad", Root: root[:], DataSize: uint64(data.Len()), Objects: uint64(index.Len())})
	if up.Error != nil {
		t.Fatal(up.Error)
	}
	u, err := w.db.UploadByID(w.ctx, up.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	put(t, up.IndexURL, index.Encode())
	put(t, up.DataURL, data.Bytes())

	// Nobody but the endpoint that opened the upload may commit it.
	eve := w.peer("eve")
	if resp := eve.call(wire.Request{Op: wire.OpPushCommit, UploadID: up.UploadID}); resp.Error == nil || resp.Error.Code != wire.CodeUnknownUpload {
		t.Fatalf("another endpoint's commit answered %+v", resp)
	}
	resp := mallory.call(wire.Request{Op: wire.OpPushCommit, UploadID: up.UploadID})
	if resp.Error == nil || resp.Error.Code != wire.CodeMalformedPack {
		t.Fatalf("commit answered %+v, want malformed_pack", resp)
	}
	if _, err := mallory.client.Pull(w.ctx, mallory.objects, "bad"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("the ref of a refused pack: %v, want ErrNotFound", err)
	}
	w.sweep()
	for _, objectKey := range []string{u.IndexKey, u.DataKey, u.LinksKey} {
		if w.present(objectKey) {
			t.Errorf("%s is still in the bucket", objectKey)
		}
	}
	if st := w.stats(); st.BasePacks != 0 || st.Uploads != 0 || st.Deletions != 0 {
		t.Fatalf("the store counts %+v", st)
	}

	// The same client, honest this time, is served.
	if res := mallory.push("good", root); res.Stored || res.Objects != uint64(len(keys)) {
		t.Fatalf("an honest push after the refusal: %+v", res)
	}
}

func TestAnAbandonedUploadLeavesTheBucket(t *testing.T) {
	const partSize = 1000
	w := newWorld(t, func(c *server.Config) { c.PartSize = partSize })
	alice := w.peer("alice")
	rootA := alice.ingest(version1())
	rootB := alice.ingest(unrelated())

	// One PUT, done; the commit never comes.
	single := alice.call(wire.Request{Op: wire.OpPushUpload, Name: "a", Root: rootA[:], DataSize: 500, Objects: 2})
	if single.Error != nil || single.DataURL == "" {
		t.Fatalf("push-upload answered %+v", single)
	}
	put(t, single.IndexURL, bytes.Repeat([]byte{1}, 104))
	put(t, single.DataURL, bytes.Repeat([]byte{2}, 500))
	ua, err := w.db.UploadByID(w.ctx, single.UploadID)
	if err != nil {
		t.Fatal(err)
	}

	// Three parts, of which one arrives.
	parts := alice.call(wire.Request{Op: wire.OpPushUpload, Name: "b", Root: rootB[:], DataSize: 2500, Objects: 2})
	if parts.Error != nil || parts.Parts == nil || len(parts.Parts.URLs) != 3 {
		t.Fatalf("push-upload answered %+v", parts)
	}
	put(t, parts.Parts.URLs[0], bytes.Repeat([]byte{3}, partSize))

	w.clock.Advance(30 * time.Minute)
	w.sweep()
	if st := w.stats(); st.Uploads != 2 || !w.present(ua.DataKey) {
		t.Fatalf("uploads were swept before their deadline: %+v", st)
	}

	w.clock.Advance(31 * time.Minute)
	w.sweep()
	if st := w.stats(); st.Uploads != 0 {
		t.Fatalf("%d uploads outlived their deadline", st.Uploads)
	}
	if w.present(ua.DataKey) || w.present(ua.IndexKey) {
		t.Fatal("an abandoned upload's objects are still in the bucket")
	}
	if status := putStatus(t, parts.Parts.URLs[1], bytes.Repeat([]byte{4}, partSize)); status == http.StatusOK {
		t.Fatal("the abandoned multipart upload still takes parts")
	}
	for _, id := range []string{single.UploadID, parts.UploadID} {
		if resp := alice.call(wire.Request{Op: wire.OpPushCommit, UploadID: id}); resp.Error == nil || resp.Error.Code != wire.CodeUnknownUpload {
			t.Fatalf("commit of an expired upload answered %+v", resp)
		}
	}
}

func putStatus(t *testing.T, url string, body []byte) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	io.Copy(io.Discard, res.Body)
	return res.StatusCode
}

func put(t *testing.T, url string, body []byte) {
	t.Helper()
	if status := putStatus(t, url, body); status != http.StatusOK {
		t.Fatalf("PUT answered %d", status)
	}
}

func TestNamesWithOddCharacters(t *testing.T) {
	w := newWorld(t, nil)
	alice := w.peer("alice")
	root := alice.ingest(version1())
	names := []string{"100%/done", "a_b", "a%b", `back\slash`, "ünïcode/ref", "axb"}
	var accepted []string
	for _, name := range names {
		if _, err := alice.client.Push(w.ctx, alice.objects, name, root, client.PushOptions{MinDedup: 0.5, TempDir: t.TempDir()}); err != nil {
			// Whether a name is one at all is core's rule; a name it refuses
			// must be refused as a bad request and nothing else.
			var werr *wire.Error
			if !errors.As(err, &werr) || werr.Code != wire.CodeBadRequest {
				t.Fatalf("push %q: %v", name, err)
			}
			continue
		}
		accepted = append(accepted, name)
	}
	if len(accepted) == 0 {
		t.Fatal("no name was accepted")
	}
	for _, name := range accepted {
		// A name is its own prefix, and the wildcards of SQL in it match
		// nothing but themselves.
		refs, err := alice.client.List(w.ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, r := range refs {
			got = append(got, r.Name)
		}
		if len(got) != 1 || got[0] != name {
			t.Errorf("list with prefix %q = %q", name, strings.Join(got, ", "))
		}
	}
}
