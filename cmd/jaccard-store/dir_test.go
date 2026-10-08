package main

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/jaccard-store/client"
)

// memory is a server that keeps what is pushed to it and gives it back: a
// reference and the objects under it.
type memory struct {
	// Pushes come side by side from push-subdirs.
	mu      sync.Mutex
	objects map[key.Key][]byte
	refs    map[string]key.Key
	pushErr error
	// listErr is what List fails with; listed are the prefixes it was
	// asked for.
	listErr error
	listed  []string
	// refused is what the push of a name fails with, for the names in it.
	refused map[string]error
	// onPush, when set, is called as a push begins.
	onPush func()
	// atOnce, when set, holds every push until that many are under way
	// together: it is how a test sees how many run side by side.
	atOnce   int
	together chan struct{} // closed once atOnce pushes were under way
	gather   sync.Once
	inFlight atomic.Int32
	peak     atomic.Int32

	connected int
	stores    []string // the directories of the stores it was handed
	packDirs  []string // where it was told to build packs
	fetched   int      // pulls that got as far as fetching
}

func newMemory() *memory {
	return &memory{objects: map[key.Key][]byte{}, refs: map[string]key.Key{}, together: make(chan struct{})}
}

func (m *memory) Push(ctx context.Context, store *localStore, name string, root key.Key, opts client.PushOptions) (client.PushResult, error) {
	n := m.inFlight.Add(1)
	defer m.inFlight.Add(-1)
	for {
		peak := m.peak.Load()
		if n <= peak || m.peak.CompareAndSwap(peak, n) {
			break
		}
	}
	if m.atOnce > 0 {
		if int(n) >= m.atOnce {
			m.gather.Do(func() { close(m.together) })
		}
		select {
		case <-m.together:
		case <-time.After(10 * time.Second):
			return client.PushResult{}, fmt.Errorf("%d pushes never were under way together", m.atOnce)
		}
	}
	if m.onPush != nil {
		m.onPush()
	}
	if err := ctx.Err(); err != nil {
		return client.PushResult{}, err
	}
	keys, err := fstree.ReachableKeys(root, store.objects.Get)
	if err != nil {
		return client.PushResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stores, m.packDirs = append(m.stores, store.dir), append(m.packDirs, opts.TempDir)
	p := opts.Progress
	p.Begin("uploading", uint64(len(keys)), client.Objects)
	for _, k := range keys {
		data, err := store.objects.Get(k)
		if err != nil {
			return client.PushResult{}, err
		}
		m.objects[k] = data
		p.Advance(1)
	}
	if m.pushErr != nil {
		return client.PushResult{}, m.pushErr
	}
	if err := m.refused[name]; err != nil {
		return client.PushResult{}, err
	}
	p.End("")
	m.refs[name] = root
	return client.PushResult{Root: root, Objects: uint64(len(keys)), DataSize: 4096}, nil
}

func (m *memory) Pull(_ context.Context, store *localStore, name string, opts client.PullOptions) (client.PullResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stores = append(m.stores, store.dir)
	p := opts.Progress
	p.Begin("looking up the reference", 0, client.NoUnit)
	root, ok := m.refs[name]
	if !ok {
		return client.PullResult{}, client.ErrNotFound
	}
	if opts.Accept != nil {
		if err := opts.Accept(root); err != nil {
			return client.PullResult{}, err
		}
	}
	p.End("in one pack")
	m.fetched++
	keys, err := fstree.ReachableKeys(root, func(k key.Key) ([]byte, error) { return m.objects[k], nil })
	if err != nil {
		return client.PullResult{}, err
	}
	for _, k := range keys {
		if err := store.objects.PutVerified(k, m.objects[k]); err != nil {
			return client.PullResult{}, err
		}
	}
	return client.PullResult{Root: root, Packs: 1, Objects: len(keys)}, nil
}

func (m *memory) List(_ context.Context, prefix string) ([]client.Ref, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listed = append(m.listed, prefix)
	if m.listErr != nil {
		return nil, m.listErr
	}
	var refs []client.Ref
	for name, root := range m.refs {
		if strings.HasPrefix(name, prefix) {
			refs = append(refs, client.Ref{Name: name, Root: root})
		}
	}
	slices.SortFunc(refs, func(a, b client.Ref) int { return cmp.Compare(a.Name, b.Name) })
	return refs, nil
}

func (m *memory) Delete(context.Context, string) error { return nil }
func (m *memory) Close() error                         { return nil }

// against runs the command with args against m and returns what it wrote
// to standard output and to standard error.
func against(t *testing.T, m *memory, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	return under(t, context.Background(), nil, m, args...)
}

// under is against with a context to run under and variables to run with.
func under(t *testing.T, ctx context.Context, env map[string]string, m *memory, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	for _, name := range append(variables, "JACCARD_NO_IGNORE", "JACCARD_TEMP_DIR", "JACCARD_PREFIX", "JACCARD_JOBS", "JACCARD_SKIP_EXISTING") {
		t.Setenv(name, "")
		os.Unsetenv(name)
	}
	for name, value := range env {
		t.Setenv(name, value)
	}
	var out, errOut bytes.Buffer
	app := newApp(&out, &errOut, func(context.Context, settings) (remote, error) {
		m.connected++
		return m, nil
	})
	err = app.RunContext(ctx, append([]string{"jaccard-store"}, args...))
	return out.String(), errOut.String(), err
}

// sourceTree makes a directory with what a tree is made of: files large
// and small, one that may be executed, directories in directories, an
// empty one, a symbolic link.
func sourceTree(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "source")
	for name, content := range map[string]string{
		"README":           "a tree to push\n",
		"bin/tool":         "#!/bin/sh\necho tool\n",
		"docs/guide.txt":   strings.Repeat("a line of the guide\n", 5000),
		"docs/deep/er.txt": "deeper\n",
		"empty.txt":        "",
	} {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(filepath.Join(dir, "bin/tool"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "hollow"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("docs/guide.txt", filepath.Join(dir, "guide")); err != nil {
		t.Fatal(err)
	}
	return dir
}

// listing describes everything under dir: for every path its kind, its
// permissions, when it was written and what is in it.
func listing(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		if rel == "." {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		desc := info.Mode().String()
		switch {
		case info.Mode()&fs.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			desc += " -> " + target
		case info.Mode().IsRegular():
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			desc += " " + info.ModTime().UTC().String() + " " + string(content)
		default:
			desc += " " + info.ModTime().UTC().String()
		}
		out[rel] = desc
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func sameListing(t *testing.T, want, got map[string]string) {
	t.Helper()
	for path, w := range want {
		g, ok := got[path]
		if !ok {
			t.Errorf("%s is missing", path)
		} else if g != w {
			t.Errorf("%s:\n got %.200s\nwant %.200s", path, g, w)
		}
	}
	for path := range got {
		if _, ok := want[path]; !ok {
			t.Errorf("%s is there and should not be", path)
		}
	}
}

// empty fails the test unless nothing is left in the directory the
// temporary stores were made in.
func empty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Errorf("%s was left behind in %s", e.Name(), dir)
	}
}

func TestPushDirThenPullDir(t *testing.T) {
	src, temp, m := sourceTree(t), t.TempDir(), newMemory()
	out, shown, err := against(t, m, "push-dir", "--temp-dir", temp, src, "snap/one")
	if err != nil {
		t.Fatalf("push-dir: %v\n%s", err, shown)
	}
	root, ok := m.refs["snap/one"]
	if !ok || !strings.HasPrefix(out, "snap/one "+root.String()+": base pack, ") {
		t.Fatalf("pushed %v, said %q", m.refs, out)
	}
	// The store was made where it was told to, the pack is built inside
	// it, and nothing of either is left.
	if len(m.stores) != 1 || filepath.Dir(m.stores[0]) != temp || m.packDirs[0] != m.stores[0] {
		t.Fatalf("store %v, packs built in %v, want both in a directory under %s", m.stores, m.packDirs, temp)
	}
	empty(t, temp)
	for _, want := range []string{
		"connecting to the server (",
		"scanning the directory: 5 files, ",
		"importing: ",
		"uploading (",
		"cleaning up (",
		"done in ",
	} {
		if !strings.Contains(shown, want) {
			t.Errorf("standard error lacks %q:\n%s", want, shown)
		}
	}

	dest := filepath.Join(t.TempDir(), "not", "there", "yet")
	out, shown, err = against(t, m, "pull-dir", "--temp-dir", temp, "snap/one", dest)
	if err != nil {
		t.Fatalf("pull-dir: %v\n%s", err, shown)
	}
	if want := dest + " " + root.String() + ": 1 packs fetched, 5 files extracted ("; !strings.HasPrefix(out, want) {
		t.Fatalf("pull-dir said %q, want it to begin with %q", out, want)
	}
	sameListing(t, listing(t, src), listing(t, dest))
	empty(t, temp)
	// Nothing but the directory asked for is left beside it either.
	if entries, _ := os.ReadDir(filepath.Dir(dest)); len(entries) != 1 {
		t.Errorf("%d entries beside the extracted directory, want it alone", len(entries))
	}
	for _, want := range []string{
		"looking up the reference: in one pack (",
		"reading the tree: 5 files, ",
		"extracting: 5 files (",
		"cleaning up (",
		"done in ",
	} {
		if !strings.Contains(shown, want) {
			t.Errorf("standard error lacks %q:\n%s", want, shown)
		}
	}
}

func TestDirCommandsNeedNoStoreAndShowNothingWhenToldSo(t *testing.T) {
	src, m := sourceTree(t), newMemory()
	// No --store, no --temp-dir: the system's temporary directory serves.
	if _, shown, err := against(t, m, "push-dir", "--no-progress", src, "quiet"); err != nil || shown != "" {
		t.Fatalf("push-dir: %v, standard error %q", err, shown)
	}
	dest := filepath.Join(t.TempDir(), "out")
	if _, shown, err := against(t, m, "pull-dir", "--no-progress", "quiet", dest); err != nil || shown != "" {
		t.Fatalf("pull-dir: %v, standard error %q", err, shown)
	}
	sameListing(t, listing(t, src), listing(t, dest))
	for _, dir := range m.stores {
		if _, err := os.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("the temporary store %s is still there (%v)", dir, err)
		}
	}
}

func TestPullDirIntoAnEmptyDirectory(t *testing.T) {
	src, m := sourceTree(t), newMemory()
	if _, _, err := against(t, m, "push-dir", src, "snap"); err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	if _, shown, err := against(t, m, "pull-dir", "snap", dest); err != nil {
		t.Fatalf("pull-dir into an empty directory: %v\n%s", err, shown)
	}
	sameListing(t, listing(t, src), listing(t, dest))
}

func TestPullDirRefusesAPlaceThatIsTaken(t *testing.T) {
	src, m := sourceTree(t), newMemory()
	if _, _, err := against(t, m, "push-dir", src, "snap"); err != nil {
		t.Fatal(err)
	}
	m.connected = 0
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("mine"), 0o644); err != nil {
		t.Fatal(err)
	}
	for dest, want := range map[string]string{
		src:  "not empty",
		file: "not a directory",
	} {
		_, _, err := against(t, m, "pull-dir", "snap", dest)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("pull-dir to %s: %v, want a refusal that says %q", dest, err, want)
		}
	}
	// Refused before the server was as much as dialed.
	if m.connected != 0 || m.fetched != 0 {
		t.Errorf("the server was dialed %d times and fetched from %d times", m.connected, m.fetched)
	}
	if content, _ := os.ReadFile(file); string(content) != "mine" {
		t.Errorf("the file in the way now holds %q", content)
	}
}

func TestPullDirOfWhatIsNoDirectoryFetchesNothing(t *testing.T) {
	// A reference whose root is a single file's content: push can make
	// one, pull-dir cannot extract it.
	m, temp := newMemory(), t.TempDir()
	blob, err := fstree.EncodeBlob([]byte("one file and no directory"))
	if err != nil {
		t.Fatal(err)
	}
	m.objects[blob.Key], m.refs["file"] = blob.Bytes, blob.Key

	dest := filepath.Join(t.TempDir(), "out")
	_, shown, err := against(t, m, "pull-dir", "--temp-dir", temp, "file", dest)
	if err == nil || !strings.Contains(err.Error(), "not a directory tree") {
		t.Fatalf("pull-dir of a file: %v", err)
	}
	if m.fetched != 0 {
		t.Error("the pack was fetched before the root was looked at")
	}
	if _, err := os.Lstat(dest); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("%s was made (%v)", dest, err)
	}
	empty(t, temp)
	if !strings.Contains(shown, "looking up the reference: failed after ") || !strings.Contains(shown, "cleaning up (") || strings.Contains(shown, "done in") {
		t.Errorf("standard error:\n%s", shown)
	}

	if _, _, err := against(t, m, "pull-dir", "--temp-dir", temp, "no/such", dest); !errors.Is(err, client.ErrNotFound) {
		t.Errorf("pull-dir of a name the server lacks: %v", err)
	}
	empty(t, temp)
}

func TestPushDirRefusesBeforeItReads(t *testing.T) {
	src, m := sourceTree(t), newMemory()
	for _, args := range [][]string{
		{"push-dir", src},                                     // no name
		{"push-dir", src, "name", "more"},                     // too much
		{"push-dir", src, "bad@name"},                         // a name no server takes
		{"push-dir", filepath.Join(src, "README"), "name"},    // a file
		{"push-dir", filepath.Join(src, "missing"), "name"},   // nothing
		{"push-dir", "--min-dedup", "-1", src, "name"},        // no fraction
		{"pull-dir", "name"},                                  // no directory
		{"pull-dir", "name", filepath.Join(src, "x"), "more"}, // too much
	} {
		if _, _, err := against(t, m, args...); err == nil {
			t.Errorf("%v succeeded", args)
		}
	}
	if m.connected != 0 || len(m.stores) != 0 {
		t.Errorf("the server was dialed %d times and %d stores were made", m.connected, len(m.stores))
	}
}

func TestAFailedPushDirCleansUpAndMarksTheStep(t *testing.T) {
	src, temp, m := sourceTree(t), t.TempDir(), newMemory()
	m.pushErr = errors.New("the bucket answered 403")
	out, shown, err := against(t, m, "push-dir", "--temp-dir", temp, src, "snap")
	if !errors.Is(err, m.pushErr) || out != "" {
		t.Fatalf("push-dir = %q, %v", out, err)
	}
	empty(t, temp)
	failed, cleaned := strings.Index(shown, "uploading: failed after "), strings.Index(shown, "cleaning up (")
	if failed < 0 || cleaned < failed || strings.Contains(shown, "done in") {
		t.Errorf("standard error:\n%s", shown)
	}
}

func TestPushDirHonorsAmberignoreUnlessToldNotTo(t *testing.T) {
	src, m := sourceTree(t), newMemory()
	if err := os.WriteFile(filepath.Join(src, ".amberignore"), []byte("docs/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := against(t, m, "push-dir", src, "ignoring"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := against(t, m, "push-dir", "--no-ignore", src, "whole"); err != nil {
		t.Fatal(err)
	}
	if m.refs["ignoring"] == m.refs["whole"] {
		t.Fatal("--no-ignore changed nothing")
	}
	whole, ignoring := filepath.Join(t.TempDir(), "whole"), filepath.Join(t.TempDir(), "ignoring")
	if _, _, err := against(t, m, "pull-dir", "whole", whole); err != nil {
		t.Fatal(err)
	}
	if _, _, err := against(t, m, "pull-dir", "ignoring", ignoring); err != nil {
		t.Fatal(err)
	}
	sameListing(t, listing(t, src), listing(t, whole))
	if _, err := os.Stat(filepath.Join(ignoring, "docs")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("docs was pushed although .amberignore names it (%v)", err)
	}
	if _, err := os.Stat(filepath.Join(ignoring, "README")); err != nil {
		t.Errorf("README: %v", err)
	}
}

// scratchStore is a packstore for a test of the two halves on their own.
func scratchStore(t *testing.T) *packstore.Store {
	t.Helper()
	objects, err := packstore.Open(filepath.Join(t.TempDir(), "packstore"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { objects.Close() })
	return objects
}

func TestAnInterruptedImportStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := importDir(ctx, scratchStore(t), sourceTree(t), ingestDefaults, newProgress(nil, true))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("importDir under a context that is over: %v", err)
	}
}

func TestAnExtractionThatFailsLeavesNothing(t *testing.T) {
	src, whole := sourceTree(t), scratchStore(t)
	quiet := newProgress(nil, true)
	root, err := importDir(context.Background(), whole, src, ingestDefaults, quiet)
	if err != nil {
		t.Fatal(err)
	}
	// A store with the directories of the tree and none of the content:
	// the tree can be read, the files cannot be written.
	hollow := scratchStore(t)
	keys, err := fstree.ReachableKeys(root, whole.Get)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if k.Type() == key.Blob {
			continue
		}
		data, err := whole.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		if err := hollow.PutVerified(k, data); err != nil {
			t.Fatal(err)
		}
	}
	parent := t.TempDir()
	dest := filepath.Join(parent, "out")
	if _, _, err := extract(context.Background(), hollow, root, dest, quiet); err == nil {
		t.Fatal("extracted a tree whose content is missing")
	}
	empty(t, parent)

	// Interrupted, it leaves nothing either.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := extract(ctx, whole, root, dest, quiet); !errors.Is(err, context.Canceled) {
		t.Fatalf("extract under a context that is over: %v", err)
	}
	empty(t, parent)

	// And whole, it is the tree.
	files, size, err := extract(context.Background(), whole, root, dest, quiet)
	if err != nil || files != 5 {
		t.Fatalf("extract = %d files, %d bytes, %v", files, size, err)
	}
	sameListing(t, listing(t, src), listing(t, dest))
}
