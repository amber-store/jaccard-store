package verify

import (
	"bytes"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/keyset"
	"github.com/amber-store/jaccard-store/packfile"
)

// keySet returns the keys of every object reachable from root, found with
// ChildKeys alone. Every object must be in objs.
func keySet(t testing.TB, root key.Key, objs objects) map[key.Key]bool {
	t.Helper()
	set := map[key.Key]bool{}
	var visit func(k key.Key)
	visit = func(k key.Key) {
		if set[k] {
			return
		}
		set[k] = true
		data, ok := objs[k]
		if !ok {
			t.Fatalf("object %s is missing from the test's objects", k)
		}
		children, err := fstree.ChildKeys(k, data)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range children {
			visit(c)
		}
	}
	visit(root)
	return set
}

// sorted returns the keys of set in ascending order.
func sorted(set map[key.Key]bool) []key.Key {
	return keyset.Normalize(slices.Collect(maps.Keys(set)))
}

// build lays the objects of keys out back to back, in reverse key order so
// that offsets and positions disagree, and returns the index and the
// uncompressed stream.
func build(t testing.TB, objs objects, keys []key.Key) (*packfile.Index, []byte) {
	t.Helper()
	var entries []packfile.Entry
	var data []byte
	for _, k := range slices.Backward(keys) {
		entries = append(entries, packfile.Entry{Key: k, Offset: uint64(len(data)), Length: uint32(len(objs[k]))})
		data = append(data, objs[k]...)
	}
	x, err := packfile.NewIndex(entries)
	if err != nil {
		t.Fatal(err)
	}
	return x, data
}

// base builds and verifies the base pack of root.
func base(t testing.TB, root key.Key, objs objects) (*packfile.Index, *packfile.Links) {
	t.Helper()
	x, data := build(t, objs, sorted(keySet(t, root, objs)))
	res, err := Pack(root, x, bytes.NewReader(data), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return x, res.Links
}

// without returns keys less k.
func without(keys []key.Key, k key.Key) []key.Key {
	return slices.DeleteFunc(slices.Clone(keys), func(o key.Key) bool { return o == k })
}

// wantMalformed fails the test unless err wraps ErrMalformed and mentions
// what.
func wantMalformed(t testing.TB, err error, what string) {
	t.Helper()
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed", err)
	}
	if !strings.Contains(err.Error(), what) {
		t.Errorf("error %q does not mention %q", err, what)
	}
}

func TestBasePack(t *testing.T) {
	_, root, objs := ingestTree(t)
	keys := sorted(keySet(t, root, objs))
	if len(keys) != len(objs) {
		t.Fatalf("the tree has %d objects, %d of them reachable", len(objs), len(keys))
	}
	x, data := build(t, objs, keys)
	res, err := Pack(root, x, bytes.NewReader(data), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Objects != uint64(len(keys)) || res.Bytes != uint64(len(data)) {
		t.Errorf("measured %d objects of %d bytes, want %d of %d", res.Objects, res.Bytes, len(keys), len(data))
	}
	if res.SharedObjects != 0 || res.SharedBytes != 0 {
		t.Errorf("a base pack shares %d objects of %d bytes", res.SharedObjects, res.SharedBytes)
	}
	if res.Links == nil || res.Links.Len() != x.Len() {
		t.Fatalf("links: %v", res.Links)
	}
	linked := 0
	for i := range x.Len() {
		e := x.Entry(i)
		children, err := fstree.ChildKeys(e.Key, objs[e.Key])
		if err != nil {
			t.Fatal(err)
		}
		var want []uint32
		for _, c := range keyset.Normalize(children) {
			pos, ok := x.Find(c)
			if !ok {
				t.Fatalf("child %s is not in the pack", c)
			}
			want = append(want, uint32(pos))
		}
		if got := res.Links.Children(i); !slices.Equal(got, want) {
			t.Errorf("links of %s %s: %v, want %v", e.Key.Type(), e.Key, got, want)
		}
		linked += len(want)
	}
	if linked < len(keys)-1 {
		t.Errorf("%d links cannot reach %d objects", linked, len(keys))
	}
	parsed, err := packfile.ParseLinks(res.Links.Encode(), x.Len())
	if err != nil {
		t.Fatalf("the links do not parse: %v", err)
	}
	for i := range x.Len() {
		if !slices.Equal(parsed.Children(i), res.Links.Children(i)) {
			t.Errorf("links of position %d changed in the round trip", i)
		}
	}
}

func TestBasePackOfOneObject(t *testing.T) {
	path := filepath.Join(t.TempDir(), "one.txt")
	if err := os.WriteFile(path, []byte("a reference of a single object"), 0o644); err != nil {
		t.Fatal(err)
	}
	root, objs := ingestPath(t, path)
	if len(objs) != 1 {
		t.Fatalf("a small file is %d objects", len(objs))
	}
	x, data := build(t, objs, []key.Key{root})
	res, err := Pack(root, x, bytes.NewReader(data), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Objects != 1 || res.Bytes != uint64(len(data)) || res.SharedObjects != 0 || res.SharedBytes != 0 {
		t.Errorf("measured %+v", res)
	}
	if res.Links == nil || res.Links.Len() != 1 || len(res.Links.Children(0)) != 0 {
		t.Errorf("links: %v", res.Links)
	}
}

func TestBasePackRefused(t *testing.T) {
	_, root, objs := ingestTree(t)
	keys := sorted(keySet(t, root, objs))
	leaf, _ := ofType(t, objs, key.Blob)
	var inner key.Key
	for _, k := range keys {
		if k.Type() == key.DirLeaf && k != root {
			inner = k
			break
		}
	}
	if inner == (key.Key{}) {
		t.Fatal("the tree has no directory leaf beneath its root")
	}
	extra := []byte("an object nothing refers to")
	extraKey, err := key.New(key.Blob, uint64(len(extra)), extra)
	if err != nil {
		t.Fatal(err)
	}
	objs[extraKey] = extra

	t.Run("a leaf left out", func(t *testing.T) {
		x, data := build(t, objs, without(keys, leaf))
		_, err := Pack(root, x, bytes.NewReader(data), nil, nil)
		wantMalformed(t, err, "missing")
	})
	t.Run("an inner object left out", func(t *testing.T) {
		x, data := build(t, objs, without(keys, inner))
		_, err := Pack(root, x, bytes.NewReader(data), nil, nil)
		wantMalformed(t, err, "missing")
	})
	t.Run("one object extra", func(t *testing.T) {
		x, data := build(t, objs, append(slices.Clone(keys), extraKey))
		_, err := Pack(root, x, bytes.NewReader(data), nil, nil)
		wantMalformed(t, err, "not reachable")
	})
	t.Run("altered bytes", func(t *testing.T) {
		for _, k := range []key.Key{leaf, inner, root} {
			x, data := build(t, objs, keys)
			pos, _ := x.Find(k)
			e := x.Entry(pos)
			data[e.Offset+uint64(e.Length)/2] ^= 0x01
			_, err := Pack(root, x, bytes.NewReader(data), nil, nil)
			wantMalformed(t, err, k.String())
		}
	})
	t.Run("the root left out", func(t *testing.T) {
		x, data := build(t, objs, without(keys, root))
		_, err := Pack(root, x, bytes.NewReader(data), nil, nil)
		wantMalformed(t, err, "is not in the pack")
	})
	t.Run("another root", func(t *testing.T) {
		x, data := build(t, objs, keys)
		_, err := Pack(extraKey, x, bytes.NewReader(data), nil, nil)
		wantMalformed(t, err, "is not in the pack")
	})
	t.Run("a root that is not the top", func(t *testing.T) {
		x, data := build(t, objs, keys)
		_, err := Pack(inner, x, bytes.NewReader(data), nil, nil)
		wantMalformed(t, err, "not reachable")
	})
	t.Run("an empty pack", func(t *testing.T) {
		x, data := build(t, objs, nil)
		_, err := Pack(root, x, bytes.NewReader(data), nil, nil)
		wantMalformed(t, err, "is not in the pack")
	})
}

// changeTree changes the tree of ingestTree in place: a new directory holds
// a file of new content, a second copy of big.bin and a third copy of the
// content "shared"; one old file changes; the directory "other" stays as it
// was.
func changeTree(t testing.TB, dir string) {
	t.Helper()
	writeFiles(t, dir, map[string][]byte{
		"new/fresh.txt":  []byte("content the base never saw"),
		"new/big.bin":    treeFiles()["big.bin"],
		"new/again.txt":  []byte("shared"),
		"dir/b.txt":      []byte("changed in the second version"),
		"new/fresh2.bin": noise(2, 100<<10),
	})
}

// patchOf returns the keys of root's key set that the parent lacks, and the
// objects and bytes of the key set that the parent holds.
func patchOf(t testing.TB, root key.Key, objs objects, parent *packfile.Index) (own []key.Key, shared, sharedBytes uint64) {
	t.Helper()
	for _, k := range sorted(keySet(t, root, objs)) {
		if pos, ok := parent.Find(k); ok {
			shared++
			sharedBytes += uint64(parent.Entry(pos).Length)
			if int(parent.Entry(pos).Length) != len(objs[k]) {
				t.Fatalf("object %s has two lengths", k)
			}
		} else {
			own = append(own, k)
		}
	}
	return own, shared, sharedBytes
}

func TestPatchPack(t *testing.T) {
	dir, root1, objs := ingestTree(t)
	parent, links := base(t, root1, objs)
	changeTree(t, dir)
	root2, objs2 := ingestPath(t, dir)
	maps.Copy(objs, objs2)

	own, shared, sharedBytes := patchOf(t, root2, objs, parent)
	if len(own) == 0 || shared == 0 || shared == uint64(parent.Len()) {
		t.Fatalf("the versions do not make a patch: %d own, %d shared of %d", len(own), shared, parent.Len())
	}
	x, data := build(t, objs, own)
	res, err := Pack(root2, x, bytes.NewReader(data), parent, links)
	if err != nil {
		t.Fatal(err)
	}
	if res.Objects != uint64(len(own)) || res.Bytes != uint64(len(data)) {
		t.Errorf("measured %d objects of %d bytes, want %d of %d", res.Objects, res.Bytes, len(own), len(data))
	}
	if res.SharedObjects != shared || res.SharedBytes != sharedBytes {
		t.Errorf("shared %d objects of %d bytes, want %d of %d", res.SharedObjects, res.SharedBytes, shared, sharedBytes)
	}
	if res.Links != nil {
		t.Error("a patch pack has links")
	}
}

// The file node of big.bin and the blob "shared" are each referred to by two
// objects of the patch, the blob by an object of the parent as well, and the
// chunks of big.bin are reached only through the parent's links.
func TestPatchPackCountsAParentObjectOnce(t *testing.T) {
	dir, root1, objs := ingestTree(t)
	parent, links := base(t, root1, objs)
	changeTree(t, dir)
	root2, objs2 := ingestPath(t, dir)
	maps.Copy(objs, objs2)
	own, shared, sharedBytes := patchOf(t, root2, objs, parent)

	referrers := map[key.Key]int{}
	for _, k := range own {
		children, err := fstree.ChildKeys(k, objs[k])
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range keyset.Normalize(children) {
			if parent.Has(c) {
				referrers[c]++
			}
		}
	}
	twice, inner := 0, 0
	for k, n := range referrers {
		if n > 1 {
			twice++
		}
		if k.Type() != key.Blob {
			inner++
		}
	}
	if twice < 2 || inner == 0 {
		t.Fatalf("the tree does not make the case: %d parent objects with two referrers, %d of them not blobs", twice, inner)
	}
	if uint64(len(referrers)) >= shared {
		t.Fatalf("all %d shared objects are referred to by the patch itself", shared)
	}

	x, data := build(t, objs, own)
	res, err := Pack(root2, x, bytes.NewReader(data), parent, links)
	if err != nil {
		t.Fatal(err)
	}
	if res.SharedObjects != shared || res.SharedBytes != sharedBytes {
		t.Errorf("shared %d objects of %d bytes, want %d of %d", res.SharedObjects, res.SharedBytes, shared, sharedBytes)
	}
}

func TestPatchPackRefused(t *testing.T) {
	dir, root1, objs := ingestTree(t)
	parent, links := base(t, root1, objs)
	changeTree(t, dir)
	root2, objs2 := ingestPath(t, dir)
	maps.Copy(objs, objs2)
	own, _, _ := patchOf(t, root2, objs, parent)
	var child key.Key
	for _, k := range own {
		if k != root2 && k.Type() == key.Blob {
			child = k
		}
	}
	held := parent.Entry(0).Key

	t.Run("a key also in the parent", func(t *testing.T) {
		x, data := build(t, objs, append(slices.Clone(own), held))
		_, err := Pack(root2, x, bytes.NewReader(data), parent, links)
		wantMalformed(t, err, "also in the parent")
	})
	t.Run("the whole key set", func(t *testing.T) {
		x, data := build(t, objs, sorted(keySet(t, root2, objs)))
		_, err := Pack(root2, x, bytes.NewReader(data), parent, links)
		wantMalformed(t, err, "also in the parent")
	})
	t.Run("a child in neither", func(t *testing.T) {
		x, data := build(t, objs, without(own, child))
		_, err := Pack(root2, x, bytes.NewReader(data), parent, links)
		wantMalformed(t, err, "missing")
	})
	t.Run("the root in neither", func(t *testing.T) {
		x, data := build(t, objs, without(own, root2))
		_, err := Pack(root2, x, bytes.NewReader(data), parent, links)
		wantMalformed(t, err, "is not in the pack")
	})
	t.Run("the root in the parent, the pack not empty", func(t *testing.T) {
		x, data := build(t, objs, own)
		_, err := Pack(root1, x, bytes.NewReader(data), parent, links)
		wantMalformed(t, err, "is not in the pack")
	})
	t.Run("altered bytes", func(t *testing.T) {
		x, data := build(t, objs, own)
		pos, _ := x.Find(child)
		data[x.Entry(pos).Offset] ^= 0x80
		_, err := Pack(root2, x, bytes.NewReader(data), parent, links)
		wantMalformed(t, err, child.String())
	})
}

// A parent that was recorded without being verified has no links. A patch
// pack on it is verified as any other, since the walk never goes into the
// parent; what it shares with the parent is all that is not measured.
func TestPatchPackOfAParentWithoutLinks(t *testing.T) {
	dir, root1, objs := ingestTree(t)
	parent, _ := base(t, root1, objs)
	changeTree(t, dir)
	root2, objs2 := ingestPath(t, dir)
	maps.Copy(objs, objs2)
	own, shared, _ := patchOf(t, root2, objs, parent)
	if len(own) == 0 || shared == 0 {
		t.Fatalf("the versions do not make a patch: %d own, %d shared", len(own), shared)
	}

	x, data := build(t, objs, own)
	res, err := Pack(root2, x, bytes.NewReader(data), parent, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Objects != uint64(len(own)) || res.Bytes != uint64(len(data)) {
		t.Errorf("measured %d objects of %d bytes, want %d of %d", res.Objects, res.Bytes, len(own), len(data))
	}
	if res.SharedObjects != 0 || res.SharedBytes != 0 || res.Links != nil {
		t.Errorf("without the parent's links: shared %d objects of %d bytes, links %v", res.SharedObjects, res.SharedBytes, res.Links)
	}

	var child key.Key
	for _, k := range own {
		if k != root2 && k.Type() == key.Blob {
			child = k
		}
	}
	t.Run("a child in neither", func(t *testing.T) {
		x, data := build(t, objs, without(own, child))
		_, err := Pack(root2, x, bytes.NewReader(data), parent, nil)
		wantMalformed(t, err, "missing")
	})
	t.Run("a key also in the parent", func(t *testing.T) {
		x, data := build(t, objs, append(slices.Clone(own), parent.Entry(0).Key))
		_, err := Pack(root2, x, bytes.NewReader(data), parent, nil)
		wantMalformed(t, err, "also in the parent")
	})
	t.Run("altered bytes", func(t *testing.T) {
		x, data := build(t, objs, own)
		pos, _ := x.Find(child)
		data[x.Entry(pos).Offset] ^= 0x80
		_, err := Pack(root2, x, bytes.NewReader(data), parent, nil)
		wantMalformed(t, err, child.String())
	})
	t.Run("an empty pack of a root in the parent", func(t *testing.T) {
		x, data := build(t, objs, nil)
		if _, err := Pack(root1, x, bytes.NewReader(data), parent, nil); err != nil {
			t.Fatal(err)
		}
	})
}

func TestEmptyPatchPack(t *testing.T) {
	_, root1, objs := ingestTree(t)
	parent, links := base(t, root1, objs)
	empty, data := build(t, objs, nil)

	t.Run("the root of the parent", func(t *testing.T) {
		res, err := Pack(root1, empty, bytes.NewReader(data), parent, links)
		if err != nil {
			t.Fatal(err)
		}
		if res.Objects != 0 || res.Bytes != 0 || res.Links != nil {
			t.Errorf("measured %+v", res)
		}
		if res.SharedObjects != uint64(parent.Len()) || res.SharedBytes != parent.DataSize() {
			t.Errorf("shared %d objects of %d bytes, want all %d of %d", res.SharedObjects, res.SharedBytes, parent.Len(), parent.DataSize())
		}
	})
	t.Run("an object beneath it", func(t *testing.T) {
		for _, k := range sorted(keySet(t, root1, objs)) {
			if k == root1 {
				continue
			}
			_, shared, sharedBytes := patchOf(t, k, objs, parent)
			res, err := Pack(k, empty, bytes.NewReader(data), parent, links)
			if err != nil {
				t.Fatalf("%s %s: %v", k.Type(), k, err)
			}
			if res.SharedObjects != shared || res.SharedBytes != sharedBytes {
				t.Errorf("%s %s: shared %d objects of %d bytes, want %d of %d", k.Type(), k, res.SharedObjects, res.SharedBytes, shared, sharedBytes)
			}
			if shared >= uint64(parent.Len()) {
				t.Errorf("%s %s reaches the whole parent", k.Type(), k)
			}
		}
	})
	t.Run("a root that is not in the parent", func(t *testing.T) {
		stranger, err := key.New(key.Blob, 8, []byte("stranger"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = Pack(stranger, empty, bytes.NewReader(data), parent, links)
		wantMalformed(t, err, "is not in the parent")
	})
}

type brokenReaderAt struct{ err error }

func (b brokenReaderAt) ReadAt([]byte, int64) (int, error) { return 0, b.err }

func TestPackPassesOnReadErrors(t *testing.T) {
	_, root, objs := ingestTree(t)
	x, data := build(t, objs, sorted(keySet(t, root, objs)))

	broken := errors.New("scratch file gone")
	if _, err := Pack(root, x, brokenReaderAt{broken}, nil, nil); err != broken {
		t.Errorf("err = %v, want the reader's error", err)
	}
	_, err := Pack(root, x, bytes.NewReader(data[:len(data)-1]), nil, nil)
	if err == nil || errors.Is(err, ErrMalformed) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("data one byte short: %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestPackWantsLinksThatAreTheParents(t *testing.T) {
	_, root, objs := ingestTree(t)
	parent, links := base(t, root, objs)
	empty, data := build(t, objs, nil)
	for _, err := range []error{
		func() error { _, err := Pack(root, empty, bytes.NewReader(data), nil, links); return err }(),
		func() error {
			_, err := Pack(root, empty, bytes.NewReader(data), parent, packfile.NewLinks(nil))
			return err
		}(),
	} {
		if err == nil || errors.Is(err, ErrMalformed) {
			t.Errorf("err = %v, want an error that does not blame the pack", err)
		}
	}
}

// A pack written by packfile.Writer and expanded by packfile.Expand is what
// Pack reads: the path a pushed pack takes on the server.
func TestPackThroughWriterAndExpand(t *testing.T) {
	dir, root1, objs := ingestTree(t)
	write := func(keys []key.Key) (*packfile.Index, []byte) {
		t.Helper()
		var compressed, expanded bytes.Buffer
		w, err := packfile.NewWriter(&compressed)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range slices.Backward(keys) {
			if err := w.Add(k, objs[k]); err != nil {
				t.Fatal(err)
			}
		}
		x, err := w.Finish()
		if err != nil {
			t.Fatal(err)
		}
		x, err = packfile.ParseIndex(x.Encode())
		if err != nil {
			t.Fatal(err)
		}
		if err := packfile.Expand(x, &compressed, &expanded); err != nil {
			t.Fatal(err)
		}
		return x, expanded.Bytes()
	}

	parent, data := write(sorted(keySet(t, root1, objs)))
	res, err := Pack(root1, parent, bytes.NewReader(data), nil, nil)
	if err != nil {
		t.Fatalf("base pack: %v", err)
	}
	links, err := packfile.ParseLinks(res.Links.Encode(), parent.Len())
	if err != nil {
		t.Fatal(err)
	}

	changeTree(t, dir)
	root2, objs2 := ingestPath(t, dir)
	maps.Copy(objs, objs2)
	own, shared, sharedBytes := patchOf(t, root2, objs, parent)
	x, data := write(own)
	res, err = Pack(root2, x, bytes.NewReader(data), parent, links)
	if err != nil {
		t.Fatalf("patch pack: %v", err)
	}
	if res.SharedObjects != shared || res.SharedBytes != sharedBytes {
		t.Errorf("shared %d objects of %d bytes, want %d of %d", res.SharedObjects, res.SharedBytes, shared, sharedBytes)
	}

	x, data = write(nil)
	res, err = Pack(root1, x, bytes.NewReader(data), parent, links)
	if err != nil {
		t.Fatalf("empty patch pack: %v", err)
	}
	if res.SharedObjects != uint64(parent.Len()) || res.SharedBytes != parent.DataSize() {
		t.Errorf("empty patch shares %d objects of %d bytes, want %d of %d", res.SharedObjects, res.SharedBytes, parent.Len(), parent.DataSize())
	}
}

// A reference may be a commit: the walk goes from it into its tree.
func TestBasePackOfACommit(t *testing.T) {
	_, tree, objs := ingestTree(t)
	root, data := testCommit(t, tree)
	objs[root] = data
	keys := sorted(keySet(t, root, objs))
	if len(keys) != len(objs) {
		t.Fatalf("the commit reaches %d of %d objects", len(keys), len(objs))
	}
	x, stream := build(t, objs, keys)
	res, err := Pack(root, x, bytes.NewReader(stream), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	rootPos, _ := x.Find(root)
	treePos, _ := x.Find(tree)
	if got := res.Links.Children(rootPos); !slices.Equal(got, []uint32{uint32(treePos)}) {
		t.Errorf("links of the commit: %v, want [%d]", got, treePos)
	}

	x, stream = build(t, objs, without(keys, tree))
	_, err = Pack(root, x, bytes.NewReader(stream), nil, nil)
	wantMalformed(t, err, "missing")
}
