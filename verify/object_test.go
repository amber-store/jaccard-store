package verify

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/amber-store/core/commit"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
)

// objects holds serialized objects by key.
type objects map[key.Key][]byte

// noise returns n bytes that do not compress, the same for the same seed.
func noise(seed byte, n int) []byte {
	b := make([]byte, n)
	rand.NewChaCha8([32]byte{seed}).Read(b)
	return b
}

// writeFiles writes files, named by slash-separated paths, under dir.
func writeFiles(t testing.TB, dir string, files map[string][]byte) {
	t.Helper()
	for name, data := range files {
		path := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// itemBits makes core cut directories and file indexes into runs of 8
// entries on average, where its default is 128, so that a tree of a few
// dozen files has directory nodes above its leaves.
const itemBits = 3

// ingestPath builds the tree of the directory or file at path with core and
// returns its root and every object it is made of.
func ingestPath(t testing.TB, path string) (key.Key, objects) {
	t.Helper()
	seq, root, err := ingest.Objects(path, ingest.Opts{NoIgnore: true, Chunk: ingest.ChunkOpts{ItemBits: itemBits}})
	if err != nil {
		t.Fatal(err)
	}
	objs := objects{}
	for o, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		objs[o.Key] = bytes.Clone(o.Bytes)
	}
	if _, ok := objs[*root]; !ok {
		t.Fatalf("root %s is not among the %d objects ingested", *root, len(objs))
	}
	return *root, objs
}

// treeFiles is a tree with a file of several chunks, an empty file, which
// core keeps as an empty Blob, nested directories, a directory of many files
// and one content, "shared", held by two files in different directories.
func treeFiles() map[string][]byte {
	files := map[string][]byte{
		"big.bin":       noise(1, 5<<19),
		"readme.txt":    []byte("a small file\n"),
		"empty.txt":     {},
		"dir/a.txt":     []byte("shared"),
		"dir/b.txt":     []byte("only in dir"),
		"dir/sub/c.txt": []byte("deep down"),
		"other/a.txt":   []byte("shared"),
		"other/d.txt":   []byte("only in other"),
	}
	for i := range 60 {
		files[fmt.Sprintf("many/%02d.txt", i)] = fmt.Appendf(nil, "file %d of many", i)
	}
	return files
}

// ingestTree ingests treeFiles from a fresh directory, which it returns for
// the tree to be changed and ingested again.
func ingestTree(t testing.TB) (dir string, root key.Key, objs objects) {
	t.Helper()
	dir = t.TempDir()
	writeFiles(t, dir, treeFiles())
	root, objs = ingestPath(t, dir)
	return dir, root, objs
}

// ofType returns one object of type typ from objs that is not empty, the
// lowest by key.
func ofType(t testing.TB, objs objects, typ key.Type) (key.Key, []byte) {
	t.Helper()
	var found *key.Key
	for k, data := range objs {
		if k.Type() == typ && len(data) > 0 && (found == nil || bytes.Compare(k[:], found[:]) < 0) {
			found = &k
		}
	}
	if found == nil {
		t.Fatalf("the tree has no %s", typ)
	}
	return *found, objs[*found]
}

// flipped returns data with one bit of its middle byte flipped, and one
// byte for no data.
func flipped(data []byte) []byte {
	if len(data) == 0 {
		return []byte{0x01}
	}
	out := bytes.Clone(data)
	out[len(out)/2] ^= 0x01
	return out
}

// testCommit returns a commit of tree as an object.
func testCommit(t testing.TB, tree key.Key) (key.Key, []byte) {
	t.Helper()
	who := commit.Identity{Name: "A Tester", Email: "tester@example.com", When: 1_700_000_000_000_000_000}
	k, data, err := commit.Commit{Tree: tree, Author: who, Committer: who, Message: "a commit"}.Object()
	if err != nil {
		t.Fatal(err)
	}
	return k, data
}

func TestObjectAcceptsEveryType(t *testing.T) {
	_, root, objs := ingestTree(t)
	seen := map[key.Type]int{}
	for k, data := range objs {
		if err := Object(k, data); err != nil {
			t.Errorf("%s %s: %v", k.Type(), k, err)
		}
		seen[k.Type()]++
	}
	for _, typ := range []key.Type{key.Blob, key.FileNode, key.DirLeaf, key.DirNode} {
		if seen[typ] == 0 {
			t.Errorf("the tree has no %s", typ)
		}
	}
	empty, err := key.New(key.Blob, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := objs[empty]; !ok {
		t.Error("the tree has no empty Blob")
	}

	xattrs := []byte("the bytes of spilled attributes")
	xk, err := key.New(key.XattrSet, uint64(len(xattrs)), xattrs)
	if err != nil {
		t.Fatal(err)
	}
	if err := Object(xk, xattrs); err != nil {
		t.Errorf("XattrSet: %v", err)
	}
	ck, cdata := testCommit(t, root)
	if err := Object(ck, cdata); err != nil {
		t.Errorf("Commit: %v", err)
	}
}

func TestObjectRefusesAFlippedByte(t *testing.T) {
	_, root, objs := ingestTree(t)
	ck, cdata := testCommit(t, root)
	objs[ck] = cdata
	for _, typ := range []key.Type{key.Blob, key.FileNode, key.DirLeaf, key.DirNode, key.Commit} {
		k, data := ofType(t, objs, typ)
		if err := Object(k, flipped(data)); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s with a flipped byte: %v, want ErrMalformed", typ, err)
		}
		if err := Object(k, data[:len(data)-1]); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s cut short: %v, want ErrMalformed", typ, err)
		}
	}
}

func TestObjectChecksTheLengthField(t *testing.T) {
	_, root, objs := ingestTree(t)
	_, blob := ofType(t, objs, key.Blob)
	_, node := ofType(t, objs, key.FileNode)
	_, cdata := testCommit(t, root)

	cases := []struct {
		name   string
		typ    key.Type
		length uint64
		data   []byte
		ok     bool
	}{
		{"a blob, its own length", key.Blob, uint64(len(blob)), blob, true},
		{"a blob, one more", key.Blob, uint64(len(blob)) + 1, blob, false},
		{"a blob, zero", key.Blob, 0, blob, false},
		{"an xattr set, its own length", key.XattrSet, uint64(len(blob)), blob, true},
		{"an xattr set, one less", key.XattrSet, uint64(len(blob)) - 1, blob, false},
		// The length of these is a logical size that only a walk of what is
		// beneath them could recompute; core checks their hash alone.
		{"a file node, another length", key.FileNode, 12345, node, true},
		{"a directory leaf, another length", key.DirLeaf, 12345, node, true},
		{"a commit, its own bytes without its tree", key.Commit, uint64(len(cdata)), cdata, false},
		{"a commit, its footprint", key.Commit, uint64(len(cdata)) + root.Length(), cdata, true},
		{"a commit that does not decode", key.Commit, uint64(len(blob)), blob, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k, err := key.New(tc.typ, tc.length, tc.data)
			if err != nil {
				t.Fatal(err)
			}
			err = Object(k, tc.data)
			if tc.ok && err != nil {
				t.Errorf("refused: %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrMalformed) {
				t.Errorf("err = %v, want ErrMalformed", err)
			}
		})
	}
}

// Object must decide as core does when it stores an object it was sent.
func TestObjectAgreesWithCore(t *testing.T) {
	_, root, objs := ingestTree(t)
	ck, cdata := testCommit(t, root)
	objs[ck] = cdata
	store, err := packstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	agree := func(what string, k key.Key, data []byte) {
		t.Helper()
		ours := Object(k, data)
		core := store.PutVerifiedDeferred(k, data)
		if core != nil && !errors.Is(core, packstore.ErrVerify) {
			t.Fatalf("%s: core failed otherwise: %v", what, core)
		}
		if (ours == nil) != (core == nil) {
			t.Errorf("%s %s: Object says %v, core says %v", what, k.Type(), ours, core)
		}
	}
	for k, data := range objs {
		agree("as ingested", k, data)
		agree("flipped", k, flipped(data))
		for _, typ := range []key.Type{key.Blob, key.FileNode, key.DirLeaf, key.DirNode, key.XattrSet, key.Commit} {
			for _, length := range []uint64{0, uint64(len(data)), uint64(len(data)) + 1, k.Length()} {
				other, err := key.New(typ, length, data)
				if err != nil {
					t.Fatal(err)
				}
				agree("retyped", other, data)
			}
		}
	}
}
