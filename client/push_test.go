package client

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/jaccard-store/keyset"
	"github.com/amber-store/jaccard-store/packfile"
)

// The candidates of these tests stand for a reference of 1000 bytes.
const refSize = 1000

func named(c *candidate) string {
	if c == nil {
		return "none"
	}
	return string(c.root[:1])
}

// cand is a candidate named by one letter.
func cand(name byte, shared, bytes uint64, distance float64) *candidate {
	return &candidate{root: key.Key{name}, objects: 1, shared: shared, bytes: bytes, distance: distance}
}

func TestChoose(t *testing.T) {
	for _, tc := range []struct {
		name       string
		candidates []*candidate
		minDedup   float64
		want       string
	}{
		{"nothing offered", nil, 0.5, "none"},
		{"the one that holds enough", []*candidate{cand('a', 600, 700, 0.3)}, 0.5, "a"},
		{"exactly the threshold", []*candidate{cand('a', 500, 500, 0.3)}, 0.5, "a"},
		{"below the threshold", []*candidate{cand('a', 499, 499, 0.3)}, 0.5, "none"},
		{"no object in common", []*candidate{{root: key.Key{'a'}, bytes: 700}}, 0, "none"},
		{"an object of no bytes in common, and no threshold",
			[]*candidate{{root: key.Key{'a'}, objects: 1, bytes: 700}}, 0, "a"},
		{"a threshold nothing meets", []*candidate{cand('a', 1000, 1000, 0)}, 1.5, "none"},

		// A pull fetches the parent and the patch: 1600 + 400 is twice
		// the reference and no more.
		{"a pull of exactly twice the reference", []*candidate{cand('a', 600, 1600, 0.5)}, 0.5, "a"},
		{"a pull of a byte more than twice", []*candidate{cand('a', 600, 1601, 0.5)}, 0.5, "none"},
		{"the whole reference inside a parent twice its size", []*candidate{cand('a', 1000, 2000, 0.5)}, 0.5, "a"},
		{"the whole reference inside a larger parent", []*candidate{cand('a', 1000, 2001, 0.5)}, 0.5, "none"},
		{"too large whatever the threshold", []*candidate{cand('a', 600, 5000, 0.5)}, 0, "none"},

		{"the one that holds the most",
			[]*candidate{cand('a', 600, 700, 0.1), cand('b', 800, 900, 0.4), cand('c', 700, 800, 0.2)}, 0.5, "b"},
		{"the one that holds the most is too large: the next",
			[]*candidate{cand('a', 900, 5000, 0.1), cand('b', 700, 800, 0.4), cand('c', 800, 900, 0.5)}, 0.5, "c"},
		{"all that hold enough are too large",
			[]*candidate{cand('a', 900, 5000, 0.1), cand('b', 800, 4000, 0.2), cand('c', 100, 100, 0.3)}, 0.5, "none"},

		// As much in common: the nearer, whatever the order of the offer.
		{"a tie in bytes goes to the nearer",
			[]*candidate{cand('a', 700, 800, 0.4), cand('b', 700, 800, 0.2)}, 0.5, "b"},
		{"a tie in bytes goes to the nearer though it is larger",
			[]*candidate{cand('a', 700, 750, 0.4), cand('b', 700, 900, 0.2)}, 0.5, "b"},
		// As much in common and as near: the smaller.
		{"a tie in distance goes to the smaller",
			[]*candidate{cand('a', 700, 900, 0.25), cand('b', 700, 750, 0.25), cand('c', 700, 800, 0.25)}, 0.5, "b"},
		{"a tie in everything goes to the first offered",
			[]*candidate{cand('a', 700, 800, 0.25), cand('b', 700, 800, 0.25)}, 0.5, "a"},
		// The distance and the size only settle ties.
		{"more in common outweighs nearer and smaller",
			[]*candidate{cand('a', 700, 700, 0.1), cand('b', 701, 1500, 0.9)}, 0.5, "b"},
	} {
		if got := named(choose(tc.candidates, refSize, tc.minDedup)); got != tc.want {
			t.Errorf("%s: chose %s, want %s", tc.name, got, tc.want)
		}
	}
}

func TestChooseOfAnEmptyReference(t *testing.T) {
	// Nothing to hold a fraction of: only a threshold of nothing is met,
	// and only by a pack that is as empty.
	if c := choose([]*candidate{{root: key.Key{'a'}, objects: 1}}, 0, 0); c == nil {
		t.Error("an empty pack was not taken for an empty reference")
	}
	if c := choose([]*candidate{{root: key.Key{'a'}, objects: 1}}, 0, 0.5); c != nil {
		t.Error("an empty pack met a threshold for an empty reference")
	}
	if c := choose([]*candidate{{root: key.Key{'a'}, objects: 1, bytes: 1}}, 0, 0); c != nil {
		t.Error("a pack of a byte was taken for an empty reference")
	}
}

func TestVerdict(t *testing.T) {
	for _, tc := range []struct {
		candidates []*candidate
		want       string
	}{
		{nil, "nothing in common"},
		{[]*candidate{{root: key.Key{'a'}, bytes: 700}}, "nothing in common"},
		{[]*candidate{cand('a', 700, 800, 0.2)}, "a parent that holds 700 B of 1000 B"},
		{[]*candidate{cand('a', 100, 100, 0.2), cand('b', 300, 300, 0.4)}, "too little in common: 300 B of 1000 B"},
		// The pull that is named is the smallest there would be.
		{[]*candidate{cand('a', 900, 5000, 0.2), cand('b', 600, 3000, 0.4)},
			"too large a parent: a pull would fetch 3.32 KiB for 1000 B"},
	} {
		chosen := choose(tc.candidates, refSize, 0.5)
		if got := verdict(tc.candidates, chosen, refSize, 0.5); got != tc.want {
			t.Errorf("verdict = %q, want %q", got, tc.want)
		}
	}
}

// The size of a reference is added up without reading the objects that
// make up most of it. It has to be what reading them would give.
func TestTheSizeOfAKeySetIsThatOfItsObjects(t *testing.T) {
	src := t.TempDir()
	noise := make([]byte, 3<<20) // several chunks, so the file has an index of them
	rand.New(rand.NewSource(1)).Read(noise)
	for name, content := range map[string][]byte{
		"README":           []byte("a tree\n"),
		"empty":            nil,
		"big.bin":          noise,
		"docs/guide.txt":   []byte(strings.Repeat("a line of the guide\n", 4000)),
		"docs/a/b/c/d.txt": []byte("deep\n"),
		"twice/one.txt":    []byte("the same content\n"),
		"twice/two.txt":    []byte("the same content\n"),
	} {
		path := filepath.Join(src, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, content, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	objects, err := packstore.Open(filepath.Join(t.TempDir(), "packstore"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	root, _, err := ingest.Dir(objects, src, ingest.Opts{NoIgnore: true})
	if err != nil {
		t.Fatal(err)
	}

	keys, size, err := keySet(root, objects, silent{})
	if err != nil {
		t.Fatal(err)
	}
	var want uint64
	types := map[key.Type]int{}
	for _, k := range keys {
		data, err := objects.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		want += uint64(len(data))
		types[k.Type()]++
	}
	if size != want {
		t.Fatalf("keySet says %d bytes; the %d objects are %d", size, len(keys), want)
	}
	// The tree has to hold what the sum treats differently: objects that
	// are read on the way, and the two kinds that are not.
	if types[key.Blob] == 0 || types[key.FileNode] == 0 || types[key.DirLeaf]+types[key.DirNode] == 0 {
		t.Fatalf("the tree lacks a kind of object this test is about: %v", types)
	}
}

// objectMap is a source of objects that has no order of its own to read
// them in.
type objectMap map[key.Key][]byte

func (m objectMap) Get(k key.Key) ([]byte, error) {
	data, ok := m[k]
	if !ok {
		return nil, fmt.Errorf("no object %s", k)
	}
	return slices.Clone(data), nil
}

// backwards is a source that reads fastest in the reverse of the order it
// is asked about, and keeps what it was asked.
type backwards struct {
	objectMap
	asked [][]key.Key
}

func (b *backwards) SortByLocation(keys []key.Key) {
	b.asked = append(b.asked, slices.Clone(keys))
	slices.Reverse(keys)
}

// blobs returns n blobs that differ, none of them empty, and their keys in
// the order of a key set.
func blobs(t *testing.T, n int) (objectMap, []key.Key) {
	t.Helper()
	objects := objectMap{}
	var keys []key.Key
	for i := range n {
		blob, err := fstree.EncodeBlob(fmt.Appendf(nil, "blob %d of %d", i, n))
		if err != nil {
			t.Fatal(err)
		}
		objects[blob.Key] = blob.Bytes
		keys = append(keys, blob.Key)
	}
	return objects, keyset.Normalize(keys)
}

// built builds a pack as a push does and returns the keys of its objects
// in the order they lie in the data, having checked each object against
// what the source holds.
func built(t *testing.T, objects ObjectSource, keys []key.Key, parent *candidate) []key.Key {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "pack-*")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	index, err := build(context.Background(), f, objects, keys, parent, silent{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	var order []key.Key
	for o, err := range packfile.Objects(index, f) {
		if err != nil {
			t.Fatal(err)
		}
		want, err := objects.Get(o.Key)
		if err != nil {
			t.Fatal(err)
		}
		if string(o.Data) != string(want) {
			t.Fatalf("object %s is %q in the pack and %q in the source", o.Key, o.Data, want)
		}
		order = append(order, o.Key)
	}
	return order
}

// A source that knows where its objects lie is asked once, about the
// objects the pack is to hold and no others, and the pack is written in
// the order it answers.
func TestBuildOrdersByLocationWhenTheSourceCan(t *testing.T) {
	objects, keys := blobs(t, 6)
	// The parent holds two of the six: the pack's own are the other four.
	var inParent []packfile.Entry
	var offset uint64
	for _, k := range []key.Key{keys[1], keys[4]} {
		length := uint32(len(objects[k]))
		inParent = append(inParent, packfile.Entry{Key: k, Offset: offset, Length: length})
		offset += uint64(length)
	}
	parentIndex, err := packfile.NewIndex(inParent)
	if err != nil {
		t.Fatal(err)
	}
	own := []key.Key{keys[0], keys[2], keys[3], keys[5]}
	given := slices.Clone(keys)

	src := &backwards{objectMap: objects}
	order := built(t, src, keys, &candidate{index: parentIndex})

	if len(src.asked) != 1 {
		t.Fatalf("the source was asked for its order %d times, want once", len(src.asked))
	}
	if !slices.Equal(src.asked[0], own) {
		t.Fatalf("the source was asked about %v, want the pack's own keys %v", src.asked[0], own)
	}
	want := slices.Clone(own)
	slices.Reverse(want)
	if !slices.Equal(order, want) {
		t.Fatalf("the pack holds %v, want the source's order %v", order, want)
	}
	if !slices.Equal(keys, given) {
		t.Fatal("the caller's keys were put into another order")
	}

	// Without a parent the pack's own keys are all of them.
	src = &backwards{objectMap: objects}
	order = built(t, src, keys, nil)
	if len(src.asked) != 1 || !slices.Equal(src.asked[0], keys) {
		t.Fatalf("for a base pack the source was asked about %v, want every key once", src.asked)
	}
	want = slices.Clone(keys)
	slices.Reverse(want)
	if !slices.Equal(order, want) {
		t.Fatalf("the base pack holds %v, want the source's order %v", order, want)
	}
}

// A source that has no order of its own is read in the order of the keys
// it is given, which a push takes from the key set.
func TestBuildKeepsKeySetOrderOtherwise(t *testing.T) {
	objects, keys := blobs(t, 6)
	if order := built(t, objects, keys, nil); !slices.Equal(order, keys) {
		t.Fatalf("the pack holds %v, want the order of the key set %v", order, keys)
	}
	// It is the order given that is kept, not one build works out: an
	// index is in key order whatever the data is in.
	given := slices.Clone(keys)
	slices.Reverse(given)
	if order := built(t, objects, given, nil); !slices.Equal(order, given) {
		t.Fatalf("the pack holds %v, want the order given %v", order, given)
	}
}
