package client

import (
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
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
