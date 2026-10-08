package sketch_test

import (
	"encoding/binary"
	"math"
	"slices"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/keyset"
	"github.com/amber-store/jaccard-store/sketch"
	"github.com/zeebo/blake3"
)

// testKey returns a canonical Blob key whose hash is derived from n, so keys
// are spread uniformly like real ones.
func testKey(t testing.TB, n int) key.Key {
	t.Helper()
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	k, err := key.NewFromHash(key.Blob, 1, blake3.Sum256(b[:]))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// keySet returns the sorted keys for the numbers in [from, to).
func keySet(t testing.TB, from, to int) []key.Key {
	t.Helper()
	ks := make([]key.Key, 0, to-from)
	for n := from; n < to; n++ {
		ks = append(ks, testKey(t, n))
	}
	slices.SortFunc(ks, keyset.Compare)
	return ks
}

func TestOfKeepsTheLowestKeysOfALargeSet(t *testing.T) {
	ks := keySet(t, 0, 1000)
	s := sketch.Of(ks)
	if len(s) != sketch.Size {
		t.Fatalf("len = %d, want %d", len(s), sketch.Size)
	}
	if !slices.Equal([]key.Key(s), ks[:sketch.Size]) {
		t.Fatal("sketch is not the lowest keys of the set")
	}
}

func TestOfKeepsEveryKeyOfASmallSet(t *testing.T) {
	ks := keySet(t, 0, 10)
	if s := sketch.Of(ks); !slices.Equal([]key.Key(s), ks) {
		t.Fatalf("sketch of a small set has %d keys, want all 10", len(s))
	}
}

func TestOfDoesNotAliasItsInput(t *testing.T) {
	ks := keySet(t, 0, 10)
	s := sketch.Of(ks)
	want := s[0]
	ks[0] = key.Key{}
	if s[0] != want {
		t.Fatal("sketch shares memory with the keys it was made of")
	}
}

func TestJaccardIsExactForSmallSets(t *testing.T) {
	a := sketch.Of(keySet(t, 0, 100))  // 0..99
	b := sketch.Of(keySet(t, 50, 150)) // 50..149: 50 shared of 150
	if got, want := sketch.Jaccard(a, b), 50.0/150.0; got != want {
		t.Fatalf("Jaccard = %v, want %v", got, want)
	}
}

func TestJaccardOfASetWithItself(t *testing.T) {
	a := sketch.Of(keySet(t, 0, 5000))
	if got := sketch.Jaccard(a, a); got != 1 {
		t.Fatalf("Jaccard = %v, want 1", got)
	}
}

func TestJaccardOfDisjointSets(t *testing.T) {
	a := sketch.Of(keySet(t, 0, 5000))
	b := sketch.Of(keySet(t, 5000, 10000))
	if got := sketch.Jaccard(a, b); got != 0 {
		t.Fatalf("Jaccard = %v, want 0", got)
	}
}

func TestJaccardOfEmptySketches(t *testing.T) {
	if got := sketch.Jaccard(nil, nil); got != 0 {
		t.Fatalf("Jaccard of two empty sketches = %v, want 0", got)
	}
	if got := sketch.Jaccard(sketch.Of(keySet(t, 0, 10)), nil); got != 0 {
		t.Fatalf("Jaccard against an empty sketch = %v, want 0", got)
	}
}

func TestJaccardEstimatesLargeSets(t *testing.T) {
	// 0..9999 against 5000..14999: 5000 shared of 15000, J = 1/3. With 256
	// samples the standard error is about 0.03.
	a := sketch.Of(keySet(t, 0, 10000))
	b := sketch.Of(keySet(t, 5000, 15000))
	if got := sketch.Jaccard(a, b); math.Abs(got-1.0/3.0) > 0.1 {
		t.Fatalf("Jaccard = %v, want about 0.333", got)
	}
}

func TestJaccardOfALargeSetAgainstASmallSubset(t *testing.T) {
	// The small set is wholly known, the large one only up to its 256th key:
	// the estimate must only count the part of the union both sketches cover.
	large := keySet(t, 0, 10000)
	small := []key.Key{large[10], large[20], large[9000]}
	got := sketch.Jaccard(sketch.Of(large), sketch.Of(small))
	if want := 2.0 / 256.0; got != want {
		t.Fatalf("Jaccard = %v, want %v", got, want)
	}
}
