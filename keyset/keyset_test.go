package keyset_test

import (
	"slices"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/keyset"
)

func TestCompareOrdersByBytes(t *testing.T) {
	lo, hi := key.Key{0: 1}, key.Key{0: 2}
	if keyset.Compare(lo, hi) >= 0 || keyset.Compare(hi, lo) <= 0 || keyset.Compare(lo, lo) != 0 {
		t.Fatal("Compare does not order keys by their bytes")
	}
}

func TestNormalizeSortsAndDropsDuplicates(t *testing.T) {
	a, b, c := key.Key{0: 1}, key.Key{0: 2}, key.Key{0: 3}
	got := keyset.Normalize([]key.Key{c, a, b, a, c})
	if !slices.Equal(got, []key.Key{a, b, c}) {
		t.Fatalf("Normalize = %v", got)
	}
}

func TestAscending(t *testing.T) {
	a, b := key.Key{1}, key.Key{2}
	for _, tc := range []struct {
		name string
		keys []key.Key
		want bool
	}{
		{"empty", nil, true},
		{"one", []key.Key{a}, true},
		{"ascending", []key.Key{a, b}, true},
		{"duplicate", []key.Key{a, a}, false},
		{"descending", []key.Key{b, a}, false},
	} {
		if got := keyset.Ascending(tc.keys); got != tc.want {
			t.Errorf("%s: Ascending = %v, want %v", tc.name, got, tc.want)
		}
	}
}
