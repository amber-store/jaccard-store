// Package keyset holds the ordering every part of jaccard-store agrees on: keys
// compare as their bytes. A core key leads with its hash, so that order is a
// uniform shuffle of the objects.
package keyset

import (
	"bytes"
	"slices"

	"github.com/amber-store/core/key"
)

// Compare orders keys by their bytes.
func Compare(a, b key.Key) int {
	return bytes.Compare(a[:], b[:])
}

// Normalize sorts keys ascending and drops duplicates, in place.
func Normalize(keys []key.Key) []key.Key {
	slices.SortFunc(keys, Compare)
	return slices.Compact(keys)
}

// Ascending reports whether keys are strictly ascending: sorted, and free of
// duplicates.
func Ascending(keys []key.Key) bool {
	for i := 1; i < len(keys); i++ {
		if Compare(keys[i-1], keys[i]) >= 0 {
			return false
		}
	}
	return true
}
