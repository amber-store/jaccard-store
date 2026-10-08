// Package sketch summarizes a set of keys by its lowest ones. Keys lead with
// their hash, so the lowest Size keys of a set are a uniform sample of it (a
// bottom-k MinHash sketch), and two sketches are enough to estimate the Jaccard
// similarity of the sets they were taken from.
package sketch

import (
	"slices"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/keyset"
)

// Size is the number of keys a sketch keeps.
const Size = 256

// Sketch is the lowest Size keys of a set, ascending. A set of fewer keys is
// its own sketch.
type Sketch []key.Key

// Of returns the sketch of the set sorted, which must be ascending and free of
// duplicates (keyset.Normalize).
func Of(sorted []key.Key) Sketch {
	return slices.Clone(sorted[:min(len(sorted), Size)])
}

// Jaccard estimates the Jaccard similarity |A∩B| / |A∪B| of the sets a and b
// were taken from. A sketch of Size keys says nothing about its set beyond its
// last key, so the estimate looks at the lowest Size keys of the union only,
// which both sketches cover; when neither sketch is full the two sets are known
// whole and the result is exact.
func Jaccard(a, b Sketch) float64 {
	limit := len(a) + len(b)
	if len(a) >= Size || len(b) >= Size {
		limit = Size
	}
	var union, shared int
	for i, j := 0, 0; union < limit && (i < len(a) || j < len(b)); union++ {
		switch {
		case i == len(a):
			j++
		case j == len(b):
			i++
		default:
			switch c := keyset.Compare(a[i], b[j]); {
			case c < 0:
				i++
			case c > 0:
				j++
			default:
				shared++
				i++
				j++
			}
		}
	}
	if union == 0 {
		return 0
	}
	return float64(shared) / float64(union)
}
