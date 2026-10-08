package verify

import (
	"fmt"

	"github.com/amber-store/core/commit"
	"github.com/amber-store/core/key"
	"github.com/zeebo/blake3"
)

// Object checks data against k by core's rule, the one its packstore applies
// to an object it is sent: data must hash, with BLAKE3, to the hash in k;
// the length field of a Blob and an XattrSet must be the length of data; and
// the length field of a Commit must be its footprint, its own length plus
// that of the trees it records, which takes a commit that decodes. The
// length field of a FileNode, a DirLeaf and a DirNode is a logical size that
// the object's own bytes do not give, so only their hash is checked. Every
// error wraps ErrMalformed.
func Object(k key.Key, data []byte) error {
	sum := blake3.Sum256(data)
	want, err := key.NewFromHash(k.Type(), k.Length(), sum)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrMalformed, k, err)
	}
	if want != k {
		return fmt.Errorf("%w: object hashes to %s, not %s", ErrMalformed, want, k)
	}
	switch k.Type() {
	case key.Blob, key.XattrSet:
		if k.Length() != uint64(len(data)) {
			return fmt.Errorf("%w: %s: length field %d, object of %d bytes", ErrMalformed, k, k.Length(), len(data))
		}
	case key.Commit:
		c, err := commit.Decode(data)
		if err != nil {
			return fmt.Errorf("%w: %s: %v", ErrMalformed, k, err)
		}
		footprint, err := commit.Footprint(uint64(len(data)), c.Trees())
		if err != nil {
			return fmt.Errorf("%w: %s: %v", ErrMalformed, k, err)
		}
		if k.Length() != footprint {
			return fmt.Errorf("%w: %s: length field %d, footprint %d (own %d bytes plus its trees)", ErrMalformed, k, k.Length(), footprint, len(data))
		}
	}
	return nil
}
