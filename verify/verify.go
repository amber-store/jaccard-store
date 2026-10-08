// Package verify decides whether an uploaded pack is what it claims to be,
// and measures the ones that are.
//
// A pack is sound when it holds exactly the objects its root needs and its
// parent lacks. The rule, given an index that already parsed and the
// uncompressed data it describes:
//
//   - No key of a patch pack is in its parent's index.
//   - The root is in the pack, unless the pack is empty and the root is in
//     the parent.
//   - Walking from the root, every object in the pack matches its key by
//     core's rule (Object) and every key it refers to is in the pack or in
//     the parent. The walk does not go into the parent: the key's presence
//     in the parent's index is enough, and the parent is taken as sound.
//   - The walk reaches every entry of the index.
//
// A sound base pack yields its links: the children of every object, as index
// positions. A sound patch pack is measured against its parent's links: the
// parent's objects reachable from the keys at which the walk crossed into
// the parent are what the reference shares with it.
package verify

import (
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/packfile"
)

// ErrMalformed wraps every error that says the pack, or an object, is not
// what it claims to be.
var ErrMalformed = errors.New("verify: malformed pack")

// Result is what a verified pack measures.
type Result struct {
	// Objects and Bytes are those of the pack, from its index.
	Objects, Bytes uint64
	// SharedObjects and SharedBytes are those of the reference that the
	// parent holds. Both are 0 for a base pack.
	SharedObjects, SharedBytes uint64
	// Links holds the children of every object of a base pack. It is nil for
	// a patch pack.
	Links *packfile.Links
}

// bitmap is a set of index positions.
type bitmap []uint64

func newBitmap(n int) bitmap { return make(bitmap, (n+63)/64) }

func (b bitmap) has(i int) bool { return b[i/64]&(1<<(i%64)) != 0 }

func (b bitmap) set(i int) { b[i/64] |= 1 << (i % 64) }

// Pack verifies the pack of root with index x and measures it. data is the
// uncompressed stream, x.DataSize() bytes long. parent and parentLinks are
// nil for a base pack and both set, to the index and the links of the
// parent, for a patch pack. A pack that fails is reported with an error
// wrapping ErrMalformed; any other error is a failure to read data, returned
// as it is, or a parent that was not given whole.
func Pack(root key.Key, x *packfile.Index, data io.ReaderAt, parent *packfile.Index, parentLinks *packfile.Links) (Result, error) {
	if (parent == nil) != (parentLinks == nil) {
		return Result{}, errors.New("verify: a patch pack needs both the index and the links of its parent")
	}
	if parent != nil && parentLinks.Len() != parent.Len() {
		return Result{}, fmt.Errorf("verify: the parent's links are for %d objects, its index has %d", parentLinks.Len(), parent.Len())
	}
	n := x.Len()
	if parent != nil {
		for i := range n {
			if k := x.Entry(i).Key; parent.Has(k) {
				return Result{}, fmt.Errorf("%w: object %s is also in the parent", ErrMalformed, k)
			}
		}
	}

	seen := newBitmap(n)
	queue := make([]uint32, 0, n)
	var reached bitmap
	var crossings []uint32
	var children [][]uint32
	if parent != nil {
		reached = newBitmap(parent.Len())
	} else {
		children = make([][]uint32, n)
	}
	cross := func(k key.Key) bool {
		if parent == nil {
			return false
		}
		pos, ok := parent.Find(k)
		if ok && !reached.has(pos) {
			reached.set(pos)
			crossings = append(crossings, uint32(pos))
		}
		return ok
	}

	if pos, ok := x.Find(root); ok {
		seen.set(pos)
		queue = append(queue, uint32(pos))
	} else if n > 0 || parent == nil {
		return Result{}, fmt.Errorf("%w: the root %s is not in the pack", ErrMalformed, root)
	} else if !cross(root) {
		return Result{}, fmt.Errorf("%w: the pack is empty and its root %s is not in the parent", ErrMalformed, root)
	}

	var object []byte
	for head := 0; head < len(queue); head++ {
		i := queue[head]
		e := x.Entry(int(i))
		object = slices.Grow(object[:0], int(e.Length))[:e.Length]
		if err := readAt(data, object, e.Offset); err != nil {
			return Result{}, err
		}
		if err := Object(e.Key, object); err != nil {
			return Result{}, err
		}
		refs, err := fstree.ChildKeys(e.Key, object)
		if err != nil {
			return Result{}, fmt.Errorf("%w: object %s: %v", ErrMalformed, e.Key, err)
		}
		if children != nil && len(refs) > 0 {
			children[i] = make([]uint32, 0, len(refs))
		}
		for _, c := range refs {
			pos, ok := x.Find(c)
			if !ok {
				if cross(c) {
					continue
				}
				return Result{}, fmt.Errorf("%w: object %s, which %s refers to, is missing", ErrMalformed, c, e.Key)
			}
			if children != nil {
				children[i] = append(children[i], uint32(pos))
			}
			if !seen.has(pos) {
				seen.set(pos)
				queue = append(queue, uint32(pos))
			}
		}
	}
	if len(queue) < n {
		for i := range n {
			if !seen.has(i) {
				return Result{}, fmt.Errorf("%w: %d objects are not reachable from the root, %s among them", ErrMalformed, n-len(queue), x.Entry(i).Key)
			}
		}
	}

	res := Result{Objects: uint64(n), Bytes: x.DataSize()}
	if parent == nil {
		res.Links = packfile.NewLinks(children)
		return res, nil
	}
	for len(crossings) > 0 {
		pos := crossings[len(crossings)-1]
		crossings = crossings[:len(crossings)-1]
		res.SharedObjects++
		res.SharedBytes += uint64(parent.Entry(int(pos)).Length)
		for _, c := range parentLinks.Children(int(pos)) {
			if !reached.has(int(c)) {
				reached.set(int(c))
				crossings = append(crossings, c)
			}
		}
	}
	return res, nil
}

// readAt fills p from offset off of r. A reader that ends early fails with
// io.ErrUnexpectedEOF.
func readAt(r io.ReaderAt, p []byte, off uint64) error {
	if len(p) == 0 {
		return nil
	}
	n, err := r.ReadAt(p, int64(off))
	if n == len(p) {
		return nil
	}
	if err == nil || err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
