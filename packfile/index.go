// Package packfile reads and writes the parts of a jaccard-store pack. A pack
// holds the objects of one reference, or, when it leans on a parent pack,
// only the objects that parent lacks. It is kept as separate parts: the index
// and the data, written by the client, and for a pack that can be a parent
// the links, written by the server. All integers are big-endian.
//
// The index names every object of the pack and where its bytes lie:
//
//	magic    "JACIDX\x00\x01"                    8 bytes
//	count    number of entries                   8 bytes
//	entries  key[32] offset[8] length[4]         44 bytes each, ascending by key
//
// The data is one zstd stream, with a window of at most MaxWindow, that
// decompresses to the objects' serialized bytes back to back, each once, in
// any order, with no framing. Offset and length locate an object in that
// uncompressed stream. The data of a pack without bytes may be empty.
//
// A valid index has at most MaxEntries entries, canonical keys in strictly
// ascending order, no length above core's amberpack.MaxPayload, and entries
// that, ordered by offset, tile the stream: the first starts at 0 and each
// starts where the previous ends. The uncompressed size of the data is
// therefore the sum of the lengths. An object may be empty; it then shares
// its offset with whatever follows it.
//
// The links record which objects each object refers to, as positions in the
// index:
//
//	magic     "JACLNK\x00\x01"                   8 bytes
//	count     number of index entries, n         8 bytes
//	starts    n + 1 positions into children      8 bytes each
//	children  index positions                    4 bytes each
//
// The children of the object at index position i are
// children[starts[i]:starts[i+1]], ascending and each listed once.
package packfile

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/keyset"
)

const (
	// IndexMagic opens an index. Its last byte is the format version.
	IndexMagic = "JACIDX\x00\x01"
	// LinksMagic opens the links. Its last byte is the format version.
	LinksMagic = "JACLNK\x00\x01"
	// IndexHeaderSize is the size of the magic and the count of an index.
	IndexHeaderSize = 16
	// EntrySize is the size of one encoded index entry.
	EntrySize = 44
	// MaxEntries is the largest number of objects a pack may hold.
	MaxEntries = 1 << 24
	// MaxWindow is the largest zstd window the data of a pack may ask for.
	MaxWindow = 64 << 20
)

// ErrMalformed wraps every error from an index, a data stream or links that
// break the format.
var ErrMalformed = errors.New("packfile: malformed pack")

// Entry locates one object in the uncompressed data stream.
type Entry struct {
	Key    key.Key
	Offset uint64
	Length uint32
}

// Object is one object of a pack: its key and its serialized bytes.
type Object struct {
	Key  key.Key
	Data []byte
}

// Index is a validated index. It is immutable and safe for concurrent use.
type Index struct {
	entries  []Entry  // ascending by key
	byOffset []uint32 // positions in entries, in stream order
	size     uint64
}

// IndexSize returns the size in bytes of an index of n entries. For an n
// whose index would not fit in a uint64 it returns math.MaxUint64, the size
// of no index, so that a count taken from a stranger cannot wrap around to
// the size of a small one.
func IndexSize(n uint64) uint64 {
	if n > (math.MaxUint64-IndexHeaderSize)/EntrySize {
		return math.MaxUint64
	}
	return IndexHeaderSize + n*EntrySize
}

// NewIndex builds an index from entries in any order. It fails, wrapping
// ErrMalformed, for what the format forbids: too many entries, a key that is
// not canonical or appears twice, a length above amberpack.MaxPayload, and
// entries that do not tile the stream. The entries are copied.
func NewIndex(entries []Entry) (*Index, error) {
	if len(entries) > MaxEntries {
		return nil, fmt.Errorf("%w: index of %d entries is above the limit of %d", ErrMalformed, len(entries), MaxEntries)
	}
	x := &Index{entries: slices.Clone(entries)}
	slices.SortFunc(x.entries, func(a, b Entry) int { return keyset.Compare(a.Key, b.Key) })
	if err := x.validate(); err != nil {
		return nil, err
	}
	return x, nil
}

// ParseIndex parses and validates an encoded index. Every error wraps
// ErrMalformed. The count is checked against MaxEntries and against the
// length of b before anything is allocated for the entries.
func ParseIndex(b []byte) (*Index, error) {
	if len(b) < IndexHeaderSize {
		return nil, fmt.Errorf("%w: index of %d bytes is shorter than its header of %d", ErrMalformed, len(b), IndexHeaderSize)
	}
	if string(b[:len(IndexMagic)]) != IndexMagic {
		return nil, fmt.Errorf("%w: bad index magic %q", ErrMalformed, b[:len(IndexMagic)])
	}
	count := binary.BigEndian.Uint64(b[len(IndexMagic):IndexHeaderSize])
	if count > MaxEntries {
		return nil, fmt.Errorf("%w: index of %d entries is above the limit of %d", ErrMalformed, count, MaxEntries)
	}
	if IndexSize(count) != uint64(len(b)) {
		return nil, fmt.Errorf("%w: %d entries do not fill an index of %d bytes", ErrMalformed, count, len(b))
	}
	x := &Index{entries: make([]Entry, count)}
	for i := range x.entries {
		raw := b[IndexHeaderSize+i*EntrySize:][:EntrySize]
		x.entries[i] = Entry{
			Key:    key.Key(raw[:key.Size]),
			Offset: binary.BigEndian.Uint64(raw[key.Size:]),
			Length: binary.BigEndian.Uint32(raw[key.Size+8:]),
		}
	}
	if err := x.validate(); err != nil {
		return nil, err
	}
	return x, nil
}

// validate checks x.entries, which are in the order they will keep, and
// fills in the stream order and the size.
func (x *Index) validate() error {
	for i, e := range x.entries {
		if err := e.Key.Validate(); err != nil {
			return fmt.Errorf("%w: key %s is not canonical: %v", ErrMalformed, e.Key, err)
		}
		if i > 0 {
			switch c := keyset.Compare(x.entries[i-1].Key, e.Key); {
			case c == 0:
				return fmt.Errorf("%w: key %s appears twice", ErrMalformed, e.Key)
			case c > 0:
				return fmt.Errorf("%w: key %s at position %d is out of order", ErrMalformed, e.Key, i)
			}
		}
		if e.Length > amberpack.MaxPayload {
			return fmt.Errorf("%w: object %s of %d bytes is above the limit of %d", ErrMalformed, e.Key, e.Length, amberpack.MaxPayload)
		}
		if e.Offset > math.MaxUint64-uint64(e.Length) {
			return fmt.Errorf("%w: object %s at offset %d overflows the stream", ErrMalformed, e.Key, e.Offset)
		}
	}

	// Objects that share an offset are ordered by length and then by key.
	// Only empty objects, and the one object that begins where they sit, can
	// share an offset in a stream that tiles, so the empty ones come first,
	// in key order, and the tiling check below passes over them.
	x.byOffset = make([]uint32, len(x.entries))
	for i := range x.byOffset {
		x.byOffset[i] = uint32(i)
	}
	slices.SortFunc(x.byOffset, func(i, j uint32) int {
		a, b := x.entries[i], x.entries[j]
		return cmp.Or(cmp.Compare(a.Offset, b.Offset), cmp.Compare(a.Length, b.Length), cmp.Compare(i, j))
	})
	var end uint64
	for n, i := range x.byOffset {
		e := x.entries[i]
		switch {
		case e.Offset < end:
			return fmt.Errorf("%w: object %s at offset %d overlaps the one before it, which ends at %d", ErrMalformed, e.Key, e.Offset, end)
		case e.Offset > end && n == 0:
			return fmt.Errorf("%w: the first object, %s, starts at offset %d, not 0", ErrMalformed, e.Key, e.Offset)
		case e.Offset > end:
			return fmt.Errorf("%w: gap of %d bytes before object %s at offset %d", ErrMalformed, e.Offset-end, e.Key, e.Offset)
		}
		end += uint64(e.Length)
	}
	x.size = end
	return nil
}

// Encode returns the index in its encoded form.
func (x *Index) Encode() []byte {
	b := make([]byte, 0, IndexSize(uint64(len(x.entries))))
	b = append(b, IndexMagic...)
	b = binary.BigEndian.AppendUint64(b, uint64(len(x.entries)))
	for _, e := range x.entries {
		b = append(b, e.Key[:]...)
		b = binary.BigEndian.AppendUint64(b, e.Offset)
		b = binary.BigEndian.AppendUint32(b, e.Length)
	}
	return b
}

// Len returns the number of entries.
func (x *Index) Len() int { return len(x.entries) }

// Entry returns the i-th entry in key order, 0 <= i < Len().
func (x *Index) Entry(i int) Entry { return x.entries[i] }

// Find returns the position of k in key order and whether the index holds k.
func (x *Index) Find(k key.Key) (int, bool) {
	i := sort.Search(len(x.entries), func(i int) bool {
		return keyset.Compare(x.entries[i].Key, k) >= 0
	})
	return i, i < len(x.entries) && x.entries[i].Key == k
}

// Has reports whether the index holds k.
func (x *Index) Has(k key.Key) bool {
	_, ok := x.Find(k)
	return ok
}

// DataSize returns the size of the uncompressed data stream: the sum of the
// lengths.
func (x *Index) DataSize() uint64 { return x.size }

// ByOffset returns the positions of the entries in the order their objects
// lie in the stream. Empty objects come before the object that begins where
// they sit, in key order. The slice is the caller's.
func (x *Index) ByOffset() []int {
	order := make([]int, len(x.byOffset))
	for n, i := range x.byOffset {
		order[n] = int(i)
	}
	return order
}
