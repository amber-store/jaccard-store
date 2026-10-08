package packfile

import (
	"encoding/binary"
	"fmt"
	"slices"
)

// linksHeaderSize is the size of the magic and the count of the links.
const linksHeaderSize = 16

// Links records, for every object of a pack, the objects it refers to, as
// positions in the pack's index. It is immutable and safe for concurrent use.
type Links struct {
	starts   []uint64 // Len() + 1 positions into children
	children []uint32
}

// NewLinks builds links from the children of each index position. It
// sorts each list and drops repeats. The lists are copied. Every child must
// be a position below len(children); ParseLinks refuses links where one is
// not.
func NewLinks(children [][]uint32) *Links {
	total := 0
	for _, list := range children {
		total += len(list)
	}
	l := &Links{
		starts:   make([]uint64, 1, len(children)+1),
		children: make([]uint32, 0, total),
	}
	for _, list := range children {
		start := len(l.children)
		l.children = append(l.children, list...)
		slices.Sort(l.children[start:])
		l.children = l.children[:start+len(slices.Compact(l.children[start:]))]
		l.starts = append(l.starts, uint64(len(l.children)))
	}
	return l
}

// ParseLinks parses and validates encoded links for an index of n entries.
// Every error wraps ErrMalformed. The count is checked against n and against
// the length of b before anything is allocated.
func ParseLinks(b []byte, n int) (*Links, error) {
	if len(b) < linksHeaderSize {
		return nil, fmt.Errorf("%w: links of %d bytes are shorter than their header of %d", ErrMalformed, len(b), linksHeaderSize)
	}
	if string(b[:len(LinksMagic)]) != LinksMagic {
		return nil, fmt.Errorf("%w: bad links magic %q", ErrMalformed, b[:len(LinksMagic)])
	}
	count := binary.BigEndian.Uint64(b[len(LinksMagic):linksHeaderSize])
	if n < 0 || count != uint64(n) {
		return nil, fmt.Errorf("%w: links are for %d objects, the index has %d", ErrMalformed, count, n)
	}
	body := b[linksHeaderSize:]
	if uint64(len(body))/8 < count+1 {
		return nil, fmt.Errorf("%w: links of %d bytes have no room for the %d starts of %d objects", ErrMalformed, len(b), count+1, count)
	}
	rest := body[(n+1)*8:]
	if len(rest)%4 != 0 {
		return nil, fmt.Errorf("%w: %d bytes of children are not a whole number of positions", ErrMalformed, len(rest))
	}
	l := &Links{
		starts:   make([]uint64, n+1),
		children: make([]uint32, len(rest)/4),
	}
	for i := range l.starts {
		l.starts[i] = binary.BigEndian.Uint64(body[i*8:])
		switch {
		case i == 0 && l.starts[0] != 0:
			return nil, fmt.Errorf("%w: the children of the first object start at %d, they must start at 0", ErrMalformed, l.starts[0])
		case l.starts[i] > uint64(len(l.children)):
			return nil, fmt.Errorf("%w: start %d of object %d lies past the %d children", ErrMalformed, l.starts[i], i, len(l.children))
		case i > 0 && l.starts[i] < l.starts[i-1]:
			return nil, fmt.Errorf("%w: starts decrease from %d to %d at object %d", ErrMalformed, l.starts[i-1], l.starts[i], i)
		}
	}
	if last := l.starts[n]; last != uint64(len(l.children)) {
		return nil, fmt.Errorf("%w: %d children are left over after the last object", ErrMalformed, uint64(len(l.children))-last)
	}
	for i := range l.children {
		l.children[i] = binary.BigEndian.Uint32(rest[i*4:])
	}
	for i := range n {
		list := l.Children(i)
		for j, c := range list {
			if uint64(c) >= count {
				return nil, fmt.Errorf("%w: object %d refers to position %d of %d", ErrMalformed, i, c, count)
			}
			if j > 0 && list[j-1] >= c {
				return nil, fmt.Errorf("%w: the children of object %d are not strictly ascending", ErrMalformed, i)
			}
		}
	}
	return l, nil
}

// Encode returns the links in their encoded form.
func (l *Links) Encode() []byte {
	b := make([]byte, 0, linksHeaderSize+len(l.starts)*8+len(l.children)*4)
	b = append(b, LinksMagic...)
	b = binary.BigEndian.AppendUint64(b, uint64(l.Len()))
	for _, s := range l.starts {
		b = binary.BigEndian.AppendUint64(b, s)
	}
	for _, c := range l.children {
		b = binary.BigEndian.AppendUint32(b, c)
	}
	return b
}

// Len returns the number of objects the links cover: the number of entries
// of the index they belong to.
func (l *Links) Len() int { return len(l.starts) - 1 }

// Children returns the index positions the object at index position i
// refers to, ascending and each once, 0 <= i < Len(). The slice is the
// Links' own and must not be changed.
func (l *Links) Children(i int) []uint32 {
	return l.children[l.starts[i]:l.starts[i+1]:l.starts[i+1]]
}
