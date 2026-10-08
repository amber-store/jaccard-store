package client

import (
	"io"
	"sync"
)

// Progress is told what a push or a pull is doing, step by step, so that a
// command line can show it. A step begins, advances zero or more times and
// ends before the next one begins. Advance is called from several goroutines
// at once: the objects of a tree are read side by side, and so are the parts
// of a large pack sent. Begin and End are not.
//
// The steps of a push: reading the tree, asking the server for the packs
// near it, comparing with them, packing, uploading, and the server's
// verification. Those of a pull: looking up the reference, checking what
// the local store holds, and fetching each pack.
//
// When Push or Pull returns an error, the step that was running has not
// ended: no End follows its Begin.
type Progress interface {
	// Begin starts a step. name says what is being done, as a phrase that
	// reads after "now": "packing", "uploading". total is how much there
	// is to do, counted in unit, and zero when that is not known.
	Begin(name string, total uint64, unit Unit)
	// Advance reports n more of the step's unit done. n is negative when
	// work that was reported has to be done again: a part that is sent a
	// second time gives back what its first attempt counted.
	Advance(n int64)
	// End finishes the step. summary says what came of it and may be
	// empty: "12,345 objects", "accepted".
	End(summary string)
}

// Unit is what a step's work is counted in.
type Unit int

const (
	// NoUnit: the step has nothing to count; only its time is known.
	NoUnit Unit = iota
	// Bytes: the step moves bytes.
	Bytes
	// Objects: the step handles objects of the store.
	Objects
)

// silent is the Progress of a caller that wants none.
type silent struct{}

func (silent) Begin(string, uint64, Unit) {}
func (silent) Advance(int64)              {}
func (silent) End(string)                 {}

// progressOf returns p, or a Progress that drops everything when p is nil.
func progressOf(p Progress) Progress {
	if p == nil {
		return silent{}
	}
	return p
}

// meter reports the bytes read through it as progress.
type meter struct {
	r io.Reader
	p Progress

	// The HTTP client may still be reading a body when the request has
	// failed and its bytes are taken back.
	mu     sync.Mutex
	n      int64
	undone bool
}

func (m *meter) Read(b []byte) (int, error) {
	n, err := m.r.Read(b)
	if n > 0 {
		m.mu.Lock()
		if !m.undone {
			m.n += int64(n)
			m.p.Advance(int64(n))
		}
		m.mu.Unlock()
	}
	return n, err
}

// undo takes back what the meter reported, for a body that did not arrive
// and is sent again. What is read through the meter afterwards is not
// reported any more. A nil meter has nothing to take back.
func (m *meter) undo() {
	if m == nil {
		return
	}
	m.mu.Lock()
	if !m.undone {
		m.undone = true
		m.p.Advance(-m.n)
	}
	m.mu.Unlock()
}
