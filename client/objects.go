package client

import (
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
)

// ObjectSource is where a push reads objects from. *packstore.Store is
// one. Get must be safe for concurrent calls and return a slice the caller
// owns.
type ObjectSource interface {
	Get(k key.Key) ([]byte, error)
}

// sorter is a source that is read fastest in an order of its own: it puts
// keys into that order in place. A push asks it about the objects of the
// pack before it reads them; a source that is not one is read in the order
// of the key set.
type sorter interface {
	SortByLocation(keys []key.Key)
}

// A packstore is a source, and one with an order of its own.
var (
	_ ObjectSource = (*packstore.Store)(nil)
	_ sorter       = (*packstore.Store)(nil)
)
