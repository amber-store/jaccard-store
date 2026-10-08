package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/amber-store/core/gc"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/core/refstore"
)

// localStore is the store directory core's CLI works on: <dir>/packstore
// holds the objects, <dir>/refs the references, <dir>/closures what the
// collector keeps.
type localStore struct {
	dir     string
	objects *packstore.Store
	refs    *refstore.Store
}

func openStore(dir string) (*localStore, error) {
	if dir == "" {
		return nil, errors.New("no store: give --store or set JACCARD_STORE")
	}
	objects, err := packstore.Open(filepath.Join(dir, "packstore"), packstore.WithSync(true))
	if err != nil {
		return nil, err
	}
	refs, err := refstore.Open(filepath.Join(dir, "refs"), true)
	if err != nil {
		objects.Close()
		return nil, err
	}
	return &localStore{dir: dir, objects: objects, refs: refs}, nil
}

func (s *localStore) Close() error {
	return errors.Join(s.refs.Close(), s.objects.Close())
}

// Ref returns the root key the local reference name points at.
func (s *localStore) Ref(name string) (key.Key, error) {
	raw, err := s.refs.Get(name)
	if errors.Is(err, refstore.ErrNotFound) {
		return key.Key{}, fmt.Errorf("no local reference %q", name)
	}
	if err != nil {
		return key.Key{}, err
	}
	return rootOf(name, raw)
}

func rootOf(name string, record []byte) (key.Key, error) {
	rec, err := reference.Decode(record)
	if err != nil {
		return key.Key{}, fmt.Errorf("local reference %q: %w", name, err)
	}
	root, err := key.Parse(rec.Key)
	if err != nil {
		return key.Key{}, fmt.Errorf("local reference %q: %w", name, err)
	}
	return root, nil
}

// span is a stretch of writes to the store that ends with a reference: the
// collector's span, under which a collection in another process does not
// take objects whose reference is still to come.
type span struct {
	store *localStore
	coll  *gc.Collector
	span  *gc.Span
}

// Begin opens a span. It is ended with End.
func (s *localStore) Begin() (*span, error) {
	coll, err := gc.Open(filepath.Join(s.dir, "closures"), s.objects, s.refs, gc.Options{})
	if err != nil {
		return nil, err
	}
	sp, err := coll.BeginSpan()
	if err != nil {
		return nil, errors.Join(err, coll.Close())
	}
	return &span{store: s, coll: coll, span: sp}, nil
}

// SetRef points the local reference name at root, whose objects were
// written under the span, the way core's CLI puts a reference.
func (sp *span) SetRef(name string, root key.Key) error {
	if err := reference.ValidateName(name); err != nil {
		return fmt.Errorf("local reference %q: %w", name, err)
	}
	refs := sp.store.refs
	var old *key.Key
	if prev, err := refs.Get(name); err == nil {
		k, err := rootOf(name, prev)
		if err != nil {
			return err
		}
		old = &k
	} else if !errors.Is(err, refstore.ErrNotFound) {
		return err
	}
	raw, err := reference.Reference{Name: name, Key: root[:], CreatedAt: time.Now().UnixNano()}.Encode()
	if err != nil {
		return err
	}
	commit, abort, err := sp.span.PrepareRef(root)
	if err != nil {
		return err
	}
	if err := refs.Put(name, raw); err != nil {
		abort()
		return err
	}
	commit()
	if old != nil {
		return sp.coll.ReleaseRef(*old)
	}
	return nil
}

// End closes the span and the collector under it.
func (sp *span) End() error {
	sp.span.End()
	return sp.coll.Close()
}
