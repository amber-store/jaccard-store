package e2e

import (
	"bytes"
	"errors"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/packfile"
	"github.com/amber-store/jaccard-store/server"
)

// held is a pack as a fetch leaves it with a caller that keeps packs as
// files: its index, and its objects back to back.
type held struct {
	index *packfile.Index
	data  []byte
}

// fetch downloads pack through the peer's client and into no store.
func (p *peer) fetch(pack client.Pack) held {
	p.t.Helper()
	var h held
	index, err := p.client.Fetch(p.w.ctx, pack, func(o packfile.Object) error {
		h.data = append(h.data, o.Data...)
		return nil
	})
	if err != nil {
		p.t.Fatalf("fetch of pack %s: %v", pack.Root, err)
	}
	h.index = index
	return h
}

// object returns the bytes the index locates k at, and whether the pack
// holds k.
func (h held) object(k key.Key) ([]byte, bool) {
	i, ok := h.index.Find(k)
	if !ok {
		return nil, false
	}
	e := h.index.Entry(i)
	return h.data[e.Offset:][:e.Length], true
}

func TestLookupAndFetchABaseAndAPatch(t *testing.T) { onEveryServer(t, lookupAndFetchABaseAndAPatch) }

// A reference is read without a packstore: a lookup names its packs, a
// fetch brings each as an index and the bytes the index locates.
func lookupAndFetchABaseAndAPatch(t *testing.T, s3 backend, mutate func(*server.Config)) {
	w := newWorld(t, s3, mutate)
	alice, bob := w.peer("alice"), w.peer("bob")

	v1 := alice.ingest(version1())
	first := alice.push("project/v1", v1)
	v2 := alice.ingest(version2())
	second := alice.push("project/v2", v2)
	if second.Parent == nil || *second.Parent != v1 {
		t.Fatalf("second push: %+v, want a patch pack of %s", second, v1)
	}

	// The patch names two packs, its own first.
	root, packs, err := bob.client.Lookup(w.ctx, "project/v2")
	if err != nil {
		t.Fatal(err)
	}
	if root != v2 || len(packs) != 2 || packs[0].Root != v2 || packs[1].Root != v1 {
		t.Fatalf("lookup = %s, %+v, want %s in its own pack and then that of %s", root, packs, v2, v1)
	}
	for i, pushed := range []client.PushResult{second, first} {
		p, recorded := packs[i], w.pack(pushed.Root)
		if p.Objects != pushed.Objects || p.Bytes != pushed.Bytes || p.DataSize != pushed.DataSize {
			t.Fatalf("lookup says of pack %s %+v; the push made it %+v", p.Root, p, pushed)
		}
		if p.IndexSize != packfile.IndexSize(p.Objects) || p.IndexSize != uint64(recorded.IndexSize) || p.DataSize != uint64(recorded.DataSize) {
			t.Fatalf("lookup says of pack %s %+v; the server recorded %+v", p.Root, p, recorded)
		}
	}

	// Fetched, each is what the server said of it.
	var all []held
	for _, p := range packs {
		h := bob.fetch(p)
		if uint64(h.index.Len()) != p.Objects || h.index.DataSize() != p.Bytes || uint64(len(h.data)) != p.Bytes {
			t.Fatalf("pack %s came as %d objects of %d bytes, %d handed over; the lookup said %d of %d",
				p.Root, h.index.Len(), h.index.DataSize(), len(h.data), p.Objects, p.Bytes)
		}
		all = append(all, h)
	}

	// Every object of the reference is in one of the two, where the index
	// says, and the reference's own pack is asked first.
	keys := alice.keys(v2)
	var own, fromParent uint64
	for _, k := range keys {
		want, err := alice.objects.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := all[0].object(k)
		if ok {
			own++
		} else if got, ok = all[1].object(k); ok {
			fromParent++
		} else {
			t.Fatalf("object %s is in neither pack", k)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("object %s read by the index differs from the source's", k)
		}
	}
	if own != second.Objects || fromParent == 0 || own+fromParent != uint64(len(keys)) {
		t.Fatalf("%d objects came from the patch pack of %d and %d from its parent, of %d", own, second.Objects, fromParent, len(keys))
	}
	// None of it went through a store.
	if has, err := bob.objects.Has(v2); err != nil || has {
		t.Fatalf("the root reached the store (%v, %v)", has, err)
	}

	// A base pack is named alone, and holds its reference whole.
	root, packs, err = bob.client.Lookup(w.ctx, "project/v1")
	if err != nil || root != v1 || len(packs) != 1 || packs[0].Root != v1 {
		t.Fatalf("lookup of the base = %s, %+v, %v", root, packs, err)
	}
	base := bob.fetch(packs[0])
	for _, k := range alice.keys(v1) {
		want, err := alice.objects.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := base.object(k); !ok || !bytes.Equal(got, want) {
			t.Fatalf("object %s of the base pack: held %v, %d bytes of %d", k, ok, len(got), len(want))
		}
	}

	if _, packs, err := bob.client.Lookup(w.ctx, "project/none"); !errors.Is(err, client.ErrNotFound) || packs != nil {
		t.Fatalf("lookup of a name the server lacks = %+v, %v, want ErrNotFound", packs, err)
	}
}
