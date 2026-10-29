package e2e

import (
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/server"
)

// objectMap is a source of objects that is no packstore: a push reads it
// through client.PushFrom.
type objectMap map[key.Key][]byte

func (m objectMap) Get(k key.Key) ([]byte, error) {
	data, ok := m[k]
	if !ok {
		return nil, fmt.Errorf("no object %s", k)
	}
	return slices.Clone(data), nil
}

// asMap returns the objects of root in the peer's store, and nothing else
// the store holds.
func (p *peer) asMap(root key.Key) objectMap {
	p.t.Helper()
	m := objectMap{}
	for _, k := range p.keys(root) {
		data, err := p.objects.Get(k)
		if err != nil {
			p.t.Fatal(err)
		}
		m[k] = data
	}
	return m
}

func (p *peer) pushFrom(objects client.ObjectSource, name string, root key.Key) client.PushResult {
	p.t.Helper()
	res, err := p.client.PushFrom(p.w.ctx, objects, name, root, client.PushOptions{MinDedup: 0.5, TempDir: p.t.TempDir()})
	if err != nil {
		p.t.Fatalf("push %s: %v", name, err)
	}
	return res
}

// onEveryServer runs a scenario against each backend twice: with a server
// that verifies what is pushed and with one that does not.
func onEveryServer(t *testing.T, scenario func(t *testing.T, s3 backend, mutate func(*server.Config))) {
	onEveryBucket(t, func(t *testing.T, s3 backend) {
		t.Run("verifying", func(t *testing.T) { scenario(t, s3, nil) })
		t.Run("no-verify", func(t *testing.T) { scenario(t, s3, trusting) })
	})
}

func TestPushFromAMapEqualsPushFromAPackstore(t *testing.T) {
	onEveryServer(t, pushFromAMapEqualsPushFromAPackstore)
}

// A push reads its objects through one method. Where they are kept makes
// no difference to what the server is given.
func pushFromAMapEqualsPushFromAPackstore(t *testing.T, s3 backend, mutate func(*server.Config)) {
	w := newWorld(t, s3, mutate)
	alice, bob := w.peer("alice"), w.peer("bob")
	unverified := mutate != nil

	v1 := alice.ingest(version1())
	first := alice.push("from/packstore", v1)
	if first.Stored || first.Root != v1 {
		t.Fatalf("push from a packstore: %+v, want a pack uploaded", first)
	}
	// The same tree from a map is the same root, which the server has.
	second := alice.pushFrom(alice.asMap(v1), "from/map", v1)
	if second.Root != first.Root || !second.Stored {
		t.Fatalf("push of the same tree from a map: %+v, want the root %s, stored", second, first.Root)
	}
	for _, name := range []string{"from/packstore", "from/map"} {
		ref, err := w.db.Ref(w.ctx, name)
		if err != nil || ref.Root != v1 {
			t.Fatalf("%s points at %s, %v", name, ref.Root, err)
		}
	}

	// A tree the server lacks, from a map: the pack a packstore would
	// have made of it, a patch of the first, in whatever order.
	v2 := alice.ingest(version2())
	third := alice.pushFrom(alice.asMap(v2), "from/map/v2", v2)
	if third.Stored || third.Parent == nil || *third.Parent != v1 || third.Unverified != unverified {
		t.Fatalf("push of a changed tree from a map: %+v, want a patch pack of %s", third, v1)
	}
	inV1 := map[key.Key]bool{}
	for _, k := range alice.keys(v1) {
		inV1[k] = true
	}
	var own, bytes uint64
	for _, k := range alice.keys(v2) {
		if inV1[k] {
			continue
		}
		data, err := alice.objects.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		own++
		bytes += uint64(len(data))
	}
	if own == 0 || third.Objects != own || third.Bytes != bytes {
		t.Fatalf("the patch pack holds %d objects of %d bytes; the key sets differ by %d of %d", third.Objects, third.Bytes, own, bytes)
	}
	if got := bob.pull("from/map/v2"); got.Root != v2 || got.Packs != 2 {
		t.Fatalf("pull: %+v, want the patch pack and its parent", got)
	}
	sameTree(t, v2, alice, bob)
}

func TestPushFromASingleBlobRoot(t *testing.T) { onEveryServer(t, pushFromASingleBlobRoot) }

// A reference may be one object that has no children, and a source may
// hold nothing but that object.
func pushFromASingleBlobRoot(t *testing.T, s3 backend, mutate func(*server.Config)) {
	w := newWorld(t, s3, mutate)
	alice := w.peer("alice")
	content := noise(7, 300)
	blob, err := fstree.EncodeBlob(content)
	if err != nil {
		t.Fatal(err)
	}
	res := alice.pushFrom(objectMap{blob.Key: blob.Bytes}, "one/blob", blob.Key)
	if res.Root != blob.Key || res.Stored || res.Parent != nil || res.Objects != 1 || res.Bytes != 300 || res.Unverified != (mutate != nil) {
		t.Fatalf("push: %+v, want a base pack of one object of 300 bytes", res)
	}

	fresh, err := packstore.Open(filepath.Join(t.TempDir(), "fresh"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	got, err := alice.client.Pull(w.ctx, fresh, "one/blob", client.PullOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Root != blob.Key || got.Packs != 1 || got.Objects != 1 {
		t.Fatalf("pull into a fresh store: %+v, want the root %s from one pack", got, blob.Key)
	}
	data, err := fresh.Get(blob.Key)
	if err != nil || string(data) != string(content) {
		t.Fatalf("the blob arrived as %d bytes, %v", len(data), err)
	}
}
