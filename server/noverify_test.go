package server_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/db"
	"github.com/amber-store/jaccard-store/packfile"
	"github.com/amber-store/jaccard-store/server"
	"github.com/amber-store/jaccard-store/wire"
)

// trusting is a harness whose server does not verify.
func trusting(t *testing.T) *harness {
	t.Helper()
	return newHarness(t, func(c *server.Config) { c.NoVerify = true })
}

// sharedWith is what the tree has of the pack with the given index: of the
// pack's objects, the ones the tree is made of, and their bytes.
func (tr tree) sharedWith(base *packfile.Index) (objects, size uint64) {
	for _, k := range tr.keys {
		if base.Has(k) {
			objects++
			size += uint64(len(tr.objects[k]))
		}
	}
	return objects, size
}

// lacking is the tree without one of its objects: one that is not its root
// and that skip, if there is one, does not hold.
func (tr tree) lacking(t *testing.T, skip *packfile.Index) tree {
	t.Helper()
	for i, k := range tr.keys {
		if k != tr.root && (skip == nil || !skip.Has(k)) {
			tr.keys = slices.Delete(slices.Clone(tr.keys), i, i+1)
			return tr
		}
	}
	t.Fatal("the tree has no object to lose")
	return tr
}

func TestWithoutVerificationAPackIsRecordedUnread(t *testing.T) {
	h := trusting(t)
	v1, v2 := newTree(t, filesV1), newTree(t, filesV2)
	index, data := v1.pack(t, nil)
	up := h.upload(alice, "v1", v1.root, nil, index, data)
	u, err := h.db.UploadByID(context.Background(), up.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	resp := h.ok(h.commit(alice, up.UploadID))
	if !resp.Unverified || !bytes.Equal(resp.Root, v1.root[:]) {
		t.Fatalf("commit answered %+v, want the root and that it is not verified", resp)
	}

	p := h.packOf(v1.root)
	if p.Verified || !p.IsBase() || p.LinksKey != "" || p.LinksSize != 0 || p.Uploader != alice {
		t.Fatalf("pack recorded as %+v", p)
	}
	if p.Objects != int64(index.Len()) || p.Bytes != int64(index.DataSize()) || p.DataSize != int64(len(data)) ||
		p.IndexSize != int64(len(index.Encode())) || p.SharedObjects != 0 || p.SharedBytes != 0 {
		t.Fatalf("pack measured as %+v", p)
	}
	// The index is all the server read, and it wrote nothing.
	if got := h.bucket.fetched(); !slices.Equal(got, []string{u.IndexKey}) {
		t.Fatalf("the server read %q, want the index alone", got)
	}
	if h.present(u.LinksKey) {
		t.Fatal("links were written for a pack that was not walked")
	}
	if _, err := h.db.UploadByID(context.Background(), up.UploadID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("the upload is still there: %v", err)
	}

	// The pack is offered to a push near it, as every base pack is.
	start := h.ok(h.handle(bob, wire.Request{Op: wire.OpPushStart, Name: "v2", Root: v2.root[:], Sketch: v2.sketch()}))
	if len(start.Candidates) != 1 || !bytes.Equal(start.Candidates[0].Root, v1.root[:]) {
		t.Fatalf("a push near the pack is offered %+v", start.Candidates)
	}

	// A patch pack on it. What it shares with the parent is what its client
	// says: nothing else can know.
	patch, patchData := v2.pack(t, index)
	objects, size := v2.sharedWith(index)
	if patch.Len() == 0 || objects == 0 {
		t.Fatalf("the test trees do not overlap partly: %d objects are new, %d shared", patch.Len(), objects)
	}
	up = h.uploadAs(bob, "v2", v2.root, &v1.root, patch, patchData, func(r *wire.Request) {
		r.SharedObjects, r.SharedBytes = objects, size
	})
	if resp := h.ok(h.commit(bob, up.UploadID)); !resp.Unverified {
		t.Fatalf("commit of the patch pack answered %+v", resp)
	}
	q := h.packOf(v2.root)
	if q.Verified || q.ParentID != p.ID || q.Objects != int64(patch.Len()) || q.Bytes != int64(patch.DataSize()) {
		t.Fatalf("patch pack recorded as %+v", q)
	}
	if q.SharedObjects != int64(objects) || q.SharedBytes != int64(size) {
		t.Fatalf("shared %d objects, %d bytes; the client said %d and %d", q.SharedObjects, q.SharedBytes, objects, size)
	}
	for _, got := range h.bucket.fetched() {
		if got == p.DataKey || got == q.DataKey {
			t.Fatalf("the server read the data %s", got)
		}
	}

	pulled := h.ok(h.handle(alice, wire.Request{Op: wire.OpPull, Name: "v2"}))
	if len(pulled.Packs) != 2 || !bytes.Equal(httpGet(t, pulled.Packs[0].DataURL), patchData) || !bytes.Equal(httpGet(t, pulled.Packs[1].DataURL), data) {
		t.Fatalf("pull of the patch pack names %+v", pulled.Packs)
	}
}

// What a verification refuses, a server without one records. That is the
// price of it, and the pack says so.
func TestWithoutVerificationAnUnsoundPackIsRecorded(t *testing.T) {
	v1 := newTree(t, filesV1)
	cases := map[string]func(t *testing.T) (*packfile.Index, []byte){
		"an object missing": func(t *testing.T) (*packfile.Index, []byte) {
			return v1.lacking(t, nil).pack(t, nil)
		},
		"data that is not zstd": func(t *testing.T) (*packfile.Index, []byte) {
			index, data := v1.pack(t, nil)
			return index, bytes.Repeat([]byte{0xAB}, len(data))
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			index, data := build(t)
			sound := newHarness(t, nil)
			up := sound.upload(alice, "v1", v1.root, nil, index, data)
			sound.refused(sound.commit(alice, up.UploadID), wire.CodeMalformedPack)

			h := trusting(t)
			up = h.upload(alice, "v1", v1.root, nil, index, data)
			if resp := h.ok(h.commit(alice, up.UploadID)); !resp.Unverified {
				t.Fatalf("commit answered %+v", resp)
			}
			if p := h.packOf(v1.root); p.Verified {
				t.Fatalf("pack recorded as %+v", p)
			}
			if pulled := h.ok(h.handle(bob, wire.Request{Op: wire.OpPull, Name: "v1"})); !bytes.Equal(pulled.Root, v1.root[:]) {
				t.Fatalf("pull answered %+v", pulled)
			}
		})
	}
}

// What a server without verification does look at: that the two objects are
// in the bucket, at the sizes announced, that the index is one, and that it
// has the root.
func TestWithoutVerificationWhatWasNotUploadedIsRefused(t *testing.T) {
	v1, v2 := newTree(t, filesV1), newTree(t, filesV2)
	index, data := v1.pack(t, nil)
	announce := func(h *harness, dataSize int) wire.Response {
		return h.ok(h.handle(alice, wire.Request{Op: wire.OpPushUpload, Name: "v1", Root: v1.root[:], DataSize: uint64(dataSize), Objects: uint64(index.Len())}))
	}
	cases := map[string]func(t *testing.T, h *harness) wire.Response{
		"data never uploaded": func(t *testing.T, h *harness) wire.Response {
			resp := announce(h, len(data))
			httpCreate(t, resp.IndexURL, index.Encode())
			return resp
		},
		"index never uploaded": func(t *testing.T, h *harness) wire.Response {
			resp := announce(h, len(data))
			httpCreate(t, resp.DataURL, data)
			return resp
		},
		"data shorter than announced": func(t *testing.T, h *harness) wire.Response {
			resp := announce(h, len(data)+1)
			httpCreate(t, resp.IndexURL, index.Encode())
			httpCreate(t, resp.DataURL, data)
			return resp
		},
		"index that is not an index": func(t *testing.T, h *harness) wire.Response {
			resp := announce(h, len(data))
			httpCreate(t, resp.IndexURL, bytes.Repeat([]byte{0xAB}, len(index.Encode())))
			httpCreate(t, resp.DataURL, data)
			return resp
		},
		"another tree's objects": func(t *testing.T, h *harness) wire.Response {
			index, data := v2.pack(t, nil)
			return h.upload(alice, "v1", v1.root, nil, index, data)
		},
		"a base pack of nothing": func(t *testing.T, h *harness) wire.Response {
			empty, data := v1.pack(t, index)
			return h.upload(alice, "v1", v1.root, nil, empty, data)
		},
	}
	for name, upload := range cases {
		t.Run(name, func(t *testing.T) {
			h := trusting(t)
			up := upload(t, h)
			u, err := h.db.UploadByID(context.Background(), up.UploadID)
			if err != nil {
				t.Fatal(err)
			}
			refusal := h.refused(h.commit(alice, up.UploadID), wire.CodeMalformedPack)
			if refusal.Message == "" {
				t.Error("the refusal gives no reason")
			}
			if _, err := h.db.PackByRoot(context.Background(), v1.root); !errors.Is(err, db.ErrNotFound) {
				t.Fatalf("the pack was recorded: %v", err)
			}
			h.refused(h.handle(alice, wire.Request{Op: wire.OpPull, Name: "v1"}), wire.CodeNotFound)
			if slices.Contains(h.bucket.fetched(), u.DataKey) {
				t.Error("the server read the data")
			}
			h.sweep()
			for _, objectKey := range []string{u.IndexKey, u.DataKey, u.LinksKey} {
				if h.present(objectKey) {
					t.Errorf("%s is still in the bucket", objectKey)
				}
			}
			h.refused(h.commit(alice, up.UploadID), wire.CodeUnknownUpload)
		})
	}
}

// An empty patch pack is one of a root the parent holds. Whether the parent
// does hold it is not looked at without verification.
func TestWithoutVerificationAnEmptyPatchPackIsTaken(t *testing.T) {
	h := trusting(t)
	v1 := newTree(t, filesV1)
	base := h.pushBase("v1", v1)
	var inside key.Key
	for _, k := range v1.keys {
		if k != v1.root && k.Type() != key.Blob {
			inside = k
		}
	}
	if inside == (key.Key{}) {
		t.Fatal("the tree has no directory or file below its root")
	}
	empty, data := v1.pack(t, base)
	up := h.uploadAs(bob, "part", inside, &v1.root, empty, data, func(r *wire.Request) { r.SharedObjects, r.SharedBytes = 1, 1 })
	if resp := h.ok(h.commit(bob, up.UploadID)); !resp.Unverified {
		t.Fatalf("commit answered %+v", resp)
	}
	if p := h.packOf(inside); p.Verified || p.Objects != 0 || p.SharedObjects != 1 {
		t.Fatalf("pack recorded as %+v", p)
	}
}

// What a client says it shares with the parent is added up with what the
// server measured: a figure that cannot be true is refused before anything
// is uploaded, whatever the server does about verification.
func TestSharedFiguresAreHeldToWhatTheParentHolds(t *testing.T) {
	for name, mutate := range map[string]func(*server.Config){
		"verifying": nil,
		"trusting":  func(c *server.Config) { c.NoVerify = true },
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, mutate)
			v1, v2 := newTree(t, filesV1), newTree(t, filesV2)
			h.pushBase("v1", v1)
			p := h.packOf(v1.root)
			req := wire.Request{Op: wire.OpPushUpload, Name: "v2", Root: v2.root[:], Parent: v1.root[:], DataSize: 10, Objects: 1}
			for what, c := range map[string]struct {
				objects, size uint64
				orphan        bool
			}{
				"more objects than the parent holds": {objects: uint64(p.Objects) + 1, size: 1},
				"more bytes than the parent holds":   {objects: 1, size: uint64(p.Bytes) + 1},
				"objects no int64 counts":            {objects: 1<<63 + 1, size: 1},
				"bytes no int64 counts":              {objects: 1, size: 1 << 63},
				"objects of no parent":               {objects: 1, orphan: true},
				"bytes of no parent":                 {size: 1, orphan: true},
			} {
				r := req
				r.SharedObjects, r.SharedBytes = c.objects, c.size
				if c.orphan {
					r.Parent = nil
				}
				refusal := h.refused(h.handle(alice, r), wire.CodeBadRequest)
				if !strings.Contains(refusal.Message, "shared") {
					t.Errorf("%s: refused with %q, which does not name the figures", what, refusal.Message)
				}
			}
			if list, err := h.db.ListUploads(context.Background()); err != nil || len(list) != 0 {
				t.Fatalf("the refused requests left uploads: %+v, %v", list, err)
			}
			// All the parent holds is the most that can be said.
			all := req
			all.SharedObjects, all.SharedBytes = uint64(p.Objects), uint64(p.Bytes)
			if resp := h.ok(h.handle(alice, all)); resp.UploadID == "" {
				t.Fatalf("an upload sharing all of the parent got %+v", resp)
			}
		})
	}
}

// A server that verifies measures what a patch pack shares with its parent,
// and does not ask the client.
func TestAVerifyingServerMeasuresWhatIsSharedItself(t *testing.T) {
	h := newHarness(t, nil)
	v1, v2 := newTree(t, filesV1), newTree(t, filesV2)
	base := h.pushBase("v1", v1)
	if h.packOf(v1.root).Verified != true {
		t.Fatal("a verified pack is recorded as one that is not")
	}
	index, data := v2.pack(t, base)
	objects, size := v2.sharedWith(base)
	if objects < 2 {
		t.Fatalf("the test trees share %d objects: a wrong figure could not be told from the right one", objects)
	}
	up := h.uploadAs(bob, "v2", v2.root, &v1.root, index, data, func(r *wire.Request) { r.SharedObjects, r.SharedBytes = 1, 1 })
	if resp := h.ok(h.commit(bob, up.UploadID)); resp.Unverified {
		t.Fatalf("commit answered %+v", resp)
	}
	p := h.packOf(v2.root)
	if !p.Verified || p.SharedObjects != int64(objects) || p.SharedBytes != int64(size) {
		t.Fatalf("pack recorded as %+v; the key sets share %d objects, %d bytes", p, objects, size)
	}
}

// A base pack that was recorded without verification has no links. A server
// that verifies, run on the same store later, still verifies a patch pack
// on it, since the walk does not go into the parent; what the pack shares
// with that parent it cannot measure, and takes from the client.
func TestAVerifyingServerOnAParentThatWasNotVerified(t *testing.T) {
	h := trusting(t)
	v1, v2 := newTree(t, filesV1), newTree(t, filesV2)
	base := h.pushBase("v1", v1)
	h.srv = h.newServer(nil)

	broken, brokenData := v2.lacking(t, base).pack(t, base)
	up := h.upload(bob, "v2", v2.root, &v1.root, broken, brokenData)
	h.refused(h.commit(bob, up.UploadID), wire.CodeMalformedPack)

	index, data := v2.pack(t, base)
	objects, size := v2.sharedWith(base)
	if objects < 2 {
		t.Fatalf("the test trees share %d objects", objects)
	}
	// One less than is true, to tell the client's figure from a measured one.
	up = h.uploadAs(bob, "v2", v2.root, &v1.root, index, data, func(r *wire.Request) {
		r.SharedObjects, r.SharedBytes = objects-1, size-1
	})
	if resp := h.ok(h.commit(bob, up.UploadID)); resp.Unverified {
		t.Fatalf("commit of a verified pack answered %+v", resp)
	}
	p := h.packOf(v2.root)
	if !p.Verified || h.packOf(v1.root).Verified {
		t.Fatalf("the patch pack is recorded as %+v, its parent as %+v", p, h.packOf(v1.root))
	}
	if p.SharedObjects != int64(objects-1) || p.SharedBytes != int64(size-1) {
		t.Fatalf("shared %d objects, %d bytes; the client said %d and %d", p.SharedObjects, p.SharedBytes, objects-1, size-1)
	}

	// A base pack pushed now is verified, and has its links.
	v3 := newTree(t, map[string]string{"README": "the third version\n"})
	h.pushBase("v3", v3)
	if q := h.packOf(v3.root); !q.Verified || q.LinksKey == "" || !h.present(q.LinksKey) {
		t.Fatalf("a base pack of the verifying server is recorded as %+v", q)
	}
}
