package e2e

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/packfile"
	"github.com/amber-store/jaccard-store/server"
	"github.com/amber-store/jaccard-store/wire"
)

// trusting turns the verification of a world's server off.
func trusting(c *server.Config) { c.NoVerify = true }

func TestRoundTripWithoutVerification(t *testing.T) {
	onEveryBucket(t, roundTripWithoutVerification)
}

// A server that does not verify stores and serves what one that does would,
// tells the client that it did not look, and has the client's count of what
// a patch pack shares with its parent: the one a verification measures.
func roundTripWithoutVerification(t *testing.T, s3 backend) {
	w := newWorld(t, s3, trusting)
	alice, bob := w.peer("alice"), w.peer("bob")

	v1 := alice.ingest(version1())
	told := &steps{t: t}
	first, err := alice.client.Push(w.ctx, alice.objects, "project/v1", v1, client.PushOptions{MinDedup: 0.5, TempDir: t.TempDir(), Progress: told})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Unverified || first.Stored || first.Parent != nil || first.Objects != uint64(len(alice.keys(v1))) {
		t.Fatalf("first push: %+v, want a base pack that was not verified", first)
	}
	last := told.list[len(told.list)-1]
	if last.name != "verifying on the server" || !strings.Contains(last.summary, "does not verify") {
		t.Fatalf("the push ended with the step %q: %q", last.name, last.summary)
	}
	base := w.pack(v1)
	if base.Verified || !base.IsBase() || base.LinksKey != "" || base.Objects != int64(first.Objects) || base.Bytes != int64(first.Bytes) {
		t.Fatalf("the server recorded the base pack as %+v", base)
	}

	v2 := alice.ingest(version2())
	second := alice.push("project/v2", v2)
	if !second.Unverified || second.Parent == nil || *second.Parent != v1 {
		t.Fatalf("second push: %+v, want a patch pack of %s that was not verified", second, v1)
	}
	inV1 := map[key.Key]bool{}
	for _, k := range alice.keys(v1) {
		inV1[k] = true
	}
	var sharedObjects, sharedBytes int64
	for _, k := range alice.keys(v2) {
		if !inV1[k] {
			continue
		}
		data, err := alice.objects.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		sharedObjects++
		sharedBytes += int64(len(data))
	}
	patch := w.pack(v2)
	if patch.Verified || patch.ParentID != base.ID || patch.Objects != int64(second.Objects) {
		t.Fatalf("the server recorded the patch pack as %+v", patch)
	}
	if sharedObjects == 0 || patch.SharedObjects != sharedObjects || patch.SharedBytes != sharedBytes {
		t.Fatalf("the server has %d objects and %d bytes held by the parent; the key sets share %d and %d",
			patch.SharedObjects, patch.SharedBytes, sharedObjects, sharedBytes)
	}
	if st := w.stats(); st.UnverifiedPacks != 2 || st.BasePacks != 1 || st.PatchPacks != 1 {
		t.Fatalf("the store counts %+v", st)
	}

	if got := bob.pull("project/v2"); got.Root != v2 || got.Packs != 2 {
		t.Fatalf("pull into an empty store: %+v", got)
	}
	sameTree(t, v2, alice, bob)

	// A root the server has is not uploaded again, here as anywhere.
	if again := bob.push("project/copy", v2); !again.Stored {
		t.Fatalf("a push of a stored root: %+v", again)
	}
}

func TestWithoutVerificationThePullFindsAnUnsoundPack(t *testing.T) {
	onEveryBucket(t, withoutVerificationThePullFindsAnUnsoundPack)
}

// The pack a verification refuses (TestAMalformedUploadIsRefusedAndRemoved)
// is recorded by a server that does not verify. Whoever pulls it finds out.
func withoutVerificationThePullFindsAnUnsoundPack(t *testing.T, s3 backend) {
	w := newWorld(t, s3, trusting)
	mallory := w.peer("mallory")
	root := mallory.ingest(version1())
	keys := mallory.keys(root)

	// A pack of everything but one object, announced honestly otherwise.
	var data bytes.Buffer
	pw, err := packfile.NewWriter(&data)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys[:len(keys)-1] { // ReachableKeys puts the root first
		object, err := mallory.objects.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		if err := pw.Add(k, object); err != nil {
			t.Fatal(err)
		}
	}
	index, err := pw.Finish()
	if err != nil {
		t.Fatal(err)
	}
	up := mallory.call(wire.Request{Op: wire.OpPushUpload, Name: "bad", Root: root[:], DataSize: uint64(data.Len()), Objects: uint64(index.Len())})
	if up.Error != nil {
		t.Fatal(up.Error)
	}
	create(t, up.IndexURL, index.Encode())
	create(t, up.DataURL, data.Bytes())
	resp := mallory.call(wire.Request{Op: wire.OpPushCommit, UploadID: up.UploadID})
	if resp.Error != nil || !resp.Unverified {
		t.Fatalf("commit answered %+v, want the pack taken as it is", resp)
	}
	if p := w.pack(root); p.Verified || p.Objects != int64(len(keys)-1) {
		t.Fatalf("the server recorded the pack as %+v", p)
	}

	bob := w.peer("bob")
	_, err = bob.client.Pull(w.ctx, bob.objects, "bad", client.PullOptions{})
	if err == nil || errors.Is(err, client.ErrNotFound) {
		t.Fatalf("pull of a pack with an object missing: %v, want it found out", err)
	}
}
