package db

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/core/key"
)

func mustCreate(t *testing.T, d *DB, u Upload) Upload {
	t.Helper()
	got, err := d.CreateUpload(ctx, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func wantNoUpload(t *testing.T, d *DB, id string) {
	t.Helper()
	_, err := d.UploadByID(ctx, id)
	wantNotFound(t, err)
}

func TestCreateUpload(t *testing.T) {
	d := open(t)
	roots := testKeys(t, "root", 4)
	base := addBase(t, d, "base", roots[0], sketchOf(roots[0]))
	addPatch(t, d, "patch", roots[1], roots[0])

	for _, parent := range []int{1, 3} {
		_, err := d.CreateUpload(ctx, upload("gone", "x", roots[2]), &roots[parent])
		if !errors.Is(err, ErrParentGone) {
			t.Fatalf("parent %d: got error %v, want ErrParentGone", parent, err)
		}
		wantNoUpload(t, d, "gone")
	}

	in := upload("up", "x", roots[2])
	in.MultipartID = "mp"
	in.ParentID = 999
	in.State = "bogus"
	got, err := d.CreateUpload(ctx, in, &roots[0])
	if err != nil {
		t.Fatal(err)
	}
	want := in
	want.ParentID = base.ID
	want.State = StatePending
	if got != want {
		t.Fatalf("created %+v, want %+v", got, want)
	}
	read, err := d.UploadByID(ctx, "up")
	if err != nil || read != want {
		t.Fatalf("read back %+v, %v; want %+v", read, err, want)
	}

	bad := roots[3]
	bad[len(bad)-1] |= 0x08
	if _, err := d.CreateUpload(ctx, upload("bad", "x", bad), nil); err == nil {
		t.Fatal("a root that is not a canonical key was recorded")
	}
	wantNoUpload(t, d, "bad")

	alone := mustCreate(t, d, upload("alone", "y", roots[3]))
	if alone.ParentID != 0 || alone.MultipartID != "" {
		t.Fatalf("an upload without a parent: %+v", alone)
	}
	list, err := d.ListUploads(ctx)
	if err != nil || len(list) != 2 {
		t.Fatalf("ListUploads: %+v, %v", list, err)
	}
	if !slices.Contains(list, want) || !slices.Contains(list, alone) {
		t.Fatalf("ListUploads = %+v", list)
	}
}

// What an upload says it shares with its parent is the client's word, and
// is recorded only if it can be true.
func TestCreateUploadHoldsWhatIsSharedToTheParent(t *testing.T) {
	d := open(t)
	roots := testKeys(t, "root", 2)
	base := addBase(t, d, "base", roots[0], sketchOf(roots[0]))
	if base.Objects == 0 || base.Bytes == 0 {
		t.Fatalf("the base pack of this test holds nothing: %+v", base)
	}
	cases := map[string]struct {
		parent         *key.Key
		objects, bytes int64
		ok             bool
	}{
		"nothing":                {&roots[0], 0, 0, true},
		"a part of the parent":   {&roots[0], 1, 1, true},
		"all the parent holds":   {&roots[0], base.Objects, base.Bytes, true},
		"nothing, and no parent": {nil, 0, 0, true},
		"more objects":           {&roots[0], base.Objects + 1, 1, false},
		"more bytes":             {&roots[0], 1, base.Bytes + 1, false},
		"objects below nothing":  {&roots[0], -1, 0, false},
		"bytes below nothing":    {&roots[0], 0, -1, false},
		"objects of no parent":   {nil, 1, 0, false},
		"bytes of no parent":     {nil, 0, 1, false},
	}
	for name, c := range cases {
		id := nextUploadID()
		in := upload(id, "x", roots[1])
		in.SharedObjects, in.SharedBytes = c.objects, c.bytes
		got, err := d.CreateUpload(ctx, in, c.parent)
		if !c.ok {
			if !errors.Is(err, ErrShared) {
				t.Errorf("%s: got error %v, want ErrShared", name, err)
			}
			wantNoUpload(t, d, id)
			continue
		}
		if err != nil || got.SharedObjects != c.objects || got.SharedBytes != c.bytes {
			t.Errorf("%s: created %+v, %v", name, got, err)
			continue
		}
		if read, err := d.UploadByID(ctx, id); err != nil || read != got {
			t.Errorf("%s: read back %+v, %v; want %+v", name, read, err, got)
		}
	}
}

// A pack on trust is recorded as one that was not verified. A base pack of
// the kind has no links, is found by its sketch like any other and can be
// leaned on; when it goes, there are no links of it to delete. A patch pack
// on trust queues nothing: links are never written for one.
func TestCommitUploadOnTrust(t *testing.T) {
	d := open(t)
	roots := testKeys(t, "root", 2)
	sk := sketchOf(roots[0])
	mustCreate(t, d, upload("base", "x", roots[0]))
	base, err := d.CommitUpload(ctx, "base", &Verified{IndexSize: 456, Objects: 10, Bytes: 1000, Sketch: sk, OnTrust: true}, t0, later)
	if err != nil {
		t.Fatal(err)
	}
	if base.Verified || base.LinksKey != "" || base.LinksSize != 0 || !base.IsBase() {
		t.Fatalf("a base pack on trust recorded as %+v", base)
	}
	if got := wantPack(t, d, roots[0]); got != base {
		t.Fatalf("read back %+v, want %+v", got, base)
	}
	// Links an earlier attempt may have written for the upload go.
	wantQueued(t, d, t0, "base.links")
	near, err := d.Nearest(ctx, sk, 3)
	if err != nil || len(near) != 1 || near[0].Pack != base {
		t.Fatalf("its sketch finds %+v, %v", near, err)
	}

	in := upload("patch", "y", roots[1])
	in.SharedObjects, in.SharedBytes = 3, 300
	u, err := d.CreateUpload(ctx, in, &roots[0])
	if err != nil {
		t.Fatal(err)
	}
	patch, err := d.CommitUpload(ctx, "patch", &Verified{
		IndexSize: 104, Objects: 2, Bytes: 50, SharedObjects: u.SharedObjects, SharedBytes: u.SharedBytes, OnTrust: true,
	}, t0, later)
	if err != nil {
		t.Fatal(err)
	}
	if patch.Verified || patch.ParentID != base.ID || patch.SharedObjects != 3 || patch.SharedBytes != 300 {
		t.Fatalf("a patch pack on trust recorded as %+v", patch)
	}
	// A verified pack beside them is not counted with them.
	addBase(t, d, "sound", testKeys(t, "sound", 1)[0], sketchOf(roots[1]))
	if s, err := d.Stats(ctx); err != nil || s.UnverifiedPacks != 2 || s.BasePacks != 2 || s.PatchPacks != 1 {
		t.Fatalf("Stats = %+v, %v", s, err)
	}

	for _, name := range []string{"x", "y"} {
		if err := d.DeleteRef(ctx, name, later); err != nil {
			t.Fatal(err)
		}
	}
	wantNoPack(t, d, roots[0])
	wantQueued(t, d, later, "base.links", "patch.data", "patch.idx", "base.data", "base.idx")
}

func TestBeginVerify(t *testing.T) {
	d := open(t)
	root := testKeys(t, "root", 1)[0]
	mustCreate(t, d, upload("up", "x", root))

	_, err := d.BeginVerify(ctx, "up", "mallory")
	wantNotFound(t, err)
	_, err = d.BeginVerify(ctx, "nothing", "alice")
	wantNotFound(t, err)

	u, err := d.BeginVerify(ctx, "up", "alice")
	if err != nil || u.State != StateVerifying || u.ID != "up" || u.Root != root {
		t.Fatalf("BeginVerify: %+v, %v", u, err)
	}
	if read, err := d.UploadByID(ctx, "up"); err != nil || read != u {
		t.Fatalf("read back %+v, %v; want %+v", read, err, u)
	}
	if _, err := d.BeginVerify(ctx, "up", "alice"); !errors.Is(err, ErrBusy) {
		t.Fatalf("a second BeginVerify: %v, want ErrBusy", err)
	}

	if err := d.EndVerify(ctx, "up"); err != nil {
		t.Fatal(err)
	}
	if read, err := d.UploadByID(ctx, "up"); err != nil || read.State != StatePending {
		t.Fatalf("after EndVerify: %+v, %v", read, err)
	}
	if _, err := d.BeginVerify(ctx, "up", "alice"); err != nil {
		t.Fatalf("BeginVerify after EndVerify: %v", err)
	}
	wantNotFound(t, d.EndVerify(ctx, "nothing"))
}

func TestCommitUploadRecordsThePack(t *testing.T) {
	d := open(t)
	roots := testKeys(t, "root", 2)
	sk := sketchOf(testKeys(t, "k", 300)...)
	in := upload("base", "x", roots[0])
	in.MultipartID = "mp"
	mustCreate(t, d, in)
	t1 := t0.Add(time.Minute)

	v := verified(sk)
	p, err := d.CommitUpload(ctx, "base", v, t1, later)
	if err != nil {
		t.Fatal(err)
	}
	want := Pack{
		ID: p.ID, Root: roots[0],
		DataKey: "base.data", IndexKey: "base.idx", LinksKey: "base.links",
		DataSize: in.DataSize, IndexSize: v.IndexSize, LinksSize: v.LinksSize,
		Objects: v.Objects, Bytes: v.Bytes,
		Uploader: "alice", UploadedAt: t1,
		Verified: true,
	}
	if p != want || wantPack(t, d, roots[0]) != want {
		t.Fatalf("pack = %+v, want %+v", p, want)
	}
	wantNoUpload(t, d, "base")
	r, err := d.Ref(ctx, "x")
	if err != nil || r != (Ref{Name: "x", Root: roots[0], PackID: p.ID, UpdatedBy: "alice", UpdatedAt: t1}) {
		t.Fatalf("ref = %+v, %v", r, err)
	}
	wantQueued(t, d, forever)
	for _, k := range []int{0, 255} {
		near, err := d.Nearest(ctx, sk[k:k+1], 3)
		if err != nil || len(near) != 1 || near[0].Pack != want {
			t.Fatalf("sketch key %d does not find the pack: %+v, %v", k, near, err)
		}
	}

	mustCreate(t, d, upload("patch", "y", roots[1]))
	_, err = d.CommitUpload(ctx, "patch", verified(nil), t1, later)
	if err == nil {
		t.Fatal("a pack without parent and without sketch was recorded")
	}
	if u, err := d.UploadByID(ctx, "patch"); err != nil || u.State != StatePending {
		t.Fatalf("the refused commit changed the upload: %+v, %v", u, err)
	}
	if err := d.FailUpload(ctx, "patch", t0, later); err != nil {
		t.Fatal(err)
	}
	drain(t, d)

	if _, err := d.CreateUpload(ctx, upload("patch", "y", roots[1]), &roots[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := d.CommitUpload(ctx, "patch", verified(sk), t1, later); err == nil {
		t.Fatal("a pack with a parent and a sketch was recorded")
	}
	v = verified(nil)
	patch, err := d.CommitUpload(ctx, "patch", v, t1, later)
	if err != nil {
		t.Fatal(err)
	}
	if patch.ParentID != p.ID || patch.LinksKey != "" || patch.LinksSize != 0 ||
		patch.SharedObjects != v.SharedObjects || patch.SharedBytes != v.SharedBytes {
		t.Fatalf("patch = %+v", patch)
	}
	// A patch pack has no links, and nothing was written under the key the
	// upload kept for them: there is nothing to clear.
	wantQueued(t, d, later)

	_, err = d.CommitUpload(ctx, "nothing", v, t1, later)
	wantNotFound(t, err)
}

func TestCommitUploadOfARootThatHasAPack(t *testing.T) {
	d := open(t)
	roots := testKeys(t, "root", 2)
	old := addBase(t, d, "old", roots[0], sketchOf(roots[0]))
	first := addBase(t, d, "x", roots[1], sketchOf(roots[1]))
	late := upload("late", "old", roots[1])
	late.MultipartID = "mp"
	late.Uploader = "bob"
	mustCreate(t, d, late)
	t1 := t0.Add(time.Minute)

	p, err := d.CommitUpload(ctx, "late", verified(sketchOf(roots[1])), t1, later)
	if err != nil {
		t.Fatal(err)
	}
	if p != first {
		t.Fatalf("got pack %+v, want the one there was, %+v", p, first)
	}
	wantNoUpload(t, d, "late")
	r, err := d.Ref(ctx, "old")
	if err != nil || r != (Ref{Name: "old", Root: roots[1], PackID: first.ID, UpdatedBy: "bob", UpdatedAt: t1}) {
		t.Fatalf("ref = %+v, %v", r, err)
	}
	wantQueued(t, d, t1.Add(-time.Second))
	wantQueued(t, d, t1, "late.data#mp", "late.data", "late.idx", "late.links")
	// The ref left the pack it pointed at before.
	wantNoPack(t, d, roots[0])
	wantQueued(t, d, later, "late.data#mp", "late.data", "late.idx", "late.links",
		old.DataKey, old.IndexKey, old.LinksKey)
}

func TestFailUpload(t *testing.T) {
	d := open(t)
	roots := testKeys(t, "root", 2)
	mustCreate(t, d, upload("one", "x", roots[0]))
	multi := upload("multi", "y", roots[1])
	multi.MultipartID = "mp"
	mustCreate(t, d, multi)
	t1 := t0.Add(time.Minute)

	if err := d.FailUpload(ctx, "one", t1, later); err != nil {
		t.Fatal(err)
	}
	wantNoUpload(t, d, "one")
	wantQueued(t, d, t1.Add(-time.Second))
	wantQueued(t, d, t1, "one.data", "one.idx", "one.links")
	drain(t, d)

	if err := d.FailUpload(ctx, "multi", t1, later); err != nil {
		t.Fatal(err)
	}
	wantNoUpload(t, d, "multi")
	wantQueued(t, d, t1, "multi.data#mp", "multi.data", "multi.idx", "multi.links")
	wantNotFound(t, d.FailUpload(ctx, "multi", t1, later))
}

func TestExpireUploads(t *testing.T) {
	d := open(t)
	roots := testKeys(t, "root", 6)
	base := addBase(t, d, "base", roots[0], sketchOf(roots[0]))
	now := t0.Add(2 * time.Hour)
	again := now.Add(Straggler) // the deadlines are past, so the second deletion counts from now
	deleteAt := now.Add(30 * time.Minute)
	at := func(id string, root int, deadline time.Time) Upload {
		u := upload(id, id, roots[root])
		u.Deadline = deadline
		return u
	}
	past := now.Add(-time.Second)

	multi := at("a-multi", 1, past)
	multi.MultipartID = "mp"
	mustCreate(t, d, multi)
	if _, err := d.CreateUpload(ctx, at("b-patch", 2, past), &roots[0]); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, d, at("c-verifying", 3, past))
	if _, err := d.BeginVerify(ctx, "c-verifying", "alice"); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, d, at("d-on-time", 4, now))
	mustCreate(t, d, at("e-early", 5, now.Add(time.Hour)))
	if err := d.DeleteRef(ctx, "base", deleteAt); err != nil {
		t.Fatal(err)
	}
	wantPack(t, d, roots[0])

	n, err := d.ExpireUploads(ctx, now, deleteAt)
	if err != nil || n != 2 {
		t.Fatalf("ExpireUploads: %d, %v; want 2", n, err)
	}
	wantNoUpload(t, d, "a-multi")
	wantNoUpload(t, d, "b-patch")
	for id, state := range map[string]string{
		"c-verifying": StateVerifying, "d-on-time": StatePending, "e-early": StatePending,
	} {
		if u, err := d.UploadByID(ctx, id); err != nil || u.State != state {
			t.Fatalf("upload %s: %+v, %v", id, u, err)
		}
	}
	first := []string{"a-multi.data#mp", "a-multi.data", "a-multi.idx", "a-multi.links", "b-patch.data", "b-patch.idx", "b-patch.links"}
	second := []string{"a-multi.data", "a-multi.idx", "a-multi.links", "b-patch.data", "b-patch.idx", "b-patch.links"}
	wantQueued(t, d, now.Add(-time.Second))
	wantQueued(t, d, now, first...)
	// The expired upload was the last hold on the base pack.
	wantNoPack(t, d, roots[0])
	collected := []string{base.DataKey, base.IndexKey, base.LinksKey}
	got := queued(t, d, again)
	slices.Sort(got)
	want := slices.Sorted(slices.Values(slices.Concat(first, second, collected)))
	if !slices.Equal(got, want) {
		t.Fatalf("deletions due at the second time:\n got %q\nwant %q", got, want)
	}
	if got := queued(t, d, again.Add(-time.Second)); len(got) != len(first)+len(collected) {
		t.Fatalf("the second deletions are due before their time: %q", got)
	}

	if n, err := d.ExpireUploads(ctx, now, deleteAt); err != nil || n != 0 {
		t.Fatalf("a second ExpireUploads: %d, %v; want 0", n, err)
	}
}

func TestResetVerifying(t *testing.T) {
	d := open(t)
	roots := testKeys(t, "root", 2)
	mustCreate(t, d, upload("a", "x", roots[0]))
	mustCreate(t, d, upload("b", "y", roots[1]))
	if _, err := d.BeginVerify(ctx, "a", "alice"); err != nil {
		t.Fatal(err)
	}

	if err := d.ResetVerifying(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		if u, err := d.UploadByID(ctx, id); err != nil || u.State != StatePending {
			t.Fatalf("upload %s: %+v, %v", id, u, err)
		}
	}
	if _, err := d.BeginVerify(ctx, "a", "alice"); err != nil {
		t.Fatalf("BeginVerify after the reset: %v", err)
	}
}

func TestDeletionsComeDueAndAreDone(t *testing.T) {
	d := open(t)
	roots := testKeys(t, "root", 2)
	mustCreate(t, d, upload("a", "x", roots[0]))
	mustCreate(t, d, upload("b", "y", roots[1]))
	t1 := t0.Add(time.Minute)
	if err := d.FailUpload(ctx, "b", t1, later); err != nil {
		t.Fatal(err)
	}
	if err := d.FailUpload(ctx, "a", t0, later); err != nil {
		t.Fatal(err)
	}

	wantQueued(t, d, t0.Add(-time.Second))
	wantQueued(t, d, t0, keysOf("a")...)
	wantQueued(t, d, t1, append(keysOf("b"), keysOf("a")...)...)
	due, err := d.DueDeletions(ctx, t1, 2)
	if err != nil || len(due) != 2 || due[0].ObjectKey != "b.data" || due[1].ObjectKey != "b.idx" {
		t.Fatalf("the first two: %+v, %v", due, err)
	}
	if due[0].ID == 0 || due[0].ID == due[1].ID {
		t.Fatalf("deletions without IDs of their own: %+v", due)
	}

	if err := d.DoneDeletion(ctx, due[0].ID); err != nil {
		t.Fatal(err)
	}
	wantQueued(t, d, t1, "b.idx", "b.links", "a.data", "a.idx", "a.links")
	if err := d.DoneDeletion(ctx, due[0].ID); err != nil {
		t.Fatalf("a deletion done twice: %v", err)
	}
	// Each upload's keys are queued a second time, for when its URLs can
	// write no more: Straggler after the deadline, which is later than the
	// failure here.
	second := upload("a", "x", roots[0]).Deadline.Add(Straggler)
	wantQueued(t, d, second.Add(-time.Second), "b.idx", "b.links", "a.data", "a.idx", "a.links")
	if got := queued(t, d, second); len(got) != 5+6 {
		t.Fatalf("due after the second round: %q", got)
	}
	if s, err := d.Stats(ctx); err != nil || s.Deletions != 11 {
		t.Fatalf("Stats: %+v, %v; want 11 deletions", s, err)
	}
}

func TestAnUploadFailedAfterItsDeadlineIsClearedAgainFromThen(t *testing.T) {
	d := open(t)
	roots := testKeys(t, "root", 1)
	u := upload("late", "x", roots[0])
	mustCreate(t, d, u)
	failedAt := u.Deadline.Add(10 * time.Minute)
	if err := d.FailUpload(ctx, "late", failedAt, later); err != nil {
		t.Fatal(err)
	}
	wantQueued(t, d, failedAt.Add(Straggler-time.Second), keysOf("late")...)
	if got := queued(t, d, failedAt.Add(Straggler)); len(got) != 6 {
		t.Fatalf("due a straggler's time after the failure: %q", got)
	}
}

func TestCommitUploadWithNothingVerified(t *testing.T) {
	d := open(t)
	roots := testKeys(t, "root", 2)
	base := addBase(t, d, "base", roots[0], sketchOf(roots[0]))

	// A patch upload: the kind a missing sketch does not give away.
	if _, err := d.CreateUpload(ctx, upload("unverified", "x", roots[1]), &roots[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := d.CommitUpload(ctx, "unverified", nil, t0, later); !errors.Is(err, ErrUnverified) {
		t.Fatalf("CommitUpload of an unverified upload whose root has no pack: %v, want ErrUnverified", err)
	}
	wantNoPack(t, d, roots[1])
	wantNotFound2(t, d, "x")
	if u, err := d.UploadByID(ctx, "unverified"); err != nil || u.State != StatePending {
		t.Fatalf("the upload after the refusal: %+v, %v", u, err)
	}
	wantQueued(t, d, later.Add(24*time.Hour))

	// The same call is fine when the root has a pack to point the ref at.
	mustCreate(t, d, upload("redundant", "again", roots[0]))
	got, err := d.CommitUpload(ctx, "redundant", nil, t0, later)
	if err != nil || got != base {
		t.Fatalf("CommitUpload onto an existing pack: %+v, %v", got, err)
	}
	if r, err := d.Ref(ctx, "again"); err != nil || r.PackID != base.ID {
		t.Fatalf("ref = %+v, %v", r, err)
	}
}

// wantNotFound2 fails the test if there is a ref called name.
func wantNotFound2(t *testing.T, d *DB, name string) {
	t.Helper()
	if _, err := d.Ref(ctx, name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ref %q: %v, want ErrNotFound", name, err)
	}
}

func TestEverythingSurvivesCloseAndOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.sqlite")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	roots := testKeys(t, "root", 5)
	sk := sketchOf(testKeys(t, "k", 20)...)
	base := addBase(t, d, "base", roots[0], sk)
	patch := addPatch(t, d, "patch", roots[1], roots[0])
	pending, err := d.CreateUpload(ctx, upload("pending", "p", roots[2]), &roots[0])
	if err != nil {
		t.Fatal(err)
	}
	mustCreate(t, d, upload("verifying", "v", roots[3]))
	verifying, err := d.BeginVerify(ctx, "verifying", "alice")
	if err != nil {
		t.Fatal(err)
	}
	failed := upload("failed", "f", roots[4])
	failed.MultipartID = "mp"
	mustCreate(t, d, failed)
	if err := d.FailUpload(ctx, "failed", later, later); err != nil {
		t.Fatal(err)
	}
	stats, err := d.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantDue := queued(t, d, later)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if got := wantPack(t, d, roots[0]); got != base {
		t.Fatalf("base = %+v, want %+v", got, base)
	}
	if got := wantPack(t, d, roots[1]); got != patch {
		t.Fatalf("patch = %+v, want %+v", got, patch)
	}
	if r, err := d.Ref(ctx, "patch"); err != nil || r.PackID != patch.ID || r.Root != roots[1] {
		t.Fatalf("ref = %+v, %v", r, err)
	}
	if near, err := d.Nearest(ctx, sk, 3); err != nil || len(near) != 1 || near[0].Pack != base || near[0].Similarity != 1 {
		t.Fatalf("Nearest: %+v, %v", near, err)
	}
	if u, err := d.UploadByID(ctx, "pending"); err != nil || u != pending {
		t.Fatalf("upload = %+v, %v; want %+v", u, err, pending)
	}
	if u, err := d.UploadByID(ctx, "verifying"); err != nil || u != verifying {
		t.Fatalf("upload = %+v, %v; want %+v", u, err, verifying)
	}
	if got, err := d.Stats(ctx); err != nil || got != stats {
		t.Fatalf("Stats: %+v, %v; want %+v", got, err, stats)
	}
	if got := queued(t, d, later); !slices.Equal(got, wantDue) || len(got) != 4 {
		t.Fatalf("deletions = %q, want %q", got, wantDue)
	}

	// What a server does at start, and its sweeper after that.
	if err := d.ResetVerifying(ctx); err != nil {
		t.Fatal(err)
	}
	if u, err := d.UploadByID(ctx, "verifying"); err != nil || u.State != StatePending {
		t.Fatalf("upload = %+v, %v", u, err)
	}
	drain(t, d)
	wantQueued(t, d, forever)
}

func TestMethodsRunAtOnce(t *testing.T) {
	d := open(t)
	const workers, rounds = 4, 25
	var wg sync.WaitGroup
	errs := make(chan error, 2*workers)
	for w := range workers {
		roots := testKeys(t, fmt.Sprintf("root%d", w), rounds)
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i, root := range roots {
				id := fmt.Sprintf("w%d-%d", w, i)
				name := fmt.Sprintf("ref%d", w)
				if _, err := d.CreateUpload(ctx, upload(id, name, root), nil); err != nil {
					errs <- err
					return
				}
				if _, err := d.CommitUpload(ctx, id, verified(sketchOf(root)), t0, later); err != nil {
					errs <- err
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for range rounds {
				if _, err := d.ListRefs(ctx, "", "", 100); err != nil {
					errs <- err
					return
				}
				if _, err := d.Stats(ctx); err != nil {
					errs <- err
					return
				}
				if _, err := d.DueDeletions(ctx, forever, 100); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	s, err := d.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Each ref moved from pack to pack, and every pack it left was collected.
	if s.Refs != workers || s.BasePacks != workers || s.Deletions != 3*workers*(rounds-1) {
		t.Fatalf("Stats = %+v", s)
	}
}
