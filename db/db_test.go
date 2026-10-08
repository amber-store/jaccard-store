package db

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/keyset"
	"github.com/amber-store/jaccard-store/sketch"
)

var (
	ctx = context.Background()
	// t0 is the time the tests begin at; later is when what they collect may
	// leave the bucket.
	t0    = time.Unix(1_800_000_000, 0).UTC()
	later = t0.Add(time.Hour)
	// forever is past every not_before a test queues.
	forever = t0.Add(1000 * time.Hour)
)

func open(t *testing.T) *DB {
	t.Helper()
	d, err := Open(filepath.Join(t.TempDir(), "store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

// testKeys returns n distinct canonical keys, ascending. Different seeds give
// different keys.
func testKeys(t *testing.T, seed string, n int) []key.Key {
	t.Helper()
	keys := make([]key.Key, n)
	for i := range keys {
		data := []byte(fmt.Sprintf("%s/%d", seed, i))
		k, err := key.New(key.Blob, uint64(len(data)), data)
		if err != nil {
			t.Fatal(err)
		}
		keys[i] = k
	}
	return keyset.Normalize(keys)
}

// sketchOf is the sketch of the given keys, in any order.
func sketchOf(keys ...key.Key) sketch.Sketch {
	return sketch.Of(keyset.Normalize(slices.Clone(keys)))
}

// upload is the upload a test opens for root under name; id also names its
// keys in the bucket.
func upload(id, name string, root key.Key) Upload {
	return Upload{
		ID:       id,
		Name:     name,
		Root:     root,
		Uploader: "alice",
		DataKey:  id + ".data",
		IndexKey: id + ".idx",
		LinksKey: id + ".links",
		DataSize: 100,
		Objects:  10,
		IssuedAt: t0,
		Deadline: t0.Add(time.Hour),
	}
}

// keysOf are the three bucket keys of the upload called id.
func keysOf(id string) []string {
	return []string{id + ".data", id + ".idx", id + ".links"}
}

var uploadIDs struct {
	sync.Mutex
	n int
}

func nextUploadID() string {
	uploadIDs.Lock()
	defer uploadIDs.Unlock()
	uploadIDs.n++
	return fmt.Sprintf("u%04d", uploadIDs.n)
}

// verified is what a test commits: a base pack with the sketch sk, or a patch
// pack when sk is nil.
func verified(sk sketch.Sketch) *Verified {
	v := &Verified{IndexSize: 16 + 44*10, Objects: 10, Bytes: 1000, Sketch: sk}
	if sk == nil {
		v.SharedObjects, v.SharedBytes = 4, 400
	} else {
		v.LinksSize = 200
	}
	return v
}

// addBase commits a base pack of root under the ref name and returns it. Its
// bucket keys are those of an upload called as the pack's DataKey says.
func addBase(t *testing.T, d *DB, name string, root key.Key, sk sketch.Sketch) Pack {
	t.Helper()
	return addPack(t, d, name, root, nil, sk)
}

// addPatch commits a patch pack of root on the base pack of parent.
func addPatch(t *testing.T, d *DB, name string, root, parent key.Key) Pack {
	t.Helper()
	return addPack(t, d, name, root, &parent, nil)
}

func addPack(t *testing.T, d *DB, name string, root key.Key, parent *key.Key, sk sketch.Sketch) Pack {
	t.Helper()
	u, err := d.CreateUpload(ctx, upload(nextUploadID(), name, root), parent)
	if err != nil {
		t.Fatal(err)
	}
	p, err := d.CommitUpload(ctx, u.ID, verified(sk), t0, later)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// queued returns the deletions due at the given time, as "key" for an object
// and "key#multipart" for an abort, in the order they were queued.
func queued(t *testing.T, d *DB, at time.Time) []string {
	t.Helper()
	due, err := d.DueDeletions(ctx, at, 1000)
	if err != nil {
		t.Fatal(err)
	}
	out := []string{}
	for _, x := range due {
		if x.MultipartID != "" {
			out = append(out, x.ObjectKey+"#"+x.MultipartID)
		} else {
			out = append(out, x.ObjectKey)
		}
	}
	return out
}

func wantQueued(t *testing.T, d *DB, at time.Time, want ...string) {
	t.Helper()
	got := queued(t, d, at)
	if want == nil {
		want = []string{}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("deletions due at %s:\n got %q\nwant %q", at.Sub(t0), got, want)
	}
}

func wantNotFound(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("got error %v, want ErrNotFound", err)
	}
}

func wantNoPack(t *testing.T, d *DB, root key.Key) {
	t.Helper()
	_, err := d.PackByRoot(ctx, root)
	wantNotFound(t, err)
}

func wantPack(t *testing.T, d *DB, root key.Key) Pack {
	t.Helper()
	p, err := d.PackByRoot(ctx, root)
	if err != nil {
		t.Fatalf("pack of %s: %v", root, err)
	}
	return p
}

func TestOpenTwiceKeepsTheSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.sqlite")
	for range 2 {
		d, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.Stats(ctx); err != nil {
			t.Fatal(err)
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
