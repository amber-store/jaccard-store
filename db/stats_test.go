package db

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/amber-store/core/key"
)

// figures fills a database with a base pack under "a", a patch pack on it
// under "p" and "p2", a second base pack under "b", an open upload and the
// six queued deletions of a failed one (its three keys, twice), and returns
// the three packs and their roots.
func figures(t *testing.T, d *DB) (a, p, b Pack, roots []key.Key) {
	t.Helper()
	roots = testKeys(t, "root", 5)
	commit := func(id, name string, root int, parent *key.Key, dataSize int64, v Verified) Pack {
		t.Helper()
		u := upload(id, name, roots[root])
		u.DataSize = dataSize
		if _, err := d.CreateUpload(ctx, u, parent); err != nil {
			t.Fatal(err)
		}
		pack, err := d.CommitUpload(ctx, id, &v, t0, later)
		if err != nil {
			t.Fatal(err)
		}
		return pack
	}
	a = commit("a", "a", 0, nil, 100, Verified{IndexSize: 60, LinksSize: 20, Objects: 3, Bytes: 1000, Sketch: sketchOf(roots[0])})
	p = commit("p", "p", 1, &roots[0], 10, Verified{IndexSize: 104, Objects: 2, Bytes: 50, SharedObjects: 2, SharedBytes: 700})
	b = commit("b", "b", 2, nil, 200, Verified{IndexSize: 148, LinksSize: 30, Objects: 5, Bytes: 3000, Sketch: sketchOf(roots[2])})
	if ok, err := d.PointRef(ctx, "p2", roots[1], "bob", t0, later); err != nil || !ok {
		t.Fatalf("PointRef: %v, %v", ok, err)
	}
	if _, err := d.CreateUpload(ctx, upload("open", "o", roots[3]), &roots[0]); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, d, upload("failed", "f", roots[4]))
	if err := d.FailUpload(ctx, "failed", t0, later); err != nil {
		t.Fatal(err)
	}
	return a, p, b, roots
}

func TestStats(t *testing.T) {
	d := open(t)
	if s, err := d.Stats(ctx); err != nil || s != (Stats{}) {
		t.Fatalf("Stats of an empty store: %+v, %v", s, err)
	}
	_, _, _, roots := figures(t, d)
	// What a root's key says its tree comes to when it is unpacked.
	unpacked := func(i int) int64 { return int64(roots[i].Length()) }

	got, err := d.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := Stats{
		Refs: 4, BasePacks: 2, PatchPacks: 1, Uploads: 1, Deletions: 6,
		// Over refs: "a", "p" and "p2" on one pack, "b".
		UnpackedBytes: unpacked(0) + 2*unpacked(1) + unpacked(2),
		ObjectBytes:   1000 + 2*(50+700) + 3000,
		// Over packs, each once.
		PackBytes:  1000 + 50 + 3000,
		DataBytes:  100 + 10 + 200,
		IndexBytes: (60 + 20) + 104 + (148 + 30),
		// Every pack has a ref.
		ReferencedPackBytes: 1000 + 50 + 3000,
	}
	if got != want {
		t.Fatalf("Stats = %+v, want %+v", got, want)
	}
	if want.UnpackedBytes == 0 {
		t.Fatal("the roots of this test unpack to nothing: the figure is not tested")
	}

	// The ref of the base pack goes; the pack stays for the patch pack
	// that leans on it, and is now what the sharing costs.
	if err := d.DeleteRef(ctx, "a", later); err != nil {
		t.Fatal(err)
	}
	want.Refs = 3
	want.UnpackedBytes -= unpacked(0)
	want.ObjectBytes -= 1000
	want.UnreferencedPacks = 1
	want.UnreferencedPackBytes, want.UnreferencedDataBytes = 1000, 100
	want.ReferencedPackBytes = 50 + 3000
	if got, err = d.Stats(ctx); err != nil || got != want {
		t.Fatalf("Stats with a pack no ref points at = %+v, %v\nwant %+v", got, err, want)
	}
	// The parts are the whole.
	if got.ReferencedPackBytes+got.UnreferencedPackBytes != got.PackBytes {
		t.Fatalf("packs of %d and %d bytes make %d", got.ReferencedPackBytes, got.UnreferencedPackBytes, got.PackBytes)
	}
}

// A database from before packs had an unpacked size gets one for every pack
// when this release opens it, from the roots.
func TestMigrationFillsTheUnpackedSizes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.sqlite")
	// The store as the first release left it: its one migration applied,
	// and packs written without the column.
	first, err := embedded.ReadFile("migrations/0001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	old, err := sql.Open("sqlite", dataSourceName(path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Exec(string(first) + "; PRAGMA user_version = 1"); err != nil {
		t.Fatal(err)
	}
	roots := testKeys(t, "root", 3)
	for i, root := range roots {
		if _, err := old.Exec(`INSERT INTO packs
			(root, data_key, index_key, data_size, index_size, links_size, objects, bytes, shared_objects, shared_bytes, uploader, uploaded_at)
			VALUES (?, ?, ?, 1, 1, 0, 1, 1, 0, 0, 'alice', 0)`, root[:], fmt.Sprint("d", i), fmt.Sprint("i", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := old.Exec("INSERT INTO refs (name, pack_id, updated_by, updated_at) VALUES ('one', 1, 'alice', 0), ('two', 3, 'alice', 0)"); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}

	d, err := Open(path)
	if err != nil {
		t.Fatalf("opening the old store with this release: %v", err)
	}
	for _, root := range roots {
		var unpacked int64
		if err := d.writer.QueryRowContext(ctx, "SELECT unpacked FROM packs WHERE root = ?", root[:]).Scan(&unpacked); err != nil {
			t.Fatal(err)
		}
		if unpacked != int64(root.Length()) || unpacked == 0 {
			t.Errorf("the pack of %s unpacks to %d, its key says %d", root, unpacked, root.Length())
		}
	}
	// And the figures of the store have them.
	if s, err := d.Stats(ctx); err != nil || s.UnpackedBytes != int64(roots[0].Length()+roots[2].Length()) {
		t.Fatalf("Stats = %+v, %v", s, err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	// Opened again there is nothing left to do, and it is as it was.
	d, err = Open(path)
	if err != nil {
		t.Fatalf("opening it a second time: %v", err)
	}
	defer d.Close()
	if s, err := d.Stats(ctx); err != nil || s.UnpackedBytes != int64(roots[0].Length()+roots[2].Length()) || s.Refs != 2 {
		t.Fatalf("Stats after the second opening = %+v, %v", s, err)
	}
}

func TestListRefInfo(t *testing.T) {
	d := open(t)
	a, p, b, _ := figures(t, d)

	got, err := d.ListRefInfo(ctx, "", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d refs, want 4", len(got))
	}
	for i, want := range []struct {
		name   string
		pack   Pack
		parent *Pack
	}{{"a", a, nil}, {"b", b, nil}, {"p", p, &a}, {"p2", p, &a}} {
		g := got[i]
		if g.Ref.Name != want.name || g.Ref.PackID != want.pack.ID || g.Ref.Root != want.pack.Root || g.Pack != want.pack {
			t.Errorf("ref %d = %+v, want %s on %+v", i, g, want.name, want.pack)
		}
		if !reflect.DeepEqual(g.Parent, want.parent) {
			t.Errorf("ref %d: parent %+v, want %+v", i, g.Parent, want.parent)
		}
	}

	got, err = d.ListRefInfo(ctx, "p", "p", 10)
	if err != nil || len(got) != 1 || got[0].Ref.Name != "p2" {
		t.Fatalf("after p: %+v, %v", got, err)
	}
	got, err = d.ListRefInfo(ctx, "", "", 2)
	if err != nil || len(got) != 2 || got[1].Ref.Name != "b" {
		t.Fatalf("the first two: %+v, %v", got, err)
	}
}

func TestListPacks(t *testing.T) {
	d := open(t)
	a, p, b, roots := figures(t, d)

	got, err := d.ListPacks(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	want := []PackInfo{
		{Pack: a, Refs: 1, Children: 1},
		{Pack: p, ParentRoot: &roots[0], Refs: 2},
		{Pack: b, Refs: 1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListPacks = %+v, want %+v", got, want)
	}

	got, err = d.ListPacks(ctx, a.ID, 1)
	if err != nil || !reflect.DeepEqual(got, want[1:2]) {
		t.Fatalf("one pack after the first: %+v, %v", got, err)
	}
	got, err = d.ListPacks(ctx, b.ID, 10)
	if err != nil || len(got) != 0 {
		t.Fatalf("after the last: %+v, %v", got, err)
	}
}

func TestTopPacks(t *testing.T) {
	d := open(t)
	if byRefs, byChildren, err := d.TopPacks(ctx, 20); err != nil || len(byRefs) != 0 || len(byChildren) != 0 {
		t.Fatalf("TopPacks of an empty store: %v, %v, %v", byRefs, byChildren, err)
	}
	a, p, b, roots := figures(t, d)
	// A second patch pack on "a" that uses less of it than the first, and
	// one on "b": "a" is leaned on twice, "b" once.
	more := testKeys(t, "more", 2)
	for i, on := range []int{0, 2} {
		id := fmt.Sprint("more", i)
		if _, err := d.CreateUpload(ctx, upload(id, id, more[i]), &roots[on]); err != nil {
			t.Fatal(err)
		}
		if _, err := d.CommitUpload(ctx, id, &Verified{IndexSize: 60, Objects: 1, Bytes: 10, SharedObjects: 1, SharedBytes: 300}, t0, later); err != nil {
			t.Fatal(err)
		}
	}

	byRefs, byChildren, err := d.TopPacks(ctx, 20)
	if err != nil {
		t.Fatal(err)
	}
	// The patch pack has two refs; every other pack one, and among equals
	// the older comes first.
	if len(byRefs) != 5 || byRefs[0].Pack != p || byRefs[0].Refs != 2 || byRefs[0].ParentRoot == nil || *byRefs[0].ParentRoot != roots[0] ||
		byRefs[1].Pack != a || byRefs[1].Refs != 1 || byRefs[1].Children != 2 || byRefs[1].LargestShare != 700 || byRefs[1].ParentRoot != nil ||
		byRefs[2].Pack != b || byRefs[2].Children != 1 || byRefs[2].LargestShare != 300 {
		t.Fatalf("by refs: %+v", byRefs)
	}
	// Leaned on: "a" by two, of which one uses 700 of its bytes and one
	// 300; "b" by one. The patch packs are leaned on by none and not here.
	if len(byChildren) != 2 || byChildren[0].Pack != a || byChildren[0].Children != 2 || byChildren[0].Refs != 1 || byChildren[0].LargestShare != 700 ||
		byChildren[1].Pack != b || byChildren[1].Children != 1 || byChildren[1].LargestShare != 300 || byChildren[1].ParentRoot != nil {
		t.Fatalf("by children: %+v", byChildren)
	}

	// A pack that lost its ref is still leaned on, and in that list alone.
	if err := d.DeleteRef(ctx, "a", later); err != nil {
		t.Fatal(err)
	}
	byRefs, byChildren, err = d.TopPacks(ctx, 1)
	if err != nil || len(byRefs) != 1 || byRefs[0].Pack != p || len(byChildren) != 1 || byChildren[0].Pack != a || byChildren[0].Refs != 0 {
		t.Fatalf("the first of each after the ref went: %+v, %+v, %v", byRefs, byChildren, err)
	}
}

func TestPackDetail(t *testing.T) {
	d := open(t)
	a, p, _, roots := figures(t, d)

	got, err := d.PackDetail(ctx, roots[0])
	if err != nil {
		t.Fatal(err)
	}
	if got.Pack != a || got.ParentRoot != nil || got.Parent != nil || got.Refs != 1 || got.PackInfo.Children != 1 ||
		!slices.Equal(got.RefNames, []string{"a"}) || !slices.Equal(got.Children, roots[1:2]) {
		t.Fatalf("detail of the base pack: %+v", got)
	}

	got, err = d.PackDetail(ctx, roots[1])
	if err != nil {
		t.Fatal(err)
	}
	if got.Pack != p || got.ParentRoot == nil || *got.ParentRoot != roots[0] || got.Parent == nil || *got.Parent != a ||
		got.Refs != 2 || got.PackInfo.Children != 0 ||
		!slices.Equal(got.RefNames, []string{"p", "p2"}) || got.Children == nil || len(got.Children) != 0 {
		t.Fatalf("detail of the patch pack: %+v", got)
	}

	_, err = d.PackDetail(ctx, roots[4])
	wantNotFound(t, err)
}
