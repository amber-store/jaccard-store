package db

import (
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
	figures(t, d)

	got, err := d.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := Stats{
		Refs: 4, BasePacks: 2, PatchPacks: 1, Uploads: 1, Deletions: 6,
		S3Bytes:      (100 + 60 + 20) + (10 + 104) + (200 + 148 + 30),
		DataBytes:    100 + 10 + 200,
		StoredBytes:  1000 + 50 + 3000,
		LogicalBytes: 1000 + 2*(50+700) + 3000,
	}
	if got != want {
		t.Fatalf("Stats = %+v, want %+v", got, want)
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
