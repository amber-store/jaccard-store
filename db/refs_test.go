package db

import (
	"slices"
	"testing"
	"time"
)

// drain carries out, as far as the database is concerned, every deletion
// queued so far.
func drain(t *testing.T, d *DB) {
	t.Helper()
	due, err := d.DueDeletions(ctx, forever, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range due {
		if err := d.DoneDeletion(ctx, x.ID); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRefMovedOffAPackCollectsIt(t *testing.T) {
	d := open(t)
	roots := testKeys(t, "root", 3)
	skA, skB := sketchOf(testKeys(t, "a", 5)...), sketchOf(testKeys(t, "b", 5)...)
	a := addBase(t, d, "x", roots[0], skA)
	b := addBase(t, d, "y", roots[1], skB)
	t1 := t0.Add(time.Minute)

	moved, err := d.PointRef(ctx, "x", roots[2], "bob", t1, later)
	if err != nil || moved {
		t.Fatalf("pointing at a root without a pack: %v, %v; want false", moved, err)
	}
	if r, err := d.Ref(ctx, "x"); err != nil || r.PackID != a.ID {
		t.Fatalf("the ref changed: %+v, %v", r, err)
	}

	moved, err = d.PointRef(ctx, "x", roots[1], "bob", t1, later)
	if err != nil || !moved {
		t.Fatalf("PointRef: %v, %v", moved, err)
	}
	r, err := d.Ref(ctx, "x")
	if err != nil {
		t.Fatal(err)
	}
	if want := (Ref{Name: "x", Root: roots[1], PackID: b.ID, UpdatedBy: "bob", UpdatedAt: t1}); r != want {
		t.Fatalf("ref = %+v, want %+v", r, want)
	}
	wantNoPack(t, d, roots[0])
	wantPack(t, d, roots[1])
	wantQueued(t, d, later.Add(-time.Second))
	wantQueued(t, d, later, a.DataKey, a.IndexKey, a.LinksKey)

	near, err := d.Nearest(ctx, skA, 3)
	if err != nil || len(near) != 0 {
		t.Fatalf("the sketch keys of a collected pack remain: %+v, %v", near, err)
	}
}

func TestPackWithTwoRefsOutlivesOne(t *testing.T) {
	d := open(t)
	root := testKeys(t, "root", 1)[0]
	a := addBase(t, d, "x", root, sketchOf(root))
	if ok, err := d.PointRef(ctx, "y", root, "bob", t0, later); err != nil || !ok {
		t.Fatalf("PointRef: %v, %v", ok, err)
	}

	if err := d.DeleteRef(ctx, "x", later); err != nil {
		t.Fatal(err)
	}
	_, err := d.Ref(ctx, "x")
	wantNotFound(t, err)
	wantNotFound(t, d.DeleteRef(ctx, "x", later))
	wantPack(t, d, root)
	wantQueued(t, d, forever)

	if err := d.DeleteRef(ctx, "y", later); err != nil {
		t.Fatal(err)
	}
	wantNoPack(t, d, root)
	wantQueued(t, d, later, a.DataKey, a.IndexKey, a.LinksKey)
}

func TestBasePackGoesWithItsLastChild(t *testing.T) {
	d := open(t)
	roots := testKeys(t, "root", 2)
	base := addBase(t, d, "base", roots[0], sketchOf(roots[0]))
	patch := addPatch(t, d, "patch", roots[1], roots[0])
	if patch.IsBase() || patch.ParentID != base.ID || patch.LinksKey != "" {
		t.Fatalf("patch = %+v", patch)
	}
	drain(t, d)

	if err := d.DeleteRef(ctx, "base", later); err != nil {
		t.Fatal(err)
	}
	wantPack(t, d, roots[0])
	wantQueued(t, d, forever)

	if err := d.DeleteRef(ctx, "patch", later); err != nil {
		t.Fatal(err)
	}
	wantNoPack(t, d, roots[1])
	wantNoPack(t, d, roots[0])
	wantQueued(t, d, later.Add(-time.Second))
	wantQueued(t, d, later, patch.DataKey, patch.IndexKey, base.DataKey, base.IndexKey, base.LinksKey)
}

func TestBasePackNamedByAnOpenUploadSurvives(t *testing.T) {
	d := open(t)
	roots := testKeys(t, "root", 2)
	base := addBase(t, d, "base", roots[0], sketchOf(roots[0]))
	u, err := d.CreateUpload(ctx, upload("open", "patch", roots[1]), &roots[0])
	if err != nil {
		t.Fatal(err)
	}
	if u.ParentID != base.ID {
		t.Fatalf("the upload names pack %d, want %d", u.ParentID, base.ID)
	}

	if err := d.DeleteRef(ctx, "base", later); err != nil {
		t.Fatal(err)
	}
	wantPack(t, d, roots[0])
	wantQueued(t, d, forever)

	if err := d.FailUpload(ctx, u.ID, t0, later); err != nil {
		t.Fatal(err)
	}
	wantNoPack(t, d, roots[0])
	wantQueued(t, d, t0, keysOf("open")...)
	wantQueued(t, d, later, append(keysOf("open"), base.DataKey, base.IndexKey, base.LinksKey)...)
}

func TestListRefs(t *testing.T) {
	d := open(t)
	root := testKeys(t, "root", 1)[0]
	addBase(t, d, "b", root, sketchOf(root))
	// In the order of their bytes.
	names := []string{"a%b", "a%c", `a\b`, `a\c`, "a_b", "ab", "axb", "b", "é", "\xff\xffa"}
	for _, name := range slices.Backward(names) {
		if ok, err := d.PointRef(ctx, name, root, "bob", t0, later); err != nil || !ok {
			t.Fatalf("PointRef %q: %v, %v", name, ok, err)
		}
	}
	list := func(prefix, after string, limit int) []string {
		t.Helper()
		refs, err := d.ListRefs(ctx, prefix, after, limit)
		if err != nil {
			t.Fatal(err)
		}
		out := []string{}
		for _, r := range refs {
			if r.Root != root || r.UpdatedBy != "bob" || !r.UpdatedAt.Equal(t0) || r.PackID == 0 {
				t.Fatalf("ref = %+v", r)
			}
			out = append(out, r.Name)
		}
		return out
	}
	for _, c := range []struct {
		prefix, after string
		limit         int
		want          []string
	}{
		{"", "", 100, names},
		{"", "", 3, names[:3]},
		{"", names[2], 3, names[3:6]},
		{"", names[len(names)-1], 3, nil},
		{"", "", 0, nil},
		{"", "", -1, nil},
		{"a", "", 100, names[:7]},
		{"a", "a%c", 2, names[2:4]},
		{"a", "0", 2, names[:2]},
		{"a", "c", 100, nil},
		{"a%", "", 100, []string{"a%b", "a%c"}},
		{"a_", "", 100, []string{"a_b"}},
		{`a\`, "", 100, []string{`a\b`, `a\c`}},
		{"%", "", 100, nil},
		{"_", "", 100, nil},
		{"b", "", 100, []string{"b"}},
		{"é", "", 100, []string{"é"}},
		{"\xff\xff", "", 100, []string{"\xff\xffa"}},
		{"\xff", "", 100, []string{"\xff\xffa"}},
	} {
		got := list(c.prefix, c.after, c.limit)
		if c.want == nil {
			c.want = []string{}
		}
		if !slices.Equal(got, c.want) {
			t.Errorf("ListRefs(%q, %q, %d) = %q, want %q", c.prefix, c.after, c.limit, got, c.want)
		}
	}

	// A name is itself and no pattern, wherever it is given.
	if r, err := d.Ref(ctx, "a_b"); err != nil || r.Name != "a_b" {
		t.Fatalf("Ref: %+v, %v", r, err)
	}
	_, err := d.Ref(ctx, "a_")
	wantNotFound(t, err)
	wantNotFound(t, d.DeleteRef(ctx, "a%", later))
	wantNotFound(t, d.DeleteRef(ctx, "a_c", later))
	if err := d.DeleteRef(ctx, "a%b", later); err != nil {
		t.Fatal(err)
	}
	if got := list("a", "", 100); !slices.Equal(got, names[1:7]) {
		t.Fatalf("after deleting a%%b: %q", got)
	}
}

func TestSuccessor(t *testing.T) {
	for _, c := range []struct {
		prefix, want string
		bounded      bool
	}{
		{"", "", false},
		{"a", "b", true},
		{"ab", "ac", true},
		{"a\xff", "b", true},
		{"a\xff\xff", "b", true},
		{"\xff", "", false},
		{"\xff\xff", "", false},
	} {
		got, bounded := successor(c.prefix)
		if got != c.want || bounded != c.bounded {
			t.Errorf("successor(%q) = %q, %v; want %q, %v", c.prefix, got, bounded, c.want, c.bounded)
		}
	}
}
