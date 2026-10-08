package db

import (
	"fmt"
	"slices"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/sketch"
)

func TestPackIsReadBackByRootAndID(t *testing.T) {
	d := open(t)
	keys := testKeys(t, "k", 3)
	p := addBase(t, d, "a", keys[0], sketchOf(keys...))

	if p.ID == 0 || p.Root != keys[0] || !p.IsBase() || p.ParentID != 0 {
		t.Fatalf("pack = %+v", p)
	}
	if p.LinksKey != p.DataKey[:len(p.DataKey)-len(".data")]+".links" {
		t.Fatalf("a base pack keeps its links key: %+v", p)
	}
	if !p.UploadedAt.Equal(t0) || p.UploadedAt.Location().String() != "UTC" {
		t.Fatalf("uploaded at %v, want %v in UTC", p.UploadedAt, t0)
	}
	v := verified(sketchOf(keys...))
	if p.DataSize != 100 || p.IndexSize != v.IndexSize || p.LinksSize != v.LinksSize ||
		p.Objects != v.Objects || p.Bytes != v.Bytes || p.SharedObjects != 0 || p.SharedBytes != 0 ||
		p.Uploader != "alice" {
		t.Fatalf("pack = %+v", p)
	}
	if got := wantPack(t, d, keys[0]); got != p {
		t.Fatalf("by root: %+v, want %+v", got, p)
	}
	got, err := d.PackByID(ctx, p.ID)
	if err != nil || got != p {
		t.Fatalf("by id: %+v, %v; want %+v", got, err, p)
	}

	wantNoPack(t, d, keys[1])
	_, err = d.PackByID(ctx, p.ID+100)
	wantNotFound(t, err)
}

func TestNearestOrdersBySimilarity(t *testing.T) {
	d := open(t)
	keys := testKeys(t, "k", 40)
	roots := testKeys(t, "root", 4)
	query := sketchOf(keys[:20]...)
	sketches := []sketch.Sketch{
		sketchOf(keys[:20]...),   // the same set: 1
		sketchOf(keys[5:25]...),  // 15 of 25
		sketchOf(keys[15:35]...), // 5 of 35
		sketchOf(keys[25:40]...), // nothing
	}
	for i, sk := range sketches {
		addBase(t, d, "r"+string(rune('a'+i)), roots[i], sk)
	}

	got, err := d.Nearest(ctx, query, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d candidates, want 3", len(got))
	}
	for i, c := range got {
		if c.Pack.Root != roots[i] {
			t.Errorf("candidate %d is %s, want %s", i, c.Pack.Root, roots[i])
		}
		if want := sketch.Jaccard(query, sketches[i]); c.Similarity != want {
			t.Errorf("candidate %d: similarity %v, want %v", i, c.Similarity, want)
		}
		if c.Pack != wantPack(t, d, roots[i]) {
			t.Errorf("candidate %d carries %+v", i, c.Pack)
		}
	}
	if got[0].Similarity != 1 || got[1].Similarity != 15.0/25 || got[2].Similarity != 5.0/35 {
		t.Fatalf("similarities %v, %v, %v", got[0].Similarity, got[1].Similarity, got[2].Similarity)
	}

	got, err = d.Nearest(ctx, query, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Pack.Root != roots[0] || got[1].Pack.Root != roots[1] {
		t.Fatalf("the best two: %+v", got)
	}
}

func TestNearestNeverOffersAPatchPack(t *testing.T) {
	d := open(t)
	keys := testKeys(t, "k", 10)
	roots := testKeys(t, "root", 2)
	addBase(t, d, "base", roots[0], sketchOf(keys...))
	addPatch(t, d, "patch", roots[1], roots[0])

	got, err := d.Nearest(ctx, sketchOf(keys...), 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Pack.Root != roots[0] {
		t.Fatalf("got %+v, want the base pack alone", got)
	}
}

func TestNearestFindsNothingWithoutASharedKey(t *testing.T) {
	d := open(t)
	keys := testKeys(t, "k", 20)
	addBase(t, d, "a", keys[0], sketchOf(keys[:10]...))

	for _, query := range []sketch.Sketch{sketchOf(keys[10:]...), nil} {
		got, err := d.Nearest(ctx, query, 3)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("got %+v, want nothing", got)
		}
	}
}

func TestNearestTiesGoToTheLowerRoot(t *testing.T) {
	d := open(t)
	keys := testKeys(t, "k", 10)
	roots := testKeys(t, "root", 17)
	// The highest root is committed first: neither the order of commits nor
	// the pack IDs decide. All 17 packs share the same keys, so the highest
	// root is also the one that is left out of the 16 that are scored.
	for i, root := range slices.Backward(roots) {
		addBase(t, d, fmt.Sprintf("r%d", i), root, sketchOf(keys...))
	}

	got, err := d.Nearest(ctx, sketchOf(keys...), 17)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 16 {
		t.Fatalf("got %d candidates, want 16", len(got))
	}
	for i, c := range got {
		if c.Pack.Root != roots[i] || c.Similarity != 1 {
			t.Fatalf("candidate %d is %s (%v), want %s", i, c.Pack.Root, c.Similarity, roots[i])
		}
	}
}

func TestNearestTiesGoToThePackSharingMoreKeys(t *testing.T) {
	d := open(t)
	keys := testKeys(t, "k", 800)
	roots := testKeys(t, "root", 2)
	var even, odd []key.Key
	for i := 0; i < 2*sketch.Size; i += 2 {
		even = append(even, keys[i])
		odd = append(odd, keys[i+1])
	}
	query := sketchOf(even...)
	// fewer shares the 56 lowest keys of the query and nothing else.
	fewer := sketchOf(append(slices.Clone(even[:56]), keys[600:800]...)...)
	// more shares the 156 highest keys of the query. Below them lie 200 keys
	// that only one of the two sketches has, so the estimate, which looks at
	// the lowest 256 keys of the union, sees 56 of the shared ones.
	more := sketchOf(append(slices.Clone(odd[:100]), even[100:]...)...)
	if a, b := sketch.Jaccard(query, fewer), sketch.Jaccard(query, more); a != b || a != 56.0/256 {
		t.Fatalf("the estimates are %v and %v, want 56/256 for both", a, b)
	}
	addBase(t, d, "fewer", roots[0], fewer)
	addBase(t, d, "more", roots[1], more)

	got, err := d.Nearest(ctx, query, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Pack.Root != roots[1] || got[1].Pack.Root != roots[0] {
		t.Fatalf("got %+v, want the pack sharing more keys first, although its root is higher", got)
	}
}

func TestNearestScoresThe16PacksSharingTheMostKeys(t *testing.T) {
	d := open(t)
	keys := testKeys(t, "k", 800)
	roots := testKeys(t, "root", 17)
	var even, odd []key.Key
	for i := 0; i < 2*sketch.Size; i += 2 {
		even = append(even, keys[i])
		odd = append(odd, keys[i+1])
	}
	query := sketchOf(even...)
	// As in the test above: more shares 156 keys and is estimated at 56/256,
	// best shares 100 keys and is estimated at 100/256. Sixteen packs like
	// more keep best, the most similar pack of all, from being scored.
	more := sketchOf(append(slices.Clone(odd[:100]), even[100:]...)...)
	best := sketchOf(append(slices.Clone(even[:100]), keys[600:800]...)...)
	if a, b := sketch.Jaccard(query, more), sketch.Jaccard(query, best); a != 56.0/256 || b != 100.0/256 {
		t.Fatalf("the estimates are %v and %v", a, b)
	}
	addBase(t, d, "best", roots[0], best)
	for i, root := range roots[1:] {
		addBase(t, d, fmt.Sprintf("more%d", i), root, more)
	}

	got, err := d.Nearest(ctx, query, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d candidates, want 3", len(got))
	}
	for i, c := range got {
		if c.Pack.Root != roots[i+1] || c.Similarity != 56.0/256 {
			t.Fatalf("candidate %d is %s (%v), want %s", i, c.Pack.Root, c.Similarity, roots[i+1])
		}
	}
}

func TestNearestLeavesOutAPackEstimatedAtZero(t *testing.T) {
	d := open(t)
	keys := testKeys(t, "k", 2*sketch.Size)
	var even, odd []key.Key
	for i := 0; i < len(keys); i += 2 {
		even = append(even, keys[i])
		odd = append(odd, keys[i+1])
	}
	query := sketchOf(even...)
	// The one key the two sketches share is the highest of both, far above
	// the lowest 256 keys of their union, which is all the estimate looks at.
	other := sketchOf(append(slices.Clone(odd[:sketch.Size-1]), even[sketch.Size-1])...)
	if j := sketch.Jaccard(query, other); j != 0 {
		t.Fatalf("the estimate is %v, want 0", j)
	}
	addBase(t, d, "a", testKeys(t, "root", 1)[0], other)

	got, err := d.Nearest(ctx, query, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("got %+v, want nothing", got)
	}
}
