package e2e

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/human"
	"github.com/amber-store/jaccard-store/packfile"
	"github.com/amber-store/jaccard-store/server"
)

// step is one step a push or a pull reported.
type step struct {
	name    string
	total   uint64
	unit    client.Unit
	done    int64
	summary string
	ended   bool
}

// steps is a client.Progress that keeps what it is told, and fails the
// test when it is told out of turn.
type steps struct {
	t    *testing.T
	mu   sync.Mutex
	list []*step
}

func (s *steps) Begin(name string, total uint64, unit client.Unit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := len(s.list); n > 0 && !s.list[n-1].ended {
		s.t.Errorf("%q began while %q was running", name, s.list[n-1].name)
	}
	s.list = append(s.list, &step{name: name, total: total, unit: unit})
}

func (s *steps) Advance(n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.list) == 0 || s.list[len(s.list)-1].ended {
		s.t.Errorf("%d reported while no step was running", n)
		return
	}
	s.list[len(s.list)-1].done += n
}

func (s *steps) End(summary string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.list) == 0 || s.list[len(s.list)-1].ended {
		s.t.Errorf("a step ended with %q while none was running", summary)
		return
	}
	s.list[len(s.list)-1].summary, s.list[len(s.list)-1].ended = summary, true
}

// names checks that the steps were these, each of them ended, and returns
// them by name. A name that comes twice is kept as its last.
func (s *steps) names(want ...string) map[string]*step {
	s.t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var got []string
	by := map[string]*step{}
	for _, st := range s.list {
		got = append(got, st.name)
		by[st.name] = st
		if !st.ended {
			s.t.Errorf("step %q never ended", st.name)
		}
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		s.t.Fatalf("steps %q, want %q", got, want)
	}
	return by
}

// whole checks that a step counted in unit and reported all of total.
func whole(t *testing.T, st *step, unit client.Unit, total uint64) {
	t.Helper()
	if st.unit != unit || st.total != total || st.done != int64(total) {
		t.Errorf("step %q: %d of %d reported in unit %v, want all of %d in unit %v (%q)",
			st.name, st.done, st.total, st.unit, total, unit, st.summary)
	}
}

func TestPushAndPullReportTheirSteps(t *testing.T) { onEveryBucket(t, pushAndPullReportTheirSteps) }

func pushAndPullReportTheirSteps(t *testing.T, s3 backend) {
	partSize := s3.partSize
	w := newWorld(t, s3, func(c *server.Config) { c.PartSize = partSize })
	alice, bob := w.peer("alice"), w.peer("bob")
	push := func(name string, files map[string][]byte) (client.PushResult, *steps) {
		t.Helper()
		seen := &steps{t: t}
		res, err := alice.client.Push(w.ctx, alice.objects, name, alice.ingest(files),
			client.PushOptions{MinDedup: 0.5, TempDir: t.TempDir(), Progress: seen})
		if err != nil {
			t.Fatalf("push %s: %v", name, err)
		}
		return res, seen
	}
	pull := func(name string) (client.PullResult, *steps) {
		t.Helper()
		seen := &steps{t: t}
		res, err := bob.client.Pull(w.ctx, bob.objects, name, client.PullOptions{Progress: seen})
		if err != nil {
			t.Fatalf("pull %s: %v", name, err)
		}
		return res, seen
	}
	// uploaded is what a pack is in the bucket: its index and its data.
	uploaded := func(res client.PushResult) uint64 { return packfile.IndexSize(res.Objects) + res.DataSize }

	// The first push finds no pack to compare with.
	first, seen := push("v1", version1())
	by := seen.names("reading the tree", "finding nearby packs", "packing", "uploading", "verifying on the server")
	whole(t, by["packing"], client.Objects, first.Objects)
	whole(t, by["uploading"], client.Bytes, uploaded(first))
	if got := by["finding nearby packs"].summary; got != "0 on the server" {
		t.Errorf("the search ended with %q", got)
	}
	if by["reading the tree"].done == 0 || by["verifying on the server"].unit != client.NoUnit {
		t.Errorf("reading %+v, verifying %+v", by["reading the tree"], by["verifying on the server"])
	}
	if got := by["packing"].summary; !strings.HasPrefix(got, "base pack of ") {
		t.Errorf("packing ended with %q", got)
	}

	// The second leans on the first: it fetches that pack's index and
	// packs what the pack lacks.
	second, seen := push("v2", version2())
	by = seen.names("reading the tree", "finding nearby packs", "comparing nearby packs", "packing", "uploading", "verifying on the server")
	whole(t, by["comparing nearby packs"], client.Bytes, packfile.IndexSize(first.Objects))
	whole(t, by["packing"], client.Objects, second.Objects)
	whole(t, by["uploading"], client.Bytes, uploaded(second))
	if got := by["packing"].summary; second.Parent == nil || !strings.HasPrefix(got, "patch pack of ") {
		t.Errorf("packing ended with %q for %+v", got, second)
	}
	if got := by["comparing nearby packs"].summary; !strings.HasPrefix(got, "a parent that holds ") {
		t.Errorf("the comparison ended with %q", got)
	}
	// The size the tree is read to have is that of its objects: a base
	// pack holds them all.
	if got, want := by["reading the tree"].summary, fmt.Sprintf("%s objects, ", human.Count(uint64(len(alice.keys(alice.ingest(version2())))))); !strings.HasPrefix(got, want) {
		t.Errorf("reading the tree ended with %q, want it to begin with %q", got, want)
	}

	// A root the server holds is done once it is known to.
	if again, seen := push("v2/again", version2()); !again.Stored {
		t.Fatalf("pushed again: %+v", again)
	} else {
		by := seen.names("reading the tree", "finding nearby packs")
		if got := by["finding nearby packs"].summary; got != "the server has this pack already" {
			t.Errorf("the search ended with %q", got)
		}
	}

	// Little in common with what is there: the pack becomes a base pack,
	// with what the nearest pack holds of it packed as well. It is large
	// enough to go up in parts, some of them side by side.
	large := version1()
	large["blob.bin"] = noise(7, int(partSize*5/2))
	third, seen := push("large", large)
	by = seen.names("reading the tree", "finding nearby packs", "comparing nearby packs", "packing", "uploading", "verifying on the server")
	if third.Parent != nil || third.DataSize <= uint64(2*partSize) {
		t.Fatalf("third push: %+v, want a base pack of more than two parts", third)
	}
	if got := by["comparing nearby packs"].summary; !strings.HasPrefix(got, "too little in common: ") {
		t.Errorf("the comparison ended with %q", got)
	}
	whole(t, by["packing"], client.Objects, third.Objects)
	whole(t, by["uploading"], client.Bytes, uploaded(third))
	parts := (third.DataSize + uint64(partSize) - 1) / uint64(partSize)
	if got, want := by["uploading"].summary, fmt.Sprintf(" in %d parts", parts); !strings.HasSuffix(got, want) {
		t.Errorf("uploading ended with %q, want it to end with %q", got, want)
	}

	// Into an empty store, the patch pack and then its parent.
	got, seen := pull("v2")
	by = seen.names("looking up the reference", "checking the local store", "fetching the pack", "checking the local store", "fetching the parent pack", "checking the local store")
	if got.Packs != 2 {
		t.Fatalf("pull: %+v", got)
	}
	whole(t, by["fetching the pack"], client.Bytes, uploaded(second))
	whole(t, by["fetching the parent pack"], client.Bytes, uploaded(first))
	if end := by["checking the local store"].summary; !strings.HasPrefix(end, "all ") {
		t.Errorf("the last check ended with %q", end)
	}

	// Pulled again, the store is found complete and nothing is fetched.
	if again, seen := pull("v2"); again.Packs != 0 {
		t.Fatalf("pulled again: %+v", again)
	} else {
		seen.names("looking up the reference", "checking the local store")
	}
}

func TestPullAsksAboutTheRootBeforeItFetches(t *testing.T) {
	onEveryBucket(t, pullAsksAboutTheRootBeforeItFetches)
}

func pullAsksAboutTheRootBeforeItFetches(t *testing.T, s3 backend) {
	w := newWorld(t, s3, nil)
	alice, bob := w.peer("alice"), w.peer("bob")
	root := alice.ingest(version1())
	alice.push("v1", root)

	// Turned down, the root costs no download: nothing reaches the store,
	// and the step the answer came in is the last.
	unwanted := errors.New("not what was wanted")
	seen := &steps{t: t}
	var asked []key.Key
	_, err := bob.client.Pull(w.ctx, bob.objects, "v1", client.PullOptions{Progress: seen, Accept: func(k key.Key) error {
		asked = append(asked, k)
		return unwanted
	}})
	if !errors.Is(err, unwanted) || len(asked) != 1 || asked[0] != root {
		t.Fatalf("pull = %v after asking about %v", err, asked)
	}
	if len(seen.list) != 1 || seen.list[0].name != "looking up the reference" || seen.list[0].ended {
		t.Fatalf("steps of a pull that was turned down: %+v", seen.list)
	}
	if has, err := bob.objects.Has(root); err != nil || has {
		t.Fatalf("the root reached the store all the same (%v, %v)", has, err)
	}

	// Accepted, the pull goes on as any other.
	res, err := bob.client.Pull(w.ctx, bob.objects, "v1", client.PullOptions{Accept: func(key.Key) error { return nil }})
	if err != nil || res.Root != root || res.Packs != 1 {
		t.Fatalf("pull = %+v, %v", res, err)
	}
	sameTree(t, root, alice, bob)
}
