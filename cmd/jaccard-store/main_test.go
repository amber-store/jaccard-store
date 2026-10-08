package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/client"
)

var variables = []string{"JACCARD_STORE", "AMBER_STORE", "JACCARD_SERVER", "JACCARD_KEY", "JACCARD_MIN_DEDUP", "JACCARD_NO_PROGRESS"}

// fakeRemote stands where the server is and records what it was asked.
type fakeRemote struct {
	settings settings
	pushed   []string
	opts     client.PushOptions
	root     key.Key
	pullRoot key.Key
	refs     []client.Ref
	listed   []string
	deleted  []string
	pushErr  error
	// deleteErr is what removing a name fails with, for the names in it.
	deleteErr map[string]error
}

func (f *fakeRemote) Push(_ context.Context, _ *localStore, name string, root key.Key, opts client.PushOptions) (client.PushResult, error) {
	f.pushed, f.root, f.opts = append(f.pushed, name), root, opts
	if opts.Progress != nil {
		opts.Progress.Begin("uploading", 2048, client.Bytes)
		opts.Progress.Advance(2048)
		if f.pushErr != nil {
			return client.PushResult{}, f.pushErr
		}
		opts.Progress.End("2.00 KiB")
	}
	return client.PushResult{Root: root, Objects: 3, DataSize: 2048}, nil
}

func (f *fakeRemote) Pull(_ context.Context, _ *localStore, name string, opts client.PullOptions) (client.PullResult, error) {
	if opts.Progress != nil {
		opts.Progress.Begin("fetching the pack", 100, client.Bytes)
		opts.Progress.Advance(100)
		opts.Progress.End("3 of 3 objects were new, 100 B")
	}
	return client.PullResult{Root: f.pullRoot, Packs: 1, Objects: 3, Bytes: 100}, nil
}

func (f *fakeRemote) List(_ context.Context, prefix string) ([]client.Ref, error) {
	f.listed = append(f.listed, prefix)
	var refs []client.Ref
	for _, r := range f.refs {
		if strings.HasPrefix(r.Name, prefix) {
			refs = append(refs, r)
		}
	}
	return refs, nil
}

func (f *fakeRemote) Delete(_ context.Context, name string) error {
	if err := f.deleteErr[name]; err != nil {
		return err
	}
	f.deleted = append(f.deleted, name)
	return nil
}

func (f *fakeRemote) Close() error { return nil }

// run runs the command with args under env alone, against f, and returns
// what it wrote to standard output.
func run(t *testing.T, f *fakeRemote, env map[string]string, args ...string) (string, error) {
	t.Helper()
	out, _, err := runBoth(t, f, env, args...)
	return out, err
}

// runBoth is run that returns what went to standard error as well.
func runBoth(t *testing.T, f *fakeRemote, env map[string]string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	for _, name := range variables {
		// Setenv registers the restore; a variable set to nothing would
		// still count as given, so it is then removed.
		t.Setenv(name, "")
		os.Unsetenv(name)
	}
	for name, value := range env {
		t.Setenv(name, value)
	}
	var out, errOut bytes.Buffer
	app := newApp(&out, &errOut, func(_ context.Context, s settings) (remote, error) {
		f.settings = s
		return f, nil
	})
	err = app.Run(append([]string{"jaccard-store"}, args...))
	return out.String(), errOut.String(), err
}

// storeWithRef makes a store directory holding one small tree under the
// reference name and returns the directory and the tree's root.
func storeWithRef(t *testing.T, name string) (string, key.Key) {
	t.Helper()
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("hello, pack"), 0o644); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	store, err := openStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	sp, err := store.Begin()
	if err != nil {
		t.Fatal(err)
	}
	root, _, err := ingest.Dir(store.objects, src, ingest.Opts{NoIgnore: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.SetRef(name, root); err != nil {
		t.Fatal(err)
	}
	if err := sp.End(); err != nil {
		t.Fatal(err)
	}
	return dir, root
}

func TestVariablesAreRead(t *testing.T) {
	f := &fakeRemote{}
	_, err := run(t, f, map[string]string{
		"JACCARD_STORE":  "/env/store",
		"JACCARD_SERVER": "env-server",
		"JACCARD_KEY":    "/env/key",
	}, "ls")
	if err != nil {
		t.Fatal(err)
	}
	if want := (settings{store: "/env/store", server: "env-server", key: "/env/key"}); f.settings != want {
		t.Fatalf("got %+v, want %+v", f.settings, want)
	}
}

func TestFlagsWinOverVariables(t *testing.T) {
	f := &fakeRemote{}
	_, err := run(t, f, map[string]string{
		"JACCARD_STORE":  "/env/store",
		"JACCARD_SERVER": "env-server",
		"JACCARD_KEY":    "/env/key",
	}, "--store", "/flag/store", "--server", "flag-server", "--key", "/flag/key", "ls")
	if err != nil {
		t.Fatal(err)
	}
	if want := (settings{store: "/flag/store", server: "flag-server", key: "/flag/key"}); f.settings != want {
		t.Fatalf("got %+v, want %+v", f.settings, want)
	}
}

func TestStoreFallsBackToCoresVariable(t *testing.T) {
	f := &fakeRemote{}
	if _, err := run(t, f, map[string]string{"AMBER_STORE": "/amber"}, "ls"); err != nil {
		t.Fatal(err)
	}
	if f.settings.store != "/amber" {
		t.Fatalf("store %q, want the one AMBER_STORE names", f.settings.store)
	}
	if _, err := run(t, f, map[string]string{"AMBER_STORE": "/amber", "JACCARD_STORE": "/jaccard"}, "ls"); err != nil {
		t.Fatal(err)
	}
	if f.settings.store != "/jaccard" {
		t.Fatalf("store %q, want JACCARD_STORE to win over AMBER_STORE", f.settings.store)
	}
}

func TestKeyHasADefault(t *testing.T) {
	f := &fakeRemote{}
	if _, err := run(t, f, nil, "ls"); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(f.settings.key, filepath.Join("jaccard-store", "client.key")) {
		t.Fatalf("default key file %q", f.settings.key)
	}
}

func TestPush(t *testing.T) {
	dir, root := storeWithRef(t, "local")
	f := &fakeRemote{}
	out, err := run(t, f, map[string]string{"JACCARD_MIN_DEDUP": "0.25"}, "--store", dir, "push", "local")
	if err != nil {
		t.Fatal(err)
	}
	if len(f.pushed) != 1 || f.pushed[0] != "local" || f.root != root || f.opts.MinDedup != 0.25 {
		t.Fatalf("pushed %v at %s with %+v", f.pushed, f.root, f.opts)
	}
	if !strings.Contains(out, "base pack") || !strings.Contains(out, root.String()) {
		t.Fatalf("output %q", out)
	}

	f = &fakeRemote{}
	if _, err := run(t, f, map[string]string{"JACCARD_MIN_DEDUP": "0.25"}, "--store", dir, "push", "--as", "remote/name", "--min-dedup", "0.75", "local"); err != nil {
		t.Fatal(err)
	}
	if f.pushed[0] != "remote/name" || f.opts.MinDedup != 0.75 {
		t.Fatalf("pushed %v with %+v: --as and --min-dedup must win", f.pushed, f.opts)
	}

	f = &fakeRemote{}
	if _, err := run(t, f, nil, "--store", dir, "push", "local"); err != nil {
		t.Fatal(err)
	}
	if f.opts.MinDedup != 0.5 {
		t.Fatalf("default --min-dedup is %v, want 0.5", f.opts.MinDedup)
	}
}

func TestPushOfAnUnknownReference(t *testing.T) {
	dir, _ := storeWithRef(t, "local")
	f := &fakeRemote{}
	if _, err := run(t, f, nil, "--store", dir, "push", "other"); err == nil || len(f.pushed) != 0 {
		t.Fatalf("push of a reference the store lacks: err %v, pushed %v", err, f.pushed)
	}
}

func TestPullSetsTheLocalReference(t *testing.T) {
	// The store holds the objects already, as after a real pull; the fake
	// only names the root.
	dir, root := storeWithRef(t, "source")
	f := &fakeRemote{pullRoot: root}
	out, err := run(t, f, nil, "--store", dir, "pull", "--as", "copy", "remote/name")
	if err != nil {
		t.Fatal(err)
	}
	store, err := openStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if got, err := store.Ref("copy"); err != nil || got != root {
		t.Fatalf("local reference copy = %s, %v; want %s", got, err, root)
	}
	if !strings.Contains(out, "copy "+root.String()) {
		t.Fatalf("output %q", out)
	}
	// Pulled again over the reference it set: the old root is released,
	// the new one prepared, and the reference stays.
	if _, err := run(t, f, nil, "--store", dir, "pull", "--as", "copy", "remote/name"); err != nil {
		t.Fatalf("pulling over an existing reference: %v", err)
	}
}

func TestListAndRemove(t *testing.T) {
	root := key.Key{1, 2, 3}
	f := &fakeRemote{refs: []client.Ref{{Name: "a/one", Root: root}, {Name: "a/two", Root: root}}}
	out, err := run(t, f, nil, "ls", "a/")
	if err != nil {
		t.Fatal(err)
	}
	if want := "a/one " + root.String() + "\na/two " + root.String() + "\n"; out != want || f.listed[0] != "a/" {
		t.Fatalf("ls printed %q for prefix %q", out, f.listed)
	}
	out, err = run(t, f, nil, "rm", "a/one")
	if err != nil || len(f.deleted) != 1 || f.deleted[0] != "a/one" {
		t.Fatalf("rm: %v, deleted %v", err, f.deleted)
	}
	// What is gone is printed as ls printed it.
	if want := "a/one " + root.String() + "\n"; out != want {
		t.Fatalf("rm printed %q, want %q", out, want)
	}
}

// served is a server with references of these names.
func served(names ...string) *fakeRemote {
	f := &fakeRemote{}
	for i, name := range names {
		f.refs = append(f.refs, client.Ref{Name: name, Root: key.Key{byte(i + 1)}})
	}
	return f
}

func TestRmTakesPatterns(t *testing.T) {
	all := []string{"a/one", "a/two", "a/deep/three", "b/one", "c", "star*name", "v1.0", "v1.1", "v2.0"}
	for _, tc := range []struct {
		args []string
		want string // what is removed, in the order it goes
	}{
		{[]string{"c"}, "c"},
		// A star takes anything, slashes too: a name is a string to the
		// server, and what is under a/ is all of it.
		{[]string{"a/*"}, "a/deep/three a/one a/two"},
		{[]string{"*/one"}, "a/one b/one"},
		{[]string{"*three"}, "a/deep/three"},
		{[]string{"v1.?"}, "v1.0 v1.1"},
		{[]string{"v?.0"}, "v1.0 v2.0"},
		{[]string{"[ab]/one"}, "a/one b/one"},
		{[]string{"[^a]/one"}, "b/one"},
		{[]string{"v[1-2].0"}, "v1.0 v2.0"},
		// A name that holds a star is asked for with the star escaped;
		// unescaped it is a pattern, which the name matches as well.
		{[]string{`star\*name`}, "star*name"},
		{[]string{"star*"}, "star*name"},
		// Several arguments: all they match, each once, by name.
		{[]string{"v2.0", "a/one", "c"}, "a/one c v2.0"},
		{[]string{"a/*", "*/one", "a/one"}, "a/deep/three a/one a/two b/one"},
		{[]string{"*"}, "a/deep/three a/one a/two b/one c star*name v1.0 v1.1 v2.0"},
	} {
		f := served(all...)
		out, err := run(t, f, nil, append([]string{"rm"}, tc.args...)...)
		if err != nil {
			t.Errorf("rm %q: %v", tc.args, err)
			continue
		}
		if got := strings.Join(f.deleted, " "); got != tc.want {
			t.Errorf("rm %q removed %q, want %q", tc.args, got, tc.want)
		}
		var printed []string
		for _, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
			name, root, _ := strings.Cut(line, " ")
			if len(root) != 64 {
				t.Errorf("rm %q printed %q", tc.args, line)
			}
			printed = append(printed, name)
		}
		if got := strings.Join(printed, " "); got != tc.want {
			t.Errorf("rm %q printed %q, want %q", tc.args, got, tc.want)
		}
	}
}

func TestLsTakesPatterns(t *testing.T) {
	all := []string{"a/one", "a/two", "a/deep/three", "b/one", "c", "star*name", "v1.0", "v1.1", "v2.0"}
	everything := "a/deep/three a/one a/two b/one c star*name v1.0 v1.1 v2.0"
	for _, tc := range []struct {
		args []string
		want string // what is listed, in the order it comes
	}{
		{nil, everything},
		{[]string{"*"}, everything},
		// Without a special character, what a name begins with: as ls
		// was before it took patterns.
		{[]string{"a/"}, "a/deep/three a/one a/two"},
		{[]string{"a/o"}, "a/one"},
		{[]string{"v1"}, "v1.0 v1.1"},
		{[]string{"c"}, "c"},
		{[]string{""}, everything},
		// With one, a pattern, and the whole name has to match it: as
		// rm reads it.
		{[]string{"a/*"}, "a/deep/three a/one a/two"},
		{[]string{"a/o*"}, "a/one"},
		{[]string{"a/o?"}, ""},
		{[]string{"*/one"}, "a/one b/one"},
		{[]string{"v1.?"}, "v1.0 v1.1"},
		{[]string{"v?.0"}, "v1.0 v2.0"},
		{[]string{"[ab]/one"}, "a/one b/one"},
		{[]string{`star\*name`}, "star*name"},
		// Several arguments: all they match, each once, by name.
		{[]string{"v2.0", "a/", "c"}, "a/deep/three a/one a/two c v2.0"},
		{[]string{"a/*", "*/one", "a/one", "a/"}, "a/deep/three a/one a/two b/one"},
		// Nothing of the kind is an answer, not a failure, and does not
		// stand in the way of what the other arguments match.
		{[]string{"x/*"}, ""},
		{[]string{"zzz"}, ""},
		{[]string{"zzz", "c", "x/*"}, "c"},
	} {
		f := served(all...)
		out, err := run(t, f, nil, append([]string{"ls"}, tc.args...)...)
		if err != nil {
			t.Errorf("ls %q: %v", tc.args, err)
			continue
		}
		var listed []string
		for _, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
			if line == "" {
				continue
			}
			name, root, _ := strings.Cut(line, " ")
			if len(root) != 64 {
				t.Errorf("ls %q printed %q", tc.args, line)
			}
			listed = append(listed, name)
		}
		if got := strings.Join(listed, " "); got != tc.want {
			t.Errorf("ls %q listed %q, want %q", tc.args, got, tc.want)
		}
		if len(f.deleted) != 0 {
			t.Errorf("ls %q removed %v", tc.args, f.deleted)
		}
	}
}

func TestLsRefusesWhatIsNoPatternBeforeItDials(t *testing.T) {
	f := served("a/one")
	if _, err := run(t, f, nil, "ls", "a/one", "a/[one"); err == nil || !strings.Contains(err.Error(), "is not a pattern") {
		t.Fatalf("ls: %v", err)
	}
	if f.settings != (settings{}) || len(f.listed) != 0 {
		t.Error("ls got as far as the server")
	}
}

// What ls shows for a pattern is what rm removes for it: the one is how to
// look before the other.
func TestLsOfAPatternListsWhatRmOfItRemoves(t *testing.T) {
	all := []string{"a/one", "a/two", "a/deep/three", "b/one", "c", "v1.0", "v1.1", "v2.0"}
	for _, pattern := range []string{"*", "a/*", "*/one", "v?.0", "[bc]*", "*e", "a/t*", "v1.[0-9]"} {
		f := served(all...)
		listed, err := run(t, f, nil, "ls", pattern)
		if err != nil {
			t.Fatal(err)
		}
		removed, err := run(t, f, nil, "rm", pattern)
		if err != nil {
			t.Fatalf("rm %q: %v", pattern, err)
		}
		if listed != removed || listed == "" {
			t.Errorf("%q: ls listed\n%s\nrm removed\n%s", pattern, listed, removed)
		}
	}
}

// The server is asked for the names that begin as the pattern does, and no
// more than that.
func TestRmListsByWhatAPatternBeginsWith(t *testing.T) {
	f := served("a/one", "a/two", "b/one")
	if _, err := run(t, f, nil, "rm", "a/t*", "b/one", `a/o\ne`); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(f.listed, " "); got != "a/t b/one a/o" {
		t.Fatalf("listed the prefixes %q", got)
	}
	f = served("a/one", "a/two", "b/one")
	if _, err := run(t, f, nil, "ls", "a/t*", "b/", "*/one"); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(f.listed, "|"); got != "a/t|b/|" {
		t.Fatalf("ls listed the prefixes %q", got)
	}
}

func TestRmRemovesNothingWhenAnArgumentMatchesNothing(t *testing.T) {
	for _, tc := range []struct {
		args     []string
		notFound bool   // the error is that of a name that is not there
		says     string // and names the argument
	}{
		{[]string{"a/*", "x/*"}, false, `no reference matches "x/*"`},
		{[]string{"x/*", "a/*"}, false, `no reference matches "x/*"`},
		{[]string{"a/one", "a/three"}, true, `"a/three"`},
		{[]string{"a"}, true, `"a"`},         // what a name begins with is not the name
		{[]string{"A/ONE"}, true, `"A/ONE"`}, // nor is another case of it
		{[]string{"a/?"}, false, `"a/?"`},
	} {
		f := served("a/one", "a/two")
		out, err := run(t, f, nil, append([]string{"rm"}, tc.args...)...)
		if err == nil || errors.Is(err, client.ErrNotFound) != tc.notFound || !strings.Contains(err.Error(), tc.says) {
			t.Errorf("rm %q: %v", tc.args, err)
		}
		if len(f.deleted) != 0 || out != "" {
			t.Errorf("rm %q removed %v and printed %q", tc.args, f.deleted, out)
		}
	}
}

func TestRmRefusesWhatIsNoPatternBeforeItDials(t *testing.T) {
	for _, args := range [][]string{{"a/[one"}, {"a/one", `b\`}, {"[]"}} {
		f := served("a/one")
		if _, err := run(t, f, nil, append([]string{"rm"}, args...)...); err == nil || !strings.Contains(err.Error(), "is not a pattern") {
			t.Errorf("rm %q: %v", args, err)
		}
		if f.settings != (settings{}) || len(f.listed) != 0 || len(f.deleted) != 0 {
			t.Errorf("rm %q got as far as the server", args)
		}
	}
}

func TestRmGoesOnPastAReferenceThatIsGoneAndStopsAtOneItCannotRemove(t *testing.T) {
	// Removed by somebody else between the listing and the removal: that
	// is no failure, and it is not printed as this command's doing.
	f := served("a/one", "a/two", "a/three")
	f.deleteErr = map[string]error{"a/three": client.ErrNotFound}
	out, err := run(t, f, nil, "rm", "a/*")
	if err != nil || strings.Join(f.deleted, " ") != "a/one a/two" || strings.Contains(out, "a/three") {
		t.Fatalf("rm = %v, removed %v, printed %q", err, f.deleted, out)
	}

	// Anything else ends it there, with the reference named and what was
	// removed before it printed.
	f = served("a/one", "a/two", "a/zero")
	broken := errors.New("the database is locked")
	f.deleteErr = map[string]error{"a/two": broken}
	out, err = run(t, f, nil, "rm", "a/*")
	if !errors.Is(err, broken) || !strings.Contains(err.Error(), `"a/two"`) {
		t.Fatalf("rm = %v", err)
	}
	if strings.Join(f.deleted, " ") != "a/one" || !strings.HasPrefix(out, "a/one ") || strings.Count(out, "\n") != 1 {
		t.Fatalf("removed %v, printed %q", f.deleted, out)
	}
}

func TestMatches(t *testing.T) {
	for _, tc := range []struct {
		pattern, name string
		want          bool
	}{
		{"a", "a", true},
		{"a", "ab", false},
		{"", "", true},
		{"*", "", true},
		{"*", "a/b/c", true},
		{"a/*", "a/", true},
		{"a/*", "a", false},
		{"a*", "a/b", true},
		{"*/*", "ab", false},
		{"?", "/", true},
		{"a?b", "a/b", true},
		{"a?b", "ab", false},
		{"[a/]", "/", true},
		{"a/b", "a/b", true},
		{`a\/b`, "a/b", true},
		{`\*`, "*", true},
		{`\*`, "a", false},
		{"é*", "éa", true},
		{"?", "é", true},
		{"a/[", "a/[", false}, // no pattern: it matches nothing
	} {
		if got := matches(tc.pattern, tc.name); got != tc.want {
			t.Errorf("matches(%q, %q) = %v, want %v", tc.pattern, tc.name, got, tc.want)
		}
	}
}

func TestLiteralPrefix(t *testing.T) {
	for pattern, want := range map[string]string{
		"":           "",
		"a/one":      "a/one",
		"a/*":        "a/",
		"*":          "",
		"a/o?e":      "a/o",
		"rel[12]/x":  "rel",
		`star\*name`: "star",
	} {
		if got := literalPrefix(pattern); got != want {
			t.Errorf("literalPrefix(%q) = %q, want %q", pattern, got, want)
		}
	}
}

func TestArgumentCounts(t *testing.T) {
	f := &fakeRemote{}
	for _, args := range [][]string{
		{"push"}, {"push", "a", "b"}, {"pull"}, {"pull", "a", "b"}, {"rm"},
	} {
		if _, err := run(t, f, nil, append([]string{"--store", t.TempDir()}, args...)...); err == nil {
			t.Errorf("%v: accepted", args)
		}
	}
}

func TestPullRefusesALocalNameBeforeItFetches(t *testing.T) {
	dir, root := storeWithRef(t, "source")
	f := &fakeRemote{pullRoot: root}
	f.settings = settings{}
	_, err := run(t, f, nil, "--store", dir, "pull", "--as", "", "remote/name")
	if err != nil {
		t.Fatalf("an empty --as means the name itself: %v", err)
	}
	fetched := false
	app := newApp(io.Discard, io.Discard, func(_ context.Context, s settings) (remote, error) {
		fetched = true
		return f, nil
	})
	bad := strings.Repeat("x", 4096)
	if err := app.Run([]string{"jaccard-store", "--store", dir, "pull", "--as", bad, "remote/name"}); err == nil {
		t.Fatal("a local name of 4096 bytes was accepted")
	}
	if fetched {
		t.Fatal("the server was dialed for a pull that could not end well")
	}
}

func TestMinDedupMustBeAFraction(t *testing.T) {
	dir, _ := storeWithRef(t, "local")
	for _, v := range []string{"-0.1", "NaN"} {
		f := &fakeRemote{}
		if _, err := run(t, f, nil, "--store", dir, "push", "--min-dedup", v, "local"); err == nil || len(f.pushed) != 0 {
			t.Errorf("--min-dedup %s: err %v, pushed %v", v, err, f.pushed)
		}
	}
	f := &fakeRemote{}
	if _, err := run(t, f, nil, "--store", dir, "push", "--min-dedup", "0", "local"); err != nil || f.opts.MinDedup != 0 {
		t.Errorf("--min-dedup 0: %v, %+v", err, f.opts)
	}
}

func TestVersionIsPrinted(t *testing.T) {
	out, err := run(t, &fakeRemote{}, nil, "--version")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "jaccard-store") || !strings.Contains(out, version) {
		t.Fatalf("--version printed %q, want the name and %q", out, version)
	}
}

func TestPushShowsWhatItDoesOnStandardError(t *testing.T) {
	dir, _ := storeWithRef(t, "local")
	out, shown, err := runBoth(t, &fakeRemote{}, nil, "--store", dir, "push", "local")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"connecting to the server (", "uploading: 2.00 KiB (", "done in "} {
		if !strings.Contains(shown, want) {
			t.Errorf("standard error lacks %q:\n%s", want, shown)
		}
	}
	// The result is the command's output, and all of it.
	if strings.Count(out, "\n") != 1 || !strings.Contains(out, "base pack, 3 objects") {
		t.Errorf("standard output: %q", out)
	}
}

func TestPullShowsWhatItDoesOnStandardError(t *testing.T) {
	dir, root := storeWithRef(t, "local")
	out, shown, err := runBoth(t, &fakeRemote{pullRoot: root}, nil, "--store", dir, "pull", "--as", "copy", "remote")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"connecting to the server (", "fetching the pack: 3 of 3 objects were new, 100 B (", "done in "} {
		if !strings.Contains(shown, want) {
			t.Errorf("standard error lacks %q:\n%s", want, shown)
		}
	}
	if strings.Count(out, "\n") != 1 || !strings.Contains(out, "1 packs fetched") {
		t.Errorf("standard output: %q", out)
	}
}

func TestNoProgressShowsNothing(t *testing.T) {
	dir, root := storeWithRef(t, "local")
	for _, tc := range []struct {
		env  map[string]string
		args []string
	}{
		{nil, []string{"--store", dir, "push", "--no-progress", "local"}},
		{map[string]string{"JACCARD_NO_PROGRESS": "true"}, []string{"--store", dir, "push", "local"}},
		{nil, []string{"--store", dir, "pull", "--no-progress", "--as", "copy", "remote"}},
		{map[string]string{"JACCARD_NO_PROGRESS": "1"}, []string{"--store", dir, "pull", "--as", "copy", "remote"}},
	} {
		out, shown, err := runBoth(t, &fakeRemote{pullRoot: root}, tc.env, tc.args...)
		if err != nil {
			t.Fatal(err)
		}
		if shown != "" {
			t.Errorf("%v %v: standard error: %q", tc.env, tc.args, shown)
		}
		if out == "" {
			t.Errorf("%v %v: no result on standard output", tc.env, tc.args)
		}
	}
}

func TestAFailedPushMarksTheStepThatFailed(t *testing.T) {
	dir, _ := storeWithRef(t, "local")
	f := &fakeRemote{pushErr: errors.New("the bucket answered 403")}
	out, shown, err := runBoth(t, f, nil, "--store", dir, "push", "local")
	if err == nil || out != "" {
		t.Fatalf("push = %q, %v", out, err)
	}
	if !strings.Contains(shown, "uploading: failed after ") || strings.Contains(shown, "done in") {
		t.Errorf("standard error:\n%s", shown)
	}
}

func TestAServerThatCannotBeReachedIsTheStepThatFailed(t *testing.T) {
	dir, _ := storeWithRef(t, "local")
	var shown bytes.Buffer
	app := newApp(io.Discard, &shown, func(context.Context, settings) (remote, error) {
		return nil, errors.New("no route")
	})
	if err := app.Run([]string{"jaccard-store", "--store", dir, "push", "local"}); err == nil {
		t.Fatal("push succeeded without a server")
	}
	if got := shown.String(); !strings.Contains(got, "connecting to the server: failed after ") {
		t.Errorf("standard error:\n%s", got)
	}
}

// A pack the server took without verifying it is called one wherever a push
// says what it did: nothing but a pull will tell whether it is sound.
func TestAPushSaysWhenTheServerDidNotVerify(t *testing.T) {
	root, parent := key.Key{1, 2, 3}, key.Key{4, 5, 6}
	for name, res := range map[string]client.PushResult{
		"base pack":  {Root: root, Objects: 3, DataSize: 2048},
		"patch pack": {Root: root, Parent: &parent, Objects: 3, DataSize: 2048},
	} {
		var out bytes.Buffer
		printPushed(&out, "ref", root, res)
		if got := out.String(); !strings.Contains(got, name) || strings.Contains(got, "verified") {
			t.Errorf("a verified %s is printed as %q", name, got)
		}
		if got := outcome(res); !strings.HasPrefix(got, name) || strings.Contains(got, "verified") {
			t.Errorf("a verified %s comes out as %q", name, got)
		}
		res.Unverified = true
		out.Reset()
		printPushed(&out, "ref", root, res)
		if got := out.String(); !strings.Contains(got, name) || !strings.HasSuffix(got, "uploaded, not verified by the server\n") || strings.Count(got, "\n") != 1 {
			t.Errorf("a %s that was not verified is printed as %q", name, got)
		}
		if got := outcome(res); !strings.HasPrefix(got, name) || !strings.HasSuffix(got, ", not verified") {
			t.Errorf("a %s that was not verified comes out as %q", name, got)
		}
	}
	// What was not uploaded was not taken on trust either.
	var out bytes.Buffer
	printPushed(&out, "ref", root, client.PushResult{Root: root, Stored: true})
	if got := out.String(); strings.Contains(got, "verified") {
		t.Errorf("a push of a stored root is printed as %q", got)
	}
}
