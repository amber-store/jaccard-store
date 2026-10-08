package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/amber-store/core/key"
)

// parentOf makes a directory that holds a directory of each of these names,
// with a file in it that says the name, and beside them what is not to be
// pushed: a file and a symbolic link to one of the directories.
func parentOf(t *testing.T, names ...string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "parent")
	for _, name := range names {
		if err := os.MkdirAll(filepath.Join(dir, name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name, "name.txt"), []byte("this is "+name+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "a-file"), []byte("not a directory\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(names[0], filepath.Join(dir, "a-link")); err != nil {
		t.Fatal(err)
	}
	return dir
}

// pushedTo returns the names of the references a server holds, in order.
func pushedTo(m *memory) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var names []string
	for name := range m.refs {
		names = append(names, name)
	}
	slices.Sort(names)
	return strings.Join(names, " ")
}

func TestPushSubdirs(t *testing.T) {
	dir, temp, m := parentOf(t, "alpha", "beta", "gamma"), t.TempDir(), newMemory()
	// A directory in a directory goes with the one it is in.
	deep := filepath.Join(dir, "gamma", "inner", "most")
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(deep, "deep.txt"), []byte("deep\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	out, shown, err := against(t, m, "push-subdirs", "--prefix", "proj/", "--temp-dir", temp, dir)
	if err != nil {
		t.Fatalf("push-subdirs: %v\n%s", err, shown)
	}
	// The directories of the first level and nothing else: not the file,
	// not the link, not the directory further down.
	if got := pushedTo(m); got != "proj/alpha proj/beta proj/gamma" {
		t.Fatalf("pushed %q", got)
	}
	// What was pushed is said as push-dir says it, by name.
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("standard output:\n%s", out)
	}
	for i, name := range []string{"proj/alpha", "proj/beta", "proj/gamma"} {
		if want := name + " " + m.refs[name].String() + ": base pack, "; !strings.HasPrefix(lines[i], want) {
			t.Errorf("line %d is %q, want it to begin with %q", i, lines[i], want)
		}
	}
	empty(t, temp)
	for _, want := range []string{
		"connecting to the server (",
		"alpha: base pack, 4.00 KiB (",
		"beta: base pack, 4.00 KiB (",
		"gamma: base pack, 4.00 KiB (",
		"3 directories: 3 pushed (",
	} {
		if !strings.Contains(shown, want) {
			t.Errorf("standard error lacks %q:\n%s", want, shown)
		}
	}
	if strings.Contains(shown, "not pushed") || strings.Contains(shown, "done in") {
		t.Errorf("standard error:\n%s", shown)
	}

	// Each is a directory as push-dir would have pushed it.
	dest := filepath.Join(t.TempDir(), "gamma")
	if _, _, err := against(t, m, "pull-dir", "proj/gamma", dest); err != nil {
		t.Fatal(err)
	}
	sameListing(t, listing(t, filepath.Join(dir, "gamma")), listing(t, dest))
}

func TestPushSubdirsNeedsItsPrefix(t *testing.T) {
	dir, m := parentOf(t, "a", "b"), newMemory()
	_, _, err := against(t, m, "push-subdirs", dir)
	if err == nil || !strings.Contains(err.Error(), "--prefix") || m.connected != 0 {
		t.Fatalf("without a prefix: %v, the server dialed %d times", err, m.connected)
	}
	// The variable will do,
	if _, _, err := under(t, context.Background(), map[string]string{"JACCARD_PREFIX": "env/"}, m, "push-subdirs", dir); err != nil {
		t.Fatal(err)
	}
	// the flag wins over it,
	if _, _, err := under(t, context.Background(), map[string]string{"JACCARD_PREFIX": "env/"}, m, "push-subdirs", "--prefix", "flag-", dir); err != nil {
		t.Fatal(err)
	}
	// and a prefix of nothing is one that was asked for.
	if _, _, err := against(t, m, "push-subdirs", "--prefix", "", dir); err != nil {
		t.Fatal(err)
	}
	if got := pushedTo(m); got != "a b env/a env/b flag-a flag-b" {
		t.Fatalf("pushed %q", got)
	}
}

func TestPushSubdirsRunsSoManyAtOnce(t *testing.T) {
	names := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	for _, tc := range []struct {
		env  map[string]string
		args []string
		want int32
	}{
		{nil, nil, 5},
		{nil, []string{"--jobs", "3"}, 3},
		{nil, []string{"-j", "2"}, 2},
		{nil, []string{"--jobs", "1"}, 1},
		{map[string]string{"JACCARD_JOBS": "4"}, nil, 4},
		{map[string]string{"JACCARD_JOBS": "4"}, []string{"--jobs", "2"}, 2},
		// More jobs than directories: as many as there are.
		{nil, []string{"--jobs", "50"}, int32(len(names))},
	} {
		dir, m := parentOf(t, names...), newMemory()
		// Every push waits until that many are under way: fewer at once
		// and the pushes fail, more and the peak says so.
		m.atOnce = int(tc.want)
		args := append(append([]string{"push-subdirs", "--prefix", "p/"}, tc.args...), dir)
		if _, shown, err := under(t, context.Background(), tc.env, m, args...); err != nil {
			t.Errorf("%v %v: %v\n%s", tc.env, tc.args, err, shown)
		}
		if got := m.peak.Load(); got != tc.want {
			t.Errorf("%v %v: %d pushes at once, want %d", tc.env, tc.args, got, tc.want)
		}
		if got := len(strings.Fields(pushedTo(m))); got != len(names) {
			t.Errorf("%v %v: %d directories pushed of %d", tc.env, tc.args, got, len(names))
		}
	}
}

func TestPushSubdirsGoesOnPastTheOnesThatFail(t *testing.T) {
	// One the server refuses, one whose name makes no reference, and
	// around them those that are fine.
	dir, temp, m := parentOf(t, "a", "b", "bad@name", "c", "d"), t.TempDir(), newMemory()
	refused := errors.New("the bucket answered 403")
	m.refused = map[string]error{"p/b": refused}
	out, shown, err := against(t, m, "push-subdirs", "--prefix", "p/", "--jobs", "2", "--temp-dir", temp, dir)
	if err == nil || err.Error() != "2 of 5 directories were not pushed" {
		t.Fatalf("push-subdirs = %v\n%s", err, shown)
	}
	if got := pushedTo(m); got != "p/a p/c p/d" {
		t.Fatalf("pushed %q", got)
	}
	if lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n"); len(lines) != 3 ||
		!strings.HasPrefix(lines[0], "p/a ") || !strings.HasPrefix(lines[1], "p/c ") || !strings.HasPrefix(lines[2], "p/d ") {
		t.Errorf("standard output:\n%s", out)
	}
	empty(t, temp)
	// Each failure as it happens, the count, and at the end every one
	// that was not pushed with all of its error.
	for _, want := range []string{
		"b: failed after ",
		"bad@name: failed after ",
		"5 directories: 3 pushed, 2 failed (",
		"not pushed:\n  b: the bucket answered 403\n  bad@name: reference \"p/bad@name\": ",
	} {
		if !strings.Contains(shown, want) {
			t.Errorf("standard error lacks %q:\n%s", want, shown)
		}
	}

	// What was not pushed is said without the progress as well: it is
	// how the command went, not how it was going.
	m = newMemory()
	m.refused = map[string]error{"p/b": refused}
	_, shown, err = against(t, m, "push-subdirs", "--prefix", "p/", "--no-progress", dir)
	if err == nil || shown != "not pushed:\n  b: the bucket answered 403\n  bad@name: reference \"p/bad@name\": reference name must not contain '@'\n" {
		t.Fatalf("push-subdirs --no-progress = %v, standard error:\n%s", err, shown)
	}
}

func TestPushSubdirsWhenEveryOneFails(t *testing.T) {
	dir, temp, m := parentOf(t, "a", "b"), t.TempDir(), newMemory()
	m.pushErr = errors.New("the bucket answered 403")
	out, _, err := against(t, m, "push-subdirs", "--prefix", "p/", "--temp-dir", temp, dir)
	if err == nil || err.Error() != "2 of 2 directories were not pushed" || out != "" {
		t.Fatalf("push-subdirs = %q, %v", out, err)
	}
	empty(t, temp)
}

func TestPushSubdirsTakesTheDirectoriesAnImportWould(t *testing.T) {
	dir := parentOf(t, "kept", "skipped", ".hidden")
	if err := os.WriteFile(filepath.Join(dir, ".amberignore"), []byte("skipped/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A directory whose name begins with a dot is a directory; what the
	// .amberignore of the parent names is left out, as an import of the
	// parent would leave it out.
	m := newMemory()
	if _, _, err := against(t, m, "push-subdirs", "--prefix", "p/", dir); err != nil {
		t.Fatal(err)
	}
	if got := pushedTo(m); got != "p/.hidden p/kept" {
		t.Errorf("pushed %q", got)
	}
	m = newMemory()
	if _, _, err := against(t, m, "push-subdirs", "--prefix", "p/", "--no-ignore", dir); err != nil {
		t.Fatal(err)
	}
	if got := pushedTo(m); got != "p/.hidden p/kept p/skipped" {
		t.Errorf("with --no-ignore pushed %q", got)
	}
}

func TestPushSubdirsRefusesBeforeItDials(t *testing.T) {
	dir, m := parentOf(t, "a"), newMemory()
	bare := t.TempDir()
	if err := os.WriteFile(filepath.Join(bare, "only-a-file"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"push-subdirs", "--prefix", "p/"},                                // no directory
		{"push-subdirs", "--prefix", "p/", dir, dir},                      // two of them
		{"push-subdirs", "--prefix", "p/", filepath.Join(dir, "missing")}, // not there
		{"push-subdirs", "--prefix", "p/", filepath.Join(dir, "a-file")},  // a file
		{"push-subdirs", "--prefix", "p/", bare},                          // no directory in it
		{"push-subdirs", "--prefix", "p@/", dir},                          // a prefix no name begins with
		{"push-subdirs", "--prefix", "p/", "--jobs", "0", dir},            // nobody to push
		{"push-subdirs", "--prefix", "p/", "--jobs", "-2", dir},           //
		{"push-subdirs", "--prefix", "p/", "--min-dedup", "-1", dir},      // no fraction
	} {
		if _, _, err := against(t, m, args...); err == nil {
			t.Errorf("%v succeeded", args[1:])
		}
	}
	if m.connected != 0 || pushedTo(m) != "" {
		t.Errorf("the server was dialed %d times and holds %q", m.connected, pushedTo(m))
	}
}

func TestAnInterruptedPushSubdirsBeginsNoMore(t *testing.T) {
	dir, temp, m := parentOf(t, "a", "b", "c", "d"), t.TempDir(), newMemory()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The interrupt comes while the first is under way, and one at a time
	// is: the others are still waiting.
	m.onPush = cancel
	out, shown, err := under(t, ctx, nil, m, "push-subdirs", "--prefix", "p/", "--jobs", "1", "--temp-dir", temp, dir)
	if err == nil || err.Error() != "interrupted: 0 pushed, 1 failed, 3 not begun" || out != "" {
		t.Fatalf("push-subdirs = %q, %v\n%s", out, err, shown)
	}
	if pushedTo(m) != "" || m.peak.Load() != 1 {
		t.Errorf("pushed %q, %d at once", pushedTo(m), m.peak.Load())
	}
	empty(t, temp)
	for _, want := range []string{"a: interrupted after ", "4 directories: 0 pushed, 1 failed, 3 not begun ("} {
		if !strings.Contains(shown, want) {
			t.Errorf("standard error lacks %q:\n%s", want, shown)
		}
	}
}

// holding is a server that has references of these names already, each
// pointing at a root of its own that no directory of a test comes to.
func holding(names ...string) *memory {
	m := newMemory()
	for i, name := range names {
		m.refs[name] = key.Key{0xEE, byte(i + 1)}
	}
	return m
}

func TestPushSubdirsSkipsWhatTheServerHas(t *testing.T) {
	dir, temp := parentOf(t, "alpha", "beta", "gamma"), t.TempDir()
	// The reference of one directory, and what is none: another reference
	// under the prefix, and references named as directories are but not
	// under it.
	m := holding("proj/beta", "proj/other", "beta", "gamma")
	had := m.refs["proj/beta"]
	out, shown, err := against(t, m, "push-subdirs", "--prefix", "proj/", "--skip-existing", "--temp-dir", temp, dir)
	if err != nil {
		t.Fatalf("push-subdirs: %v\n%s", err, shown)
	}
	if got := pushedTo(m); got != "beta gamma proj/alpha proj/beta proj/gamma proj/other" {
		t.Fatalf("the server holds %q", got)
	}
	// The directory whose reference was there was not read, let alone
	// pushed, and the reference is as it was.
	if m.refs["proj/beta"] != had || len(m.stores) != 2 {
		t.Fatalf("proj/beta points at %s, was %s; %d directories were pushed", m.refs["proj/beta"], had, len(m.stores))
	}
	// The server was asked once, for the names that begin as these do.
	if !slices.Equal(m.listed, []string{"proj/"}) {
		t.Fatalf("the server was asked to list %q", m.listed)
	}
	// Every directory has its line, in their order.
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("standard output:\n%s", out)
	}
	for i, want := range []string{
		"proj/alpha " + m.refs["proj/alpha"].String() + ": base pack, ",
		"proj/beta " + had.String() + ": the server has the reference already, not pushed",
		"proj/gamma " + m.refs["proj/gamma"].String() + ": base pack, ",
	} {
		if !strings.HasPrefix(lines[i], want) {
			t.Errorf("line %d is %q, want it to begin with %q", i, lines[i], want)
		}
	}
	empty(t, temp)
	for _, want := range []string{
		"listing the server's references: 1 of 3 directories there already (",
		"alpha: base pack, 4.00 KiB (",
		"gamma: base pack, 4.00 KiB (",
		"2 directories: 2 pushed (",
	} {
		if !strings.Contains(shown, want) {
			t.Errorf("standard error lacks %q:\n%s", want, shown)
		}
	}
	if strings.Contains(shown, "beta") {
		t.Errorf("standard error speaks of the directory that was left out:\n%s", shown)
	}

	// Without the option nothing is asked, and the directory is pushed
	// over what was there.
	if _, _, err := against(t, m, "push-subdirs", "--prefix", "proj/", dir); err != nil {
		t.Fatal(err)
	}
	if m.refs["proj/beta"] == had || len(m.listed) != 1 || len(m.stores) != 5 {
		t.Fatalf("without --skip-existing: proj/beta points at %s, listed %q, %d pushes in all", m.refs["proj/beta"], m.listed, len(m.stores))
	}
}

func TestPushSubdirsWithNothingLeftToPush(t *testing.T) {
	dir, m := parentOf(t, "a", "b"), holding("p/a", "p/b")
	out, shown, err := against(t, m, "push-subdirs", "--prefix", "p/", "--skip-existing", dir)
	if err != nil {
		t.Fatalf("push-subdirs: %v\n%s", err, shown)
	}
	want := "p/a " + m.refs["p/a"].String() + ": the server has the reference already, not pushed\n" +
		"p/b " + m.refs["p/b"].String() + ": the server has the reference already, not pushed\n"
	if out != want || len(m.stores) != 0 {
		t.Fatalf("%d directories were pushed; standard output:\n%s", len(m.stores), out)
	}
	if !strings.Contains(shown, "listing the server's references: 2 of 2 directories there already (") ||
		!strings.Contains(shown, "done in ") || strings.Contains(shown, "pushed (") {
		t.Errorf("standard error:\n%s", shown)
	}

	// Shown or not, it comes to the same,
	out, shown, err = against(t, m, "push-subdirs", "--prefix", "p/", "--skip-existing", "--no-progress", dir)
	if err != nil || out != want || shown != "" {
		t.Fatalf("--no-progress: %v, standard output:\n%s\nstandard error:\n%s", err, out, shown)
	}
	// and the variable will do for the option.
	out, _, err = under(t, context.Background(), map[string]string{"JACCARD_SKIP_EXISTING": "true"}, m, "push-subdirs", "--prefix", "p/", dir)
	if err != nil || out != want || len(m.stores) != 0 {
		t.Fatalf("by the variable: %v, %d directories were pushed; standard output:\n%s", err, len(m.stores), out)
	}
	// A prefix of nothing asks for every reference.
	m = holding("a")
	if _, _, err = against(t, m, "push-subdirs", "--prefix", "", "--skip-existing", dir); err != nil {
		t.Fatal(err)
	}
	if got := pushedTo(m); got != "a b" || len(m.stores) != 1 || !slices.Equal(m.listed, []string{""}) {
		t.Fatalf("with a prefix of nothing: the server holds %q after %d pushes, listed %q", got, len(m.stores), m.listed)
	}
}

// Which references are there cannot be guessed: when the server does not
// say, nothing is pushed.
func TestPushSubdirsThatCannotAskPushesNothing(t *testing.T) {
	dir, m := parentOf(t, "a", "b"), newMemory()
	m.listErr = errors.New("the server is away")
	out, shown, err := against(t, m, "push-subdirs", "--prefix", "p/", "--skip-existing", dir)
	if err == nil || err.Error() != "listing the references the server has: the server is away" {
		t.Fatalf("push-subdirs = %v", err)
	}
	if out != "" || len(m.stores) != 0 || pushedTo(m) != "" {
		t.Fatalf("%d directories were pushed; standard output:\n%s", len(m.stores), out)
	}
	if !strings.Contains(shown, "listing the server's references: failed after ") {
		t.Errorf("standard error:\n%s", shown)
	}
}

// What fails is counted among what there was to push.
func TestPushSubdirsThatSkipsStillFailsForWhatFails(t *testing.T) {
	dir, temp, m := parentOf(t, "a", "b", "c", "d"), t.TempDir(), holding("p/a", "p/d")
	m.refused = map[string]error{"p/b": errors.New("the bucket answered 403")}
	out, shown, err := against(t, m, "push-subdirs", "--prefix", "p/", "--skip-existing", "--temp-dir", temp, dir)
	if err == nil || err.Error() != "1 of 2 directories were not pushed" {
		t.Fatalf("push-subdirs = %v\n%s", err, shown)
	}
	if lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n"); len(lines) != 3 ||
		!strings.HasSuffix(lines[0], "not pushed") || !strings.HasPrefix(lines[1], "p/c ") || !strings.HasSuffix(lines[2], "not pushed") {
		t.Errorf("standard output:\n%s", out)
	}
	for _, want := range []string{
		"2 of 4 directories there already (",
		"2 directories: 1 pushed, 1 failed (",
		"not pushed:\n  b: the bucket answered 403\n",
	} {
		if !strings.Contains(shown, want) {
			t.Errorf("standard error lacks %q:\n%s", want, shown)
		}
	}
	empty(t, temp)
}
