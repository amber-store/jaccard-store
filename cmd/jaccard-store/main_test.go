package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/client"
)

var variables = []string{"JACCARD_STORE", "AMBER_STORE", "JACCARD_SERVER", "JACCARD_KEY", "JACCARD_MIN_DEDUP"}

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
}

func (f *fakeRemote) Push(_ context.Context, _ *localStore, name string, root key.Key, opts client.PushOptions) (client.PushResult, error) {
	f.pushed, f.root, f.opts = append(f.pushed, name), root, opts
	return client.PushResult{Root: root, Objects: 3, DataSize: 2048}, nil
}

func (f *fakeRemote) Pull(_ context.Context, _ *localStore, name string) (client.PullResult, error) {
	return client.PullResult{Root: f.pullRoot, Packs: 1, Objects: 3, Bytes: 100}, nil
}

func (f *fakeRemote) List(_ context.Context, prefix string) ([]client.Ref, error) {
	f.listed = append(f.listed, prefix)
	return f.refs, nil
}

func (f *fakeRemote) Delete(_ context.Context, name string) error {
	f.deleted = append(f.deleted, name)
	return nil
}

func (f *fakeRemote) Close() error { return nil }

// run runs the command with args under env alone, against f.
func run(t *testing.T, f *fakeRemote, env map[string]string, args ...string) (string, error) {
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
	var out bytes.Buffer
	app := newApp(&out, io.Discard, func(_ context.Context, s settings) (remote, error) {
		f.settings = s
		return f, nil
	})
	err := app.Run(append([]string{"jaccard-store"}, args...))
	return out.String(), err
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
	if _, err := run(t, f, nil, "rm", "a/one"); err != nil || len(f.deleted) != 1 || f.deleted[0] != "a/one" {
		t.Fatalf("rm: %v, deleted %v", err, f.deleted)
	}
}

func TestArgumentCounts(t *testing.T) {
	f := &fakeRemote{}
	for _, args := range [][]string{
		{"push"}, {"push", "a", "b"}, {"pull"}, {"pull", "a", "b"}, {"rm"}, {"rm", "a", "b"}, {"ls", "a", "b"},
	} {
		if _, err := run(t, f, nil, append([]string{"--store", t.TempDir()}, args...)...); err == nil {
			t.Errorf("%v: accepted", args)
		}
	}
}
