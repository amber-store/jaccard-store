package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/core/tarexport"
	"github.com/amber-store/core/tarextract"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/human"
	"github.com/urfave/cli/v2"
	"golang.org/x/sys/unix"
)

// push-dir and pull-dir take a directory to the server and bring one back
// without a store of the user's. The store a push and a pull work on is
// made in a temporary directory for the one command and removed when the
// command ends, however it ends.

// tempStore is a store that lives for one command: a packstore and nothing
// else. Nothing refers to its objects but the command itself, so it has
// neither references nor a collector.
type tempStore struct {
	*localStore
}

// newTempStore makes a store in a new directory under parent, or under the
// system's temporary directory when parent is empty.
func newTempStore(parent string) (*tempStore, error) {
	dir, err := os.MkdirTemp(parent, "jaccard-store-*")
	if err != nil {
		return nil, fmt.Errorf("temporary store: %w", err)
	}
	// Nothing here outlives the command: waiting for the disk buys nothing.
	objects, err := packstore.Open(filepath.Join(dir, "packstore"), packstore.WithSync(false))
	if err != nil {
		return nil, errors.Join(fmt.Errorf("temporary store: %w", err), os.RemoveAll(dir))
	}
	return &tempStore{&localStore{dir: dir, objects: objects}}, nil
}

// remove closes the store and deletes its directory.
func (t *tempStore) remove() error {
	return errors.Join(t.objects.Close(), os.RemoveAll(t.dir))
}

// discard removes the temporary store as the last step of what show shows.
// err is how the command went until here: the step it failed in is marked
// before this one begins. What comes back is err, and with it whatever
// kept the store from being removed.
func discard(temp *tempStore, show reporter, err error) error {
	if err != nil {
		show.Fail(err)
	}
	show.Begin("cleaning up", 0, client.NoUnit)
	if rerr := temp.remove(); rerr != nil {
		rerr = fmt.Errorf("removing the temporary store %s: %w", temp.dir, rerr)
		show.Fail(rerr)
		return errors.Join(err, rerr)
	}
	show.End("")
	return err
}

func runPushDir(c *cli.Context, connect dialer) error {
	if c.NArg() != 2 {
		return fmt.Errorf("push-dir takes two arguments DIR and NAME, got %d", c.NArg())
	}
	dir, name := c.Args().Get(0), c.Args().Get(1)
	how, err := dirPushOf(c)
	if err != nil {
		return err
	}
	// Before anything is read: a name the server will not take should not
	// cost an import.
	if err := reference.ValidateName(name); err != nil {
		return fmt.Errorf("reference %q: %w", name, err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	// A single file can be imported, but not brought back by pull-dir.
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}

	show := newProgress(c.App.ErrWriter, c.Bool("no-progress"))
	// The server first: one that cannot be reached should not cost an
	// import either. The connection keeps itself alive meanwhile.
	server, err := reach(c.Context, connect, readSettings(c), show)
	if err != nil {
		return err
	}
	defer server.Close()
	root, res, err := how.push(c.Context, server, dir, name, show)
	show.Close(err)
	if err != nil {
		return err
	}
	printPushed(c.App.Writer, name, root, res)
	return nil
}

// dirPush is how a directory is pushed: what push-dir and push-subdirs
// take from their flags.
type dirPush struct {
	minDedup float64
	noIgnore bool
	tempDir  string
}

func dirPushOf(c *cli.Context) (dirPush, error) {
	minDedup, err := minDedupOf(c)
	return dirPush{minDedup: minDedup, noIgnore: c.Bool("no-ignore"), tempDir: c.String("temp-dir")}, err
}

// push takes the directory dir to the server as the reference name, through
// a store of its own that it removes again. show is told the steps, the
// cleaning up included, and of a step that fails; ending the show is the
// caller's.
func (h dirPush) push(ctx context.Context, server remote, dir, name string, show reporter) (key.Key, client.PushResult, error) {
	if err := reference.ValidateName(name); err != nil {
		return key.Key{}, client.PushResult{}, fmt.Errorf("reference %q: %w", name, err)
	}
	temp, err := newTempStore(h.tempDir)
	if err != nil {
		return key.Key{}, client.PushResult{}, err
	}
	var res client.PushResult
	opts := ingestDefaults
	opts.NoIgnore = h.noIgnore
	root, err := importDir(ctx, temp.objects, dir, opts, show)
	if err == nil {
		// The pack is built beside the store and goes with it.
		res, err = server.Push(ctx, temp.localStore, name, root,
			client.PushOptions{MinDedup: h.minDedup, TempDir: temp.dir, Progress: show})
	}
	return root, res, discard(temp, show, err)
}

// ingestDefaults are the options a directory is imported with: core's own,
// and those of its CLI. The chunking decides every key, so what two people
// push of the same directory is the same pack only while it stays as it is.
var ingestDefaults = ingest.Opts{}

// importDir builds the tree of dir and writes its objects to objects, in
// two steps: scanning says how much there is to read, importing reads it.
// It returns the tree's root.
func importDir(ctx context.Context, objects *packstore.Store, dir string, opts ingest.Opts, show reporter) (key.Key, error) {
	show.Begin("scanning the directory", 0, client.NoUnit)
	files, size, err := ingest.ScanWith(dir, opts)
	if err != nil {
		return key.Key{}, fmt.Errorf("scanning %s: %w", dir, err)
	}
	show.End(fmt.Sprintf("%s files, %s", human.Count(uint64(files)), human.Bytes(uint64(size))))

	show.Begin("importing", uint64(size), client.Bytes)
	opts.Progress = importing{show}
	built, root, err := ingest.Objects(dir, opts)
	if err != nil {
		return key.Key{}, fmt.Errorf("importing %s: %w", dir, err)
	}
	stats, err := objects.WriteParallel(func(yield func(packstore.Object, error) bool) {
		for o, err := range built {
			// The build knows nothing of ctx: an interrupt reaches it
			// here, between two objects.
			if err == nil {
				err = ctx.Err()
			}
			if !yield(packstore.Object{Key: o.Key, Data: o.Bytes}, err) || err != nil {
				return
			}
		}
	}, packstore.WriteOpts{})
	if err != nil {
		return key.Key{}, fmt.Errorf("importing %s: %w", dir, err)
	}
	show.End(fmt.Sprintf("%s objects, %s", human.Count(uint64(stats.Stored)), human.Bytes(uint64(stats.BytesStored))))
	return *root, nil
}

// importing passes the bytes a build has read on as the progress of the
// running step.
type importing struct{ show reporter }

func (importing) FileDone()        {}
func (i importing) AddBytes(n int) { i.show.Advance(int64(n)) }

func runPullDir(c *cli.Context, connect dialer) error {
	if c.NArg() != 2 {
		return fmt.Errorf("pull-dir takes two arguments NAME and DIR, got %d", c.NArg())
	}
	name, dest := c.Args().Get(0), c.Args().Get(1)
	// Before anything is fetched: a place that is taken should not cost a
	// download.
	if err := vacant(dest); err != nil {
		return err
	}

	show := newProgress(c.App.ErrWriter, c.Bool("no-progress"))
	server, err := reach(c.Context, connect, readSettings(c), show)
	if err != nil {
		return err
	}
	defer server.Close()
	temp, err := newTempStore(c.String("temp-dir"))
	if err != nil {
		show.Close(err)
		return err
	}
	var files, size uint64
	res, err := server.Pull(c.Context, temp.localStore, name, client.PullOptions{
		Progress: show,
		// Asked before a byte is fetched: only a directory can be
		// extracted.
		Accept: func(root key.Key) error {
			if !isTree(root) {
				return fmt.Errorf("%q is not a directory tree (its root is a %v): pull-dir cannot extract it", name, root.Type())
			}
			return nil
		},
	})
	if err == nil {
		files, size, err = extract(c.Context, temp.objects, res.Root, dest, show)
	}
	err = discard(temp, show, err)
	show.Close(err)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "%s %s: %d packs fetched, %d files extracted (%s)\n",
		dest, res.Root, res.Packs, files, human.Bytes(size))
	return nil
}

// isTree reports whether root is something a directory can be made of: a
// directory object, or a commit, which stands for its tree.
func isTree(root key.Key) bool {
	t := root.Type()
	return t == key.DirLeaf || t == key.DirNode || t == key.Commit
}

// vacant reports whether a directory can be extracted to dest: nothing is
// there, or an empty directory is.
func vacant(dest string) error {
	info, err := os.Lstat(dest)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return err
	case !info.IsDir():
		return fmt.Errorf("%s is there already, and is not a directory", dest)
	}
	entries, err := os.ReadDir(dest)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("%s is there already, and is not empty", dest)
	}
	return nil
}

// extract writes the tree of root to dest, in two steps: reading the tree
// says how much there is to write, extracting writes it. It returns the
// number of regular files written and their bytes.
//
// The tree is written beside dest and moved there when it is whole: a
// directory of the name asked for is never half of one, and an extraction
// that fails leaves nothing behind.
func extract(ctx context.Context, objects *packstore.Store, root key.Key, dest string, show reporter) (files, size uint64, err error) {
	// Neither the export nor the extraction knows of ctx: an interrupt
	// reaches them here, where they read an object.
	get := func(k key.Key) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return objects.Get(k)
	}

	show.Begin("reading the tree", 0, client.Objects)
	files, size, err = measure(root, func(k key.Key) ([]byte, error) {
		show.Advance(1)
		return get(k)
	})
	if err != nil {
		return 0, 0, fmt.Errorf("reading the tree of %s: %w", root, err)
	}
	show.End(fmt.Sprintf("%s files, %s", human.Count(files), human.Bytes(size)))

	show.Begin("extracting", size, client.Bytes)
	dest = filepath.Clean(dest)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return 0, 0, err
	}
	// Mkdir and not MkdirTemp: the directory becomes dest, and is to have
	// the mode a new directory has.
	partial := filepath.Join(filepath.Dir(dest), fmt.Sprintf(".%s.partial-%d", filepath.Base(dest), os.Getpid()))
	if err := os.Mkdir(partial, 0o755); err != nil {
		return 0, 0, err
	}
	// The export is streamed straight into the extraction. A failed export
	// closes the pipe with its cause, which the extraction then returns; a
	// failed extraction closes it for the export, which would wait for a
	// reader otherwise.
	pr, pw := io.Pipe()
	exported := make(chan struct{})
	go func() {
		defer close(exported)
		pw.CloseWithError(tarexport.Write(pw, root, func(k key.Key) ([]byte, error) {
			data, err := get(k)
			// A blob is file content and nothing else is: its bytes are
			// what the step counts.
			if err == nil && k.Type() == key.Blob {
				show.Advance(int64(len(data)))
			}
			return data, err
		}))
	}()
	err = tarextract.Extract(pr, partial)
	pr.CloseWithError(err)
	<-exported
	if err == nil {
		// The system's rename and not os.Rename, which refuses every
		// directory in its way: an empty one gives way to this, in one
		// step, and anything else that turned up at dest since it was
		// looked at does not.
		if rerr := unix.Rename(partial, dest); rerr != nil {
			err = &os.LinkError{Op: "rename", Old: partial, New: dest, Err: rerr}
		}
	}
	if err != nil {
		return 0, 0, errors.Join(fmt.Errorf("extracting to %s: %w", dest, err), removeTree(partial))
	}
	show.End(human.Count(files) + " files")
	return files, size, nil
}

// measure walks the directories of the tree at dir and counts the regular
// files in it and their bytes: what an extraction writes.
func measure(dir key.Key, get func(key.Key) ([]byte, error)) (files, size uint64, err error) {
	entries, err := fstree.CollectEntries(dir, get)
	if err != nil {
		return 0, 0, err
	}
	for _, e := range entries {
		switch e.Mode & unix.S_IFMT {
		case unix.S_IFDIR:
			sub, err := key.Parse(e.ContentKey)
			if err != nil {
				return 0, 0, fmt.Errorf("directory %q: %w", e.Name, err)
			}
			f, s, err := measure(sub, get)
			if err != nil {
				return 0, 0, err
			}
			files, size = files+f, size+s
		case unix.S_IFREG:
			content, err := key.Parse(e.ContentKey)
			if err != nil {
				return 0, 0, fmt.Errorf("file %q: %w", e.Name, err)
			}
			files++
			// The length in a content key is the content's.
			size += content.Length()
		}
	}
	return files, size, nil
}

// removeTree removes dir and everything in it, also where an extraction
// has left directories that may not be written to.
func removeTree(dir string) error {
	if os.RemoveAll(dir) == nil {
		return nil
	}
	filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			os.Chmod(path, 0o700)
		}
		return nil
	})
	return os.RemoveAll(dir)
}
