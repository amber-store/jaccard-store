package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/amber-store/core/amberignore"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/human"
	"github.com/urfave/cli/v2"
)

// push-subdirs is push-dir for every directory in a directory: each of
// them becomes a reference of its own, named by a prefix and the
// directory's name. Several are pushed at once. One that fails does not
// stop the others: they are all tried, and the command says at its end
// which were not pushed, and fails if any was not.
//
// With --skip-existing the server is first asked which of the references
// it has, and the directories of those are left alone: a run that was cut
// short is taken up where it stopped, without reading again what is there.

// subdir is one directory push-subdirs has to push, and what came of it.
type subdir struct {
	name string // of the directory
	ref  string // of the reference it becomes
	root key.Key
	res  client.PushResult
	err  error
	done bool // its push was begun, and is over
	// there says that the server had the reference before anything was
	// pushed and that the directory is left out for it; root is then what
	// the server's reference points at, not what the directory comes to.
	there bool
}

func runPushSubdirs(c *cli.Context, connect dialer) error {
	dir, err := one(c, "DIR")
	if err != nil {
		return err
	}
	// The prefix has to be asked for. Without one the references would be
	// named as the directories are and nothing else, among whatever else
	// the server holds, and that should be meant when it happens: it can
	// be, with a prefix of nothing.
	if !c.IsSet("prefix") {
		return errors.New("push-subdirs needs --prefix (or JACCARD_PREFIX): what the names of the references begin with")
	}
	prefix := c.String("prefix")
	if prefix != "" {
		if err := reference.ValidateName(prefix); err != nil {
			return fmt.Errorf("--prefix %q: %w", prefix, err)
		}
	}
	jobs := c.Int("jobs")
	if jobs < 1 {
		return fmt.Errorf("--jobs: %d is not a number of directories to push at once: want 1 or more", jobs)
	}
	how, err := dirPushOf(c)
	if err != nil {
		return err
	}
	names, err := subdirsOf(dir, how.noIgnore)
	if err != nil {
		return err
	}
	// More likely the wrong directory than a wish to push nothing.
	if len(names) == 0 {
		return fmt.Errorf("%s holds no directory to push", dir)
	}
	dirs := make([]subdir, len(names))
	for i, name := range names {
		dirs[i] = subdir{name: name, ref: prefix + name}
	}

	quiet := c.Bool("no-progress")
	show := newProgress(c.App.ErrWriter, quiet)
	// One connection for all of them: every request is a stream of its
	// own on it.
	server, err := reach(c.Context, connect, readSettings(c), show)
	if err != nil {
		return err
	}
	defer server.Close()
	todo := len(dirs)
	if c.Bool("skip-existing") {
		// Asked once, before anything is read: the names all begin with the
		// prefix, so one listing has every reference that could be one of
		// them.
		show.Begin("listing the server's references", 0, client.NoUnit)
		listed, err := server.List(c.Context, prefix)
		if err != nil {
			err = fmt.Errorf("listing the references the server has: %w", err)
			show.Close(err)
			return err
		}
		has := make(map[string]key.Key, len(listed))
		for _, r := range listed {
			has[r.Name] = r.Root
		}
		for i := range dirs {
			if root, ok := has[dirs[i].ref]; ok {
				dirs[i].root, dirs[i].there = root, true
				todo--
			}
		}
		show.End(fmt.Sprintf("%s of %s there already", human.Count(uint64(len(dirs)-todo)), directories(len(dirs))))
	}
	if todo == 0 {
		// Nothing is left to push, and that is not a failure.
		show.Close(nil)
		for _, d := range dirs {
			printThere(c.App.Writer, d.ref, d.root)
		}
		return nil
	}
	show.Stop()

	b := newBoard(c.App.ErrWriter, quiet, todo)
	var running sync.WaitGroup
	free := make(chan struct{}, jobs)
	for i := range dirs {
		d := &dirs[i]
		if d.there {
			continue
		}
		// A directory waits here for one of the jobs to be free. One that
		// is still waiting when the command is interrupted is not begun.
		select {
		case free <- struct{}{}:
		case <-c.Context.Done():
		}
		if c.Context.Err() != nil {
			break
		}
		running.Go(func() {
			defer func() { <-free }()
			r := b.add(d.name)
			d.root, d.res, d.err = how.push(c.Context, server, filepath.Join(dir, d.name), d.ref, r)
			d.done = true
			b.finish(r, outcome(d.res), d.err)
		})
	}
	running.Wait()

	var failed, waiting []subdir
	for _, d := range dirs {
		switch {
		case d.there:
		case !d.done:
			waiting = append(waiting, d)
		case d.err != nil:
			failed = append(failed, d)
		}
	}
	b.Close(len(waiting))

	// What was pushed, to standard output, as push-dir says it of one, and
	// among it, in the order of the directories, what was left out because
	// it was there.
	for _, d := range dirs {
		switch {
		case d.there:
			printThere(c.App.Writer, d.ref, d.root)
		case d.done && d.err == nil:
			printPushed(c.App.Writer, d.ref, d.root, d.res)
		}
	}
	// What was not, to standard error, each with all of its error.
	if len(failed) > 0 {
		fmt.Fprintln(c.App.ErrWriter, "not pushed:")
		for _, d := range failed {
			fmt.Fprintf(c.App.ErrWriter, "  %s: %v\n", d.name, d.err)
		}
	}
	// The counts are of the directories there were to push: those left out
	// because the server had them are none of these.
	switch {
	case c.Context.Err() != nil:
		return fmt.Errorf("interrupted: %s pushed, %s failed, %s not begun",
			human.Count(uint64(todo-len(failed)-len(waiting))), human.Count(uint64(len(failed))), human.Count(uint64(len(waiting))))
	case len(failed) > 0:
		return fmt.Errorf("%s of %s were not pushed", human.Count(uint64(len(failed))), directories(todo))
	}
	return nil
}

// printThere writes the line of a directory that --skip-existing left out:
// the reference and the root the server has for it, which is not
// necessarily what the directory would come to now.
func printThere(w io.Writer, name string, root key.Key) {
	fmt.Fprintf(w, "%s %s: the server has the reference already, not pushed\n", name, root)
}

// subdirsOf returns the names of the directories in dir, in their order:
// those that an import of dir itself would take in. So a directory that
// the .amberignore of dir names is left out, unless noIgnore; a file is,
// and a symbolic link, whatever it points at. Directories in directories
// are not looked at: they go with the one they are in.
func subdirsOf(dir string, noIgnore bool) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	// A nil matcher ignores nothing.
	var ignore *amberignore.Matcher
	if !noIgnore {
		if ignore, err = amberignore.Root(dir); err != nil {
			return nil, err
		}
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() && !ignore.Ignored(e.Name(), true) {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

// outcome says in a few words what a push did, for a line that has the
// directory's name before it and not much room after.
func outcome(res client.PushResult) string {
	switch {
	case res.Stored:
		return "the server has the pack already"
	case res.Parent != nil:
		return "patch pack, " + human.Bytes(res.DataSize) + unverified(res)
	}
	return "base pack, " + human.Bytes(res.DataSize) + unverified(res)
}

// unverified is what outcome adds for a pack the server took as it came.
func unverified(res client.PushResult) string {
	if res.Unverified {
		return ", not verified"
	}
	return ""
}
