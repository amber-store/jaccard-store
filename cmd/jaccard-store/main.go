// Command jaccard-store pushes references from a local Amber-Store Core
// store to a jaccard-store server and pulls them back.
//
//	jaccard-store --store DIR --server ENDPOINT_ID push [--as NAME] [--min-dedup F] [--no-progress] REF
//	jaccard-store --store DIR --server ENDPOINT_ID pull [--as REF] [--no-progress] NAME
//	jaccard-store --server ENDPOINT_ID push-dir [--min-dedup F] [--no-ignore] [--temp-dir DIR] [--no-progress] DIR NAME
//	jaccard-store --server ENDPOINT_ID pull-dir [--temp-dir DIR] [--no-progress] NAME DIR
//	jaccard-store --server ENDPOINT_ID ls [PREFIX]
//	jaccard-store --server ENDPOINT_ID rm PATTERN...
//
// The store is the directory core's own CLI works on (a packstore and a
// refstore); ingesting into it and restoring from it are that CLI's job.
// push-dir and pull-dir need no store: they take a directory to the server
// and bring one back through a store they make for the one command and
// remove again (dir.go).
// The server is named by its endpoint ID alone. Every option is a flag and
// an environment variable; the flag wins.
//
// A push and a pull show on standard error what they are doing: a line for
// every step with the time it took, and for the step that is running a bar,
// the rate and the time left (progress.go).
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/human"
	"github.com/amber-store/jaccard-store/node"
	"github.com/amber-store/jaccard-store/wire"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/urfave/cli/v2"
)

// version is the release this binary was built as. A release build sets it
// with -ldflags "-X main.version=..."; any other build is "dev".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// The first signal ends the context and the command winds down; from
	// then on signals are the system's again, so a second one ends the
	// process at once.
	go func() {
		<-ctx.Done()
		stop()
	}()
	if err := newApp(os.Stdout, os.Stderr, dial).RunContext(ctx, os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "jaccard-store:", err)
		os.Exit(1)
	}
}

// remote is the server as the commands use it.
type remote interface {
	Push(ctx context.Context, store *localStore, name string, root key.Key, opts client.PushOptions) (client.PushResult, error)
	Pull(ctx context.Context, store *localStore, name string, opts client.PullOptions) (client.PullResult, error)
	List(ctx context.Context, prefix string) ([]client.Ref, error)
	Delete(ctx context.Context, name string) error
	Close() error
}

// dialer connects to the server the settings name.
type dialer func(ctx context.Context, s settings) (remote, error)

// settings is what the global flags and the environment say.
type settings struct {
	store  string
	server string
	key    string
}

func readSettings(c *cli.Context) settings {
	return settings{store: c.String("store"), server: c.String("server"), key: c.String("key")}
}

// defaultKeyFile is where the client's key lives unless told otherwise.
func defaultKeyFile() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "jaccard-store", "client.key")
}

// newApp returns the command. connect is how it reaches a server, which
// lets a test put something else in the server's place.
func newApp(stdout, stderr io.Writer, connect dialer) *cli.App {
	// Flags that several commands have; each command gets its own.
	noProgress := func() cli.Flag {
		return &cli.BoolFlag{Name: "no-progress", EnvVars: []string{"JACCARD_NO_PROGRESS"},
			Usage: "do not show on standard error what is being done"}
	}
	minDedup := func() cli.Flag {
		return &cli.Float64Flag{Name: "min-dedup", EnvVars: []string{"JACCARD_MIN_DEDUP"}, Value: 0.5,
			Usage: "upload a patch pack only against a base pack that holds at least this `FRACTION` of the reference's bytes"}
	}
	tempDir := func() cli.Flag {
		return &cli.StringFlag{Name: "temp-dir", EnvVars: []string{"JACCARD_TEMP_DIR"},
			Usage: "`DIR`ectory the temporary store is made in and removed from (default: the system's temporary directory)"}
	}
	return &cli.App{
		Name:            "jaccard-store",
		Version:         version,
		Usage:           "push and pull references of an Amber-Store Core store",
		Writer:          stdout,
		ErrWriter:       stderr,
		HideHelpCommand: true,
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "store", EnvVars: []string{"JACCARD_STORE", "AMBER_STORE"},
				Usage: "local store `DIR`ectory, as core's CLI uses it"},
			&cli.StringFlag{Name: "server", EnvVars: []string{"JACCARD_SERVER"},
				Usage: "`ENDPOINT_ID` of the server"},
			&cli.StringFlag{Name: "key", EnvVars: []string{"JACCARD_KEY"}, Value: defaultKeyFile(),
				Usage: "key `FILE` of this client, created on first use; the server records its endpoint ID with what it uploads"},
		},
		Commands: []*cli.Command{
			{
				Name:      "push",
				Usage:     "make NAME on the server point at what the local reference REF points at, uploading a pack if the server lacks it",
				ArgsUsage: "REF",
				// Options come before REF: the flag package stops at the
				// first argument that is not one.
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "as", Usage: "`NAME` of the reference on the server (default: REF)"},
					minDedup(),
					noProgress(),
				},
				Action: func(c *cli.Context) error { return runPush(c, connect) },
			},
			{
				Name:      "pull",
				Usage:     "fetch the server's reference NAME into the local store and set the local reference REF",
				ArgsUsage: "NAME",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "as", Usage: "`REF`, the name of the local reference (default: NAME)"},
					noProgress(),
				},
				Action: func(c *cli.Context) error { return runPull(c, connect) },
			},
			{
				Name: "push-dir",
				Usage: "make NAME on the server point at the content of the directory DIR, without a local store: " +
					"DIR is imported into a temporary one that is removed afterwards",
				ArgsUsage: "DIR NAME",
				Flags: []cli.Flag{
					minDedup(),
					&cli.BoolFlag{Name: "no-ignore", EnvVars: []string{"JACCARD_NO_IGNORE"},
						Usage: "do not honor .amberignore files"},
					tempDir(),
					noProgress(),
				},
				Action: func(c *cli.Context) error { return runPushDir(c, connect) },
			},
			{
				Name: "pull-dir",
				Usage: "extract the server's reference NAME to the directory DIR, which is not there yet or empty, without a local store: " +
					"the packs are fetched into a temporary one that is removed afterwards",
				ArgsUsage: "NAME DIR",
				Flags: []cli.Flag{
					tempDir(),
					noProgress(),
				},
				Action: func(c *cli.Context) error { return runPullDir(c, connect) },
			},
			{
				Name:      "ls",
				Usage:     "list the server's references, optionally those whose names begin with PREFIX",
				ArgsUsage: "[PREFIX]",
				Action:    func(c *cli.Context) error { return runList(c, connect) },
			},
			{
				Name: "rm",
				Usage: "remove from the server the references that a PATTERN matches, and print them; " +
					"packs nothing refers to any more are collected there. A PATTERN is a name, or one with " +
					"* (any characters, slashes too), ? (any one), [a-c] (one of these) and \\ (the next character as it is); " +
					"quote it, or the shell reads it first",
				ArgsUsage: "PATTERN...",
				Action:    func(c *cli.Context) error { return runRemove(c, connect) },
			},
		},
	}
}

// reach connects to the server, as the first step of what show shows. When
// that fails the show is over.
func reach(ctx context.Context, connect dialer, s settings, show *progress) (remote, error) {
	show.Begin("connecting to the server", 0, client.NoUnit)
	server, err := connect(ctx, s)
	if err != nil {
		show.Close(err)
		return nil, err
	}
	show.End("")
	return server, nil
}

// one returns the single argument a command takes.
func one(c *cli.Context, what string) (string, error) {
	if c.NArg() != 1 {
		return "", fmt.Errorf("%s takes one argument %s, got %d", c.Command.Name, what, c.NArg())
	}
	return c.Args().First(), nil
}

func runPush(c *cli.Context, connect dialer) error {
	ref, err := one(c, "REF")
	if err != nil {
		return err
	}
	name := c.String("as")
	if name == "" {
		name = ref
	}
	minDedup, err := minDedupOf(c)
	if err != nil {
		return err
	}
	s := readSettings(c)
	store, err := openStore(s.store)
	if err != nil {
		return err
	}
	defer store.Close()
	root, err := store.Ref(ref)
	if err != nil {
		return err
	}
	show := newProgress(c.App.ErrWriter, c.Bool("no-progress"))
	server, err := reach(c.Context, connect, s, show)
	if err != nil {
		return err
	}
	defer server.Close()
	res, err := server.Push(c.Context, store, name, root, client.PushOptions{MinDedup: minDedup, Progress: show})
	show.Close(err)
	if err != nil {
		return err
	}
	printPushed(c.App.Writer, name, root, res)
	return nil
}

// minDedupOf returns what --min-dedup says, if that is a fraction.
func minDedupOf(c *cli.Context) (float64, error) {
	minDedup := c.Float64("min-dedup")
	if !(minDedup >= 0) { // which a NaN is not either
		return 0, fmt.Errorf("--min-dedup: %v is not a fraction: want 0 or more", minDedup)
	}
	return minDedup, nil
}

// printPushed writes the result of a push: what name points at now and
// what was uploaded for it.
func printPushed(w io.Writer, name string, root key.Key, res client.PushResult) {
	switch {
	case res.Stored:
		fmt.Fprintf(w, "%s %s: the server has the pack already\n", name, root)
	case res.Parent != nil:
		fmt.Fprintf(w, "%s %s: patch pack of %s, %d objects, %s uploaded\n",
			name, root, res.Parent, res.Objects, human.Bytes(res.DataSize))
	default:
		fmt.Fprintf(w, "%s %s: base pack, %d objects, %s uploaded\n",
			name, root, res.Objects, human.Bytes(res.DataSize))
	}
}

func runPull(c *cli.Context, connect dialer) error {
	name, err := one(c, "NAME")
	if err != nil {
		return err
	}
	ref := c.String("as")
	if ref == "" {
		ref = name
	}
	// Before anything is fetched: a name the store will not take should
	// not cost a download.
	if err := reference.ValidateName(ref); err != nil {
		return fmt.Errorf("local reference %q: %w", ref, err)
	}
	s := readSettings(c)
	store, err := openStore(s.store)
	if err != nil {
		return err
	}
	defer store.Close()
	show := newProgress(c.App.ErrWriter, c.Bool("no-progress"))
	server, err := reach(c.Context, connect, s, show)
	if err != nil {
		return err
	}
	defer server.Close()

	// The objects and the reference that makes them reachable are written
	// in one span, so a collection running beside this cannot fall between.
	span, err := store.Begin()
	if err != nil {
		show.Close(err)
		return err
	}
	res, err := server.Pull(c.Context, store, name, client.PullOptions{Progress: show})
	if err == nil {
		err = span.SetRef(ref, res.Root)
	}
	err = errors.Join(err, span.End())
	show.Close(err)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "%s %s: %d packs fetched, %d objects written (%s)\n",
		ref, res.Root, res.Packs, res.Objects, human.Bytes(res.Bytes))
	return nil
}

func runList(c *cli.Context, connect dialer) error {
	if c.NArg() > 1 {
		return fmt.Errorf("ls takes at most one argument PREFIX, got %d", c.NArg())
	}
	server, err := connect(c.Context, readSettings(c))
	if err != nil {
		return err
	}
	defer server.Close()
	refs, err := server.List(c.Context, c.Args().First())
	if err != nil {
		return err
	}
	for _, r := range refs {
		if _, err := fmt.Fprintf(c.App.Writer, "%s %s\n", r.Name, r.Root); err != nil {
			return err
		}
	}
	return nil
}

// connection is a server reached over iroh.
type connection struct {
	conn   *node.Conn
	client *client.Client
}

// dial connects to the server by its endpoint ID, with this client's key.
func dial(ctx context.Context, s settings) (remote, error) {
	if s.server == "" {
		return nil, errors.New("no server: give --server or set JACCARD_SERVER")
	}
	id, err := irohkey.ParseEndpointID(s.server)
	if err != nil {
		return nil, fmt.Errorf("--server: not an endpoint ID: %w", err)
	}
	if s.key == "" {
		return nil, errors.New("no key file: give --key or set JACCARD_KEY")
	}
	sk, err := node.LoadOrCreateKey(s.key)
	if err != nil {
		return nil, err
	}
	conn, err := node.Dial(ctx, node.DialConfig{Key: sk, ALPN: wire.ALPN, Server: id})
	if err != nil {
		return nil, fmt.Errorf("reaching the server: %w", err)
	}
	return &connection{conn: conn, client: client.New(conn, nil)}, nil
}

func (c *connection) Push(ctx context.Context, store *localStore, name string, root key.Key, opts client.PushOptions) (client.PushResult, error) {
	return c.client.Push(ctx, store.objects, name, root, opts)
}

func (c *connection) Pull(ctx context.Context, store *localStore, name string, opts client.PullOptions) (client.PullResult, error) {
	return c.client.Pull(ctx, store.objects, name, opts)
}

func (c *connection) List(ctx context.Context, prefix string) ([]client.Ref, error) {
	return c.client.List(ctx, prefix)
}

func (c *connection) Delete(ctx context.Context, name string) error {
	return c.client.Delete(ctx, name)
}

func (c *connection) Close() error { return c.conn.Close() }
