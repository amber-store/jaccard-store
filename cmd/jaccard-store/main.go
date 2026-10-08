// Command jaccard-store pushes references from a local Amber-Store Core
// store to a jaccard-store server and pulls them back.
//
//	jaccard-store --store DIR --server ENDPOINT_ID push [--as NAME] [--min-dedup F] REF
//	jaccard-store --store DIR --server ENDPOINT_ID pull [--as REF] NAME
//	jaccard-store --server ENDPOINT_ID ls [PREFIX]
//	jaccard-store --server ENDPOINT_ID rm NAME
//
// The store is the directory core's own CLI works on (a packstore and a
// refstore); ingesting into it and restoring from it are that CLI's job.
// The server is named by its endpoint ID alone. Every option is a flag and
// an environment variable; the flag wins.
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
	Pull(ctx context.Context, store *localStore, name string) (client.PullResult, error)
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
					&cli.Float64Flag{Name: "min-dedup", EnvVars: []string{"JACCARD_MIN_DEDUP"}, Value: 0.5,
						Usage: "upload a patch pack when the nearest base pack holds at least this `FRACTION` of the bytes"},
				},
				Action: func(c *cli.Context) error { return runPush(c, connect) },
			},
			{
				Name:      "pull",
				Usage:     "fetch the server's reference NAME into the local store and set the local reference REF",
				ArgsUsage: "NAME",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "as", Usage: "`REF`, the name of the local reference (default: NAME)"},
				},
				Action: func(c *cli.Context) error { return runPull(c, connect) },
			},
			{
				Name:      "ls",
				Usage:     "list the server's references, optionally those whose names begin with PREFIX",
				ArgsUsage: "[PREFIX]",
				Action:    func(c *cli.Context) error { return runList(c, connect) },
			},
			{
				Name:      "rm",
				Usage:     "remove the reference NAME from the server; packs nothing refers to any more are collected there",
				ArgsUsage: "NAME",
				Action:    func(c *cli.Context) error { return runRemove(c, connect) },
			},
		},
	}
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
	minDedup := c.Float64("min-dedup")
	if !(minDedup >= 0) { // which a NaN is not either
		return fmt.Errorf("--min-dedup: %v is not a fraction: want 0 or more", minDedup)
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
	server, err := connect(c.Context, s)
	if err != nil {
		return err
	}
	defer server.Close()
	res, err := server.Push(c.Context, store, name, root, client.PushOptions{MinDedup: minDedup})
	if err != nil {
		return err
	}
	switch {
	case res.Stored:
		fmt.Fprintf(c.App.Writer, "%s %s: the server has the pack already\n", name, root)
	case res.Parent != nil:
		fmt.Fprintf(c.App.Writer, "%s %s: patch pack of %s, %d objects, %s uploaded\n",
			name, root, res.Parent, res.Objects, human(res.DataSize))
	default:
		fmt.Fprintf(c.App.Writer, "%s %s: base pack, %d objects, %s uploaded\n",
			name, root, res.Objects, human(res.DataSize))
	}
	return nil
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
	server, err := connect(c.Context, s)
	if err != nil {
		return err
	}
	defer server.Close()

	// The objects and the reference that makes them reachable are written
	// in one span, so a collection running beside this cannot fall between.
	span, err := store.Begin()
	if err != nil {
		return err
	}
	res, err := server.Pull(c.Context, store, name)
	if err == nil {
		err = span.SetRef(ref, res.Root)
	}
	if err = errors.Join(err, span.End()); err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "%s %s: %d packs fetched, %d objects written (%s)\n",
		ref, res.Root, res.Packs, res.Objects, human(res.Bytes))
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

func runRemove(c *cli.Context, connect dialer) error {
	name, err := one(c, "NAME")
	if err != nil {
		return err
	}
	server, err := connect(c.Context, readSettings(c))
	if err != nil {
		return err
	}
	defer server.Close()
	return server.Delete(c.Context, name)
}

// human formats a byte count with a binary unit.
func human(n uint64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	f, u := float64(n), 0
	for f >= 1024 && u < len(units)-1 {
		f /= 1024
		u++
	}
	if u == 0 {
		return fmt.Sprintf("%d B", n)
	}
	return fmt.Sprintf("%.2f %s", f, units[u])
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

func (c *connection) Pull(ctx context.Context, store *localStore, name string) (client.PullResult, error) {
	return c.client.Pull(ctx, store.objects, name)
}

func (c *connection) List(ctx context.Context, prefix string) ([]client.Ref, error) {
	return c.client.List(ctx, prefix)
}

func (c *connection) Delete(ctx context.Context, name string) error {
	return c.client.Delete(ctx, name)
}

func (c *connection) Close() error { return c.conn.Close() }
