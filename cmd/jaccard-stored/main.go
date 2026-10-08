// Command jaccard-stored is the jaccard-store server. It answers pushes and
// pulls over iroh, keeps the packs in an S3 bucket and what it knows of them
// in a directory of its own, and serves an admin page.
//
//	jaccard-stored --data DIR --s3-bucket NAME
//
// Every option is a flag and an environment variable; the flag wins. The S3
// credentials come from the AWS SDK's default chain (AWS_ACCESS_KEY_ID and
// the rest). The endpoint ID the server logs at start is all a client needs
// to reach it.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/amber-store/jaccard-store/admin"
	"github.com/amber-store/jaccard-store/bucket"
	"github.com/amber-store/jaccard-store/db"
	"github.com/amber-store/jaccard-store/node"
	"github.com/amber-store/jaccard-store/server"
	"github.com/amber-store/jaccard-store/wire"
	"github.com/urfave/cli/v2"
)

const (
	keyFile     = "server.key"
	dbFile      = "store.sqlite"
	scratchDir  = "scratch"
	sweepEvery  = 30 * time.Second
	minPartSize = 5 << 20 // what S3 takes for a part that is not the last
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := newApp(os.Stderr, serve).RunContext(ctx, os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "jaccard-stored:", err)
		os.Exit(1)
	}
}

// settings is what the flags and the environment say.
type settings struct {
	data          string
	bucket        bucket.Config
	adminAddr     string
	uploadTimeout time.Duration
	urlTTL        time.Duration
	partSize      int64
	verifyJobs    int
}

// newApp returns the command. run is what it does once the settings are
// read, which lets a test look at the settings alone.
func newApp(stderr io.Writer, run func(context.Context, io.Writer, settings) error) *cli.App {
	return &cli.App{
		Name:            "jaccard-stored",
		Usage:           "serve references as pack files in an S3 bucket, over iroh",
		ErrWriter:       stderr,
		HideHelpCommand: true,
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "data", EnvVars: []string{"JACCARD_DATA"}, Required: true,
				Usage: "`DIR`ectory of the server: its key, its database, its scratch space"},
			&cli.StringFlag{Name: "s3-bucket", EnvVars: []string{"JACCARD_S3_BUCKET"}, Required: true,
				Usage: "`NAME` of the bucket the packs live in"},
			&cli.StringFlag{Name: "s3-prefix", EnvVars: []string{"JACCARD_S3_PREFIX"},
				Usage: "`PREFIX` of every object name in the bucket"},
			&cli.StringFlag{Name: "s3-endpoint", EnvVars: []string{"JACCARD_S3_ENDPOINT"},
				Usage: "`URL` of an S3 service other than AWS"},
			&cli.StringFlag{Name: "s3-region", EnvVars: []string{"JACCARD_S3_REGION"},
				Usage: "`REGION` of the bucket (default: what the AWS configuration says)"},
			&cli.BoolFlag{Name: "s3-path-style", EnvVars: []string{"JACCARD_S3_PATH_STYLE"},
				Usage: "address the bucket in the path, not in the host name"},
			&cli.StringFlag{Name: "admin-addr", EnvVars: []string{"JACCARD_ADMIN_ADDR"}, Value: "127.0.0.1:8080",
				Usage: "`ADDR`ess the admin page listens on; it has no authentication"},
			&cli.DurationFlag{Name: "upload-timeout", EnvVars: []string{"JACCARD_UPLOAD_TIMEOUT"}, Value: time.Hour,
				Usage: "how long an upload may take before what it left is removed"},
			&cli.DurationFlag{Name: "url-ttl", EnvVars: []string{"JACCARD_URL_TTL"}, Value: time.Hour,
				Usage: "how long a download URL is valid"},
			&cli.StringFlag{Name: "part-size", EnvVars: []string{"JACCARD_PART_SIZE"}, Value: "64MiB",
				Usage: "`SIZE` above which data is uploaded in parts, and of a part"},
			&cli.IntFlag{Name: "verify-jobs", EnvVars: []string{"JACCARD_VERIFY_JOBS"}, Value: 2,
				Usage: "packs verified at once; each needs scratch space for its uncompressed data"},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 0 {
				return fmt.Errorf("unexpected argument %q", c.Args().First())
			}
			s, err := readSettings(c)
			if err != nil {
				return err
			}
			return run(c.Context, c.App.ErrWriter, s)
		},
	}
}

func readSettings(c *cli.Context) (settings, error) {
	s := settings{
		data: c.String("data"),
		bucket: bucket.Config{
			Bucket:    c.String("s3-bucket"),
			Prefix:    c.String("s3-prefix"),
			Endpoint:  c.String("s3-endpoint"),
			Region:    c.String("s3-region"),
			PathStyle: c.Bool("s3-path-style"),
		},
		adminAddr:     c.String("admin-addr"),
		uploadTimeout: c.Duration("upload-timeout"),
		urlTTL:        c.Duration("url-ttl"),
		verifyJobs:    c.Int("verify-jobs"),
	}
	var err error
	if s.partSize, err = parseSize(c.String("part-size")); err != nil {
		return settings{}, fmt.Errorf("--part-size: %w", err)
	}
	switch {
	case s.partSize < minPartSize:
		return settings{}, fmt.Errorf("--part-size: %s is below the 5MiB S3 takes for a part", c.String("part-size"))
	case s.uploadTimeout <= 0:
		return settings{}, errors.New("--upload-timeout: want a duration above zero")
	case s.urlTTL <= 0:
		return settings{}, errors.New("--url-ttl: want a duration above zero")
	case s.verifyJobs < 1:
		return settings{}, errors.New("--verify-jobs: want at least 1")
	}
	return s, nil
}

// parseSize reads a count of bytes: a plain number, or one with the unit
// KiB, MiB or GiB.
func parseSize(s string) (int64, error) {
	units := []struct {
		suffix string
		factor int64
	}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}}
	factor := int64(1)
	digits := strings.TrimSpace(s)
	for _, u := range units {
		if rest, ok := strings.CutSuffix(digits, u.suffix); ok {
			digits, factor = strings.TrimSpace(rest), u.factor
			break
		}
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || n <= 0 || n > (1<<62)/factor {
		return 0, fmt.Errorf("%q is not a size: want bytes, or a number with KiB, MiB or GiB", s)
	}
	return n * factor, nil
}

// serve runs the server until ctx is done.
func serve(ctx context.Context, stderr io.Writer, s settings) (err error) {
	log := slog.New(slog.NewTextHandler(stderr, nil))
	if err := os.MkdirAll(s.data, 0o700); err != nil {
		return err
	}
	sk, err := node.LoadOrCreateKey(filepath.Join(s.data, keyFile))
	if err != nil {
		return err
	}
	database, err := db.Open(filepath.Join(s.data, dbFile))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, database.Close()) }()
	b, err := bucket.New(ctx, s.bucket)
	if err != nil {
		return err
	}
	srv, err := server.New(server.Config{
		DB:            database,
		Bucket:        b,
		Scratch:       filepath.Join(s.data, scratchDir),
		UploadTimeout: s.uploadTimeout,
		URLTTL:        s.urlTTL,
		PartSize:      s.partSize,
		VerifyJobs:    s.verifyJobs,
		Log:           log,
	})
	if err != nil {
		return err
	}

	// The admin listener comes first: an address that is taken should stop
	// the server before it has announced itself.
	listener, err := net.Listen("tcp", s.adminAddr)
	if err != nil {
		return fmt.Errorf("admin page: %w", err)
	}
	web := &http.Server{Handler: admin.Handler(database), ReadHeaderTimeout: 10 * time.Second}
	webDone := make(chan error, 1)
	go func() { webDone <- web.Serve(listener) }()
	defer func() {
		stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		web.Shutdown(stop)
	}()
	log.Info("admin page", "addr", "http://"+listener.Addr().String())

	ep, err := node.Bind(ctx, node.ServerConfig{Key: sk, ALPN: wire.ALPN, Log: log})
	if err != nil {
		return err
	}
	defer ep.Shutdown(context.Background())
	log.Info("serving", "endpoint", ep.ID().String(), "bucket", s.bucket.Bucket)

	go srv.RunSweeper(ctx, sweepEvery)
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx, ep) }()
	select {
	case err := <-served:
		return err
	case err := <-webDone:
		return fmt.Errorf("admin page: %w", err)
	}
}
