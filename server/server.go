// Package server is the jaccard-store server: it answers the protocol of
// package wire over iroh, keeps what it knows in a database (package db),
// and keeps the packs in a bucket (package bucket).
//
// The server never carries pack bytes for a client. A push uploads its
// index and data straight to the bucket through pre-signed URLs, and the
// server then downloads the pack, walks it from the root (package verify)
// and only then lets a reference point at it. A pull is answered with
// pre-signed URLs.
//
// Nothing is deleted from the bucket directly. Whatever decides that an
// object has to go writes it to a queue in the same database transaction,
// and Sweep carries the queue out, so a crash at any point leaves nothing
// behind that a later sweep does not find.
//
// Access is open: every endpoint may push, pull, list and delete. The one
// thing tied to an endpoint is an upload, which only the endpoint that
// opened it may commit.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/db"
	"github.com/amber-store/jaccard-store/wire"
	"github.com/tmc/go-iroh/iroh"
)

const (
	defaultUploadTimeout = time.Hour
	defaultURLTTL        = time.Hour
	defaultPartSize      = 64 << 20
	defaultVerifyJobs    = 2

	// maxParts is the most parts S3 takes in one multipart upload.
	maxParts = 10000
	// straggler is how long after an upload's deadline its keys are
	// deleted a second time: a PUT that began before the deadline can
	// land after it.
	straggler = time.Hour
	// requestTimeout bounds the wait for a request frame on a stream.
	requestTimeout = 30 * time.Second
	// shutdownGrace is how long Serve waits for requests under way.
	shutdownGrace = 10 * time.Second
)

// Bucket is what the server needs of the bucket: the method set of
// *bucket.Bucket.
type Bucket interface {
	Key(root key.Key, uploadID, ext string) string
	Size(ctx context.Context, objectKey string) (int64, error)
	Get(ctx context.Context, objectKey string) (io.ReadCloser, error)
	Put(ctx context.Context, objectKey string, body []byte) error
	Delete(ctx context.Context, objectKey string) error
	CreateMultipart(ctx context.Context, objectKey string) (string, error)
	AbortMultipart(ctx context.Context, objectKey, uploadID string) error
	PresignGet(ctx context.Context, objectKey string, ttl time.Duration) (string, error)
	PresignPut(ctx context.Context, objectKey string, ttl time.Duration) (string, error)
	PresignPart(ctx context.Context, objectKey, uploadID string, part int32, ttl time.Duration) (string, error)
	PresignComplete(ctx context.Context, objectKey, uploadID string, ttl time.Duration) (string, error)
}

// Config configures a server. DB, Bucket and Scratch are required.
type Config struct {
	DB     *db.DB
	Bucket Bucket
	// Scratch is a directory for the uncompressed data of the packs being
	// verified. It is the server's alone and is emptied by New.
	Scratch string
	// UploadTimeout is how long an upload may take from the issue of its
	// URLs to its commit. Zero means one hour.
	UploadTimeout time.Duration
	// URLTTL is how long a pre-signed GET is valid, and so how long the
	// objects of a collected pack stay in the bucket. Zero means one hour.
	URLTTL time.Duration
	// PartSize is the size above which data is uploaded in parts, and the
	// size of a part. Zero means 64 MiB.
	PartSize int64
	// VerifyJobs is how many packs are verified at once. Zero means 2.
	VerifyJobs int
	// Now is the clock. Nil means time.Now.
	Now func() time.Time
	// Log receives what the server has to say. Nil means slog.Default().
	Log *slog.Logger
}

// Server answers requests. It is safe for concurrent use.
type Server struct {
	db            *db.DB
	bucket        Bucket
	scratch       string
	uploadTimeout time.Duration
	urlTTL        time.Duration
	partSize      int64
	verifying     chan struct{}
	now           func() time.Time
	log           *slog.Logger
}

// New returns a server over cfg. It empties the scratch directory and
// returns the uploads a previous process was verifying to pending.
func New(cfg Config) (*Server, error) {
	if cfg.DB == nil || cfg.Bucket == nil || cfg.Scratch == "" {
		return nil, errors.New("server: a database, a bucket and a scratch directory are required")
	}
	s := &Server{
		db:            cfg.DB,
		bucket:        cfg.Bucket,
		scratch:       cfg.Scratch,
		uploadTimeout: cfg.UploadTimeout,
		urlTTL:        cfg.URLTTL,
		partSize:      cfg.PartSize,
		now:           cfg.Now,
		log:           cfg.Log,
	}
	if s.uploadTimeout <= 0 {
		s.uploadTimeout = defaultUploadTimeout
	}
	if s.urlTTL <= 0 {
		s.urlTTL = defaultURLTTL
	}
	if s.partSize <= 0 {
		s.partSize = defaultPartSize
	}
	jobs := cfg.VerifyJobs
	if jobs <= 0 {
		jobs = defaultVerifyJobs
	}
	s.verifying = make(chan struct{}, jobs)
	if s.now == nil {
		s.now = time.Now
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	if err := emptyDir(s.scratch); err != nil {
		return nil, fmt.Errorf("server: scratch directory: %w", err)
	}
	if err := s.db.ResetVerifying(context.Background()); err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	return s, nil
}

// emptyDir creates dir if it is missing and removes what is in it.
func emptyDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// Handle answers one request of the endpoint remote.
func (s *Server) Handle(ctx context.Context, remote string, req wire.Request) wire.Response {
	switch req.Op {
	case wire.OpPushStart:
		return s.pushStart(ctx, remote, req)
	case wire.OpPushUpload:
		return s.pushUpload(ctx, remote, req)
	case wire.OpPushCommit:
		return s.pushCommit(ctx, remote, req)
	case wire.OpPull:
		return s.pull(ctx, req)
	case wire.OpList:
		return s.list(ctx, req)
	case wire.OpDelete:
		return s.delete(ctx, req)
	default:
		return wire.Errorf(wire.CodeBadRequest, "unknown operation %q", req.Op)
	}
}

// internal logs err and answers with the code that invites a retry. The
// client is told what failed, not why: the cause may name keys and hosts
// that are the server's business.
func (s *Server) internal(what string, err error) wire.Response {
	s.log.Error(what, "error", err)
	return wire.Errorf(wire.CodeInternal, "%s failed", what)
}

// collectAt is when the objects of a pack collected now may be deleted:
// once every URL handed out for them has expired.
func (s *Server) collectAt(now time.Time) time.Time {
	return now.Add(s.urlTTL)
}

// Serve accepts connections on ep and answers their streams, one request
// each, until ctx is done. It then waits a moment for the requests under
// way and returns nil.
func (s *Server) Serve(ctx context.Context, ep *iroh.Endpoint) error {
	var wg sync.WaitGroup
	for ctx.Err() == nil {
		conn, err := ep.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			select {
			case <-ep.Closed():
				return errors.New("server: the endpoint was closed")
			default:
			}
			s.log.Error("accept", "error", err)
			// A failure that persists must not spin the loop.
			time.Sleep(100 * time.Millisecond)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.serveConn(ctx, conn)
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(shutdownGrace):
		s.log.Warn("stopping with requests under way")
	}
	return nil
}

// serveConn answers the streams of one connection until the peer closes it
// or ctx is done. Closing a QUIC connection discards what its streams have
// not delivered yet, so the connection is left for the peer to close.
func (s *Server) serveConn(ctx context.Context, conn *iroh.Conn) {
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	remote := conn.RemoteID().String()
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		stream, err := conn.AcceptStreamConn(ctx)
		if err != nil {
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.serveStream(ctx, remote, stream)
		}()
	}
}

func (s *Server) serveStream(ctx context.Context, remote string, stream net.Conn) {
	defer stream.Close()
	stream.SetReadDeadline(time.Now().Add(requestTimeout))
	var req wire.Request
	if err := wire.ReadFrame(stream, &req); err != nil {
		s.log.Debug("reading a request", "remote", remote, "error", err)
		return
	}
	stream.SetReadDeadline(time.Time{})
	started := time.Now()
	resp := s.Handle(ctx, remote, req)
	code := ""
	if resp.Error != nil {
		code = resp.Error.Code
	}
	s.log.Info("request", "op", req.Op, "remote", remote, "name", req.Name, "error", code, "took", time.Since(started).Round(time.Millisecond))
	if err := wire.WriteFrame(stream, resp); err != nil {
		s.log.Debug("writing a response", "remote", remote, "error", err)
	}
}
