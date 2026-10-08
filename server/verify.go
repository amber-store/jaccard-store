package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/bucket"
	"github.com/amber-store/jaccard-store/db"
	"github.com/amber-store/jaccard-store/packfile"
	"github.com/amber-store/jaccard-store/sketch"
	"github.com/amber-store/jaccard-store/verify"
	"github.com/amber-store/jaccard-store/wire"
)

// malformed is the error of an upload that is not a valid pack. Its text is
// for the client that sent the pack.
type malformed struct{ reason string }

func (m *malformed) Error() string { return m.reason }

func malformedf(format string, args ...any) error {
	return &malformed{reason: fmt.Sprintf(format, args...)}
}

// pushCommit verifies the pack of an upload and, if it is sound, records it
// and points the upload's ref at it.
func (s *Server) pushCommit(ctx context.Context, remote string, req wire.Request) wire.Response {
	if req.UploadID == "" {
		return wire.Errorf(wire.CodeBadRequest, "upload_id: missing")
	}
	u, err := s.db.BeginVerify(ctx, req.UploadID, remote)
	switch {
	case errors.Is(err, db.ErrNotFound):
		return wire.Errorf(wire.CodeUnknownUpload, "no upload %s of this endpoint", req.UploadID)
	case errors.Is(err, db.ErrBusy):
		return wire.Errorf(wire.CodeBadRequest, "upload %s is being verified already", req.UploadID)
	case err != nil:
		return s.internal("opening the upload", err)
	}
	// From here on the upload is ours until it is committed, failed, or
	// handed back with EndVerify.
	handBack := func() {
		if err := s.db.EndVerify(context.WithoutCancel(ctx), u.ID); err != nil {
			s.log.Error("returning an upload to pending", "upload", u.ID, "error", err)
		}
	}
	now := s.now()
	if now.After(u.Deadline) {
		// Expired, and the sweeper has not come by yet. It will.
		handBack()
		return wire.Errorf(wire.CodeUnknownUpload, "upload %s has expired", u.ID)
	}

	// A root that has a pack already needs no second one: the upload is
	// redundant and is committed with nothing verified. That pack can be
	// collected before the commit lands, though; CommitUpload says so, and
	// the upload is then verified after all. An unverified upload is never
	// recorded.
	var v *db.Verified
	if _, err := s.db.PackByRoot(ctx, u.Root); errors.Is(err, db.ErrNotFound) {
		if v, err = s.verified(ctx, remote, u); err != nil {
			return s.refuse(ctx, u, handBack, err)
		}
	} else if err != nil {
		handBack()
		return s.internal("looking the root up", err)
	}
	for {
		now = s.now()
		pack, err := s.db.CommitUpload(context.WithoutCancel(ctx), u.ID, v, now, s.collectAt(now))
		if errors.Is(err, db.ErrUnverified) && v == nil {
			if v, err = s.verified(ctx, remote, u); err != nil {
				return s.refuse(ctx, u, handBack, err)
			}
			continue
		}
		if err != nil {
			handBack()
			return s.internal("recording the pack", err)
		}
		return wire.Response{Root: pack.Root[:]}
	}
}

// verified verifies the upload u and returns what it found.
func (s *Server) verified(ctx context.Context, remote string, u db.Upload) (*db.Verified, error) {
	v, err := s.verifyUpload(ctx, u)
	if err != nil {
		var bad *malformed
		if errors.As(err, &bad) {
			s.log.Info("malformed pack refused", "upload", u.ID, "root", u.Root, "remote", remote, "reason", bad.reason)
		}
		return nil, err
	}
	return &v, nil
}

// refuse answers for an upload whose verification ended with err. A pack
// that is malformed is discarded for good; if the verification could not
// be made, or the discarding fails, the upload goes back to pending, where
// a commit can be tried again and the sweeper finds it at its deadline.
func (s *Server) refuse(ctx context.Context, u db.Upload, handBack func(), err error) wire.Response {
	var bad *malformed
	if !errors.As(err, &bad) {
		handBack()
		return s.internal("verifying the pack", err)
	}
	now := s.now()
	if err := s.db.FailUpload(context.WithoutCancel(ctx), u.ID, now, s.collectAt(now)); err != nil {
		handBack()
		return s.internal("discarding a malformed upload", err)
	}
	return wire.Errorf(wire.CodeMalformedPack, "%s", bad.reason)
}

// verifyUpload checks the pack of u as the specification's section on
// verification says and measures it. For a base pack it writes the links
// to the bucket. An error of type *malformed means the pack is bad; any
// other means the check could not be made.
func (s *Server) verifyUpload(ctx context.Context, u db.Upload) (db.Verified, error) {
	select {
	case s.verifying <- struct{}{}:
		defer func() { <-s.verifying }()
	case <-ctx.Done():
		return db.Verified{}, ctx.Err()
	}

	indexSize := int64(packfile.IndexSize(uint64(u.Objects)))
	if err := s.checkSize(ctx, "index", u.IndexKey, indexSize); err != nil {
		return db.Verified{}, err
	}
	if err := s.checkSize(ctx, "data", u.DataKey, u.DataSize); err != nil {
		return db.Verified{}, err
	}

	raw, err := s.read(ctx, u.IndexKey, indexSize)
	if err != nil {
		return db.Verified{}, fmt.Errorf("index: %w", err)
	}
	index, err := packfile.ParseIndex(raw)
	if err != nil {
		return db.Verified{}, malformedf("index: %v", err)
	}
	if int64(index.Len()) != u.Objects {
		return db.Verified{}, malformedf("index: %d entries, %d were announced", index.Len(), u.Objects)
	}
	// Before a byte of the data is fetched: the index is all it takes to
	// announce more than the scratch space holds.
	if index.DataSize() > s.maxPackBytes {
		return db.Verified{}, malformedf("index: a pack of %d bytes is above this server's limit of %d", index.DataSize(), s.maxPackBytes)
	}

	data, err := s.expand(ctx, u, index)
	if err != nil {
		return db.Verified{}, err
	}
	defer func() {
		data.Close()
		os.Remove(data.Name())
	}()

	var parent *packfile.Index
	var parentLinks *packfile.Links
	if u.ParentID != 0 {
		if parent, parentLinks, err = s.parentOf(ctx, u.ParentID); err != nil {
			return db.Verified{}, fmt.Errorf("parent: %w", err)
		}
	}

	res, err := verify.Pack(u.Root, index, data, parent, parentLinks)
	if errors.Is(err, verify.ErrMalformed) {
		return db.Verified{}, malformedf("%v", err)
	}
	if err != nil {
		return db.Verified{}, err
	}

	v := db.Verified{
		IndexSize:     indexSize,
		Objects:       int64(res.Objects),
		Bytes:         int64(res.Bytes),
		SharedObjects: int64(res.SharedObjects),
		SharedBytes:   int64(res.SharedBytes),
	}
	if u.ParentID == 0 {
		links := res.Links.Encode()
		if err := s.bucket.Put(ctx, u.LinksKey, links); err != nil {
			return db.Verified{}, fmt.Errorf("writing the links: %w", err)
		}
		v.LinksSize = int64(len(links))
		// The index is in key order, so its head is the sketch.
		keys := make([]key.Key, min(index.Len(), sketch.Size))
		for i := range keys {
			keys[i] = index.Entry(i).Key
		}
		v.Sketch = sketch.Of(keys)
	}
	return v, nil
}

// checkSize holds an uploaded object to the size its upload announced.
func (s *Server) checkSize(ctx context.Context, what, objectKey string, want int64) error {
	got, err := s.bucket.Size(ctx, objectKey)
	if errors.Is(err, bucket.ErrNotFound) {
		return malformedf("%s: not uploaded", what)
	}
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if got != want {
		return malformedf("%s: %d bytes uploaded, %d were announced", what, got, want)
	}
	return nil
}

// read returns an object of the bucket that is known to be size bytes.
func (s *Server) read(ctx context.Context, objectKey string, size int64) ([]byte, error) {
	body, err := s.bucket.Get(ctx, objectKey)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	b := make([]byte, size)
	if _, err := io.ReadFull(body, b); err != nil {
		return nil, err
	}
	return b, nil
}

// expand downloads the data of u and decompresses it into a scratch file,
// which the caller closes and removes.
func (s *Server) expand(ctx context.Context, u db.Upload, index *packfile.Index) (*os.File, error) {
	body, err := s.bucket.Get(ctx, u.DataKey)
	if err != nil {
		return nil, fmt.Errorf("data: %w", err)
	}
	defer body.Close()
	f, err := os.CreateTemp(s.scratch, "pack-*")
	if err != nil {
		return nil, err
	}
	// A reader that fails is the bucket's trouble, not the pack's: it is
	// remembered, so that what the decoder makes of a stream cut short by
	// it is not held against the client.
	src := &sourceErr{r: body}
	err = packfile.Expand(index, src, f)
	if err != nil {
		f.Close()
		os.Remove(f.Name())
		if src.err != nil {
			return nil, fmt.Errorf("data: %w", src.err)
		}
		if errors.Is(err, packfile.ErrMalformed) {
			return nil, malformedf("data: %v", err)
		}
		return nil, fmt.Errorf("data: %w", err)
	}
	return f, nil
}

// sourceErr remembers the error, other than the end, a reader failed with.
type sourceErr struct {
	r   io.Reader
	err error
}

func (s *sourceErr) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if err != nil && err != io.EOF {
		s.err = err
	}
	return n, err
}

// parentOf fetches the index and the links of the base pack id. They were
// verified, or written by this server, when the pack was recorded.
func (s *Server) parentOf(ctx context.Context, id int64) (*packfile.Index, *packfile.Links, error) {
	p, err := s.db.PackByID(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	raw, err := s.read(ctx, p.IndexKey, p.IndexSize)
	if err != nil {
		return nil, nil, fmt.Errorf("index: %w", err)
	}
	index, err := packfile.ParseIndex(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("index: %w", err)
	}
	if raw, err = s.read(ctx, p.LinksKey, p.LinksSize); err != nil {
		return nil, nil, fmt.Errorf("links: %w", err)
	}
	links, err := packfile.ParseLinks(raw, index.Len())
	if err != nil {
		return nil, nil, fmt.Errorf("links: %w", err)
	}
	return index, links, nil
}
