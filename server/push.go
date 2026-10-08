package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/jaccard-store/db"
	"github.com/amber-store/jaccard-store/keyset"
	"github.com/amber-store/jaccard-store/packfile"
	"github.com/amber-store/jaccard-store/sketch"
	"github.com/amber-store/jaccard-store/wire"
)

const (
	// maxSketch is the longest sketch a request may carry. The server uses
	// the lowest sketch.Size keys of it.
	maxSketch = 4096
	// candidates is how many base packs push-start offers.
	candidates = 3
)

// The extensions of a pack's three objects.
const (
	extIndex = "idx"
	extData  = "data"
	extLinks = "links"
)

// target reads the name and the root every push request carries.
func target(req wire.Request) (string, key.Key, *wire.Response) {
	if err := reference.ValidateName(req.Name); err != nil {
		resp := wire.Errorf(wire.CodeBadRequest, "name: %v", err)
		return "", key.Key{}, &resp
	}
	root, err := key.Parse(req.Root)
	if err != nil {
		resp := wire.Errorf(wire.CodeBadRequest, "root: %v", err)
		return "", key.Key{}, &resp
	}
	return req.Name, root, nil
}

// parseSketch reads the sketch of a request and cuts it to sketch.Size.
func parseSketch(raw [][]byte) (sketch.Sketch, error) {
	if len(raw) == 0 {
		return nil, errors.New("sketch: empty")
	}
	if len(raw) > maxSketch {
		return nil, errors.New("sketch: too many keys")
	}
	sk := make(sketch.Sketch, len(raw))
	for i, b := range raw {
		k, err := key.Parse(b)
		if err != nil {
			return nil, errors.New("sketch: " + err.Error())
		}
		sk[i] = k
	}
	if !keyset.Ascending(sk) {
		return nil, errors.New("sketch: keys are not strictly ascending")
	}
	return sk[:min(len(sk), sketch.Size)], nil
}

// pushStart points the ref at the pack of the root if there is one, and
// otherwise names the base packs nearest to the sketch.
func (s *Server) pushStart(ctx context.Context, remote string, req wire.Request) wire.Response {
	name, root, bad := target(req)
	if bad != nil {
		return *bad
	}
	sk, err := parseSketch(req.Sketch)
	if err != nil {
		return wire.Errorf(wire.CodeBadRequest, "%v", err)
	}
	now := s.now()
	stored, err := s.db.PointRef(ctx, name, root, remote, now, s.collectAt(now))
	if err != nil {
		return s.internal("pointing the reference", err)
	}
	if stored {
		return wire.Response{Stored: true}
	}
	near, err := s.db.Nearest(ctx, sk, candidates)
	if err != nil {
		return s.internal("searching the nearest packs", err)
	}
	var resp wire.Response
	for _, c := range near {
		url, err := s.bucket.PresignGet(ctx, c.Pack.IndexKey, s.urlTTL)
		if err != nil {
			return s.internal("signing an index URL", err)
		}
		resp.Candidates = append(resp.Candidates, wire.Candidate{
			Root:     c.Pack.Root[:],
			Distance: 1 - c.Similarity,
			Objects:  uint64(c.Pack.Objects),
			Bytes:    uint64(c.Pack.Bytes),
			DataSize: uint64(c.Pack.DataSize),
			IndexURL: url,
		})
	}
	return resp
}

// pushUpload opens an upload and answers with the URLs it goes to.
func (s *Server) pushUpload(ctx context.Context, remote string, req wire.Request) wire.Response {
	name, root, bad := target(req)
	if bad != nil {
		return *bad
	}
	var parent *key.Key
	if len(req.Parent) > 0 {
		p, err := key.Parse(req.Parent)
		if err != nil {
			return wire.Errorf(wire.CodeBadRequest, "parent: %v", err)
		}
		if p == root {
			return wire.Errorf(wire.CodeBadRequest, "parent: a pack cannot be its own parent")
		}
		parent = &p
	}
	if req.Objects > packfile.MaxEntries {
		return wire.Errorf(wire.CodeBadRequest, "objects: %d is more than a pack holds", req.Objects)
	}
	if req.DataSize > maxDataSize {
		return wire.Errorf(wire.CodeBadRequest, "data_size: %d bytes is more than an object of the bucket holds", req.DataSize)
	}
	if req.Bytes > s.maxPackBytes {
		return wire.Errorf(wire.CodeBadRequest, "bytes: a pack of %d bytes is above this server's limit of %d", req.Bytes, s.maxPackBytes)
	}

	now := s.now()
	stored, err := s.db.PointRef(ctx, name, root, remote, now, s.collectAt(now))
	if err != nil {
		return s.internal("pointing the reference", err)
	}
	if stored {
		return wire.Response{Stored: true}
	}

	id, err := newUploadID()
	if err != nil {
		return s.internal("naming the upload", err)
	}
	u := db.Upload{
		ID:       id,
		Name:     name,
		Root:     root,
		Uploader: remote,
		DataKey:  s.bucket.Key(root, id, extData),
		IndexKey: s.bucket.Key(root, id, extIndex),
		LinksKey: s.bucket.Key(root, id, extLinks),
		DataSize: int64(req.DataSize),
		Objects:  int64(req.Objects),
		State:    db.StatePending,
		IssuedAt: now,
		Deadline: now.Add(s.uploadTimeout),
	}
	size := int64(req.DataSize)
	if size > s.partSize {
		// The multipart upload exists before the database knows of it. A
		// crash in between leaves one without parts, which holds no bytes.
		if u.MultipartID, err = s.bucket.CreateMultipart(ctx, u.DataKey); err != nil {
			return s.internal("starting the multipart upload", err)
		}
	}
	if _, err = s.db.CreateUpload(ctx, u, parent); err != nil {
		if u.MultipartID != "" {
			if aerr := s.bucket.AbortMultipart(ctx, u.DataKey, u.MultipartID); aerr != nil {
				s.log.Warn("aborting a multipart upload that was not recorded", "error", aerr)
			}
		}
		if errors.Is(err, db.ErrParentGone) {
			return wire.Errorf(wire.CodeParentGone, "parent %s is not a base pack of this store", parent)
		}
		return s.internal("recording the upload", err)
	}

	resp := wire.Response{UploadID: id, Deadline: u.Deadline.Unix()}
	if resp.IndexURL, err = s.bucket.PresignPut(ctx, u.IndexKey, s.uploadTimeout); err != nil {
		return s.internal("signing the index URL", err)
	}
	if u.MultipartID == "" {
		if resp.DataURL, err = s.bucket.PresignPut(ctx, u.DataKey, s.uploadTimeout); err != nil {
			return s.internal("signing the data URL", err)
		}
		return resp
	}
	partSize := partSizeFor(size, s.partSize)
	parts := &wire.Parts{PartSize: uint64(partSize)}
	for n := int64(0); n*partSize < size; n++ {
		url, err := s.bucket.PresignPart(ctx, u.DataKey, u.MultipartID, int32(n+1), s.uploadTimeout)
		if err != nil {
			return s.internal("signing a part URL", err)
		}
		parts.URLs = append(parts.URLs, url)
	}
	if parts.CompleteURL, err = s.bucket.PresignComplete(ctx, u.DataKey, u.MultipartID, s.uploadTimeout); err != nil {
		return s.internal("signing the completion URL", err)
	}
	resp.Parts = parts
	return resp
}

// partSizeFor returns the part size for data of size bytes: the configured
// one, grown in whole mebibytes until maxParts parts are enough. size is at
// most maxDataSize, so nothing here overflows.
func partSizeFor(size, configured int64) int64 {
	part := configured
	need := size / maxParts
	if size%maxParts != 0 {
		need++
	}
	if need > part {
		const mib = 1 << 20
		part = (need + mib - 1) / mib * mib
	}
	return part
}

// newUploadID returns 128 random bits in hex. The ID is all a client needs
// to commit an upload, beside being the endpoint that opened it.
func newUploadID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
