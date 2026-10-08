package server

import (
	"context"
	"errors"

	"github.com/amber-store/core/reference"
	"github.com/amber-store/jaccard-store/db"
	"github.com/amber-store/jaccard-store/wire"
)

// maxList is the most references one list request returns.
const maxList = 1000

// pull answers with the root of a ref and where its pack, and that pack's
// parent, can be fetched.
func (s *Server) pull(ctx context.Context, req wire.Request) wire.Response {
	if err := reference.ValidateName(req.Name); err != nil {
		return wire.Errorf(wire.CodeBadRequest, "name: %v", err)
	}
	ref, err := s.db.Ref(ctx, req.Name)
	if errors.Is(err, db.ErrNotFound) {
		return wire.Errorf(wire.CodeNotFound, "no reference %q", req.Name)
	}
	if err != nil {
		return s.internal("reading the reference", err)
	}
	pack, err := s.db.PackByID(ctx, ref.PackID)
	if err != nil {
		return s.internal("reading the pack", err)
	}
	packs := []db.Pack{pack}
	if !pack.IsBase() {
		parent, err := s.db.PackByID(ctx, pack.ParentID)
		if err != nil {
			return s.internal("reading the parent pack", err)
		}
		packs = append(packs, parent)
	}
	resp := wire.Response{Root: ref.Root[:]}
	for _, p := range packs {
		out := wire.Pack{
			Root:      p.Root[:],
			Objects:   uint64(p.Objects),
			Bytes:     uint64(p.Bytes),
			DataSize:  uint64(p.DataSize),
			IndexSize: uint64(p.IndexSize),
		}
		if out.IndexURL, err = s.bucket.PresignGet(ctx, p.IndexKey, s.urlTTL); err != nil {
			return s.internal("signing an index URL", err)
		}
		if out.DataURL, err = s.bucket.PresignGet(ctx, p.DataKey, s.urlTTL); err != nil {
			return s.internal("signing a data URL", err)
		}
		resp.Packs = append(resp.Packs, out)
	}
	return resp
}

// list answers with the refs under a prefix, a page at a time.
func (s *Server) list(ctx context.Context, req wire.Request) wire.Response {
	limit := req.Limit
	if limit <= 0 || limit > maxList {
		limit = maxList
	}
	// One more than asked for tells whether there is a next page.
	refs, err := s.db.ListRefs(ctx, req.Prefix, req.After, limit+1)
	if err != nil {
		return s.internal("listing the references", err)
	}
	var resp wire.Response
	if len(refs) > limit {
		refs, resp.More = refs[:limit], true
	}
	for _, r := range refs {
		resp.Refs = append(resp.Refs, wire.Ref{Name: r.Name, Root: r.Root[:]})
	}
	return resp
}

// delete removes a ref. The packs it leaves without a reference go with it.
func (s *Server) delete(ctx context.Context, req wire.Request) wire.Response {
	if err := reference.ValidateName(req.Name); err != nil {
		return wire.Errorf(wire.CodeBadRequest, "name: %v", err)
	}
	err := s.db.DeleteRef(ctx, req.Name, s.collectAt(s.now()))
	if errors.Is(err, db.ErrNotFound) {
		return wire.Errorf(wire.CodeNotFound, "no reference %q", req.Name)
	}
	if err != nil {
		return s.internal("deleting the reference", err)
	}
	return wire.Response{}
}
