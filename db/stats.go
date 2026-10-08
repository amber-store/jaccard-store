package db

import (
	"context"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/db/dbq"
)

// Stats are the figures of the store as a whole.
type Stats struct {
	Refs, BasePacks, PatchPacks, Uploads, Deletions int64
	S3Bytes                                         int64 // data + index + links
	DataBytes                                       int64 // data only
	StoredBytes                                     int64 // uncompressed bytes in packs
	LogicalBytes                                    int64 // over refs: bytes + shared_bytes of the pack
}

// Stats returns the figures of the store, all read at one moment.
func (d *DB) Stats(ctx context.Context) (s Stats, err error) {
	err = d.read(ctx, func(q *dbq.Queries) error {
		if s.Refs, err = q.CountRefs(ctx); err != nil {
			return err
		}
		if s.Uploads, err = q.CountUploads(ctx); err != nil {
			return err
		}
		if s.Deletions, err = q.CountDeletions(ctx); err != nil {
			return err
		}
		if s.LogicalBytes, err = q.LogicalBytes(ctx); err != nil {
			return err
		}
		totals, err := q.PackTotals(ctx)
		if err != nil {
			return err
		}
		s.BasePacks, s.PatchPacks = totals.BasePacks, totals.PatchPacks
		s.S3Bytes, s.DataBytes, s.StoredBytes = totals.S3Bytes, totals.DataBytes, totals.StoredBytes
		return nil
	})
	return s, err
}

// RefInfo is a ref with its pack and, for a patch pack, its parent.
type RefInfo struct {
	Ref    Ref
	Pack   Pack
	Parent *Pack
}

// ListRefInfo lists refs as ListRefs does, each with its pack and the
// pack's parent.
func (d *DB) ListRefInfo(ctx context.Context, prefix, after string, limit int) ([]RefInfo, error) {
	var out []RefInfo
	err := d.read(ctx, func(q *dbq.Queries) error {
		refs, err := listRefs(ctx, q, prefix, after, limit)
		if err != nil {
			return err
		}
		packs := map[int64]*Pack{}
		pack := func(id int64) (*Pack, error) {
			if p, ok := packs[id]; ok {
				return p, nil
			}
			p, err := packByID(ctx, q, id)
			if err != nil {
				return nil, err
			}
			packs[id] = &p
			return &p, nil
		}
		out = make([]RefInfo, len(refs))
		for i, r := range refs {
			p, err := pack(r.PackID)
			if err != nil {
				return err
			}
			out[i] = RefInfo{Ref: r, Pack: *p}
			if p.IsBase() {
				continue
			}
			parent, err := pack(p.ParentID)
			if err != nil {
				return err
			}
			out[i].Parent = new(*parent)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PackInfo is a pack with what leans on it: the number of refs that point
// at it and of packs that name it as parent. ParentRoot is nil for a base
// pack.
type PackInfo struct {
	Pack       Pack
	ParentRoot *key.Key
	Refs       int64
	Children   int64
}

// ListPacks returns at most limit packs whose IDs are above afterID, in the
// order of their IDs.
func (d *DB) ListPacks(ctx context.Context, afterID int64, limit int) ([]PackInfo, error) {
	var out []PackInfo
	err := d.read(ctx, func(q *dbq.Queries) error {
		rows, err := q.ListPacks(ctx, dbq.ListPacksParams{AfterID: afterID, N: atMost(limit)})
		if err != nil {
			return err
		}
		out = make([]PackInfo, len(rows))
		for i, r := range rows {
			p, err := packOf(packRow{
				ID:            r.ID,
				Root:          r.Root,
				ParentID:      r.ParentID,
				DataKey:       r.DataKey,
				IndexKey:      r.IndexKey,
				LinksKey:      r.LinksKey,
				DataSize:      r.DataSize,
				IndexSize:     r.IndexSize,
				LinksSize:     r.LinksSize,
				Objects:       r.Objects,
				Bytes:         r.Bytes,
				SharedObjects: r.SharedObjects,
				SharedBytes:   r.SharedBytes,
				Uploader:      r.Uploader,
				UploadedAt:    r.UploadedAt,
			})
			if err != nil {
				return err
			}
			out[i] = PackInfo{Pack: p, Refs: r.Refs, Children: r.Children}
			if p.IsBase() {
				continue
			}
			parentRoot, err := keyOf(r.ParentRoot)
			if err != nil {
				return err
			}
			out[i].ParentRoot = &parentRoot
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PackDetail is a pack with everything that leans on it by name: the refs
// that point at it and the roots of the packs that name it as parent, both
// ascending and empty rather than nil when there are none. Parent is nil
// for a base pack. The field Children hides the count of the same name,
// which is PackInfo.Children.
type PackDetail struct {
	PackInfo
	Parent   *Pack
	RefNames []string
	Children []key.Key
}

// PackDetail returns the pack of root with what leans on it, or
// ErrNotFound.
func (d *DB) PackDetail(ctx context.Context, root key.Key) (PackDetail, error) {
	var out PackDetail
	err := d.read(ctx, func(q *dbq.Queries) error {
		p, err := packByRoot(ctx, q, root)
		if err != nil {
			return err
		}
		out.Pack = p
		if !p.IsBase() {
			parent, err := packByID(ctx, q, p.ParentID)
			if err != nil {
				return err
			}
			out.Parent = &parent
			out.ParentRoot = new(parent.Root)
		}
		if out.RefNames, err = q.RefNamesOfPack(ctx, p.ID); err != nil {
			return err
		}
		children, err := q.ChildRoots(ctx, nullInt(p.ID))
		if err != nil {
			return err
		}
		out.Children = make([]key.Key, len(children))
		for i, c := range children {
			if out.Children[i], err = keyOf(c); err != nil {
				return err
			}
		}
		out.Refs = int64(len(out.RefNames))
		out.PackInfo.Children = int64(len(out.Children))
		return nil
	})
	if err != nil {
		return PackDetail{}, err
	}
	return out, nil
}
