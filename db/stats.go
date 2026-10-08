package db

import (
	"context"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/db/dbq"
)

// Stats are the figures of the store as a whole.
//
// The sizes are a chain. Each is what the one before comes to after one
// more thing the store does to save room:
//
//	UnpackedBytes    every ref unpacked into a directory of its own
//	ObjectBytes      content addressing: each ref the objects it is made
//	                 of, every object once. One pack for each ref would
//	                 hold this, uncompressed.
//	PackBytes        packs shared between refs: what the packs that are
//	                 there hold, uncompressed
//	DataBytes        compression: what the bucket holds of them
//
// The third can be more than the second. A patch pack saves its ref the
// objects its parent holds, and a pack that several refs point at is there
// once; but a pack that no ref points at any more, kept because patch
// packs lean on it, is all there whatever part of it they use. So
// PackBytes is given in its two parts as well: ReferencedPackBytes, which
// is never more than ObjectBytes, and UnreferencedPackBytes, which is what
// the sharing costs.
type Stats struct {
	Refs, BasePacks, PatchPacks, Uploads, Deletions int64
	// UnreferencedPacks are the packs no ref points at.
	UnreferencedPacks int64
	// UnverifiedPacks are the packs recorded without being verified. What
	// ObjectBytes has of a patch pack among them, and of a patch pack whose
	// parent is among them, is the figure of the client that uploaded it.
	UnverifiedPacks int64

	UnpackedBytes int64 // over refs: the tree of each, as its root key sizes it
	ObjectBytes   int64 // over refs: bytes + shared_bytes of the pack
	PackBytes     int64 // over packs: their objects, uncompressed
	DataBytes     int64 // over packs: their data in the bucket
	IndexBytes    int64 // over packs: their indexes and links in the bucket

	// Of PackBytes and DataBytes, what is in packs a ref points at and in
	// packs none does.
	ReferencedPackBytes, UnreferencedPackBytes int64
	UnreferencedDataBytes                      int64
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
		refs, err := q.RefTotals(ctx)
		if err != nil {
			return err
		}
		s.UnpackedBytes, s.ObjectBytes = refs.UnpackedBytes, refs.ObjectBytes
		packs, err := q.PackTotals(ctx)
		if err != nil {
			return err
		}
		s.BasePacks, s.PatchPacks = packs.BasePacks, packs.PatchPacks
		s.UnverifiedPacks = packs.UnverifiedPacks
		s.PackBytes, s.DataBytes, s.IndexBytes = packs.PackBytes, packs.DataBytes, packs.IndexBytes
		unreferenced, err := q.UnreferencedTotals(ctx)
		if err != nil {
			return err
		}
		s.UnreferencedPacks = unreferenced.Packs
		s.UnreferencedPackBytes, s.UnreferencedDataBytes = unreferenced.PackBytes, unreferenced.DataBytes
		s.ReferencedPackBytes = s.PackBytes - s.UnreferencedPackBytes
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
				Verified:      r.Verified,
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

// TopPack is a pack in one of the lists of TopPacks.
type TopPack struct {
	PackInfo
	// LargestShare is the most of the pack's bytes that one of the patch
	// packs leaning on it uses: 0 when none leans on it. Against the
	// pack's own bytes it says what a pull of such a patch pack fetches
	// for nothing.
	LargestShare int64
}

// TopPacks returns the packs the most is hung on, at most n of each kind
// and the most first: those the most refs point at, and those the most
// patch packs lean on. A pack that has neither is in neither list.
func (d *DB) TopPacks(ctx context.Context, n int) (byRefs, byChildren []TopPack, err error) {
	err = d.read(ctx, func(q *dbq.Queries) error {
		refs, err := q.TopPacksByRefs(ctx, atMost(n))
		if err != nil {
			return err
		}
		byRefs = make([]TopPack, len(refs))
		for i, r := range refs {
			row := packRow{
				ID: r.ID, Root: r.Root, ParentID: r.ParentID, DataKey: r.DataKey, IndexKey: r.IndexKey, LinksKey: r.LinksKey,
				DataSize: r.DataSize, IndexSize: r.IndexSize, LinksSize: r.LinksSize, Objects: r.Objects, Bytes: r.Bytes,
				SharedObjects: r.SharedObjects, SharedBytes: r.SharedBytes, Uploader: r.Uploader, UploadedAt: r.UploadedAt,
				Verified: r.Verified,
			}
			if byRefs[i], err = topPackOf(row, r.ParentRoot, r.Refs, r.Children, r.LargestShare); err != nil {
				return err
			}
		}
		children, err := q.TopPacksByChildren(ctx, atMost(n))
		if err != nil {
			return err
		}
		byChildren = make([]TopPack, len(children))
		for i, r := range children {
			row := packRow{
				ID: r.ID, Root: r.Root, ParentID: r.ParentID, DataKey: r.DataKey, IndexKey: r.IndexKey, LinksKey: r.LinksKey,
				DataSize: r.DataSize, IndexSize: r.IndexSize, LinksSize: r.LinksSize, Objects: r.Objects, Bytes: r.Bytes,
				SharedObjects: r.SharedObjects, SharedBytes: r.SharedBytes, Uploader: r.Uploader, UploadedAt: r.UploadedAt,
				Verified: r.Verified,
			}
			if byChildren[i], err = topPackOf(row, nil, r.Refs, r.Children, r.LargestShare); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return byRefs, byChildren, nil
}

// topPackOf makes a TopPack of a row and what was counted beside it.
// parentRoot is nil for a base pack.
func topPackOf(row packRow, parentRoot []byte, refs, children, largestShare int64) (TopPack, error) {
	p, err := packOf(row)
	if err != nil {
		return TopPack{}, err
	}
	out := TopPack{PackInfo: PackInfo{Pack: p, Refs: refs, Children: children}, LargestShare: largestShare}
	if !p.IsBase() {
		root, err := keyOf(parentRoot)
		if err != nil {
			return TopPack{}, err
		}
		out.ParentRoot = &root
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
