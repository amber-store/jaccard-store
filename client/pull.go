package client

import (
	"context"
	"fmt"
	"io"
	"math"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/jaccard-store/packfile"
	"github.com/amber-store/jaccard-store/wire"
)

// PullResult says what a pull did.
type PullResult struct {
	Root key.Key
	// Packs is how many packs were downloaded: 0 when the store held the
	// reference already, 1 when its own pack was enough, 2 with the parent.
	Packs int
	// Objects and Bytes are the objects written to the store and their
	// size.
	Objects int
	Bytes   uint64
}

// Pull imports the objects of the server's reference name into objects and
// returns its root. It downloads the reference's pack and then, only if
// objects are still missing, the pack's parent. Every object is checked
// against its key before it is written. The caller sets the local ref.
func (c *Client) Pull(ctx context.Context, objects *packstore.Store, name string) (PullResult, error) {
	resp, err := c.call(ctx, wire.Request{Op: wire.OpPull, Name: name})
	if refused(err, wire.CodeNotFound) {
		return PullResult{}, fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	if err != nil {
		return PullResult{}, err
	}
	root, err := key.Parse(resp.Root)
	if err != nil {
		return PullResult{}, fmt.Errorf("pull: the server's root: %w", err)
	}
	// A reference is read from its pack and that pack's parent, nothing
	// else: more packs than that are not the answer to a pull.
	if len(resp.Packs) == 0 || len(resp.Packs) > 2 {
		return PullResult{}, fmt.Errorf("pull: the server named %d packs for %q", len(resp.Packs), name)
	}
	res := PullResult{Root: root}
	for _, p := range resp.Packs {
		if complete(root, objects) == nil {
			return res, nil
		}
		if err := c.importPack(ctx, objects, p, &res); err != nil {
			return PullResult{}, fmt.Errorf("pull: pack %x: %w", p.Root, err)
		}
	}
	if err := complete(root, objects); err != nil {
		return PullResult{}, fmt.Errorf("pull: the packs of %q do not hold all of it: %w", name, err)
	}
	return res, nil
}

// complete reports whether every object reachable from root is in objects.
func complete(root key.Key, objects *packstore.Store) error {
	_, err := fstree.CheckComplete(root, objects.Get, objects.Has, 0)
	return err
}

// importPack downloads one pack and writes the objects the store lacks.
func (c *Client) importPack(ctx context.Context, objects *packstore.Store, p wire.Pack, res *PullResult) error {
	if p.Objects > packfile.MaxEntries {
		return fmt.Errorf("the server announced %d objects, more than a pack holds", p.Objects)
	}
	if p.IndexSize != packfile.IndexSize(p.Objects) {
		return fmt.Errorf("the server announced an index of %d bytes for %d objects", p.IndexSize, p.Objects)
	}
	raw, err := c.getAll(ctx, "index", p.IndexURL, p.IndexSize)
	if err != nil {
		return err
	}
	index, err := packfile.ParseIndex(raw)
	if err != nil {
		return fmt.Errorf("index: %w", err)
	}
	// The index decides how much is written to the store, so it is held
	// to what the server said of the pack before any of it is.
	if uint64(index.Len()) != p.Objects || index.DataSize() != p.Bytes {
		return fmt.Errorf("index: %d objects of %d bytes, the server announced %d of %d",
			index.Len(), index.DataSize(), p.Objects, p.Bytes)
	}
	body, err := c.get(ctx, "data", p.DataURL)
	if err != nil {
		return err
	}
	defer body.Close()
	// No more of the body is read than the data is said to be, and one
	// byte, which a longer body gives away.
	data := io.LimitReader(body, int64(min(p.DataSize, math.MaxInt64-1))+1)
	for o, err := range packfile.Objects(index, data) {
		if err != nil {
			return fmt.Errorf("data: %w", orCause(ctx, err))
		}
		has, err := objects.Has(o.Key)
		if err != nil {
			return err
		}
		if has {
			continue
		}
		if err := objects.PutVerified(o.Key, o.Data); err != nil {
			return fmt.Errorf("object %s: %w", o.Key, err)
		}
		res.Objects++
		res.Bytes += uint64(len(o.Data))
	}
	res.Packs++
	return nil
}
