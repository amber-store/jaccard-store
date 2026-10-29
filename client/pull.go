package client

import (
	"context"
	"fmt"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/jaccard-store/human"
	"github.com/amber-store/jaccard-store/wire"
)

// PullOptions tune a pull.
type PullOptions struct {
	// Progress is told what the pull is doing. Nil means nobody is.
	Progress Progress
	// Accept, when it is set, is asked about the reference's root before
	// anything is fetched. An error from it ends the pull and is what
	// Pull returns: a caller that can only use some roots does not
	// download the others to find out.
	Accept func(root key.Key) error
}

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
func (c *Client) Pull(ctx context.Context, objects *packstore.Store, name string, opts PullOptions) (PullResult, error) {
	p := progressOf(opts.Progress)
	p.Begin("looking up the reference", 0, NoUnit)
	root, packs, err := c.lookup(ctx, name)
	if err != nil {
		return PullResult{}, err
	}
	if opts.Accept != nil {
		if err := opts.Accept(root); err != nil {
			return PullResult{}, err
		}
	}
	if len(packs) == 1 {
		p.End("in one pack")
	} else {
		p.End("in a pack and its parent")
	}
	res := PullResult{Root: root}
	for i, pack := range packs {
		if complete(root, objects, p) == nil {
			return res, nil
		}
		step := "fetching the pack"
		if i > 0 {
			step = "fetching the parent pack"
		}
		if err := c.importPack(ctx, objects, pack, &res, step, p); err != nil {
			return PullResult{}, fmt.Errorf("pull: pack %x: %w", pack.Root, err)
		}
	}
	if err := complete(root, objects, p); err != nil {
		return PullResult{}, fmt.Errorf("pull: the packs of %q do not hold all of it: %w", name, err)
	}
	return res, nil
}

// complete reports whether every object reachable from root is in objects.
// It is a step of its own: on a large reference the walk takes a while.
func complete(root key.Key, objects *packstore.Store, p Progress) error {
	p.Begin("checking the local store", 0, Objects)
	visited, err := fstree.CheckComplete(root,
		func(k key.Key) ([]byte, error) {
			p.Advance(1)
			return objects.Get(k)
		},
		func(k key.Key) (bool, error) {
			p.Advance(1)
			return objects.Has(k)
		}, 0)
	if err != nil {
		p.End("objects are missing")
		return err
	}
	p.End(fmt.Sprintf("all %s objects are here", human.Count(uint64(len(visited)))))
	return nil
}

// importPack downloads one pack and writes the objects the store lacks. It
// is the step named step of the pull: the bytes of the index and of the
// data are reported to p as they arrive.
func (c *Client) importPack(ctx context.Context, objects *packstore.Store, pack wire.Pack, res *PullResult, step string, p Progress) error {
	if err := checkAnnounced(pack); err != nil {
		return err
	}
	p.Begin(step, pack.IndexSize+dataLimit(pack), Bytes)
	index, err := c.index(ctx, pack, p)
	if err != nil {
		return err
	}
	written, size := 0, uint64(0)
	for o, err := range c.objects(ctx, pack, index, p) {
		if err != nil {
			return err
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
		written++
		size += uint64(len(o.Data))
	}
	res.Packs++
	res.Objects += written
	res.Bytes += size
	p.End(fmt.Sprintf("%s of %s objects were new, %s", human.Count(uint64(written)), human.Count(pack.Objects), human.Bytes(size)))
	return nil
}
