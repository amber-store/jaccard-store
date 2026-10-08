package client

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/jaccard-store/keyset"
	"github.com/amber-store/jaccard-store/packfile"
	"github.com/amber-store/jaccard-store/sketch"
	"github.com/amber-store/jaccard-store/wire"
)

// PushOptions tune a push.
type PushOptions struct {
	// MinDedup is the threshold for a patch pack: the fraction of the
	// reference's bytes the best candidate must hold. At 0 any candidate
	// sharing an object qualifies; above 1 none does.
	MinDedup float64
	// TempDir is where the pack is built before it is uploaded. Empty means
	// os.TempDir().
	TempDir string
	// Parallel is how many parts of a large pack are uploaded at once. Zero
	// means 4.
	Parallel int
}

// PushResult says what a push did.
type PushResult struct {
	Root key.Key
	// Stored: the server had a pack for the root already, so nothing was
	// uploaded and the fields below are zero.
	Stored bool
	// Parent is the base pack the uploaded pack leans on; nil for a base
	// pack.
	Parent *key.Key
	// Objects and Bytes are the objects in the uploaded pack and their
	// uncompressed size; DataSize is the size of its compressed data.
	Objects  uint64
	Bytes    uint64
	DataSize uint64
}

// Push makes name on the server point at root, whose objects are all in
// objects. Unless the server has a pack for root already, it uploads one:
// a patch pack against the nearest base pack if that holds at least
// opts.MinDedup of the bytes, and a base pack otherwise.
func (c *Client) Push(ctx context.Context, objects *packstore.Store, name string, root key.Key, opts PushOptions) (PushResult, error) {
	keys, err := fstree.ReachableKeys(root, objects.Get)
	if err != nil {
		return PushResult{}, fmt.Errorf("push: listing the objects of %s: %w", root, err)
	}
	keys = keyset.Normalize(keys)
	res, err := c.push(ctx, objects, name, root, keys, opts)
	if refused(err, wire.CodeParentGone) {
		// The base pack chosen was collected between the two requests. The
		// server's offer is different now.
		res, err = c.push(ctx, objects, name, root, keys, opts)
	}
	return res, err
}

// candidate is a base pack the server offered, with what it shares with
// the key set being pushed.
type candidate struct {
	root   key.Key
	index  *packfile.Index
	shared uint64 // bytes of the key set's objects the pack holds
}

func (c *Client) push(ctx context.Context, objects *packstore.Store, name string, root key.Key, keys []key.Key, opts PushOptions) (PushResult, error) {
	start := wire.Request{Op: wire.OpPushStart, Name: name, Root: root[:]}
	for _, k := range sketch.Of(keys) {
		start.Sketch = append(start.Sketch, k[:])
	}
	offer, err := c.call(ctx, start)
	if err != nil {
		return PushResult{}, err
	}
	if offer.Stored {
		return PushResult{Root: root, Stored: true}, nil
	}
	best, err := c.best(ctx, keys, offer.Candidates)
	if err != nil {
		return PushResult{}, err
	}

	f, err := os.CreateTemp(opts.TempDir, "jaccard-pack-*")
	if err != nil {
		return PushResult{}, fmt.Errorf("push: %w", err)
	}
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()
	index, patch, err := build(f, objects, keys, best, opts.MinDedup)
	if err != nil {
		return PushResult{}, fmt.Errorf("push: building the pack: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		return PushResult{}, fmt.Errorf("push: %w", err)
	}
	res := PushResult{Root: root, Objects: uint64(index.Len()), Bytes: index.DataSize(), DataSize: uint64(info.Size())}

	upload := wire.Request{Op: wire.OpPushUpload, Name: name, Root: root[:], DataSize: res.DataSize, Objects: res.Objects}
	if patch {
		res.Parent = &best.root
		upload.Parent = best.root[:]
	}
	urls, err := c.call(ctx, upload)
	if err != nil {
		return PushResult{}, err
	}
	if urls.Stored {
		// Somebody pushed the same root while the pack was being built.
		return PushResult{Root: root, Stored: true}, nil
	}

	encoded := index.Encode()
	if _, err := c.put(ctx, "index", urls.IndexURL, bytes.NewReader(encoded), int64(len(encoded))); err != nil {
		return PushResult{}, fmt.Errorf("push: %w", err)
	}
	if urls.Parts != nil {
		err = c.putParts(ctx, f, info.Size(), urls.Parts, opts.Parallel)
	} else {
		if _, err = f.Seek(0, 0); err == nil {
			_, err = c.put(ctx, "data", urls.DataURL, f, info.Size())
		}
	}
	if err != nil {
		return PushResult{}, fmt.Errorf("push: %w", err)
	}

	if _, err := c.call(ctx, wire.Request{Op: wire.OpPushCommit, UploadID: urls.UploadID}); err != nil {
		return PushResult{}, err
	}
	return res, nil
}

// best fetches the indexes of the offered packs and returns the one that
// holds the most bytes of keys, or nil when none holds any. A candidate
// whose index cannot be had is passed over: it may have been collected
// since the offer, and a base pack is always a correct answer.
func (c *Client) best(ctx context.Context, keys []key.Key, offered []wire.Candidate) (*candidate, error) {
	var best *candidate
	for _, o := range offered {
		root, err := key.Parse(o.Root)
		if err != nil || o.Objects > packfile.MaxEntries {
			continue
		}
		raw, err := c.getAll(ctx, "candidate index", o.IndexURL, packfile.IndexSize(o.Objects))
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue
		}
		index, err := packfile.ParseIndex(raw)
		if err != nil {
			continue
		}
		cand := &candidate{root: root, index: index}
		for _, k := range keys {
			if i, ok := index.Find(k); ok {
				cand.shared += uint64(index.Entry(i).Length)
			}
		}
		// The offer comes nearest first, so a tie stays with the nearer.
		if best == nil || cand.shared > best.shared {
			best = cand
		}
	}
	return best, nil
}

// build writes the pack of keys to f and reports whether it is a patch
// pack of best. The objects best lacks go in first; if best then holds at
// least minDedup of the bytes, that is the pack. Otherwise the objects best
// holds are appended to the same stream and the pack is a base pack.
func build(f *os.File, objects *packstore.Store, keys []key.Key, best *candidate, minDedup float64) (*packfile.Index, bool, error) {
	var own, shared []key.Key
	for _, k := range keys {
		if best != nil && best.index.Has(k) {
			shared = append(shared, k)
		} else {
			own = append(own, k)
		}
	}
	w, err := packfile.NewWriter(f)
	if err != nil {
		return nil, false, err
	}
	add := func(keys []key.Key) error {
		// The pack's order is free, so the packstore is read in its own.
		objects.SortByLocation(keys)
		for _, k := range keys {
			data, err := objects.Get(k)
			if err != nil {
				return fmt.Errorf("object %s: %w", k, err)
			}
			if err := w.Add(k, data); err != nil {
				return err
			}
		}
		return nil
	}
	if err := add(own); err != nil {
		return nil, false, err
	}
	patch := false
	if len(shared) > 0 {
		total := best.shared + w.Bytes()
		dedup := 0.0
		if total > 0 {
			dedup = float64(best.shared) / float64(total)
		}
		patch = dedup >= minDedup
	}
	if !patch {
		if err := add(shared); err != nil {
			return nil, false, err
		}
	}
	index, err := w.Finish()
	if err != nil {
		return nil, false, err
	}
	return index, patch, nil
}
