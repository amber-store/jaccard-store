package client

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/jaccard-store/human"
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
	// Progress is told what the push is doing. Nil means nobody is.
	Progress Progress
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
	p := progressOf(opts.Progress)
	// Only the objects with children are read, so the count that runs is
	// of those; the step ends with the count of all.
	p.Begin("reading the tree", 0, Objects)
	keys, err := fstree.ReachableKeys(root, func(k key.Key) ([]byte, error) {
		p.Advance(1)
		return objects.Get(k)
	})
	if err != nil {
		return PushResult{}, fmt.Errorf("push: listing the objects of %s: %w", root, err)
	}
	keys = keyset.Normalize(keys)
	p.End(human.Count(uint64(len(keys))) + " objects")
	res, err := c.push(ctx, objects, name, root, keys, opts)
	if refused(err, wire.CodeParentGone) {
		// The base pack chosen was collected between the two requests. The
		// server's offer is different now. The step the refusal came in
		// ends here, for the next push begins its own.
		p.End("the base pack is gone from the server: once more")
		res, err = c.push(ctx, objects, name, root, keys, opts)
	}
	return res, err
}

// maxCandidates is how many base packs a server offers.
const maxCandidates = 3

// candidate is a base pack the server offered, with what it shares with
// the key set being pushed.
type candidate struct {
	root   key.Key
	index  *packfile.Index
	shared uint64 // bytes of the key set's objects the pack holds
}

func (c *Client) push(ctx context.Context, objects *packstore.Store, name string, root key.Key, keys []key.Key, opts PushOptions) (PushResult, error) {
	p := progressOf(opts.Progress)
	start := wire.Request{Op: wire.OpPushStart, Name: name, Root: root[:]}
	for _, k := range sketch.Of(keys) {
		start.Sketch = append(start.Sketch, k[:])
	}
	p.Begin("finding nearby packs", 0, NoUnit)
	offer, err := c.call(ctx, start)
	if err != nil {
		return PushResult{}, err
	}
	if offer.Stored {
		p.End("the server has this pack already")
		return PushResult{Root: root, Stored: true}, nil
	}
	p.End(fmt.Sprintf("%d on the server", min(len(offer.Candidates), maxCandidates)))
	best, err := c.best(ctx, keys, offer.Candidates, p)
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
	index, patch, err := build(ctx, f, objects, keys, best, opts.MinDedup, p)
	if err != nil {
		return PushResult{}, fmt.Errorf("push: building the pack: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		return PushResult{}, fmt.Errorf("push: %w", err)
	}
	res := PushResult{Root: root, Objects: uint64(index.Len()), Bytes: index.DataSize(), DataSize: uint64(info.Size())}
	kind := "base pack"
	if patch {
		kind = "patch pack"
	}
	p.End(fmt.Sprintf("%s of %s objects, %s → %s", kind, human.Count(res.Objects), human.Bytes(res.Bytes), human.Bytes(res.DataSize)))

	upload := wire.Request{Op: wire.OpPushUpload, Name: name, Root: root[:], DataSize: res.DataSize, Objects: res.Objects, Bytes: res.Bytes}
	if patch {
		res.Parent = &best.root
		upload.Parent = best.root[:]
	}
	// The step begins with asking for the URLs: the server takes a moment
	// to lay out a large upload.
	encoded := index.Encode()
	sent := uint64(len(encoded)) + res.DataSize
	p.Begin("uploading", sent, Bytes)
	urls, err := c.call(ctx, upload)
	if err != nil {
		return PushResult{}, err
	}
	if urls.Stored {
		// Somebody pushed the same root while the pack was being built.
		p.End("not needed: the server has this pack by now")
		return PushResult{Root: root, Stored: true}, nil
	}

	if err := c.create(ctx, "index", urls.IndexURL, bytes.NewReader(encoded), int64(len(encoded)), p); err != nil {
		return PushResult{}, fmt.Errorf("push: %w", err)
	}
	how := human.Bytes(sent)
	if urls.Parts != nil {
		how += fmt.Sprintf(" in %s parts", human.Count(uint64(len(urls.Parts.URLs))))
		err = c.putParts(ctx, f, info.Size(), urls.Parts, opts.Parallel, p)
	} else {
		err = c.create(ctx, "data", urls.DataURL, f, info.Size(), p)
	}
	if err != nil {
		return PushResult{}, fmt.Errorf("push: %w", err)
	}
	p.End(how)

	// The server fetches the pack from the bucket and walks its objects,
	// and says nothing until it is through.
	p.Begin("verifying on the server", 0, NoUnit)
	if _, err := c.call(ctx, wire.Request{Op: wire.OpPushCommit, UploadID: urls.UploadID}); err != nil {
		return PushResult{}, err
	}
	p.End("accepted")
	return res, nil
}

// best fetches the indexes of the offered packs and returns the one that
// holds the most bytes of keys, or nil when none holds any. A candidate
// whose index cannot be had is passed over: it may have been collected
// since the offer, and a base pack is always a correct answer.
func (c *Client) best(ctx context.Context, keys []key.Key, offered []wire.Candidate, p Progress) (*candidate, error) {
	// The server offers three packs at most; no more than that are fetched,
	// whatever it sends.
	offered = offered[:min(len(offered), maxCandidates)]
	if len(offered) == 0 {
		return nil, nil
	}
	var indexes uint64
	for _, o := range offered {
		if o.Objects <= packfile.MaxEntries {
			indexes += packfile.IndexSize(o.Objects)
		}
	}
	p.Begin("comparing nearby packs", indexes, Bytes)
	var best *candidate
	for _, o := range offered {
		root, err := key.Parse(o.Root)
		if err != nil || o.Objects > packfile.MaxEntries {
			continue
		}
		raw, err := c.getAll(ctx, "candidate index", o.IndexURL, packfile.IndexSize(o.Objects), p)
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
	if best == nil || best.shared == 0 {
		p.End("nothing in common")
	} else {
		which := "it"
		if len(offered) > 1 {
			which = fmt.Sprintf("the best of %d", len(offered))
		}
		p.End(fmt.Sprintf("%s holds %s of the reference", which, human.Bytes(best.shared)))
	}
	return best, nil
}

// build writes the pack of keys to f and reports whether it is a patch
// pack of best. The objects best lacks go in first; if best then holds at
// least minDedup of the bytes, that is the pack. Otherwise the objects best
// holds are appended to the same stream and the pack is a base pack.
//
// build begins the step of packing and leaves it running: its caller ends
// it, with the size the pack came to.
func build(ctx context.Context, f *os.File, objects *packstore.Store, keys []key.Key, best *candidate, minDedup float64, p Progress) (*packfile.Index, bool, error) {
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
			// Reading and compressing a large reference takes minutes,
			// and nothing else on the way looks at the context.
			if err := ctx.Err(); err != nil {
				return err
			}
			data, err := objects.Get(k)
			if err != nil {
				return fmt.Errorf("object %s: %w", k, err)
			}
			if err := w.Add(k, data); err != nil {
				return err
			}
			p.Advance(1)
		}
		return nil
	}
	p.Begin("packing", uint64(len(own)), Objects)
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
	if !patch && len(shared) > 0 {
		p.End(human.Count(uint64(len(own))) + " objects; too little in common for a patch pack")
		p.Begin("packing shared objects", uint64(len(shared)), Objects)
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
