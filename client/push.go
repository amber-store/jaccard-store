package client

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync/atomic"

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
	// reference's bytes a base pack must hold to become its parent. At 0
	// any pack sharing an object qualifies; above 1 none does. A pack
	// that qualifies is still passed over when it would make a pull fetch
	// more than twice the reference.
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
// a patch pack against one of the base packs the server offers, if one of
// them is fit to be its parent (see choose), and a base pack otherwise.
func (c *Client) Push(ctx context.Context, objects *packstore.Store, name string, root key.Key, opts PushOptions) (PushResult, error) {
	p := progressOf(opts.Progress)
	keys, size, err := keySet(root, objects, p)
	if err != nil {
		return PushResult{}, fmt.Errorf("push: listing the objects of %s: %w", root, err)
	}
	res, err := c.push(ctx, objects, name, root, keys, size, opts)
	if refused(err, wire.CodeParentGone) {
		// The base pack chosen was collected between the two requests. The
		// server's offer is different now. The step the refusal came in
		// ends here, for the next push begins its own.
		p.End("the base pack is gone from the server: once more")
		res, err = c.push(ctx, objects, name, root, keys, size, opts)
	}
	return res, err
}

// keySet lists the objects reachable from root, in the order every package
// agrees on, and adds up their bytes. It is the step of reading the tree.
//
// The size costs no reading of its own. The walk reads every object that
// has children, and those are measured as they pass; the others are blobs
// and sets of extended attributes, whose keys say how long they are.
func keySet(root key.Key, objects *packstore.Store, p Progress) (keys []key.Key, size uint64, err error) {
	// Only the objects with children are read, so the count that runs is
	// of those; the step ends with the count of all.
	p.Begin("reading the tree", 0, Objects)
	var read atomic.Uint64
	keys, err = fstree.ReachableKeys(root, func(k key.Key) ([]byte, error) {
		p.Advance(1)
		data, err := objects.Get(k)
		read.Add(uint64(len(data)))
		return data, err
	})
	if err != nil {
		return nil, 0, err
	}
	keys = keyset.Normalize(keys)
	size = read.Load()
	for _, k := range keys {
		if t := k.Type(); t == key.Blob || t == key.XattrSet {
			size += k.Length()
		}
	}
	p.End(fmt.Sprintf("%s objects, %s", human.Count(uint64(len(keys))), human.Bytes(size)))
	return keys, size, nil
}

// maxCandidates is how many base packs a server offers.
const maxCandidates = 3

// maxPull is how much a pull of the reference may have to fetch for the
// reference to be pushed as a patch pack, as a multiple of the reference's
// own size. A pull of a patch pack fetches the parent whole and the patch;
// a parent that is mostly something else makes every pull pay for what the
// reference does not hold. Past this, a base pack is pushed instead: more
// to upload once, less to download every time.
const maxPull = 2

// candidate is a base pack the server offered, with what it has to do with
// the key set being pushed. Sizes are of objects as they are, not as a
// pack compresses them.
type candidate struct {
	root     key.Key
	index    *packfile.Index
	distance float64 // the server's estimate of the Jaccard distance
	bytes    uint64  // of all the pack's objects: what a pull fetches of it
	objects  int     // of the key set that the pack holds
	shared   uint64  // and their bytes
}

// pull is what a pull of a reference of size bytes fetches when it is
// pushed as a patch of c: the parent whole, and the objects it lacks.
func (c *candidate) pull(size uint64) uint64 {
	return c.bytes + (size - min(size, c.shared))
}

// dedup is the part of a reference of size bytes that c holds.
func (c *candidate) dedup(size uint64) float64 {
	if size == 0 {
		return 0
	}
	return float64(c.shared) / float64(size)
}

// fit reports whether a reference of size bytes may be a patch of c: c
// holds an object of it, holds at least minDedup of its bytes, and does
// not make a pull fetch more than maxPull times the reference.
func (c *candidate) fit(size uint64, minDedup float64) bool {
	// What the parent holds besides the reference is what a pull fetches
	// for nothing, and may be as much as the reference itself for each
	// time over one that maxPull allows.
	return c.objects > 0 && c.dedup(size) >= minDedup && c.bytes-c.shared <= (maxPull-1)*size
}

// choose returns the pack a reference of size bytes is made a patch of, or
// nil when none of the candidates is fit for it and the reference is to be
// a base pack.
//
// Of the candidates that are fit, the one that holds the most of the
// reference's bytes is taken: it leaves the least to upload and to store.
// Among those that hold the same, the nearest by the server's estimate of
// the Jaccard distance; among those as near, the smallest, which is the
// least to fetch.
func choose(candidates []*candidate, size uint64, minDedup float64) *candidate {
	var best *candidate
	for _, c := range candidates {
		if !c.fit(size, minDedup) {
			continue
		}
		if best == nil || better(c, best) {
			best = c
		}
	}
	return best
}

// better reports whether a is to be preferred to b as a parent.
func better(a, b *candidate) bool {
	switch {
	case a.shared != b.shared:
		return a.shared > b.shared
	case a.distance != b.distance:
		return a.distance < b.distance
	}
	return a.bytes < b.bytes
}

func (c *Client) push(ctx context.Context, objects *packstore.Store, name string, root key.Key, keys []key.Key, size uint64, opts PushOptions) (PushResult, error) {
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
	parent, err := c.parent(ctx, keys, size, offer.Candidates, opts.MinDedup, p)
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
	index, err := build(ctx, f, objects, keys, parent, p)
	if err != nil {
		return PushResult{}, fmt.Errorf("push: building the pack: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		return PushResult{}, fmt.Errorf("push: %w", err)
	}
	res := PushResult{Root: root, Objects: uint64(index.Len()), Bytes: index.DataSize(), DataSize: uint64(info.Size())}
	kind := "base pack"
	if parent != nil {
		kind = "patch pack"
	}
	p.End(fmt.Sprintf("%s of %s objects, %s → %s", kind, human.Count(res.Objects), human.Bytes(res.Bytes), human.Bytes(res.DataSize)))

	upload := wire.Request{Op: wire.OpPushUpload, Name: name, Root: root[:], DataSize: res.DataSize, Objects: res.Objects, Bytes: res.Bytes}
	if parent != nil {
		res.Parent = &parent.root
		upload.Parent = parent.root[:]
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

// parent fetches the indexes of the offered packs, compares them with the
// key set and returns the one the pack is to be a patch of, or nil for a
// base pack: see choose. size is the bytes of the key set's objects.
//
// A candidate whose index cannot be had is passed over: it may have been
// collected since the offer, and a base pack is always a correct answer.
func (c *Client) parent(ctx context.Context, keys []key.Key, size uint64, offered []wire.Candidate, minDedup float64, p Progress) (*candidate, error) {
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
	var candidates []*candidate
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
		cand := &candidate{root: root, index: index, distance: o.Distance, bytes: index.DataSize()}
		// A distance that is no number orders nothing: such a pack is
		// taken for the farthest there is.
		if !(cand.distance >= 0) {
			cand.distance = 1
		}
		for _, k := range keys {
			if i, ok := index.Find(k); ok {
				cand.objects++
				cand.shared += uint64(index.Entry(i).Length)
			}
		}
		candidates = append(candidates, cand)
	}
	chosen := choose(candidates, size, minDedup)
	p.End(verdict(candidates, chosen, size, minDedup))
	return chosen, nil
}

// verdict says in a few words what came of comparing the candidates: which
// was chosen, or why none was.
func verdict(candidates []*candidate, chosen *candidate, size uint64, minDedup float64) string {
	if chosen != nil {
		return fmt.Sprintf("a parent that holds %s of %s", human.Bytes(chosen.shared), human.Bytes(size))
	}
	// The one that would have been taken, had it been fit.
	var best *candidate
	for _, c := range candidates {
		if c.objects > 0 && (best == nil || better(c, best)) {
			best = c
		}
	}
	switch {
	case best == nil:
		return "nothing in common"
	case best.dedup(size) < minDedup:
		return fmt.Sprintf("too little in common: %s of %s", human.Bytes(best.shared), human.Bytes(size))
	}
	// Enough in common, and no parent all the same: the ones that hold
	// enough are too large.
	for _, c := range candidates {
		if c.objects > 0 && c.dedup(size) >= minDedup && c.pull(size) < best.pull(size) {
			best = c
		}
	}
	return fmt.Sprintf("too large a parent: a pull would fetch %s for %s", human.Bytes(best.pull(size)), human.Bytes(size))
}

// build writes to f the pack of the objects of keys that parent lacks: all
// of them when parent is nil, which makes a base pack.
//
// build begins the step of packing and leaves it running: its caller ends
// it, with the size the pack came to.
func build(ctx context.Context, f *os.File, objects *packstore.Store, keys []key.Key, parent *candidate, p Progress) (*packfile.Index, error) {
	// A list of its own: it is put into the store's order below, and keys
	// is the caller's.
	var own []key.Key
	for _, k := range keys {
		if parent == nil || !parent.index.Has(k) {
			own = append(own, k)
		}
	}
	w, err := packfile.NewWriter(f)
	if err != nil {
		return nil, err
	}
	p.Begin("packing", uint64(len(own)), Objects)
	// The pack's order is free, so the packstore is read in its own.
	objects.SortByLocation(own)
	for _, k := range own {
		// Reading and compressing a large reference takes minutes, and
		// nothing else on the way looks at the context.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := objects.Get(k)
		if err != nil {
			return nil, fmt.Errorf("object %s: %w", k, err)
		}
		if err := w.Add(k, data); err != nil {
			return nil, err
		}
		p.Advance(1)
	}
	return w.Finish()
}
