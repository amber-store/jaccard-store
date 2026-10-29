package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"math"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/packfile"
	"github.com/amber-store/jaccard-store/verify"
	"github.com/amber-store/jaccard-store/wire"
)

// Pack is one pack of a reference as the server named it in a lookup.
// Its URLs expire: fetch it soon after the lookup.
type Pack struct {
	Root      key.Key
	Objects   uint64 // objects in the pack
	Bytes     uint64 // their uncompressed size
	DataSize  uint64 // size of the compressed data
	IndexSize uint64

	// Pre-signed GETs for the pack's two objects.
	indexURL, dataURL string
}

// announced is p in the server's words.
func (p Pack) announced() wire.Pack {
	return wire.Pack{
		Root:      p.Root[:],
		Objects:   p.Objects,
		Bytes:     p.Bytes,
		DataSize:  p.DataSize,
		IndexSize: p.IndexSize,
		IndexURL:  p.indexURL,
		DataURL:   p.dataURL,
	}
}

// ErrBadPack wraps every error of a fetch that is the pack's fault, not
// the transfer's: what the server announced of the pack cannot be, the
// index or the data in the bucket is not what was announced, or an object
// is not what its key names. Asking again brings the same pack.
var ErrBadPack = errors.New("client: bad pack")

// badPack is an error that is the pack's fault. It reads as the error it
// holds, so that a pull words its failures as it did before a fetch told
// them apart, and it is ErrBadPack as well.
type badPack struct{ err error }

func (e *badPack) Error() string   { return e.err.Error() }
func (e *badPack) Unwrap() []error { return []error{ErrBadPack, e.err} }

// bad marks err as the pack's fault.
func bad(err error) error { return &badPack{err: err} }

// StatusCode returns the HTTP status of the bucket's answer that err
// reports, or 0 when err is not one. A URL that has expired is answered
// 403: a lookup gives new ones.
func StatusCode(err error) int {
	var se *statusError
	if errors.As(err, &se) {
		return se.status
	}
	return 0
}

// Lookup returns the root name points at and the packs that hold it: the
// reference's own pack and, for a patch pack, its parent, in that order.
// It fetches nothing. An unknown name is ErrNotFound.
func (c *Client) Lookup(ctx context.Context, name string) (key.Key, []Pack, error) {
	root, named, err := c.lookup(ctx, name)
	if err != nil {
		return key.Key{}, nil, err
	}
	packs := make([]Pack, len(named))
	for i, p := range named {
		packRoot, err := key.Parse(p.Root)
		if err != nil {
			return key.Key{}, nil, fmt.Errorf("pull: the root of pack %d of %q: %w", i+1, name, err)
		}
		packs[i] = Pack{
			Root:      packRoot,
			Objects:   p.Objects,
			Bytes:     p.Bytes,
			DataSize:  p.DataSize,
			IndexSize: p.IndexSize,
			indexURL:  p.IndexURL,
			dataURL:   p.DataURL,
		}
	}
	return root, packs, nil
}

// lookup asks the server for the reference name and returns its root and
// the packs the server names for it, one or two, as they came.
func (c *Client) lookup(ctx context.Context, name string) (key.Key, []wire.Pack, error) {
	resp, err := c.call(ctx, wire.Request{Op: wire.OpPull, Name: name})
	if refused(err, wire.CodeNotFound) {
		return key.Key{}, nil, fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	if err != nil {
		return key.Key{}, nil, err
	}
	root, err := key.Parse(resp.Root)
	if err != nil {
		return key.Key{}, nil, fmt.Errorf("pull: the server's root: %w", err)
	}
	// A reference is read from its pack and that pack's parent, nothing
	// else: more packs than that are not the answer to a pull.
	if len(resp.Packs) == 0 || len(resp.Packs) > 2 {
		return key.Key{}, nil, fmt.Errorf("pull: the server named %d packs for %q", len(resp.Packs), name)
	}
	return root, resp.Packs, nil
}

// Fetch downloads p. It holds the index to what the server announced,
// then hands each object to each, in offset order, after checking it
// against its key. Objects handed over back to back are the pack's
// uncompressed data at the index's offsets. Fetch returns the index.
//
// An index or object that is wrong fails with an error wrapping
// ErrBadPack; a failure of the transfer or of each is returned as it is.
// The transfers go through the client's http.Client, which is where a
// limit on a transfer that stalls is set. No error names the pack: the
// caller knows which one it asked for.
func (c *Client) Fetch(ctx context.Context, p Pack, each func(o packfile.Object) error) (*packfile.Index, error) {
	pack := p.announced()
	if err := checkAnnounced(pack); err != nil {
		return nil, err
	}
	index, err := c.index(ctx, pack, silent{})
	if err != nil {
		return nil, err
	}
	for o, err := range c.objects(ctx, pack, index, silent{}) {
		if err != nil {
			return nil, err
		}
		// Nothing leaves here that is not what its key names.
		if err := verify.Object(o.Key, o.Data); err != nil {
			return nil, bad(err)
		}
		if err := each(o); err != nil {
			return nil, err
		}
	}
	return index, nil
}

// checkAnnounced refuses what a server cannot truthfully say of a pack,
// before anything is asked of the bucket.
func checkAnnounced(pack wire.Pack) error {
	if pack.Objects > packfile.MaxEntries {
		return bad(fmt.Errorf("the server announced %d objects, more than a pack holds", pack.Objects))
	}
	if pack.IndexSize != packfile.IndexSize(pack.Objects) {
		return bad(fmt.Errorf("the server announced an index of %d bytes for %d objects", pack.IndexSize, pack.Objects))
	}
	return nil
}

// dataLimit is how much of the pack's data is read at most, but for the
// one byte that gives a longer body away.
func dataLimit(pack wire.Pack) uint64 {
	return min(pack.DataSize, math.MaxInt64-1)
}

// index downloads the index of a pack that passed checkAnnounced and holds
// it to what the server said of the pack. The bytes are reported to p as
// they arrive.
func (c *Client) index(ctx context.Context, pack wire.Pack, p Progress) (*packfile.Index, error) {
	body, err := c.get(ctx, "index", pack.IndexURL)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	raw, err := io.ReadAll(io.LimitReader(&meter{r: body, p: p}, int64(pack.IndexSize)+1))
	if err != nil {
		return nil, fmt.Errorf("index: %w", err)
	}
	if uint64(len(raw)) != pack.IndexSize {
		return nil, bad(fmt.Errorf("index: the object is not the %d bytes the server announced", pack.IndexSize))
	}
	index, err := packfile.ParseIndex(raw)
	if err != nil {
		return nil, bad(fmt.Errorf("index: %w", err))
	}
	// The index decides how much a caller writes, so it is held to what
	// the server said of the pack before any of it is.
	if uint64(index.Len()) != pack.Objects || index.DataSize() != pack.Bytes {
		return nil, bad(fmt.Errorf("index: %d objects of %d bytes, the server announced %d of %d",
			index.Len(), index.DataSize(), pack.Objects, pack.Bytes))
	}
	return index, nil
}

// objects downloads the data of a pack and yields its objects in offset
// order, as packfile.Objects does and as unchecked against their keys. No
// more of the data is read than the server said there is. The compressed
// bytes are reported to p as they arrive. An error ends the sequence: one
// that wraps ErrBadPack when the data is not what index describes.
func (c *Client) objects(ctx context.Context, pack wire.Pack, index *packfile.Index, p Progress) iter.Seq2[packfile.Object, error] {
	return func(yield func(packfile.Object, error) bool) {
		body, err := c.get(ctx, "data", pack.DataURL)
		if err != nil {
			yield(packfile.Object{}, err)
			return
		}
		defer body.Close()
		data := io.LimitReader(&meter{r: body, p: p}, int64(dataLimit(pack))+1)
		for o, err := range packfile.Objects(index, data) {
			if err != nil {
				err = fmt.Errorf("data: %w", orCause(ctx, err))
				if errors.Is(err, packfile.ErrMalformed) {
					err = bad(err)
				}
				yield(packfile.Object{}, err)
				return
			}
			if !yield(o, nil) {
				return
			}
		}
	}
}
