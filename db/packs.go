package db

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/db/dbq"
	"github.com/amber-store/jaccard-store/keyset"
	"github.com/amber-store/jaccard-store/sketch"
)

// Pack is a verified pack: the content of one root, as the server measured
// it.
type Pack struct {
	ID            int64
	Root          key.Key
	ParentID      int64 // 0: a base pack
	DataKey       string
	IndexKey      string
	LinksKey      string // "" for a patch pack
	DataSize      int64
	IndexSize     int64
	LinksSize     int64
	Objects       int64
	Bytes         int64
	SharedObjects int64
	SharedBytes   int64
	Uploader      string
	UploadedAt    time.Time
}

// IsBase reports whether p is a base pack: it has no parent and holds the
// whole key set of its root.
func (p Pack) IsBase() bool {
	return p.ParentID == 0
}

// Candidate is a base pack a new reference could lean on, with the Jaccard
// similarity their sketches estimate.
type Candidate struct {
	Pack       Pack
	Similarity float64
}

// packRow is a row of packs without its sketch. The queries that select
// those columns have row types of their own, which convert to this one.
type packRow = dbq.PackByIDRow

func packOf(r packRow) (Pack, error) {
	root, err := keyOf(r.Root)
	if err != nil {
		return Pack{}, err
	}
	return Pack{
		ID:            r.ID,
		Root:          root,
		ParentID:      r.ParentID.Int64,
		DataKey:       r.DataKey,
		IndexKey:      r.IndexKey,
		LinksKey:      r.LinksKey.String,
		DataSize:      r.DataSize,
		IndexSize:     r.IndexSize,
		LinksSize:     r.LinksSize,
		Objects:       r.Objects,
		Bytes:         r.Bytes,
		SharedObjects: r.SharedObjects,
		SharedBytes:   r.SharedBytes,
		Uploader:      r.Uploader,
		UploadedAt:    timeOf(r.UploadedAt),
	}, nil
}

func packByID(ctx context.Context, q *dbq.Queries, id int64) (Pack, error) {
	row, err := q.PackByID(ctx, id)
	if err != nil {
		return Pack{}, notFound(err)
	}
	return packOf(row)
}

func packByRoot(ctx context.Context, q *dbq.Queries, root key.Key) (Pack, error) {
	row, err := q.PackByRoot(ctx, root[:])
	if err != nil {
		return Pack{}, notFound(err)
	}
	return packOf(packRow(row))
}

// PackByRoot returns the pack of root, or ErrNotFound.
func (d *DB) PackByRoot(ctx context.Context, root key.Key) (p Pack, err error) {
	err = d.read(ctx, func(q *dbq.Queries) error {
		p, err = packByRoot(ctx, q, root)
		return err
	})
	return p, err
}

// PackByID returns the pack with the given ID, or ErrNotFound.
func (d *DB) PackByID(ctx context.Context, id int64) (p Pack, err error) {
	err = d.read(ctx, func(q *dbq.Queries) error {
		p, err = packByID(ctx, q, id)
		return err
	})
	return p, err
}

// Nearest returns at most n base packs whose key sets are similar to the set
// sk was taken from, nearest first, each with the similarity the two
// sketches estimate.
//
// A pack whose estimate is above zero shares a key with sk, so sketch_keys
// finds every one of them. The 16 packs sharing the most keys are scored with
// sketch.Jaccard, and the best of those scoring above zero are returned. Ties go to the
// pack sharing more keys, then to the lower root.
func (d *DB) Nearest(ctx context.Context, sk sketch.Sketch, n int) ([]Candidate, error) {
	sk = sk[:min(len(sk), sketch.Size)]
	if len(sk) == 0 || n <= 0 {
		return nil, nil
	}
	keys := make([][]byte, len(sk))
	for i := range sk {
		keys[i] = sk[i][:]
	}
	type score struct {
		id         int64
		root       key.Key
		shared     int64
		similarity float64
	}
	var out []Candidate
	err := d.read(ctx, func(q *dbq.Queries) error {
		rows, err := q.SharingPacks(ctx, keys)
		if err != nil {
			return err
		}
		var scores []score
		for _, r := range rows {
			root, err := keyOf(r.Root)
			if err != nil {
				return err
			}
			other, err := sketchOfBlob(r.Sketch)
			if err != nil {
				return fmt.Errorf("db: sketch of pack %s: %w", root, err)
			}
			if j := sketch.Jaccard(sk, other); j > 0 {
				scores = append(scores, score{id: r.ID, root: root, shared: r.Shared, similarity: j})
			}
		}
		slices.SortFunc(scores, func(a, b score) int {
			switch {
			case a.similarity != b.similarity:
				if a.similarity > b.similarity {
					return -1
				}
				return 1
			case a.shared != b.shared:
				if a.shared > b.shared {
					return -1
				}
				return 1
			}
			return keyset.Compare(a.root, b.root)
		})
		for _, s := range scores[:min(len(scores), n)] {
			p, err := packByID(ctx, q, s.id)
			if err != nil {
				return err
			}
			out = append(out, Candidate{Pack: p, Similarity: s.similarity})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// sketchBlob is a sketch as packs.sketch holds it: its keys back to back.
func sketchBlob(sk sketch.Sketch) []byte {
	b := make([]byte, 0, len(sk)*key.Size)
	for _, k := range sk {
		b = append(b, k[:]...)
	}
	return b
}

func sketchOfBlob(b []byte) (sketch.Sketch, error) {
	if len(b)%key.Size != 0 {
		return nil, fmt.Errorf("%d bytes are not a whole number of keys", len(b))
	}
	sk := make(sketch.Sketch, len(b)/key.Size)
	for i := range sk {
		copy(sk[i][:], b[i*key.Size:])
	}
	return sk, nil
}
