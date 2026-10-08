package db

import (
	"context"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/db/dbq"
)

// Ref is a name pointing at the pack of a root.
type Ref struct {
	Name      string
	Root      key.Key
	PackID    int64
	UpdatedBy string
	UpdatedAt time.Time
}

// refRow is a row of refs with the root of its pack.
type refRow = dbq.RefByNameRow

func refOf(r refRow) (Ref, error) {
	root, err := keyOf(r.Root)
	if err != nil {
		return Ref{}, err
	}
	return Ref{
		Name:      r.Name,
		Root:      root,
		PackID:    r.PackID,
		UpdatedBy: r.UpdatedBy,
		UpdatedAt: timeOf(r.UpdatedAt),
	}, nil
}

// collect deletes the packs that are no longer live, pass after pass until a
// pass finds none, and queues their bucket keys for deletion not before
// deleteAt. Their rows in sketch_keys go with them.
func collect(ctx context.Context, q *dbq.Queries, deleteAt time.Time) error {
	for {
		dead, err := q.DeleteDeadPacks(ctx)
		if err != nil {
			return err
		}
		if len(dead) == 0 {
			return nil
		}
		for _, p := range dead {
			if err := queue(ctx, q, deleteAt, p.DataKey, p.IndexKey, p.LinksKey.String); err != nil {
				return err
			}
		}
	}
}

// queue queues the bucket keys that are not empty for deletion not before
// the given time.
func queue(ctx context.Context, q *dbq.Queries, notBefore time.Time, objectKeys ...string) error {
	for _, k := range objectKeys {
		if k == "" {
			continue
		}
		err := q.InsertDeletion(ctx, dbq.InsertDeletionParams{ObjectKey: k, NotBefore: notBefore.Unix()})
		if err != nil {
			return err
		}
	}
	return nil
}

func pointRef(ctx context.Context, q *dbq.Queries, name string, packID int64, by string, now time.Time) error {
	return q.UpsertRef(ctx, dbq.UpsertRefParams{
		Name:      name,
		PackID:    packID,
		UpdatedBy: by,
		UpdatedAt: now.Unix(),
	})
}

// PointRef points name at the pack of root and collects what that leaves
// without a reference; it returns false, having changed nothing, when root
// has no pack. Collected keys are queued for deletion not before deleteAt.
func (d *DB) PointRef(ctx context.Context, name string, root key.Key, by string, now, deleteAt time.Time) (bool, error) {
	found := false
	err := d.write(ctx, func(q *dbq.Queries) error {
		p, err := q.PackByRoot(ctx, root[:])
		if notFound(err) == ErrNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		if err := pointRef(ctx, q, name, p.ID, by, now); err != nil {
			return err
		}
		return collect(ctx, q, deleteAt)
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

// Ref returns the ref called name, or ErrNotFound.
func (d *DB) Ref(ctx context.Context, name string) (r Ref, err error) {
	err = d.read(ctx, func(q *dbq.Queries) error {
		row, err := q.RefByName(ctx, name)
		if err != nil {
			return notFound(err)
		}
		r, err = refOf(row)
		return err
	})
	return r, err
}

// ListRefs returns at most limit refs whose names begin with prefix and come
// after the name after, in the order of the names' bytes. Every character of
// the prefix stands for itself.
func (d *DB) ListRefs(ctx context.Context, prefix, after string, limit int) ([]Ref, error) {
	var out []Ref
	err := d.read(ctx, func(q *dbq.Queries) (err error) {
		out, err = listRefs(ctx, q, prefix, after, limit)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func listRefs(ctx context.Context, q *dbq.Queries, prefix, after string, limit int) ([]Ref, error) {
	var rows []refRow
	if upper, bounded := successor(prefix); bounded {
		between, err := q.ListRefsBetween(ctx, dbq.ListRefsBetweenParams{
			Lower: prefix,
			After: after,
			Upper: upper,
			N:     atMost(limit),
		})
		if err != nil {
			return nil, err
		}
		for _, r := range between {
			rows = append(rows, refRow(r))
		}
	} else {
		from, err := q.ListRefsFrom(ctx, dbq.ListRefsFromParams{Lower: prefix, After: after, N: atMost(limit)})
		if err != nil {
			return nil, err
		}
		for _, r := range from {
			rows = append(rows, refRow(r))
		}
	}
	out := make([]Ref, len(rows))
	for i, r := range rows {
		var err error
		if out[i], err = refOf(r); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// successor returns the lowest string above every string that begins with
// prefix: the names with the prefix are those from prefix up to it. There is
// none, and bounded is false, when the prefix is empty or all 0xff bytes;
// then every string from prefix on begins with it.
func successor(prefix string) (upper string, bounded bool) {
	b := []byte(prefix)
	for len(b) > 0 && b[len(b)-1] == 0xff {
		b = b[:len(b)-1]
	}
	if len(b) == 0 {
		return "", false
	}
	b[len(b)-1]++
	return string(b), true
}

// DeleteRef removes the ref called name and collects what that leaves
// without a reference. Collected keys are queued for deletion not before
// deleteAt. The error is ErrNotFound when there is no such ref.
func (d *DB) DeleteRef(ctx context.Context, name string, deleteAt time.Time) error {
	return d.write(ctx, func(q *dbq.Queries) error {
		n, err := q.DeleteRef(ctx, name)
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNotFound
		}
		return collect(ctx, q, deleteAt)
	})
}
