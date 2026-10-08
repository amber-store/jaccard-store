package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/db/dbq"
	"github.com/amber-store/jaccard-store/sketch"
)

// The states of an upload.
const (
	// StatePending is the state of an upload the server waits for: it may be
	// committed, and it expires at its deadline.
	StatePending = "pending"
	// StateVerifying is the state of an upload whose pack the server is
	// verifying. It does not expire.
	StateVerifying = "verifying"
)

// Upload is an open upload: a pack a client has announced and the server has
// not recorded yet. It owns its three bucket keys from the start, whether or
// not all of them get written.
type Upload struct {
	ID          string
	Name        string
	Root        key.Key
	ParentID    int64 // 0: none
	Uploader    string
	DataKey     string
	IndexKey    string
	LinksKey    string
	MultipartID string // "" for one PUT
	DataSize    int64
	Objects     int64
	// SharedObjects and SharedBytes are what the client says the ref has in
	// common with the parent: of the parent's objects, the ones the ref is
	// made of. A verification measures the same and does not look at these.
	SharedObjects int64
	SharedBytes   int64
	State         string
	IssuedAt      time.Time
	Deadline      time.Time
}

// Verified is what CommitUpload records of a pack beyond its upload.
type Verified struct {
	IndexSize, LinksSize       int64
	Objects, Bytes             int64
	SharedObjects, SharedBytes int64
	Sketch                     sketch.Sketch // base packs; nil for a patch pack
	// OnTrust says that the pack was not verified after all: its data was
	// not read, and the pack is recorded as one that is not verified. A
	// base pack on trust has no links.
	OnTrust bool
}

// Deletion is a queued delete in the bucket that is due.
type Deletion struct {
	ID          int64
	ObjectKey   string
	MultipartID string // set: abort this multipart upload of ObjectKey
}

func uploadOf(r dbq.Upload) (Upload, error) {
	root, err := keyOf(r.Root)
	if err != nil {
		return Upload{}, err
	}
	return Upload{
		ID:          r.ID,
		Name:        r.Name,
		Root:        root,
		ParentID:    r.ParentID.Int64,
		Uploader:    r.Uploader,
		DataKey:     r.DataKey,
		IndexKey:    r.IndexKey,
		LinksKey:    r.LinksKey,
		MultipartID: r.MultipartID.String,
		DataSize:    r.DataSize,
		Objects:     r.Objects,

		SharedObjects: r.SharedObjects,
		SharedBytes:   r.SharedBytes,
		State:         r.State,
		IssuedAt:      timeOf(r.IssuedAt),
		Deadline:      timeOf(r.Deadline),
	}, nil
}

// CreateUpload records u as a pending upload and returns it as recorded:
// whatever u.State and u.ParentID say, and with its times cut to the second.
// parent, when set, must be the root of a base pack, or the error is
// ErrParentGone; u.ParentID is filled from it, and is 0 without it. From
// here on the upload holds its parent live. A root that is not a canonical
// key is refused.
//
// What u says it shares with the parent is the client's word, and is held
// to what can be true of it: no more objects and no more bytes than the
// parent holds, and nothing without a parent. Anything else is ErrShared.
// The figures are added up with those the server measured, and one that is
// absurd would spoil the sum.
func (d *DB) CreateUpload(ctx context.Context, u Upload, parent *key.Key) (Upload, error) {
	if err := u.Root.Validate(); err != nil {
		return Upload{}, fmt.Errorf("db: root of upload %s: %w", u.ID, err)
	}
	u.ParentID = 0
	u.State = StatePending
	u.IssuedAt = timeOf(u.IssuedAt.Unix())
	u.Deadline = timeOf(u.Deadline.Unix())
	err := d.write(ctx, func(q *dbq.Queries) error {
		var holds, holdsBytes int64
		if parent != nil {
			p, err := q.PackByRoot(ctx, parent[:])
			if notFound(err) == ErrNotFound || (err == nil && p.ParentID.Valid) {
				return ErrParentGone
			}
			if err != nil {
				return err
			}
			u.ParentID = p.ID
			holds, holdsBytes = p.Objects, p.Bytes
		}
		if u.SharedObjects < 0 || u.SharedObjects > holds || u.SharedBytes < 0 || u.SharedBytes > holdsBytes {
			return ErrShared
		}
		return q.InsertUpload(ctx, dbq.InsertUploadParams{
			ID:          u.ID,
			Name:        u.Name,
			Root:        u.Root[:],
			ParentID:    nullInt(u.ParentID),
			Uploader:    u.Uploader,
			DataKey:     u.DataKey,
			IndexKey:    u.IndexKey,
			LinksKey:    u.LinksKey,
			MultipartID: nullString(u.MultipartID),
			DataSize:    u.DataSize,
			Objects:     u.Objects,

			SharedObjects: u.SharedObjects,
			SharedBytes:   u.SharedBytes,
			State:         u.State,
			IssuedAt:      u.IssuedAt.Unix(),
			Deadline:      u.Deadline.Unix(),
		})
	})
	if err != nil {
		return Upload{}, err
	}
	return u, nil
}

// Straggler is how long after an upload's deadline its bucket keys are
// deleted a second time. The URLs an upload was given stay valid until its
// deadline whatever becomes of the upload, and a PUT that began before the
// deadline can land after it: an object written after the first deletion
// would otherwise stay in the bucket with nothing left that knows of it.
const Straggler = time.Hour

// CommitUpload records the pack of an upload, points the upload's ref at it
// and forgets the upload. v is what the verification found, or, with
// v.OnTrust, what there is to say of a pack that was not verified. A base
// pack, which is one without a parent, comes with its sketch and, if it was
// verified, keeps the upload's links key; a patch pack comes without a
// sketch and has no links: nothing was written under the upload's links
// key, and nothing is recorded of it. Neither has a base pack on trust; its
// upload's links key is queued for deletion at now, in case an earlier
// attempt to commit it, by a server that verified, wrote links there.
//
// If the root has a pack by now, nothing is recorded and v is not looked
// at: the upload's keys and its multipart upload are queued for deletion
// and the ref is pointed at the pack there is. A caller that saw such a pack
// and verified nothing passes a nil v; should the pack be gone again by the
// time of the commit, the error is ErrUnverified, nothing has changed, and
// the caller verifies after all.
//
// CommitUpload returns the pack the ref points at, and collects what the
// ref left; collected keys are queued for deletion not before deleteAt. The
// error is ErrNotFound when there is no such upload.
func (d *DB) CommitUpload(ctx context.Context, id string, v *Verified, now, deleteAt time.Time) (p Pack, err error) {
	err = d.write(ctx, func(q *dbq.Queries) error {
		u, err := q.UploadByID(ctx, id)
		if err != nil {
			return notFound(err)
		}
		existing, err := q.PackByRoot(ctx, u.Root)
		var packID int64
		switch {
		case err == nil:
			packID = existing.ID
			if err := discard(ctx, q, u, now); err != nil {
				return err
			}
		case notFound(err) == ErrNotFound:
			if v == nil {
				return ErrUnverified
			}
			if packID, err = insertPack(ctx, q, u, *v, now); err != nil {
				return err
			}
			if err := q.DeleteUpload(ctx, u.ID); err != nil {
				return err
			}
			if v.OnTrust && !u.ParentID.Valid {
				// The pack keeps no links, but the upload may have some: a
				// server that verified can have written them and then
				// failed to commit, and the commit come again to one that
				// does not verify. No URL was ever signed for them.
				if err := queue(ctx, q, now, u.LinksKey); err != nil {
					return err
				}
			}
		default:
			return err
		}
		was, err := heldBy(ctx, q, u.Name)
		if err != nil {
			return err
		}
		if err := pointRef(ctx, q, u.Name, packID, u.Uploader, now); err != nil {
			return err
		}
		// The upload has let go of its parent; if the pack was recorded it
		// holds the parent in the upload's place.
		if err := collect(ctx, q, deleteAt, was, u.ParentID.Int64); err != nil {
			return err
		}
		p, err = packByID(ctx, q, packID)
		return err
	})
	return p, err
}

// insertPack records the pack that the upload u was verified to be, with its
// sketch if it is a base pack, and returns its ID.
func insertPack(ctx context.Context, q *dbq.Queries, u dbq.Upload, v Verified, now time.Time) (int64, error) {
	base := !u.ParentID.Valid
	if base != (v.Sketch != nil) {
		return 0, errors.New("db: a base pack needs a sketch, and a patch pack must not have one")
	}
	root, err := keyOf(u.Root)
	if err != nil {
		return 0, err
	}
	params := dbq.InsertPackParams{
		Unpacked:      unpackedOf(root),
		Root:          u.Root,
		ParentID:      u.ParentID,
		DataKey:       u.DataKey,
		IndexKey:      u.IndexKey,
		DataSize:      u.DataSize,
		IndexSize:     v.IndexSize,
		LinksSize:     v.LinksSize,
		Objects:       v.Objects,
		Bytes:         v.Bytes,
		SharedObjects: v.SharedObjects,
		SharedBytes:   v.SharedBytes,
		Uploader:      u.Uploader,
		UploadedAt:    now.Unix(),
	}
	sk := v.Sketch[:min(len(v.Sketch), sketch.Size)]
	if !v.OnTrust {
		params.Verified = 1
	}
	if base {
		params.Sketch = sketchBlob(sk)
		// Links come of a verification: without one, nothing was written
		// under the upload's links key.
		if !v.OnTrust {
			params.LinksKey = nullString(u.LinksKey)
		}
	}
	id, err := q.InsertPack(ctx, params)
	if err != nil {
		return 0, err
	}
	for _, k := range sk {
		if err := q.InsertSketchKey(ctx, dbq.InsertSketchKeyParams{Key: k[:], PackID: id}); err != nil {
			return 0, err
		}
	}
	return id, nil
}

// discard forgets the upload u and queues what it owns in the bucket for
// deletion at now, and once more Straggler after its deadline (or after now,
// if that is later), when no URL of the upload can write any more. The abort
// of its multipart upload is queued first: once the upload is aborted it
// cannot be completed any more, so the delete that follows it is the last
// word on the data.
func discard(ctx context.Context, q *dbq.Queries, u dbq.Upload, now time.Time) error {
	if err := q.DeleteUpload(ctx, u.ID); err != nil {
		return err
	}
	if u.MultipartID.Valid {
		err := q.InsertDeletion(ctx, dbq.InsertDeletionParams{
			ObjectKey:   u.DataKey,
			MultipartID: u.MultipartID,
			NotBefore:   now.Unix(),
		})
		if err != nil {
			return err
		}
	}
	if err := queue(ctx, q, now, u.DataKey, u.IndexKey, u.LinksKey); err != nil {
		return err
	}
	again := timeOf(u.Deadline)
	if now.After(again) {
		again = now
	}
	return queue(ctx, q, again.Add(Straggler), u.DataKey, u.IndexKey, u.LinksKey)
}

// DueDeletions returns at most limit queued deletions whose time has come at
// now, in the order they were queued.
func (d *DB) DueDeletions(ctx context.Context, now time.Time, limit int) ([]Deletion, error) {
	var out []Deletion
	err := d.read(ctx, func(q *dbq.Queries) error {
		rows, err := q.DueDeletions(ctx, dbq.DueDeletionsParams{Now: now.Unix(), N: atMost(limit)})
		if err != nil {
			return err
		}
		for _, r := range rows {
			out = append(out, Deletion{ID: r.ID, ObjectKey: r.ObjectKey, MultipartID: r.MultipartID.String})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// FailUpload forgets an upload, queues its three keys and its multipart
// upload for deletion at now and its keys once more Straggler after its
// deadline, and collects: the upload may have been the last hold on its
// parent. Collected keys are queued for deletion not before deleteAt. The
// error is ErrNotFound when there is no such upload.
func (d *DB) FailUpload(ctx context.Context, id string, now, deleteAt time.Time) error {
	return d.write(ctx, func(q *dbq.Queries) error {
		u, err := q.UploadByID(ctx, id)
		if err != nil {
			return notFound(err)
		}
		if err := discard(ctx, q, u, now); err != nil {
			return err
		}
		return collect(ctx, q, deleteAt, u.ParentID.Int64)
	})
}

// DoneDeletion removes a deletion from the queue: the bucket has confirmed
// it.
func (d *DB) DoneDeletion(ctx context.Context, id int64) error {
	return d.write(ctx, func(q *dbq.Queries) error {
		return q.DeleteDeletion(ctx, id)
	})
}

// UploadByID returns the open upload with the given ID, or ErrNotFound.
func (d *DB) UploadByID(ctx context.Context, id string) (u Upload, err error) {
	err = d.read(ctx, func(q *dbq.Queries) error {
		row, err := q.UploadByID(ctx, id)
		if err != nil {
			return notFound(err)
		}
		u, err = uploadOf(row)
		return err
	})
	return u, err
}

// ListUploads returns the open uploads, oldest first.
func (d *DB) ListUploads(ctx context.Context) ([]Upload, error) {
	var out []Upload
	err := d.read(ctx, func(q *dbq.Queries) error {
		rows, err := q.ListUploads(ctx)
		if err != nil {
			return err
		}
		out = make([]Upload, len(rows))
		for i, r := range rows {
			if out[i], err = uploadOf(r); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BeginVerify moves an upload from pending to verifying and returns it, so
// that it is verified once at a time and does not expire meanwhile. The
// error is ErrNotFound when there is no such upload or it is not
// uploader's, and ErrBusy when it is being verified already. The deadline is
// not looked at: whether an upload may still be committed is the caller's
// call.
func (d *DB) BeginVerify(ctx context.Context, id, uploader string) (u Upload, err error) {
	err = d.write(ctx, func(q *dbq.Queries) error {
		row, err := q.UploadByID(ctx, id)
		if err != nil {
			return notFound(err)
		}
		if row.Uploader != uploader {
			return ErrNotFound
		}
		if row.State == StateVerifying {
			return ErrBusy
		}
		if err := setState(ctx, q, id, StateVerifying); err != nil {
			return err
		}
		row.State = StateVerifying
		u, err = uploadOf(row)
		return err
	})
	return u, err
}

// EndVerify returns an upload to pending: the verification could not run,
// and the upload may be committed again. The error is ErrNotFound when
// there is no such upload.
func (d *DB) EndVerify(ctx context.Context, id string) error {
	return d.write(ctx, func(q *dbq.Queries) error {
		return setState(ctx, q, id, StatePending)
	})
}

func setState(ctx context.Context, q *dbq.Queries, id, state string) error {
	n, err := q.SetUploadState(ctx, dbq.SetUploadStateParams{ID: id, State: state})
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// ExpireUploads fails every pending upload whose deadline is before now, as
// FailUpload does. Uploads being verified are left alone. It returns how
// many uploads it failed.
func (d *DB) ExpireUploads(ctx context.Context, now, deleteAt time.Time) (int, error) {
	expired := 0
	err := d.write(ctx, func(q *dbq.Queries) error {
		rows, err := q.ExpiredUploads(ctx, now.Unix())
		if err != nil || len(rows) == 0 {
			return err
		}
		var parents []int64
		for _, u := range rows {
			if err := discard(ctx, q, u, now); err != nil {
				return err
			}
			parents = append(parents, u.ParentID.Int64)
		}
		expired = len(rows)
		return collect(ctx, q, deleteAt, parents...)
	})
	if err != nil {
		return 0, err
	}
	return expired, nil
}

// ResetVerifying returns every upload that is being verified to pending. A
// server does this when it starts: no verification outlives the process
// that ran it.
func (d *DB) ResetVerifying(ctx context.Context) error {
	return d.write(ctx, func(q *dbq.Queries) error {
		return q.ResetVerifying(ctx)
	})
}
