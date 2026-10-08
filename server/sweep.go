package server

import (
	"context"
	"errors"
	"time"
)

// sweepBatch is how many deletions Sweep reads from the queue at a time.
const sweepBatch = 256

// Sweep does one round of housekeeping: uploads past their deadline are
// forgotten and their objects queued for deletion, and the deletions that
// are due are carried out. A deletion the bucket did not confirm stays in
// the queue for the next round; Sweep reports those failures.
func (s *Server) Sweep(ctx context.Context) error {
	now := s.now()
	expired, err := s.db.ExpireUploads(ctx, now, now.Add(straggler), s.collectAt(now))
	if err != nil {
		return err
	}
	if expired > 0 {
		s.log.Info("uploads expired", "count", expired)
	}
	var errs []error
	for {
		due, err := s.db.DueDeletions(ctx, now, sweepBatch)
		if err != nil {
			return errors.Join(append(errs, err)...)
		}
		failed := 0
		for _, d := range due {
			if d.MultipartID != "" {
				err = s.bucket.AbortMultipart(ctx, d.ObjectKey, d.MultipartID)
			} else {
				err = s.bucket.Delete(ctx, d.ObjectKey)
			}
			if err == nil {
				err = s.db.DoneDeletion(ctx, d.ID)
			}
			if err != nil {
				failed++
				errs = append(errs, err)
			}
		}
		// A row that failed is due still, so another pass would only read
		// it again: go on while whole batches come and go.
		if len(due) < sweepBatch || failed > 0 {
			return errors.Join(errs...)
		}
	}
}

// RunSweeper calls Sweep now and then every interval until ctx is done.
func (s *Server) RunSweeper(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if err := s.Sweep(ctx); err != nil && ctx.Err() == nil {
			s.log.Warn("sweep", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
