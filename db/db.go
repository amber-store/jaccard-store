// Package db is the server's state, one SQLite database: the packs it has
// verified, the sketches of the base packs among them, the refs that point at
// packs, the uploads that are open, and the queue every delete in the bucket
// goes through.
//
// A pack is live while a ref points at it, a pack names it as parent, or an
// open upload names it as parent. Whatever takes such a hold away (a ref that
// moves or is deleted, an upload that ends) deletes, in the same transaction,
// the pack it was taken from if nothing else holds it, and after a patch pack
// its base in turn. Only those packs are looked at: nothing else can have
// died, so collecting costs the same however many packs there are. The
// bucket keys of every pack
// deleted are queued in deletions in that transaction too, as are the keys of
// an upload that ends without a pack, so a crash leaves nothing in the bucket
// that the database does not know of.
//
// Every method is one transaction. Times are stored as unix seconds and
// returned in UTC.
package db

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/db/dbq"
	_ "modernc.org/sqlite" // the pure-Go SQLite driver, registered as "sqlite"
)

var (
	// ErrNotFound is returned when the pack, ref or upload asked for is not
	// in the database.
	ErrNotFound = errors.New("db: not found")
	// ErrBusy is returned by BeginVerify for an upload that is being
	// verified already.
	ErrBusy = errors.New("db: upload is being verified")
	// ErrUnverified is returned by CommitUpload when it is given nothing
	// that was verified and the upload's root has no pack to fall back on.
	ErrUnverified = errors.New("db: the upload was not verified")
	// ErrParentGone is returned by CreateUpload when the parent it names is
	// not the root of a base pack.
	ErrParentGone = errors.New("db: parent is not a base pack")
)

// maxReaders is the number of connections the reads share. Writes have one
// connection of their own.
const maxReaders = 4

// busyTimeout bounds how long a connection waits for a lock another
// connection holds before it fails with SQLite's busy error.
const busyTimeout = 30 * time.Second

//go:embed migrations/*.sql
var embedded embed.FS

// DB is an open database. Its methods may be called from several goroutines
// at once.
type DB struct {
	// writer is a pool of one connection, so write transactions queue in Go
	// and never meet SQLite's lock. They begin IMMEDIATE: a write never has
	// to upgrade a read lock, which SQLite refuses with a busy error that
	// waiting does not cure.
	writer *sql.DB
	// readers serve the read-only transactions, which in WAL mode neither
	// wait for the writer nor hold it up.
	readers *sql.DB
}

// Open opens the database at path, creating it if it is missing, in WAL mode
// with foreign keys enforced, and brings its schema up to date.
func Open(path string) (*DB, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	writer, err := openPool(dataSourceName(abs, "_txlock=immediate"), 1)
	if err != nil {
		return nil, err
	}
	if err := prepare(context.Background(), writer); err != nil {
		writer.Close()
		return nil, fmt.Errorf("db: open %s: %w", path, err)
	}
	readers, err := openPool(dataSourceName(abs, "_pragma=query_only(1)"), maxReaders)
	if err != nil {
		writer.Close()
		return nil, err
	}
	return &DB{writer: writer, readers: readers}, nil
}

// Close closes the database. The last connection to go writes the log back
// into the database file.
func (d *DB) Close() error {
	return errors.Join(d.readers.Close(), d.writer.Close())
}

// dataSourceName names the database at the absolute path to the driver. The
// pragmas apply to every connection. It is a file: URI, so that '?', '#' and
// '%' in the path are escaped.
func dataSourceName(abs string, extra ...string) string {
	params := append([]string{
		fmt.Sprintf("_pragma=busy_timeout(%d)", busyTimeout.Milliseconds()),
		"_pragma=journal_mode(WAL)",
		"_pragma=synchronous(FULL)",
		"_pragma=foreign_keys(1)",
	}, extra...)
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}
	return u.String() + "?" + strings.Join(params, "&")
}

func openPool(dsn string, size int) (*sql.DB, error) {
	pool, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	pool.SetMaxOpenConns(size)
	pool.SetMaxIdleConns(size)
	return pool, nil
}

// prepare checks the pragmas SQLite accepts silently and then ignores (a
// filesystem without shared memory leaves the journal mode as it was) and
// applies the migrations that are missing.
func prepare(ctx context.Context, pool *sql.DB) error {
	var mode string
	if err := pool.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		return fmt.Errorf("read journal_mode: %w", err)
	}
	if !strings.EqualFold(mode, "wal") {
		return fmt.Errorf("journal_mode is %q, want wal", mode)
	}
	var foreignKeys int
	if err := pool.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		return fmt.Errorf("read foreign_keys: %w", err)
	}
	if foreignKeys != 1 {
		return fmt.Errorf("foreign_keys is %d, want 1", foreignKeys)
	}
	return migrate(ctx, pool)
}

// migrate applies, in one transaction, the migrations the database has not
// seen. They are the files of migrations/ in the order of their names;
// PRAGMA user_version records how many have been applied.
func migrate(ctx context.Context, pool *sql.DB) error {
	names, err := fs.Glob(embedded, "migrations/*.sql")
	if err != nil {
		return err
	}
	slices.Sort(names)
	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("read user_version: %w", err)
	}
	if version < 0 || version > len(names) {
		return fmt.Errorf("schema version %d is not one this release knows (it has %d)", version, len(names))
	}
	if version == len(names) {
		return nil
	}
	for _, name := range names[version:] {
		body, err := fs.ReadFile(embedded, name)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", len(names))); err != nil {
		return fmt.Errorf("set user_version: %w", err)
	}
	return tx.Commit()
}

// write runs fn in a write transaction, which is committed if fn returns nil
// and rolled back otherwise.
func (d *DB) write(ctx context.Context, fn func(q *dbq.Queries) error) error {
	return inTx(ctx, d.writer, nil, fn)
}

// read runs fn in a read-only transaction: every query of fn sees the
// database as it was at the first.
func (d *DB) read(ctx context.Context, fn func(q *dbq.Queries) error) error {
	return inTx(ctx, d.readers, &sql.TxOptions{ReadOnly: true}, fn)
}

func inTx(ctx context.Context, pool *sql.DB, opts *sql.TxOptions, fn func(q *dbq.Queries) error) error {
	tx, err := pool.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(dbq.New(tx)); err != nil {
		return err
	}
	return tx.Commit()
}

// notFound turns the absence of a row into ErrNotFound.
func notFound(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

// keyOf is the key a column holds.
func keyOf(b []byte) (key.Key, error) {
	k, err := key.Parse(b)
	if err != nil {
		return key.Key{}, fmt.Errorf("db: stored key: %w", err)
	}
	return k, nil
}

func timeOf(seconds int64) time.Time {
	return time.Unix(seconds, 0).UTC()
}

func nullInt(v int64) sql.NullInt64 {
	return sql.NullInt64{Int64: v, Valid: v != 0}
}

func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

// atMost is limit as SQLite's LIMIT takes it, where a negative number means
// no limit at all.
func atMost(limit int) int64 {
	return int64(max(limit, 0))
}
