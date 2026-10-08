# jaccard-store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** A server over iroh that keeps references as base and patch packs in an S3 bucket, with a client library, a CLI and an embedded admin page.

**Architecture:** The server coordinates and never carries pack bytes: clients move index and data between themselves and S3 through pre-signed URLs, and the server verifies an upload by downloading it and walking it from the root. Server state is one SQLite database; every S3 delete goes through a queue in it.

**Tech Stack:** Go 1.26, `github.com/amber-store/core` v0.10.0, `github.com/tmc/go-iroh` v0.3.0, AWS SDK for Go v2, modernc SQLite with sqlc, klauspost zstd, fxamacker CBOR, `urfave/cli/v2`, plain HTML/JS for the admin page.

**Spec:** `docs/superpowers/specs/2026-10-08-jaccard-store-design.md`. Read it before any task: this plan pins names and order, the spec says what the software does.

## Global Constraints

- No `internal/` packages. Every package is at the top level of the module; sqlc's output is `db/dbq`.
- Objects and keys are core's (`github.com/amber-store/core/key`); nothing imports rebma.
- Every integer in the index, the links and the frame length is big-endian.
- The server reaches S3 only through the AWS SDK for Go v2; the client only through `net/http`. No minio-go.
- Statistics are computed by the server. No request carries a figure the server stores as a statistic.
- Access is open: no request is refused for who sent it, except `push-commit` of somebody else's upload.
- Every option of both commands is a flag and an environment variable (spec 7.5, 9.3); the flag wins.
- Run Go through the flake: `nix develop -c go test ./...`. Do not run `go get` or `go mod tidy` while `tools/deps.go` exists; every dependency is already in `go.mod`.
- Built binaries are removed after use. Nothing is committed that `go build` produced.
- Tests come first: write the test, see it fail, then write the code.
- Comment density and naming follow the experiment's code (`../jaccard-store-experiment`): a package comment that explains the format or the rule, a doc comment on every exported name, no narration inside functions.

## Review Focus

1. **Hostile index and data.** Access is open, so an index may declare 2^60 entries, offsets that overflow when added to lengths, or a data stream that expands far past what the index says. Expected: refused as malformed before memory or disk is spent on it. Tests: `packfile` (Task 1).
2. **An upload abandoned halfway, single or multipart, and a deadline that passes during verification.** Expected: parts and objects leave the bucket after the deadline; an upload being verified is not expired under the verifier. Tests: `db` (Task 4), `e2e` (Task 11).
3. **A server restart with uploads in `verifying` and deletions still queued.** Expected: uploads return to `pending`, the queue drains after reopen. Tests: `db` (Task 4), `server` (Task 7).
4. **`%`, `_` and `\` in ref names and list prefixes.** Expected: matched literally, never as SQL wildcards. Tests: `db` (Task 4).
5. **The smallest references.** A ref of one object, a ref whose root already sits in a base pack (empty patch pack), a ref pushed twice. Expected: all round-trip. Tests: `verify` (Task 2), `e2e` (Task 11).

---

## File Structure

```
keyset/keyset.go            Compare, Normalize, Ascending                       (done)
sketch/sketch.go            Size, Sketch, Of, Jaccard                           (done)
packfile/index.go           Entry, Index: parse, validate, encode, find
packfile/writer.go          Writer: one zstd stream and the index it implies
packfile/reader.go          Objects (stream in offset order), Expand (to a file)
packfile/links.go           Links: build, encode, parse
verify/object.go            Object: bytes against a key, by core's rule
verify/verify.go            Pack: the walk, then the measuring
wire/wire.go                ALPN, frames, Request, Response, error codes
db/migrations/0001_init.sql the schema of spec 7.1 (uploads gets links_key)
db/queries/*.sql            sqlc queries
db/dbq/                     sqlc output, committed
db/db.go                    Open, Close, migrations, the transaction helper
db/packs.go                 packs, Nearest
db/refs.go                  refs, collection
db/uploads.go               uploads, deletions
db/stats.go                 what the admin page reads
bucket/bucket.go            object calls, pre-signing, multipart
node/key.go                 the key file
node/serve.go               Bind: endpoint, addresses, pkarr, mDNS
node/dial.go                Dial: resolve by ID, connect
server/server.go            Config, New, Serve, Handle
server/push.go              push-start, push-upload, push-commit
server/pull.go              pull, list, delete
server/verify.go            download, scratch file, verify.Pack, links upload
server/sweep.go             Sweep, RunSweeper
admin/admin.go              Handler: the JSON API and the embedded page
admin/web/index.html, app.js, style.css
client/client.go            Client, call, List, Delete
client/push.go              Push
client/pull.go              Pull
client/http.go              GET, PUT, the multipart upload
cmd/jaccard-stored/main.go
cmd/jaccard-store/main.go, store.go
e2e/e2e_test.go             both sides in one process
```

## Interfaces

Every task builds against these names. A change to one of them is a change to the plan.

### packfile

```go
const (
	IndexMagic      = "JACIDX\x00\x01"
	LinksMagic      = "JACLNK\x00\x01"
	IndexHeaderSize = 16
	EntrySize       = 44
	MaxEntries      = 1 << 24
	MaxWindow       = 64 << 20
)

var ErrMalformed = errors.New("packfile: malformed pack")

type Entry struct {
	Key    key.Key
	Offset uint64
	Length uint32
}

type Object struct {
	Key  key.Key
	Data []byte
}

// IndexSize returns the size in bytes of an index of n entries.
func IndexSize(n uint64) uint64

// NewIndex builds an index from entries in any order. It fails, wrapping
// ErrMalformed, for what spec 4.1 forbids.
func NewIndex(entries []Entry) (*Index, error)
// ParseIndex parses and validates an encoded index. Every error wraps ErrMalformed.
func ParseIndex(b []byte) (*Index, error)
func (x *Index) Encode() []byte
func (x *Index) Len() int
func (x *Index) Entry(i int) Entry          // i-th in key order
func (x *Index) Find(k key.Key) (int, bool)
func (x *Index) Has(k key.Key) bool
func (x *Index) DataSize() uint64           // uncompressed bytes: the sum of the lengths
func (x *Index) ByOffset() []int            // positions, ordered by offset

// NewWriter starts a pack whose compressed data goes to data.
func NewWriter(data io.Writer) (*Writer, error)
func (w *Writer) Add(k key.Key, object []byte) error
func (w *Writer) Len() int
func (w *Writer) Bytes() uint64             // uncompressed bytes added
// Finish ends the zstd stream (data itself is not closed) and returns the index.
func (w *Writer) Finish() (*Index, error)

// Objects decompresses data and yields the pack's objects in offset order.
// A stream shorter or longer than x says ends the sequence with an error
// wrapping ErrMalformed. The yielded Data is the caller's.
func Objects(x *Index, data io.Reader) iter.Seq2[Object, error]
// Expand decompresses data into w and fails, wrapping ErrMalformed, unless
// it is exactly x.DataSize() bytes. It never writes more than that.
func Expand(x *Index, data io.Reader, w io.Writer) error

// NewLinks builds links from the children of each index position. It
// sorts each list and drops repeats.
func NewLinks(children [][]uint32) *Links
// ParseLinks parses links for an index of n entries; errors wrap ErrMalformed.
func ParseLinks(b []byte, n int) (*Links, error)
func (l *Links) Encode() []byte
func (l *Links) Len() int
func (l *Links) Children(i int) []uint32
```

### verify

```go
var ErrMalformed = errors.New("verify: malformed pack")

// Result is what a verified pack measures.
type Result struct {
	Objects, Bytes             uint64          // of the pack, from its index
	SharedObjects, SharedBytes uint64          // of the ref, held by the parent
	Links                      *packfile.Links // base packs; nil for a patch pack
}

// Object checks data against k by core's rule: the BLAKE3 hash, the length
// field of a Blob and an XattrSet, the footprint of a Commit.
func Object(k key.Key, data []byte) error

// Pack verifies a pack as spec 6 says, steps 4 to 6 and the measuring. data
// is the uncompressed stream. parent and parentLinks are nil for a base
// pack and both set for a patch pack. A pack that fails is reported with
// an error wrapping ErrMalformed; any other error is a failure to read.
func Pack(root key.Key, x *packfile.Index, data io.ReaderAt, parent *packfile.Index, parentLinks *packfile.Links) (Result, error)
```

### wire

```go
const (
	ALPN     = "amber/jaccard-store/1"
	MaxFrame = 16 << 20
)

type Op string

const (
	OpPushStart  Op = "push-start"
	OpPushUpload Op = "push-upload"
	OpPushCommit Op = "push-commit"
	OpPull       Op = "pull"
	OpList       Op = "list"
	OpDelete     Op = "delete"
)

const (
	CodeBadRequest    = "bad_request"
	CodeNotFound      = "not_found"
	CodeParentGone    = "parent_gone"
	CodeUnknownUpload = "unknown_upload"
	CodeMalformedPack = "malformed_pack"
	CodeInternal      = "internal"
)

type Request struct {
	Op       Op       `cbor:"op"`
	Name     string   `cbor:"name,omitempty"`
	Root     []byte   `cbor:"root,omitempty"`
	Sketch   [][]byte `cbor:"sketch,omitempty"`
	Parent   []byte   `cbor:"parent,omitempty"`
	DataSize uint64   `cbor:"data_size,omitempty"`
	Objects  uint64   `cbor:"objects,omitempty"`
	UploadID string   `cbor:"upload_id,omitempty"`
	Prefix   string   `cbor:"prefix,omitempty"`
	After    string   `cbor:"after,omitempty"`
	Limit    int      `cbor:"limit,omitempty"`
}

type Response struct {
	Error      *Error      `cbor:"error,omitempty"`
	Stored     bool        `cbor:"stored,omitempty"`
	Candidates []Candidate `cbor:"candidates,omitempty"`
	UploadID   string      `cbor:"upload_id,omitempty"`
	Deadline   int64       `cbor:"deadline,omitempty"` // unix seconds
	IndexURL   string      `cbor:"index_url,omitempty"`
	DataURL    string      `cbor:"data_url,omitempty"`
	Parts      *Parts      `cbor:"parts,omitempty"`
	Root       []byte      `cbor:"root,omitempty"`
	Packs      []Pack      `cbor:"packs,omitempty"`
	Refs       []Ref       `cbor:"refs,omitempty"`
	More       bool        `cbor:"more,omitempty"`
}

type Error struct {
	Code    string `cbor:"code"`
	Message string `cbor:"message"`
}
func (e *Error) Error() string

type Candidate struct {
	Root     []byte  `cbor:"root"`
	Distance float64 `cbor:"distance"`
	Objects  uint64  `cbor:"objects"`
	Bytes    uint64  `cbor:"bytes"`
	DataSize uint64  `cbor:"data_size"`
	IndexURL string  `cbor:"index_url"`
}

type Parts struct {
	PartSize    uint64   `cbor:"part_size"`
	URLs        []string `cbor:"urls"`
	CompleteURL string   `cbor:"complete_url"`
}

type Pack struct {
	Root      []byte `cbor:"root"`
	Objects   uint64 `cbor:"objects"`
	Bytes     uint64 `cbor:"bytes"`
	DataSize  uint64 `cbor:"data_size"`
	IndexSize uint64 `cbor:"index_size"`
	IndexURL  string `cbor:"index_url"`
	DataURL   string `cbor:"data_url"`
}

type Ref struct {
	Name string `cbor:"name"`
	Root []byte `cbor:"root"`
}

// WriteFrame writes v as one frame: a 4-byte big-endian length and CBOR.
func WriteFrame(w io.Writer, v any) error
// ReadFrame reads one frame into v. A frame above MaxFrame is refused.
func ReadFrame(r io.Reader, v any) error
```

### db

```go
var (
	ErrNotFound   = errors.New("db: not found")
	ErrBusy       = errors.New("db: upload is being verified")
	ErrParentGone = errors.New("db: parent is not a base pack")
)

const (
	StatePending   = "pending"
	StateVerifying = "verifying"
)

type Pack struct {
	ID            int64
	Root          key.Key
	ParentID      int64  // 0: a base pack
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
func (p Pack) IsBase() bool

type Candidate struct {
	Pack       Pack
	Similarity float64
}

type Ref struct {
	Name      string
	Root      key.Key
	PackID    int64
	UpdatedBy string
	UpdatedAt time.Time
}

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
	State       string
	IssuedAt    time.Time
	Deadline    time.Time
}

// Verified is what CommitUpload records of a pack beyond its upload.
type Verified struct {
	IndexSize, LinksSize       int64
	Objects, Bytes             int64
	SharedObjects, SharedBytes int64
	Sketch                     sketch.Sketch // base packs; nil for a patch pack
}

type Deletion struct {
	ID          int64
	ObjectKey   string
	MultipartID string // set: abort this multipart upload of ObjectKey
}

func Open(path string) (*DB, error) // WAL, foreign keys on, migrations applied
func (d *DB) Close() error

func (d *DB) PackByRoot(ctx context.Context, root key.Key) (Pack, error) // ErrNotFound
func (d *DB) PackByID(ctx context.Context, id int64) (Pack, error)       // ErrNotFound
// Nearest returns at most n base packs, nearest first, as spec 5.1 says.
func (d *DB) Nearest(ctx context.Context, sk sketch.Sketch, n int) ([]Candidate, error)

// PointRef points name at the pack of root and collects what that leaves
// without a reference; false when root has no pack. Collected keys are
// queued for deletion not before deleteAt.
func (d *DB) PointRef(ctx context.Context, name string, root key.Key, by string, now, deleteAt time.Time) (bool, error)
func (d *DB) Ref(ctx context.Context, name string) (Ref, error) // ErrNotFound
func (d *DB) ListRefs(ctx context.Context, prefix, after string, limit int) ([]Ref, error)
func (d *DB) DeleteRef(ctx context.Context, name string, deleteAt time.Time) error // ErrNotFound

// CreateUpload records u. parent, when set, must be the root of a base
// pack, or the error is ErrParentGone; u.ParentID is filled from it.
func (d *DB) CreateUpload(ctx context.Context, u Upload, parent *key.Key) (Upload, error)
func (d *DB) UploadByID(ctx context.Context, id string) (Upload, error) // ErrNotFound
// BeginVerify moves an upload from pending to verifying. ErrNotFound: no
// such upload, or not uploader's. ErrBusy: already being verified.
func (d *DB) BeginVerify(ctx context.Context, id, uploader string) (Upload, error)
// EndVerify returns an upload to pending: the verification could not run.
func (d *DB) EndVerify(ctx context.Context, id string) error
// CommitUpload records the verified pack, points the upload's ref at it
// and forgets the upload. If the root has a pack by now, the upload's keys
// are queued for deletion at now and the ref is pointed at that pack.
// Either way it returns the pack the ref points at.
func (d *DB) CommitUpload(ctx context.Context, id string, v Verified, now, deleteAt time.Time) (Pack, error)
// FailUpload forgets an upload, queues its three keys and its multipart
// upload for deletion at now, and collects.
func (d *DB) FailUpload(ctx context.Context, id string, now, deleteAt time.Time) error
// ExpireUploads fails every pending upload whose deadline is before now,
// queueing its keys a second time at again. It returns how many.
func (d *DB) ExpireUploads(ctx context.Context, now, again, deleteAt time.Time) (int, error)
func (d *DB) ResetVerifying(ctx context.Context) error
func (d *DB) ListUploads(ctx context.Context) ([]Upload, error)

func (d *DB) DueDeletions(ctx context.Context, now time.Time, limit int) ([]Deletion, error)
func (d *DB) DoneDeletion(ctx context.Context, id int64) error

type Stats struct {
	Refs, BasePacks, PatchPacks, Uploads, Deletions int64
	S3Bytes      int64 // data + index + links
	DataBytes    int64 // data only
	StoredBytes  int64 // uncompressed bytes in packs
	LogicalBytes int64 // over refs: bytes + shared_bytes of the pack
}
func (d *DB) Stats(ctx context.Context) (Stats, error)

// RefInfo is a ref with its pack and, for a patch pack, its parent.
type RefInfo struct {
	Ref    Ref
	Pack   Pack
	Parent *Pack
}
func (d *DB) ListRefInfo(ctx context.Context, prefix, after string, limit int) ([]RefInfo, error)

// PackInfo is a pack with what leans on it.
type PackInfo struct {
	Pack       Pack
	ParentRoot *key.Key
	Refs       int64
	Children   int64
}
func (d *DB) ListPacks(ctx context.Context, afterID int64, limit int) ([]PackInfo, error)

type PackDetail struct {
	PackInfo
	Parent   *Pack
	RefNames []string
	Children []key.Key
}
func (d *DB) PackDetail(ctx context.Context, root key.Key) (PackDetail, error) // ErrNotFound
```

### bucket

```go
var ErrNotFound = errors.New("bucket: no such object")

type Config struct {
	Bucket    string
	Prefix    string
	Endpoint  string // "" for AWS
	Region    string // "" for the SDK's
	PathStyle bool
}

func New(ctx context.Context, cfg Config) (*Bucket, error)
// Key returns "<prefix>packs/<root hex>/<uploadID>.<ext>".
func (b *Bucket) Key(root key.Key, uploadID, ext string) string
func (b *Bucket) Size(ctx context.Context, objectKey string) (int64, error) // ErrNotFound
func (b *Bucket) Get(ctx context.Context, objectKey string) (io.ReadCloser, error)
func (b *Bucket) Put(ctx context.Context, objectKey string, body []byte) error
func (b *Bucket) Delete(ctx context.Context, objectKey string) error // absent is success
func (b *Bucket) CreateMultipart(ctx context.Context, objectKey string) (uploadID string, err error)
func (b *Bucket) AbortMultipart(ctx context.Context, objectKey, uploadID string) error // absent is success
func (b *Bucket) PresignGet(ctx context.Context, objectKey string, ttl time.Duration) (string, error)
func (b *Bucket) PresignPut(ctx context.Context, objectKey string, ttl time.Duration) (string, error)
func (b *Bucket) PresignPart(ctx context.Context, objectKey, uploadID string, part int32, ttl time.Duration) (string, error)
func (b *Bucket) PresignComplete(ctx context.Context, objectKey, uploadID string, ttl time.Duration) (string, error)
```

### node

```go
// LoadOrCreateKey reads the key file at path: 64 hex characters and a line
// feed. A missing file is created with a new key, mode 0600.
func LoadOrCreateKey(path string) (irohkey.SecretKey, error)

type ServerConfig struct {
	Key   irohkey.SecretKey
	ALPN  string
	Local bool // tests: 127.0.0.1, no relay, no pkarr, no mDNS
	Log   *slog.Logger
}
// Bind binds the server's endpoint. Unless cfg.Local it waits for the
// relay (15 s at most, then warns), advertises the interface addresses,
// and announces through pkarr and mDNS until ctx is done.
func Bind(ctx context.Context, cfg ServerConfig) (*iroh.Endpoint, error)

type DialConfig struct {
	Key    irohkey.SecretKey
	ALPN   string
	Server irohkey.EndpointID
	Addrs  []netip.AddrPort // tests: dial these, no relay, no lookups
	Log    *slog.Logger
}
// Dial resolves the server by its ID through mDNS, pkarr and DNS and
// connects, racing the candidates. Close ends the connection and the
// endpoint under it.
func Dial(ctx context.Context, cfg DialConfig) (*Conn, error)
func (c *Conn) OpenStreamConn(ctx context.Context) (net.Conn, error)
func (c *Conn) Close() error
```

### server

```go
// Bucket is what the server needs of package bucket.
type Bucket interface { /* the method set of *bucket.Bucket */ }

type Config struct {
	DB            *db.DB
	Bucket        Bucket
	Scratch       string
	UploadTimeout time.Duration    // 0: 1h
	URLTTL        time.Duration    // 0: 1h
	PartSize      int64            // 0: 64 MiB
	VerifyJobs    int              // 0: 2
	Now           func() time.Time // nil: time.Now
	Log           *slog.Logger
}

func New(cfg Config) (*Server, error) // empties Scratch, resets verifying uploads
func (s *Server) Handle(ctx context.Context, remote string, req wire.Request) wire.Response
func (s *Server) Serve(ctx context.Context, ep *iroh.Endpoint) error
func (s *Server) Sweep(ctx context.Context) error
func (s *Server) RunSweeper(ctx context.Context, every time.Duration)
```

### client

```go
var ErrNotFound = errors.New("client: no such reference")

// Opener opens one stream per request; *node.Conn and *iroh.Conn do.
type Opener interface {
	OpenStreamConn(ctx context.Context) (net.Conn, error)
}

func New(conn Opener, hc *http.Client) *Client // hc nil: a client without timeout

type PushOptions struct {
	MinDedup float64 // 0 is taken literally; the CLI passes 0.5
	TempDir  string  // "": os.TempDir()
	Parallel int     // part uploads at once; 0: 4
}

type PushResult struct {
	Root     key.Key
	Stored   bool     // the server had the pack already
	Parent   *key.Key // nil: a base pack
	Objects  uint64   // objects in the pack
	Bytes    uint64   // their uncompressed bytes
	DataSize uint64
}

func (c *Client) Push(ctx context.Context, objects *packstore.Store, name string, root key.Key, opts PushOptions) (PushResult, error)

type PullResult struct {
	Root    key.Key
	Packs   int    // packs downloaded
	Objects int    // objects written
	Bytes   uint64 // their bytes
}

// Pull imports the objects of name into objects and returns its root. The
// caller sets the local ref.
func (c *Client) Pull(ctx context.Context, objects *packstore.Store, name string) (PullResult, error)

type Ref struct {
	Name string
	Root key.Key
}
func (c *Client) List(ctx context.Context, prefix string) ([]Ref, error)
func (c *Client) Delete(ctx context.Context, name string) error // ErrNotFound
```

### admin

```go
func Handler(d *db.DB) http.Handler
```

JSON, all sizes and counts as numbers, keys as hex, times as RFC 3339:

```
GET /api/stats        {refs, base_packs, patch_packs, uploads, deletions, s3_bytes,
                       data_bytes, stored_bytes, logical_bytes, dedup_rate, compression}
GET /api/refs         {refs: [{name, root, updated_by, updated_at, kind, ref_objects,
                       ref_bytes, shared_objects, shared_bytes, dedup, parent_root,
                       parent_unreachable_objects, parent_unreachable_bytes}], more}
GET /api/packs        {packs: [{id, root, kind, parent_root, objects, bytes, data_size,
                       index_size, links_size, shared_objects, shared_bytes, uploader,
                       uploaded_at, refs, children}], more}
GET /api/packs/{root} {pack, parent, refs: [name], children: [root]}
GET /api/uploads      {uploads: [{id, name, root, parent_root, uploader, data_size,
                       objects, multipart, state, issued_at, deadline}]}
```

`kind` is `"base"` or `"patch"`. `parent_root` and `parent` are null for a base pack. `/api/refs` takes `prefix`, `after`, `limit`; `/api/packs` takes `after` (an id) and `limit`.

---

## Tasks

Tasks 1 and 2 share an owner. Tasks 1+2, 4, 5 and 6 are independent of each other and run in parallel; 3 and the page of 9 can be written beside them. 7 needs 1 to 5; 8 needs 1, 3 and 6; 10 and 11 need everything.

### Task 0: Scaffold (done)

- [x] `flake.nix`, `flake.lock`, `.envrc`, `.gitignore`, `sqlc.yaml`, `go.mod` with every dependency, `tools/deps.go`
- [x] `keyset`, `sketch` ported from the experiment onto core keys, tests green

### Task 1: packfile

**Files:** `packfile/index.go`, `writer.go`, `reader.go`, `links.go`, and a `_test.go` beside each.

- [x] Tests for the index: round trip; `Find` and `Has`; `ByOffset`; and one refusal each for bad magic, a short header, a length that does not match the count, more than `MaxEntries`, a count of 2^60 in a 16-byte file (no allocation: compare with the file size first), unsorted keys, duplicate keys, a non-canonical key, a length above `amberpack.MaxPayload`, a gap, an overlap, a first offset above 0, and an offset near 2^64 whose end overflows.
- [x] `index.go`.
- [x] Tests for the writer and readers: objects added in one order come back from `Objects` in offset order with their bytes; an empty pack writes nothing and reads as nothing; a key added twice fails at `Finish`; `Objects` and `Expand` refuse a stream cut short, a stream with bytes left over, and a stream whose window is above `MaxWindow` (build one with `zstd.WithWindowSize(128<<20)`); `Expand` never writes past `DataSize`.
- [x] `writer.go`, `reader.go`. The encoder is klauspost's at its default level; the decoder is built with `zstd.WithDecoderMaxWindow(MaxWindow)` and `zstd.WithDecoderConcurrency(1)`.
- [x] Tests for links: round trip; repeats dropped; a child position at or above n refused; starts that decrease or run past the children refused; a count that differs from n refused.
- [x] `links.go`.
- [x] `nix develop -c go test ./packfile` green, `go vet` clean.

### Task 2: verify

**Files:** `verify/object.go`, `verify.go`, tests beside them. Test trees are built by ingesting a temporary directory with core (`ingest.Objects`), which yields real objects of every type.

- [x] Tests for `Object`: a blob, a file node and a directory leaf pass; a flipped byte fails; a blob whose key carries another length fails.
- [x] `object.go`, after `verifyObject` in core's `packstore/verify.go` (it is unexported there).
- [x] Tests for `Pack`, base: a whole key set passes and its `Links` name every child; one object left out fails as missing; one object extra fails as unreached; an object whose bytes were altered fails; a root that is not in the pack fails; a ref of one object passes.
- [x] Tests for `Pack`, patch: a patch against a base of an earlier version of the tree passes, and `SharedObjects` and `SharedBytes` equal the intersection of the two key sets computed directly; a key also in the parent fails; a child in neither fails; an empty patch whose root is in the parent passes with the root's closure as shared; an empty patch whose root is not in the parent fails; a parent object reached along two paths is counted once.
- [x] `verify.go`: breadth first from the root, a bitmap over the index for visited, children through `fstree.ChildKeys`; the crossings into the parent are collected and then followed through `parentLinks`.
- [x] `nix develop -c go test ./verify` green.

### Task 3: wire

**Files:** `wire/wire.go`, `wire_test.go`.

- [x] Tests: a request and a response of every operation round-trip; a frame above `MaxFrame` is refused before it is read; a truncated frame is an error; `Error` formats as `code: message`.
- [x] `wire.go`.

### Task 4: db

**Files:** as in the file structure; `db/generate.go` holds `//go:generate sqlc generate -f ../sqlc.yaml`. The pattern for opening modernc SQLite in WAL mode with embedded migrations is `../clamp/store/sqlite.go` and `schema.go`.

- [x] The schema of spec 7.1 in `db/migrations/0001_init.sql`, with `links_key TEXT NOT NULL` added to `uploads`.
- [x] Tests, then code, for packs and `Nearest`: three base packs with known overlaps come back in order of similarity with the estimates `sketch.Jaccard` gives; a patch pack is never a candidate; a query sharing no key gets nothing; ties go to the lower root.
- [x] Tests, then code, for refs and collection: a ref moved off a pack collects it and queues its keys at `deleteAt`; two refs on one pack, one deleted, keeps it; a base pack with a child survives the loss of its own ref and goes when the child's ref goes, both in one call; a base pack named by an open upload survives; `ListRefs` pages by `after` and matches `%`, `_` and `\` in a prefix literally.
- [x] Tests, then code, for uploads and deletions: `CreateUpload` with a parent that is a patch pack or unknown is `ErrParentGone`; `BeginVerify` by another uploader is `ErrNotFound`, twice is `ErrBusy`; `CommitUpload` writes pack, sketch keys and ref and removes the upload; `CommitUpload` when the root has a pack queues the upload's three keys and points the ref at the old pack; `FailUpload` queues three keys and, for a multipart upload, the abort; `ExpireUploads` leaves `verifying` uploads and uploads before their deadline alone and queues the keys twice; `ResetVerifying`; `DueDeletions` returns only what is due and `DoneDeletion` removes it; everything survives `Close` and `Open`.
- [x] Tests, then code, for `Stats`, `ListRefInfo`, `ListPacks`, `PackDetail`.
- [x] `nix develop -c sh -c 'cd db && go generate && cd .. && go test ./db'` green; `db/dbq` committed.

### Task 5: bucket

**Files:** `bucket/bucket.go`, `bucket_test.go`. Tests run against `gofakes3` with the `s3mem` backend behind `httptest.NewServer`, path style, static credentials.

- [x] Tests: `Put`, `Size`, `Get`, `Delete`; `Size` of an absent key is `ErrNotFound`; `Delete` of an absent key succeeds; a `PresignPut` URL takes a plain `http.NewRequest("PUT", ...)` and the object is there; a `PresignGet` URL returns it; a multipart upload of three parts through `PresignPart` URLs, completed by POSTing the XML body to the `PresignComplete` URL with plain HTTP, yields the joined object; `AbortMultipart` of a finished or unknown upload succeeds; `Key` formats as the spec says, with and without a prefix.
- [x] `bucket.go`. `PresignComplete` builds `POST <object url>?uploadId=<id>` and signs it with `v4.Signer.PresignHTTP` (service `s3`, payload hash `UNSIGNED-PAYLOAD`, `X-Amz-Expires` in the query); the rest uses `s3.Client` and `s3.PresignClient`.
- [x] `nix develop -c go test ./bucket` green.

### Task 6: node

**Files:** `node/key.go`, `serve.go`, `dial.go`, tests. The code to follow is transport-iroh's: `../transport-iroh/cmd/amber-serve/main.go` and `addrs.go` for the server, `cmd/amber/dial.go` for the client.

- [x] Tests for the key file: created on first use with mode 0600 and read back the same; a file of another shape is an error that does not echo its contents.
- [x] `key.go`.
- [x] Test: a server bound with `Local` and a client dialing it with `Addrs` exchange a message over a stream, and the server sees the client's endpoint ID as `RemoteID`.
- [x] `serve.go`, `dial.go`.
- [x] `nix develop -c go test ./node` green.

### Task 7: server

**Files:** as in the file structure. Tests drive `Handle` directly with a real `db.DB` in a temporary directory and a real `bucket.Bucket` on gofakes3, uploading with plain HTTP; a fake clock is passed as `Config.Now`.

- [x] Tests, then code, for `push-start`: an unknown root gets candidates with distances and working index URLs; a known root is `stored` and the ref points at it; a sketch that is empty, unsorted or of 5000 keys is `bad_request`; a bad name is `bad_request`.
- [x] Tests, then code, for `push-upload`: one PUT for small data, parts for data above `PartSize` with `ceil(size/part)` URLs; an unknown parent is `parent_gone`; the upload row carries three keys.
- [x] Tests, then code, for `push-commit` (`server/verify.go`): a good base pack is recorded with links in the bucket and a sketch in the database; a good patch pack is recorded with the server's shared figures; wrong sizes, a bad index and a pack failing the walk are `malformed_pack` and leave nothing in the bucket after a sweep; somebody else's upload is `unknown_upload`; a bucket that fails is `internal` and the upload is pending again.
- [x] Tests, then code, for `pull`, `list`, `delete`.
- [x] Tests, then code, for `Sweep`: an expired upload's objects and multipart upload are gone; an upload being verified is left; a deletion that fails stays queued; `New` resets `verifying` and empties the scratch directory.
- [x] `Serve`: one goroutine per connection and per stream, one frame in, one frame out.

### Task 8: client

**Files:** as in the file structure.

- [x] Tests for `client/http.go` against `httptest`: PUT with a content length; GET; the multipart upload sends parts in parallel, collects ETags, POSTs the completion XML, and treats a 200 answer carrying `<Error>` as a failure.
- [x] `http.go`.
- [x] `push.go`, `pull.go`, `client.go` per spec 9.1 and 9.2; their tests are Task 11's.

### Task 9: admin

**Files:** `admin/admin.go`, `admin_test.go`, `admin/web/*`.

- [x] Tests through `httptest` for every route against a database filled through package `db`: the figures of spec 8, 404 for an unknown root, paging.
- [x] `admin.go`.
- [x] The page: overview, refs, packs with detail, uploads; hash routing; no build step; no external resources.

### Task 10: commands

**Files:** `cmd/jaccard-stored/main.go`, `cmd/jaccard-store/main.go`, `store.go`, tests for flag and environment parsing.

- [x] `jaccard-stored`: flags and variables of spec 7.5; opens the database, the bucket, the endpoint, runs `Serve`, the sweeper and the admin listener until a signal.
- [x] `jaccard-store`: flags and variables of spec 9.3; `push`, `pull`, `ls`, `rm`. Local refs are core `reference` records; a pull writes inside the collector's span, as core's CLI does (`cmd/amber-store/store.go`, `ref.go` in core).
- [x] Tests: every variable is read, and the flag wins over it.

### Task 11: end to end

**Files:** `e2e/doc.go`, `e2e/e2e_test.go`.

- [x] One process: gofakes3, `server.New`, `node.Bind` with `Local`, `node.Dial` with `Addrs`, two local core stores. Cases: a base pack; a second version pushed as a patch pack and pulled into an empty store, equal to the source; a root already stored; a ref moved and the old packs gone from the bucket after sweeps past the URL lifetime; an upload that expires, single and multipart; a malformed upload refused and removed; the multipart path with a 5 MiB part size; two clients pushing one root at once; a pull that skips the parent because the store has its content; a ref of one object; a ref whose root sits in a base pack.

### Task 12: finish

- [x] `README.md`: what it is, the two commands, the variables, the limits of spec 13.
- [x] Remove `tools/deps.go`, `go mod tidy`, full `go vet ./...` and `go test -race ./...`.
- [x] Create the private repository `amber-store/jaccard-store` and push.
- [x] A fresh review of the whole branch against the spec.
