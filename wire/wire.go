// Package wire is the protocol between a jaccard-store client and server.
//
// A connection speaks ALPN. Each request is one bidirectional stream: the
// client writes one frame and closes its side, the server writes one frame
// and closes. A frame is a 4-byte big-endian length followed by that many
// bytes of CBOR, MaxFrame at most.
//
// A request is a Request with Op set and the fields of that operation; a
// response is a Response with either Error or the result fields. Fields an
// operation does not use are left out of the encoding.
//
//	push-start   name, root, sketch              stored | candidates
//	push-upload  name, root, parent, data_size,  stored | upload_id, deadline,
//	             objects, bytes, shared_objects, index_url, data_url | parts
//	             shared_bytes
//	push-commit  upload_id                       root, unverified
//	pull         name                            root, packs
//	list         prefix, after, limit            refs, more
//	delete       name
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/fxamacker/cbor/v2"
)

const (
	// ALPN names the protocol in the QUIC handshake.
	ALPN = "amber/jaccard-store/1"
	// MaxFrame bounds the body of a frame. The largest answer is the one to
	// push-upload for a pack of 10,000 parts, a pre-signed URL each.
	MaxFrame = 16 << 20
	// MaxRequest bounds the body of a request, which is far smaller than an
	// answer can be: the largest carries a sketch of 4096 keys. A server
	// reads requests from anybody, and a frame is allocated on the word of
	// its first four bytes.
	MaxRequest = 1 << 20
)

// ErrFrameTooLarge is returned for a frame whose body exceeds MaxFrame,
// whether it is to be written or was announced by a peer.
var ErrFrameTooLarge = errors.New("wire: frame exceeds the limit")

// Op names an operation.
type Op string

// The operations.
const (
	OpPushStart  Op = "push-start"
	OpPushUpload Op = "push-upload"
	OpPushCommit Op = "push-commit"
	OpPull       Op = "pull"
	OpList       Op = "list"
	OpDelete     Op = "delete"
)

// The error codes.
const (
	// CodeBadRequest: the request is not one the server can act on.
	CodeBadRequest = "bad_request"
	// CodeNotFound: there is no reference of that name.
	CodeNotFound = "not_found"
	// CodeParentGone: the parent named is not a base pack the server
	// holds; the push starts over.
	CodeParentGone = "parent_gone"
	// CodeUnknownUpload: the upload expired, never was, or is another
	// endpoint's.
	CodeUnknownUpload = "unknown_upload"
	// CodeMalformedPack: the uploaded pack failed verification, or is not
	// all there, and is gone.
	CodeMalformedPack = "malformed_pack"
	// CodeInternal: the server could not do its part; the request may be
	// repeated.
	CodeInternal = "internal"
)

// Request is what a client sends. Keys travel as their 32 bytes.
type Request struct {
	Op       Op       `cbor:"op"`
	Name     string   `cbor:"name,omitempty"`
	Root     []byte   `cbor:"root,omitempty"`
	Sketch   [][]byte `cbor:"sketch,omitempty"`
	Parent   []byte   `cbor:"parent,omitempty"`
	DataSize uint64   `cbor:"data_size,omitempty"`
	Objects  uint64   `cbor:"objects,omitempty"`
	// Bytes is the uncompressed size of the pack's data: the sum of the
	// lengths in its index. A server that takes no pack that large says so
	// before anything is uploaded.
	Bytes uint64 `cbor:"bytes,omitempty"`
	// SharedObjects and SharedBytes are what the reference has in common
	// with the parent named: the objects of the parent that the reference
	// is made of, and their uncompressed bytes. A server that verifies the
	// pack measures them itself; one that does not records these.
	SharedObjects uint64 `cbor:"shared_objects,omitempty"`
	SharedBytes   uint64 `cbor:"shared_bytes,omitempty"`
	UploadID      string `cbor:"upload_id,omitempty"`
	Prefix        string `cbor:"prefix,omitempty"`
	After         string `cbor:"after,omitempty"`
	Limit         int    `cbor:"limit,omitempty"`
}

// Response is what a server answers.
type Response struct {
	Error      *Error      `cbor:"error,omitempty"`
	Stored     bool        `cbor:"stored,omitempty"`
	Candidates []Candidate `cbor:"candidates,omitempty"`
	UploadID   string      `cbor:"upload_id,omitempty"`
	// Deadline is when the upload expires, in seconds since the Unix epoch.
	Deadline int64  `cbor:"deadline,omitempty"`
	IndexURL string `cbor:"index_url,omitempty"`
	DataURL  string `cbor:"data_url,omitempty"`
	Parts    *Parts `cbor:"parts,omitempty"`
	Root     []byte `cbor:"root,omitempty"`
	// Unverified says, in the answer to push-commit, that the pack the
	// reference points at was recorded without being verified: the server
	// saw that it was uploaded and read no more of it than its index.
	Unverified bool   `cbor:"unverified,omitempty"`
	Packs      []Pack `cbor:"packs,omitempty"`
	Refs       []Ref  `cbor:"refs,omitempty"`
	More       bool   `cbor:"more,omitempty"`
}

// Error is a refusal. It is an error, so a client can return it as it came.
type Error struct {
	Code    string `cbor:"code"`
	Message string `cbor:"message"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Errorf returns the response that refuses a request with code.
func Errorf(code, format string, args ...any) Response {
	return Response{Error: &Error{Code: code, Message: fmt.Sprintf(format, args...)}}
}

// Candidate is a base pack near the key set of a push.
type Candidate struct {
	Root []byte `cbor:"root"`
	// Distance is the estimated Jaccard distance: 1 minus the similarity
	// the two sketches give.
	Distance float64 `cbor:"distance"`
	Objects  uint64  `cbor:"objects"`
	Bytes    uint64  `cbor:"bytes"`
	DataSize uint64  `cbor:"data_size"`
	// IndexURL is a pre-signed GET for the pack's index.
	IndexURL string `cbor:"index_url"`
}

// Parts is how data too large for one PUT is uploaded: part i, counted from
// 1, is PartSize bytes (the last one what is left) and goes to URLs[i-1];
// the upload is completed with a POST to CompleteURL.
type Parts struct {
	PartSize    uint64   `cbor:"part_size"`
	URLs        []string `cbor:"urls"`
	CompleteURL string   `cbor:"complete_url"`
}

// Pack is one pack a pull reads, with pre-signed GETs for its two objects.
type Pack struct {
	Root      []byte `cbor:"root"`
	Objects   uint64 `cbor:"objects"`
	Bytes     uint64 `cbor:"bytes"`
	DataSize  uint64 `cbor:"data_size"`
	IndexSize uint64 `cbor:"index_size"`
	IndexURL  string `cbor:"index_url"`
	DataURL   string `cbor:"data_url"`
}

// Ref is a reference: a name and the root key it points at.
type Ref struct {
	Name string `cbor:"name"`
	Root []byte `cbor:"root"`
}

// decMode bounds what a frame may make the decoder allocate. The limits are
// above anything a valid message holds: 10,000 part URLs, 4096 sketch keys,
// 1000 references.
var decMode = func() cbor.DecMode {
	m, err := cbor.DecOptions{
		MaxArrayElements: 16384,
		MaxMapPairs:      64,
		MaxNestedLevels:  8,
	}.DecMode()
	if err != nil {
		panic(err)
	}
	return m
}()

// WriteFrame writes v as one frame.
func WriteFrame(w io.Writer, v any) error {
	body, err := cbor.Marshal(v)
	if err != nil {
		return fmt.Errorf("wire: encoding a frame: %w", err)
	}
	if len(body) > MaxFrame {
		return fmt.Errorf("%w: %d bytes to write", ErrFrameTooLarge, len(body))
	}
	frame := make([]byte, 4, 4+len(body))
	binary.BigEndian.PutUint32(frame, uint32(len(body)))
	_, err = w.Write(append(frame, body...))
	return err
}

// ReadFrame reads one frame into v. It returns io.EOF when r ends before a
// frame begins and io.ErrUnexpectedEOF when it ends inside one. A frame
// above MaxFrame is refused without reading its body.
func ReadFrame(r io.Reader, v any) error {
	return readFrame(r, v, MaxFrame)
}

// ReadRequest reads one request, as ReadFrame does, and refuses a frame
// above MaxRequest.
func ReadRequest(r io.Reader, req *Request) error {
	return readFrame(r, req, MaxRequest)
}

func readFrame(r io.Reader, v any, limit uint32) error {
	var prefix [4]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(prefix[:])
	if n > limit {
		return fmt.Errorf("%w: %d bytes announced, %d allowed", ErrFrameTooLarge, n, limit)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	if err := decMode.Unmarshal(body, v); err != nil {
		return fmt.Errorf("wire: decoding a frame: %w", err)
	}
	return nil
}
