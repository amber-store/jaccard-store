package wire_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/amber-store/jaccard-store/wire"
)

func roundTrip[T any](t *testing.T, in T) T {
	t.Helper()
	var buf bytes.Buffer
	if err := wire.WriteFrame(&buf, in); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	var out T
	if err := wire.ReadFrame(&buf, &out); err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("ReadFrame left %d bytes of the frame unread", buf.Len())
	}
	return out
}

func TestRequestsRoundTrip(t *testing.T) {
	root := bytes.Repeat([]byte{7}, 32)
	for _, req := range []wire.Request{
		{Op: wire.OpPushStart, Name: "a/b", Root: root, Sketch: [][]byte{bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)}},
		{Op: wire.OpPushUpload, Name: "a/b", Root: root, Parent: bytes.Repeat([]byte{9}, 32), DataSize: 1 << 40, Objects: 12345},
		{Op: wire.OpPushUpload, Name: "a/b", Root: root},
		{Op: wire.OpPushCommit, UploadID: "0123456789abcdef"},
		{Op: wire.OpPull, Name: "a/b"},
		{Op: wire.OpList, Prefix: "a/", After: "a/b", Limit: 1000},
		{Op: wire.OpDelete, Name: "a/b"},
	} {
		if got := roundTrip(t, req); !reflect.DeepEqual(got, req) {
			t.Errorf("%s: got %+v, want %+v", req.Op, got, req)
		}
	}
}

func TestResponsesRoundTrip(t *testing.T) {
	root := bytes.Repeat([]byte{7}, 32)
	for name, resp := range map[string]wire.Response{
		"stored": {Stored: true},
		"candidates": {Candidates: []wire.Candidate{
			{Root: root, Distance: 0.25, Objects: 10, Bytes: 1000, DataSize: 400, IndexURL: "https://s3/idx?sig=1"},
		}},
		"single put": {UploadID: "u1", Deadline: 1_800_000_000, IndexURL: "https://s3/i", DataURL: "https://s3/d"},
		"parts": {UploadID: "u2", Deadline: 1_800_000_000, IndexURL: "https://s3/i", Parts: &wire.Parts{
			PartSize: 64 << 20, URLs: []string{"https://s3/p1", "https://s3/p2"}, CompleteURL: "https://s3/c",
		}},
		"pull": {Root: root, Packs: []wire.Pack{
			{Root: root, Objects: 3, Bytes: 30, DataSize: 20, IndexSize: 148, IndexURL: "https://s3/i", DataURL: "https://s3/d"},
		}},
		"list":  {Refs: []wire.Ref{{Name: "a", Root: root}}, More: true},
		"error": {Error: &wire.Error{Code: wire.CodeMalformedPack, Message: "object missing"}},
		"empty": {},
	} {
		if got := roundTrip(t, resp); !reflect.DeepEqual(got, resp) {
			t.Errorf("%s: got %+v, want %+v", name, got, resp)
		}
	}
}

func TestFrameIsLengthPrefixedBigEndian(t *testing.T) {
	var buf bytes.Buffer
	if err := wire.WriteFrame(&buf, wire.Request{Op: wire.OpPull, Name: "x"}); err != nil {
		t.Fatal(err)
	}
	b := buf.Bytes()
	if n := binary.BigEndian.Uint32(b[:4]); int(n) != len(b)-4 {
		t.Fatalf("length prefix says %d, body is %d bytes", n, len(b)-4)
	}
}

// oversized yields a length prefix above MaxFrame and fails the test if
// anything past the prefix is asked for.
type oversized struct {
	t      *testing.T
	prefix *bytes.Reader
}

func (o *oversized) Read(p []byte) (int, error) {
	if o.prefix.Len() == 0 {
		o.t.Fatal("ReadFrame read past the length of a frame it must refuse")
	}
	return o.prefix.Read(p)
}

func TestReadFrameRefusesOversizedBeforeReadingIt(t *testing.T) {
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], wire.MaxFrame+1)
	var req wire.Request
	err := wire.ReadFrame(&oversized{t: t, prefix: bytes.NewReader(prefix[:])}, &req)
	if !errors.Is(err, wire.ErrFrameTooLarge) {
		t.Fatalf("ReadFrame = %v, want ErrFrameTooLarge", err)
	}
}

func TestReadFrameTruncated(t *testing.T) {
	var buf bytes.Buffer
	if err := wire.WriteFrame(&buf, wire.Request{Op: wire.OpPull, Name: "some-name"}); err != nil {
		t.Fatal(err)
	}
	whole := buf.Bytes()
	for _, n := range []int{0, 2, 4, len(whole) - 1} {
		var req wire.Request
		err := wire.ReadFrame(bytes.NewReader(whole[:n]), &req)
		if err == nil {
			t.Errorf("ReadFrame of %d of %d bytes succeeded", n, len(whole))
		}
		if n == 0 && err != io.EOF {
			t.Errorf("ReadFrame of nothing = %v, want io.EOF", err)
		}
		if n > 0 && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("ReadFrame of %d of %d bytes = %v, want io.ErrUnexpectedEOF", n, len(whole), err)
		}
	}
}

func TestReadFrameRefusesGarbage(t *testing.T) {
	body := []byte{0xff, 0xff, 0xff}
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	frame = append(frame, body...)
	var req wire.Request
	if err := wire.ReadFrame(bytes.NewReader(frame), &req); err == nil {
		t.Fatal("ReadFrame decoded a body that is not CBOR")
	}
}

func TestWriteFrameRefusesOversized(t *testing.T) {
	big := wire.Response{IndexURL: strings.Repeat("x", wire.MaxFrame)}
	if err := wire.WriteFrame(io.Discard, big); !errors.Is(err, wire.ErrFrameTooLarge) {
		t.Fatalf("WriteFrame = %v, want ErrFrameTooLarge", err)
	}
}

func TestErrorFormats(t *testing.T) {
	err := &wire.Error{Code: wire.CodeNotFound, Message: `no reference "x"`}
	if got, want := err.Error(), `not_found: no reference "x"`; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	var target *wire.Error
	if !errors.As(error(err), &target) || target.Code != wire.CodeNotFound {
		t.Fatal("a *wire.Error is not found by errors.As")
	}
}

func TestErrorf(t *testing.T) {
	resp := wire.Errorf(wire.CodeBadRequest, "name %q", "x")
	if resp.Error == nil || resp.Error.Code != wire.CodeBadRequest || resp.Error.Message != `name "x"` {
		t.Fatalf("Errorf = %+v", resp)
	}
}
