package packfile

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/key"
)

// noise returns n bytes that do not compress, the same for the same seed.
func noise(seed byte, n int) []byte {
	b := make([]byte, n)
	rand.NewChaCha8([32]byte{seed}).Read(b)
	return b
}

// testObjects returns objects of several sizes, an empty one among them, in
// an order that is not key order.
func testObjects(t testing.TB) []Object {
	t.Helper()
	payloads := [][]byte{
		[]byte("the first object"),
		noise(1, 300<<10),
		{},
		bytes.Repeat([]byte("abc"), 5000),
		[]byte("x"),
		noise(2, 70<<10),
	}
	for i := 0; i < 20; i++ {
		payloads = append(payloads, fmt.Appendf(nil, "small object %d", i))
	}
	objects := make([]Object, len(payloads))
	for i, p := range payloads {
		objects[i] = Object{Key: testKey(t, p), Data: p}
	}
	return objects
}

// writePack writes objects through a Writer and returns the index and the
// compressed data.
func writePack(t testing.TB, objects []Object) (*Index, []byte) {
	t.Helper()
	var data bytes.Buffer
	w, err := NewWriter(&data)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range objects {
		if err := w.Add(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	x, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return x, data.Bytes()
}

// stream returns the uncompressed data stream of objects added in order.
func stream(objects []Object) []byte {
	var raw []byte
	for _, o := range objects {
		raw = append(raw, o.Data...)
	}
	return raw
}

func TestWriterIndex(t *testing.T) {
	objects := testObjects(t)
	var data bytes.Buffer
	w, err := NewWriter(&data)
	if err != nil {
		t.Fatal(err)
	}
	var total uint64
	for i, o := range objects {
		if w.Len() != i || w.Bytes() != total {
			t.Fatalf("before object %d: Len %d, Bytes %d, want %d, %d", i, w.Len(), w.Bytes(), i, total)
		}
		if err := w.Add(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
		total += uint64(len(o.Data))
	}
	if w.Len() != len(objects) || w.Bytes() != total {
		t.Fatalf("Len %d, Bytes %d, want %d, %d", w.Len(), w.Bytes(), len(objects), total)
	}
	x, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if x.Len() != len(objects) || x.DataSize() != total {
		t.Fatalf("index has %d entries of %d bytes, want %d of %d", x.Len(), x.DataSize(), len(objects), total)
	}
	var offset uint64
	for i, o := range objects {
		pos, ok := x.Find(o.Key)
		if !ok {
			t.Fatalf("object %d is not in the index", i)
		}
		if e := x.Entry(pos); e.Offset != offset || e.Length != uint32(len(o.Data)) {
			t.Errorf("object %d lies at %d+%d, want %d+%d", i, e.Offset, e.Length, offset, len(o.Data))
		}
		offset += uint64(len(o.Data))
	}
	if data.Len() == 0 {
		t.Error("no data was written")
	}
}

func TestWriterEmpty(t *testing.T) {
	x, data := writePack(t, nil)
	if x.Len() != 0 || x.DataSize() != 0 {
		t.Errorf("empty pack: %d entries of %d bytes", x.Len(), x.DataSize())
	}
	if len(data) != 0 {
		t.Errorf("an empty pack wrote %d bytes of data", len(data))
	}
	for o, err := range Objects(x, bytes.NewReader(data)) {
		t.Errorf("an empty pack yielded %s, %v", o.Key, err)
	}
	var out bytes.Buffer
	if err := Expand(x, bytes.NewReader(data), &out); err != nil {
		t.Errorf("Expand of an empty pack: %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("Expand of an empty pack wrote %d bytes", out.Len())
	}
}

func TestWriterDuplicateKey(t *testing.T) {
	objects := testObjects(t)
	var data bytes.Buffer
	w, err := NewWriter(&data)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range append(objects[:3:3], objects[1]) {
		if err := w.Add(o.Key, o.Data); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if _, err := w.Finish(); !errors.Is(err, ErrMalformed) {
		t.Errorf("Finish after a key added twice: %v, want ErrMalformed", err)
	}
}

func TestWriterRefusesAtAdd(t *testing.T) {
	var data bytes.Buffer
	w, err := NewWriter(&data)
	if err != nil {
		t.Fatal(err)
	}
	reserved := testKey(t, []byte("reserved"))
	reserved[key.Size-1] |= 0x08
	if err := w.Add(reserved, []byte("reserved")); !errors.Is(err, ErrMalformed) {
		t.Errorf("Add of a non-canonical key: %v, want ErrMalformed", err)
	}
	big := make([]byte, amberpack.MaxPayload+1)
	if err := w.Add(testKey(t, []byte("big")), big); !errors.Is(err, ErrMalformed) {
		t.Errorf("Add above MaxPayload: %v, want ErrMalformed", err)
	}
	if w.Len() != 0 || w.Bytes() != 0 {
		t.Errorf("refused objects were counted: Len %d, Bytes %d", w.Len(), w.Bytes())
	}
	x, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if x.Len() != 0 || data.Len() != 0 {
		t.Errorf("refused objects were written: %d entries, %d bytes", x.Len(), data.Len())
	}
}

func TestWriterAfterFinish(t *testing.T) {
	var data bytes.Buffer
	w, err := NewWriter(&data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	p := []byte("late")
	if err := w.Add(testKey(t, p), p); err == nil {
		t.Error("Add after Finish succeeded")
	}
	if _, err := w.Finish(); err == nil {
		t.Error("a second Finish succeeded")
	}
}
