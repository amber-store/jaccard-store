package packfile

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// compress returns raw as one zstd stream.
func compress(t testing.TB, raw []byte, opts ...zstd.EOption) []byte {
	t.Helper()
	var out bytes.Buffer
	enc, err := zstd.NewWriter(&out, opts...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enc.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

// collect drains Objects, returning what it yielded before the error, if any.
func collect(x *Index, data io.Reader) ([]Object, error) {
	var objects []Object
	for o, err := range Objects(x, data) {
		if err != nil {
			return objects, err
		}
		objects = append(objects, o)
	}
	return objects, nil
}

// limitWriter fails the test when more than limit bytes are written to it.
type limitWriter struct {
	t       *testing.T
	limit   uint64
	written uint64
}

func (w *limitWriter) Write(p []byte) (int, error) {
	w.written += uint64(len(p))
	if w.written > w.limit {
		w.t.Fatalf("%d bytes written, the index says %d", w.written, w.limit)
	}
	return len(p), nil
}

// failingReader yields its bytes and then err.
type failingReader struct {
	r   io.Reader
	err error
}

func (f *failingReader) Read(p []byte) (int, error) {
	n, err := f.r.Read(p)
	if err == io.EOF {
		err = f.err
	}
	return n, err
}

func TestObjectsInOffsetOrder(t *testing.T) {
	objects := testObjects(t)
	x, data := writePack(t, objects)
	got, err := collect(x, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(objects) {
		t.Fatalf("%d objects came back, want %d", len(got), len(objects))
	}
	for i, o := range objects {
		if got[i].Key != o.Key {
			t.Fatalf("object %d is %s, want %s", i, got[i].Key, o.Key)
		}
		if !bytes.Equal(got[i].Data, o.Data) {
			t.Errorf("object %d: bytes differ", i)
		}
	}
}

func TestObjectsStopsWhenTold(t *testing.T) {
	objects := testObjects(t)
	x, data := writePack(t, objects)
	n := 0
	for _, err := range Objects(x, bytes.NewReader(data)) {
		if err != nil {
			t.Fatal(err)
		}
		if n++; n == 3 {
			break
		}
	}
	if n != 3 {
		t.Errorf("yielded %d objects before the break", n)
	}
}

func TestExpand(t *testing.T) {
	objects := testObjects(t)
	x, data := writePack(t, objects)
	var out bytes.Buffer
	if err := Expand(x, bytes.NewReader(data), &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), stream(objects)) {
		t.Error("the expanded stream is not the objects back to back")
	}
}

func TestEmptyObjectsNeedNoData(t *testing.T) {
	empty := Object{Key: testKey(t, nil), Data: []byte{}}
	x, data := writePack(t, []Object{empty})
	if len(data) != 0 {
		t.Fatalf("a pack of one empty object wrote %d bytes", len(data))
	}
	got, err := collect(x, bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != empty.Key || len(got[0].Data) != 0 {
		t.Errorf("got %v", got)
	}
	if err := Expand(x, bytes.NewReader(nil), io.Discard); err != nil {
		t.Errorf("Expand: %v", err)
	}
	// A zstd frame without content is as good as no bytes.
	frame := compress(t, nil, zstd.WithZeroFrames(true))
	if len(frame) == 0 {
		t.Fatal("no frame was written")
	}
	if _, err := collect(x, bytes.NewReader(frame)); err != nil {
		t.Errorf("Objects over an empty frame: %v", err)
	}
	if err := Expand(x, bytes.NewReader(frame), io.Discard); err != nil {
		t.Errorf("Expand over an empty frame: %v", err)
	}
}

func TestReadersRefuse(t *testing.T) {
	objects := testObjects(t)
	x, data := writePack(t, objects)
	raw := stream(objects)
	last := len(objects[len(objects)-1].Data)

	cases := []struct {
		name string
		data []byte
	}{
		{"no data at all", nil},
		{"cut in the frame header", data[:3]},
		{"cut in the middle", data[:len(data)/2]},
		{"cut one byte short", data[:len(data)-1]},
		{"a whole stream, one object short", compress(t, raw[:len(raw)-last])},
		{"a whole stream, one byte short", compress(t, raw[:len(raw)-1])},
		{"a whole stream, one byte long", compress(t, append(bytes.Clone(raw), 0))},
		{"a whole stream, far too long", compress(t, append(bytes.Clone(raw), make([]byte, 4<<20)...))},
		{"a second frame", append(bytes.Clone(data), compress(t, []byte("more"))...)},
		{"bytes that are not zstd after the frame", append(bytes.Clone(data), "trailing junk"...)},
		{"not zstd", []byte("this is not a zstd stream at all")},
		{"a window above MaxWindow", compress(t, raw, zstd.WithWindowSize(128<<20), zstd.WithEncoderLevel(zstd.SpeedFastest))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := collect(x, bytes.NewReader(tc.data))
			if !errors.Is(err, ErrMalformed) {
				t.Errorf("Objects: %v after %d objects, want ErrMalformed", err, len(got))
			}
			for i, o := range got {
				if o.Key != objects[i].Key || !bytes.Equal(o.Data, objects[i].Data) {
					t.Errorf("Objects yielded a wrong object %d before failing", i)
				}
			}
			w := &limitWriter{t: t, limit: x.DataSize()}
			if err := Expand(x, bytes.NewReader(tc.data), w); !errors.Is(err, ErrMalformed) {
				t.Errorf("Expand: %v, want ErrMalformed", err)
			}
		})
	}
}

// The window case above is only a test of MaxWindow if the same stream reads
// well once the limit is out of the way.
func TestWindowCaseIsAValidStream(t *testing.T) {
	raw := stream(testObjects(t))
	data := compress(t, raw, zstd.WithWindowSize(128<<20), zstd.WithEncoderLevel(zstd.SpeedFastest))
	dec, err := zstd.NewReader(bytes.NewReader(data), zstd.WithDecoderMaxWindow(256<<20), zstd.WithDecoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	got, err := io.ReadAll(dec)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, raw) {
		t.Error("the stream does not decode to the objects")
	}
}

// A stream that expands far past what the index says is refused without
// being expanded.
func TestReadersDoNotExpandPastTheIndex(t *testing.T) {
	small := Object{Key: testKey(t, []byte("small")), Data: []byte("small")}
	x, _ := writePack(t, []Object{small})
	var packed bytes.Buffer
	enc, err := zstd.NewWriter(&packed)
	if err != nil {
		t.Fatal(err)
	}
	enc.Write(small.Data)
	zeros := make([]byte, 1<<20)
	for range 256 {
		enc.Write(zeros)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	bomb := packed.Bytes()
	if len(bomb) > 1<<20 {
		t.Fatalf("the bomb is %d bytes", len(bomb))
	}
	runtime.GC()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got, err := collect(x, bytes.NewReader(bomb))
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrMalformed) {
		t.Errorf("Objects: %v, want ErrMalformed", err)
	}
	if len(got) != 1 || !bytes.Equal(got[0].Data, small.Data) {
		t.Errorf("Objects yielded %d objects before failing", len(got))
	}
	if spent := after.TotalAlloc - before.TotalAlloc; spent > 32<<20 {
		t.Errorf("Objects allocated %d bytes", spent)
	}

	w := &limitWriter{t: t, limit: x.DataSize()}
	runtime.ReadMemStats(&before)
	err = Expand(x, bytes.NewReader(bomb), w)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrMalformed) {
		t.Errorf("Expand: %v, want ErrMalformed", err)
	}
	if w.written != x.DataSize() {
		t.Errorf("Expand wrote %d bytes, want %d", w.written, x.DataSize())
	}
	if spent := after.TotalAlloc - before.TotalAlloc; spent > 32<<20 {
		t.Errorf("Expand allocated %d bytes", spent)
	}
}

// A failure to read the data is not a verdict on the pack.
func TestReadersPassOnReadErrors(t *testing.T) {
	objects := testObjects(t)
	x, data := writePack(t, objects)
	broken := errors.New("connection lost")
	// The last cut is no cut: the source fails where the stream should end,
	// so that the stream ends there cannot be told.
	for _, cut := range []int{0, len(data) / 2, len(data)} {
		_, err := collect(x, &failingReader{r: bytes.NewReader(data[:cut]), err: broken})
		if err != broken {
			t.Errorf("Objects, source failing at %d: %v, want the source's error", cut, err)
		}
		err = Expand(x, &failingReader{r: bytes.NewReader(data[:cut]), err: broken}, io.Discard)
		if err != broken {
			t.Errorf("Expand, source failing at %d: %v, want the source's error", cut, err)
		}
	}
}

type failingWriter struct{ err error }

func (f failingWriter) Write([]byte) (int, error) { return 0, f.err }

func TestExpandPassesOnWriteErrors(t *testing.T) {
	x, data := writePack(t, testObjects(t))
	full := errors.New("disk full")
	err := Expand(x, bytes.NewReader(data), failingWriter{full})
	if err != full {
		t.Errorf("Expand: %v, want the writer's error", err)
	}
}

// A pack without bytes may come as no data at all or as a zstd stream of
// nothing: writers have produced both, and a reader takes both.
func TestAPackWithoutBytesReadsFromNothingAndFromAnEmptyFrame(t *testing.T) {
	var frame bytes.Buffer
	enc, err := zstd.NewWriter(&frame, zstd.WithZeroFrames(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	if frame.Len() == 0 {
		t.Fatal("the test has no empty frame to read")
	}
	empty, err := NewIndex(nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"no data": nil, "an empty frame": frame.Bytes()} {
		for o, err := range Objects(empty, bytes.NewReader(data)) {
			t.Fatalf("%s: Objects yielded %v, %v", name, o.Key, err)
		}
		var out bytes.Buffer
		if err := Expand(empty, bytes.NewReader(data), &out); err != nil || out.Len() != 0 {
			t.Fatalf("%s: Expand wrote %d bytes, %v", name, out.Len(), err)
		}
	}
}
