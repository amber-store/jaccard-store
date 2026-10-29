package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/packfile"
	"github.com/amber-store/jaccard-store/wire"
)

// fakeServer is the server's side of a connection: it answers every
// request with what answer makes of it and keeps what it was asked.
type fakeServer struct {
	answer func(wire.Request) wire.Response

	mu    sync.Mutex
	asked []wire.Request
}

func (s *fakeServer) OpenStreamConn(context.Context) (net.Conn, error) {
	near, far := net.Pipe()
	go func() {
		defer far.Close()
		var req wire.Request
		if err := wire.ReadRequest(far, &req); err != nil {
			return
		}
		s.mu.Lock()
		s.asked = append(s.asked, req)
		s.mu.Unlock()
		wire.WriteFrame(far, s.answer(req))
	}()
	return near, nil
}

func (s *fakeServer) requests() []wire.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.asked)
}

// fakeBucket holds the two objects of one pack. A path named in refuse is
// answered with that status; a path named in cut is announced whole and
// ends after half of it.
type fakeBucket struct {
	index, data []byte
	refuse      map[string]int
	cut         map[string]bool

	mu    sync.Mutex
	asked []string
}

func (b *fakeBucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	b.asked = append(b.asked, r.URL.Path)
	b.mu.Unlock()
	if status := b.refuse[r.URL.Path]; status != 0 {
		w.WriteHeader(status)
		return
	}
	var body []byte
	switch r.URL.Path {
	case "/index":
		body = b.index
	case "/data":
		body = b.data
	default:
		http.NotFound(w, r)
		return
	}
	if b.cut[r.URL.Path] {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Write(body[:len(body)/2])
		return
	}
	w.Write(body)
}

func (b *fakeBucket) requests() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.asked)
}

// shelved is a pack in a bucket of its own.
type shelved struct {
	index  *packfile.Index
	bucket *fakeBucket
	// announced is what an honest server says of the pack.
	announced wire.Pack
	// pack is what a lookup makes of that.
	pack Pack
}

// shelve packs the objects of keys, in that order, and puts the pack in a
// bucket. The first key stands for the pack's root.
func shelve(t *testing.T, objects objectMap, keys []key.Key) *shelved {
	t.Helper()
	var data bytes.Buffer
	w, err := packfile.NewWriter(&data)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if err := w.Add(k, objects[k]); err != nil {
			t.Fatal(err)
		}
	}
	index, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	b := &fakeBucket{index: index.Encode(), data: data.Bytes()}
	srv := httptest.NewServer(b)
	t.Cleanup(srv.Close)
	s := &shelved{index: index, bucket: b}
	s.announced = wire.Pack{
		Root:      keys[0][:],
		Objects:   uint64(index.Len()),
		Bytes:     index.DataSize(),
		DataSize:  uint64(data.Len()),
		IndexSize: uint64(len(b.index)),
		IndexURL:  srv.URL + "/index",
		DataURL:   srv.URL + "/data",
	}
	s.pack = Pack{
		Root:      keys[0],
		Objects:   s.announced.Objects,
		Bytes:     s.announced.Bytes,
		DataSize:  s.announced.DataSize,
		IndexSize: s.announced.IndexSize,
		indexURL:  s.announced.IndexURL,
		dataURL:   s.announced.DataURL,
	}
	return s
}

// backToFront returns keys in the reverse of their order, so that a pack of
// them lies in another order than its index.
func backToFront(keys []key.Key) []key.Key {
	keys = slices.Clone(keys)
	slices.Reverse(keys)
	return keys
}

// clientOf returns a client of a server that answers every request alike.
func clientOf(resp wire.Response) (*Client, *fakeServer) {
	srv := &fakeServer{answer: func(wire.Request) wire.Response { return resp }}
	return New(srv, nil), srv
}

// fetched is what a fetch handed over.
type fetched struct {
	keys []key.Key
	data []byte
}

func (f *fetched) each(o packfile.Object) error {
	f.keys = append(f.keys, o.Key)
	f.data = append(f.data, o.Data...)
	return nil
}

func TestLookupOfAnUnknownName(t *testing.T) {
	c, srv := clientOf(wire.Errorf(wire.CodeNotFound, "no reference %q", "acme/none"))
	_, packs, err := c.Lookup(context.Background(), "acme/none")
	if !errors.Is(err, ErrNotFound) || packs != nil {
		t.Fatalf("Lookup = %v, %v, want ErrNotFound", packs, err)
	}
	asked := srv.requests()
	if len(asked) != 1 || asked[0].Op != wire.OpPull || asked[0].Name != "acme/none" {
		t.Fatalf("the server was asked %+v, want one pull of the name", asked)
	}

	// Another refusal is the server's own, not a name it lacks.
	c, _ = clientOf(wire.Errorf(wire.CodeInternal, "the database is away"))
	_, _, err = c.Lookup(context.Background(), "acme/none")
	var refusal *wire.Error
	if !errors.As(err, &refusal) || refusal.Code != wire.CodeInternal || errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup = %v, want the server's refusal", err)
	}
}

func TestLookupReturnsWhatTheServerNamed(t *testing.T) {
	objects, keys := blobs(t, 5)
	own := shelve(t, objects, keys[:2])
	parent := shelve(t, objects, keys[2:])
	root := keys[0]

	c, _ := clientOf(wire.Response{Root: root[:], Packs: []wire.Pack{own.announced, parent.announced}})
	got, packs, err := c.Lookup(context.Background(), "acme/ref")
	if err != nil {
		t.Fatal(err)
	}
	if got != root || len(packs) != 2 || packs[0] != own.pack || packs[1] != parent.pack {
		t.Fatalf("Lookup = %s, %+v, want %s and the two packs in the server's order", got, packs, root)
	}
	if packs[0].Root != keys[0] || packs[0].Objects != 2 || packs[1].Root != keys[2] || packs[1].Objects != 3 {
		t.Fatalf("the packs are %+v", packs)
	}
	// A lookup is the server's answer and no more.
	if asked := append(own.bucket.requests(), parent.bucket.requests()...); len(asked) != 0 {
		t.Fatalf("the lookup fetched %v from the bucket", asked)
	}

	c, _ = clientOf(wire.Response{Root: root[:], Packs: []wire.Pack{own.announced}})
	if _, packs, err = c.Lookup(context.Background(), "acme/ref"); err != nil || len(packs) != 1 || packs[0] != own.pack {
		t.Fatalf("Lookup of a reference in one pack = %+v, %v", packs, err)
	}
}

func TestLookupRefusesThreePacks(t *testing.T) {
	objects, keys := blobs(t, 3)
	pack := shelve(t, objects, keys).announced
	root := keys[0]
	odd := pack
	odd.Root = []byte("no key")

	for _, tc := range []struct {
		name string
		resp wire.Response
	}{
		{"three packs", wire.Response{Root: root[:], Packs: []wire.Pack{pack, pack, pack}}},
		{"no pack", wire.Response{Root: root[:]}},
		{"a root that is no key", wire.Response{Root: []byte("no key"), Packs: []wire.Pack{pack}}},
		{"a pack whose root is no key", wire.Response{Root: root[:], Packs: []wire.Pack{pack, odd}}},
	} {
		c, _ := clientOf(tc.resp)
		_, packs, err := c.Lookup(context.Background(), "acme/ref")
		if err == nil || packs != nil || errors.Is(err, ErrNotFound) {
			t.Errorf("%s: Lookup = %+v, %v, want the answer refused", tc.name, packs, err)
		}
	}
}

func TestFetchHandsOverObjectsInOffsetOrder(t *testing.T) {
	objects, keys := blobs(t, 7)
	// The data lies in another order than the index, which is by key.
	order := backToFront(keys)
	s := shelve(t, objects, order)

	// The pack as a lookup returns it, URLs and all.
	c, _ := clientOf(wire.Response{Root: order[0][:], Packs: []wire.Pack{s.announced}})
	_, packs, err := c.Lookup(context.Background(), "acme/ref")
	if err != nil {
		t.Fatal(err)
	}
	var got fetched
	index, err := c.Fetch(context.Background(), packs[0], got.each)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(index.Encode(), s.index.Encode()) {
		t.Fatal("Fetch returned another index than the pack's")
	}
	if !slices.Equal(got.keys, order) {
		t.Fatalf("the objects came as %v, want the order they lie in %v", got.keys, order)
	}
	var expanded bytes.Buffer
	if err := packfile.Expand(s.index, bytes.NewReader(s.bucket.data), &expanded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.data, expanded.Bytes()) {
		t.Fatalf("the objects back to back are %d bytes that differ from the %d the pack expands to", len(got.data), expanded.Len())
	}
	// Which makes the index's offsets positions in what was handed over.
	for i := range index.Len() {
		e := index.Entry(i)
		if !bytes.Equal(got.data[e.Offset:e.Offset+uint64(e.Length)], objects[e.Key]) {
			t.Fatalf("object %s is not at its offset %d", e.Key, e.Offset)
		}
	}
	if asked := s.bucket.requests(); !slices.Equal(asked, []string{"/index", "/data"}) {
		t.Fatalf("the bucket was asked for %v, want the index and then the data", asked)
	}
}

func TestFetchOfAPackWithoutBytes(t *testing.T) {
	// One empty object: an index of one entry and no data at all.
	blob, err := fstree.EncodeBlob(nil)
	if err != nil {
		t.Fatal(err)
	}
	s := shelve(t, objectMap{blob.Key: nil}, []key.Key{blob.Key})
	if len(s.bucket.data) != 0 {
		t.Fatalf("a pack without bytes has %d bytes of data", len(s.bucket.data))
	}
	var got fetched
	index, err := New(nil, nil).Fetch(context.Background(), s.pack, got.each)
	if err != nil {
		t.Fatal(err)
	}
	if index.Len() != 1 || len(got.keys) != 1 || got.keys[0] != blob.Key || len(got.data) != 0 {
		t.Fatalf("Fetch handed over %v, %d bytes", got.keys, len(got.data))
	}
}

func TestFetchRefusesAnObjectThatDoesNotMatchItsKey(t *testing.T) {
	objects, keys := blobs(t, 5)
	// The third object in the data is not the bytes its key names.
	wrong := keys[2]
	forged := objectMap{}
	for k, data := range objects {
		forged[k] = data
	}
	forged[wrong] = bytes.ToUpper(objects[wrong])
	s := shelve(t, forged, keys)

	var got fetched
	index, err := New(nil, nil).Fetch(context.Background(), s.pack, got.each)
	if !errors.Is(err, ErrBadPack) || index != nil {
		t.Fatalf("Fetch = %v, want ErrBadPack and no index", err)
	}
	if StatusCode(err) != 0 {
		t.Fatalf("a bad object has the HTTP status %d", StatusCode(err))
	}
	if !slices.Equal(got.keys, keys[:2]) {
		t.Fatalf("each was given %v, want the two sound objects before %s and nothing after", got.keys, wrong)
	}
}

func TestFetchRefusesAnIndexOtherThanAnnounced(t *testing.T) {
	objects, keys := blobs(t, 6)
	// As many objects as the packs below, of more bytes.
	longer, longerKeys := blobs(t, 100)
	other := shelve(t, longer, longerKeys[:3])
	garbage := bytes.Repeat([]byte{0xff}, int(packfile.IndexSize(3)))

	for _, tc := range []struct {
		name   string
		change func(s *shelved)
		// unasked: what the server announced is refused as it stands.
		unasked bool
	}{
		{"more bytes announced than the index gives", func(s *shelved) { s.pack.Bytes++ }, false},
		{"fewer bytes announced than the index gives", func(s *shelved) { s.pack.Bytes-- }, false},
		{"the index of another pack", func(s *shelved) { s.bucket.index = other.bucket.index }, false},
		{"an index that is none", func(s *shelved) { s.bucket.index = garbage }, false},
		{"an index shorter than announced", func(s *shelved) { s.bucket.index = s.bucket.index[:len(s.bucket.index)-1] }, false},
		{"an index longer than announced", func(s *shelved) { s.bucket.index = append(slices.Clone(s.bucket.index), 0) }, false},
		{"an index of fewer objects than announced", func(s *shelved) {
			s.pack.Objects, s.pack.IndexSize = 4, packfile.IndexSize(4)
		}, false},
		{"an index size that is not that of the objects", func(s *shelved) { s.pack.IndexSize++ }, true},
		{"more objects than a pack holds", func(s *shelved) {
			s.pack.Objects = packfile.MaxEntries + 1
			s.pack.IndexSize = packfile.IndexSize(s.pack.Objects)
		}, true},
	} {
		s := shelve(t, objects, keys[3:])
		tc.change(s)
		var got fetched
		index, err := New(nil, nil).Fetch(context.Background(), s.pack, got.each)
		if !errors.Is(err, ErrBadPack) || index != nil {
			t.Errorf("%s: Fetch = %v, want ErrBadPack and no index", tc.name, err)
		}
		if len(got.keys) != 0 {
			t.Errorf("%s: each was given %v", tc.name, got.keys)
		}
		asked := s.bucket.requests()
		if tc.unasked && len(asked) != 0 {
			t.Errorf("%s: the bucket was asked for %v", tc.name, asked)
		}
		// The data of a pack whose index is wrong is not asked for.
		if slices.Contains(asked, "/data") {
			t.Errorf("%s: the data was asked for all the same", tc.name)
		}
	}
}

func TestFetchRefusesDataOtherThanTheIndexDescribes(t *testing.T) {
	objects, keys := blobs(t, 6)
	other := shelve(t, objects, keys[:2])

	for _, tc := range []struct {
		name   string
		change func(s *shelved)
	}{
		{"data that is no zstd", func(s *shelved) { s.bucket.data = bytes.Repeat([]byte("x"), len(s.bucket.data)) }},
		{"the data of another pack", func(s *shelved) { s.bucket.data = other.bucket.data }},
		{"no data", func(s *shelved) { s.bucket.data = nil }},
		// No more is read than was announced, so the stream ends early.
		{"more data than announced", func(s *shelved) { s.pack.DataSize -= 2 }},
	} {
		s := shelve(t, objects, keys[2:])
		tc.change(s)
		_, err := New(nil, nil).Fetch(context.Background(), s.pack, func(packfile.Object) error { return nil })
		if !errors.Is(err, ErrBadPack) {
			t.Errorf("%s: Fetch = %v, want ErrBadPack", tc.name, err)
		}
	}
}

func TestFetchReportsTheBucketsStatus(t *testing.T) {
	objects, keys := blobs(t, 4)
	for _, path := range []string{"/index", "/data"} {
		s := shelve(t, objects, keys)
		// What a bucket answers to a URL that has expired.
		s.bucket.refuse = map[string]int{path: http.StatusForbidden}
		var got fetched
		index, err := New(nil, nil).Fetch(context.Background(), s.pack, got.each)
		if err == nil || index != nil {
			t.Fatalf("%s refused: Fetch = %v, want an error and no index", path, err)
		}
		if StatusCode(err) != http.StatusForbidden {
			t.Errorf("%s refused: StatusCode(%v) = %d, want 403", path, err, StatusCode(err))
		}
		if errors.Is(err, ErrBadPack) {
			t.Errorf("%s refused: %v is ErrBadPack, and the pack was never seen", path, err)
		}
		if len(got.keys) != 0 {
			t.Errorf("%s refused: each was given %v", path, got.keys)
		}
		// The status is found under what a caller wraps the error in.
		if got := StatusCode(fmt.Errorf("pack %s: %w", s.pack.Root, err)); got != http.StatusForbidden {
			t.Errorf("StatusCode of the error wrapped = %d, want 403", got)
		}
	}
	for _, err := range []error{nil, errors.New("no answer of a bucket"), ErrBadPack, ErrNotFound, context.Canceled} {
		if got := StatusCode(err); got != 0 {
			t.Errorf("StatusCode(%v) = %d, want 0", err, got)
		}
	}
}

func TestFetchTellsABrokenTransferFromABadPack(t *testing.T) {
	objects, keys := blobs(t, 40)
	for _, path := range []string{"/index", "/data"} {
		s := shelve(t, objects, keys)
		s.bucket.cut = map[string]bool{path: true}
		index, err := New(nil, nil).Fetch(context.Background(), s.pack, func(packfile.Object) error { return nil })
		if err == nil || index != nil {
			t.Fatalf("%s cut short: Fetch = %v, want an error and no index", path, err)
		}
		if errors.Is(err, ErrBadPack) {
			t.Errorf("%s cut short: %v is ErrBadPack, and it was the transfer that failed", path, err)
		}
		if StatusCode(err) != 0 {
			t.Errorf("%s cut short: StatusCode = %d, want 0", path, StatusCode(err))
		}
	}

	// A fetch that was called off says so.
	s := shelve(t, objects, keys)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := New(nil, nil).Fetch(ctx, s.pack, func(packfile.Object) error { return nil })
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrBadPack) {
		t.Fatalf("Fetch called off = %v, want context.Canceled", err)
	}
}

func TestFetchReturnsTheErrorOfEach(t *testing.T) {
	objects, keys := blobs(t, 5)
	s := shelve(t, objects, keys)
	full := errors.New("the disk is full")
	var given []key.Key
	index, err := New(nil, nil).Fetch(context.Background(), s.pack, func(o packfile.Object) error {
		given = append(given, o.Key)
		if len(given) == 2 {
			return full
		}
		return nil
	})
	if err != full || index != nil {
		t.Fatalf("Fetch = %v, want the error of each as it is and no index", err)
	}
	if errors.Is(err, ErrBadPack) {
		t.Fatalf("the error of each is ErrBadPack: %v", err)
	}
	if !slices.Equal(given, keys[:2]) {
		t.Fatalf("each was given %v, want nothing after the object it failed on", given)
	}
}
