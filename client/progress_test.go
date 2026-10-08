package client

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// tally is a Progress that adds up what it is told.
type tally struct {
	mu sync.Mutex
	n  int64
}

func (t *tally) Begin(string, uint64, Unit) {}
func (t *tally) End(string)                 {}

func (t *tally) Advance(n int64) {
	t.mu.Lock()
	t.n += n
	t.mu.Unlock()
}

func (t *tally) count() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.n
}

func TestAnUploadReportsItsBytes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
	}))
	defer srv.Close()
	body := bytes.Repeat([]byte("x"), 100_000)
	var seen tally
	if err := newTestClient().create(context.Background(), "data", srv.URL, bytes.NewReader(body), int64(len(body)), &seen); err != nil {
		t.Fatal(err)
	}
	if seen.count() != int64(len(body)) {
		t.Fatalf("reported %d bytes of %d", seen.count(), len(body))
	}
}

func TestARefusedUploadTakesItsBytesBack(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()
	body := bytes.Repeat([]byte("x"), 100_000)
	var seen tally
	if err := newTestClient().create(context.Background(), "data", srv.URL, bytes.NewReader(body), int64(len(body)), &seen); err == nil {
		t.Fatal("create succeeded against a bucket that refuses")
	}
	if seen.count() != 0 {
		t.Fatalf("%d bytes are still counted of an upload that did not arrive", seen.count())
	}
}

func TestAPartSentAgainIsCountedOnce(t *testing.T) {
	b := &busy{fakeMultipart: fakeMultipart{parts: map[string][]byte{}, answer: completed}, failures: map[string]int{}, n: 2, status: http.StatusServiceUnavailable}
	srv := httptest.NewServer(b)
	defer srv.Close()
	data := bytes.Repeat([]byte("abcdefghij"), 25)
	var seen tally
	if err := newTestClient().putParts(context.Background(), bytes.NewReader(data), int64(len(data)), partsFor(srv.URL, 3, 100), 2, &seen); err != nil {
		t.Fatal(err)
	}
	if seen.count() != int64(len(data)) {
		t.Fatalf("reported %d bytes of %d after parts were sent three times", seen.count(), len(data))
	}
}

// A redirected PUT is sent twice by the HTTP client itself, which needs the
// body a second time; its bytes count once all the same.
func TestARedirectedUploadIsSentAgainAndCountedOnce(t *testing.T) {
	var got []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path != "/there" {
			http.Redirect(w, r, "/there", http.StatusTemporaryRedirect)
			return
		}
		got = body
		w.Header().Set("ETag", `"e"`)
	}))
	defer srv.Close()
	body := bytes.Repeat([]byte("y"), 50_000)
	var seen tally
	etag, err := newTestClient().putPart(context.Background(), "part", srv.URL+"/here", bytes.NewReader(body), 0, int64(len(body)), &seen)
	if err != nil || etag != `"e"` {
		t.Fatalf("putPart = %q, %v", etag, err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("the redirect's target received %d bytes of %d", len(got), len(body))
	}
	if seen.count() != int64(len(body)) {
		t.Fatalf("reported %d bytes of %d", seen.count(), len(body))
	}
}

func TestADownloadReportsItsBytes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(bytes.Repeat([]byte("z"), 4096))
	}))
	defer srv.Close()
	var seen tally
	if _, err := newTestClient().getAll(context.Background(), "index", srv.URL, 4096, &seen); err != nil {
		t.Fatal(err)
	}
	if seen.count() != 4096 {
		t.Fatalf("reported %d bytes of 4096", seen.count())
	}
}

func TestAMeterStopsReportingOnceItsBytesAreTakenBack(t *testing.T) {
	var seen tally
	m := &meter{r: bytes.NewReader(make([]byte, 100)), p: &seen}
	buf := make([]byte, 40)
	m.Read(buf)
	m.undo()
	m.Read(buf)
	m.undo()
	if seen.count() != 0 {
		t.Fatalf("%d bytes counted after they were taken back", seen.count())
	}
	(*meter)(nil).undo()
}
