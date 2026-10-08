package client

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amber-store/jaccard-store/wire"
)

func newTestClient() *Client {
	c := New(nil, nil)
	c.retryWait = time.Millisecond
	return c
}

func TestPutSendsLengthAndBodyAndReturnsETag(t *testing.T) {
	var got []byte
	var length int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method %s, want PUT", r.Method)
		}
		length = r.ContentLength
		got, _ = io.ReadAll(r.Body)
		w.Header().Set("ETag", `"abc"`)
	}))
	defer srv.Close()

	body := []byte("pack bytes")
	etag, err := newTestClient().putPart(context.Background(), "data", srv.URL+"/k?sig=s3cret", bytes.NewReader(body), 0, int64(len(body)), silent{})
	if err != nil {
		t.Fatal(err)
	}
	if etag != `"abc"` || !bytes.Equal(got, body) || length != int64(len(body)) {
		t.Fatalf("etag %q, body %q, Content-Length %d", etag, got, length)
	}
}

func TestCreateCarriesTheConditionAndAPartDoesNot(t *testing.T) {
	var conditions []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conditions = append(conditions, r.Header.Get("If-None-Match"))
		io.Copy(io.Discard, r.Body)
	}))
	defer srv.Close()
	c := newTestClient()
	if err := c.create(context.Background(), "index", srv.URL, strings.NewReader("x"), 1, silent{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.putPart(context.Background(), "part", srv.URL, strings.NewReader("x"), 0, 1, silent{}); err != nil {
		t.Fatal(err)
	}
	if len(conditions) != 2 || conditions[0] != "*" || conditions[1] != "" {
		t.Fatalf("If-None-Match was sent as %q, want it on the create alone", conditions)
	}
}

func TestCreateOfAnObjectThatExistsFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPreconditionFailed)
		io.WriteString(w, `<Error><Code>PreconditionFailed</Code><Message>At least one of the pre-conditions you specified did not hold</Message></Error>`)
	}))
	defer srv.Close()
	err := newTestClient().create(context.Background(), "data", srv.URL, strings.NewReader("x"), 1, silent{})
	if err == nil || !strings.Contains(err.Error(), "PreconditionFailed") {
		t.Fatalf("create = %v, want the bucket's refusal", err)
	}
}

func TestPutOfNothingSendsALengthOfZero(t *testing.T) {
	length := int64(-2)
	var encoding []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		length, encoding = r.ContentLength, r.TransferEncoding
	}))
	defer srv.Close()
	if err := newTestClient().create(context.Background(), "data", srv.URL, strings.NewReader(""), 0, silent{}); err != nil {
		t.Fatal(err)
	}
	if length != 0 || len(encoding) != 0 {
		t.Fatalf("Content-Length %d, Transfer-Encoding %v: want 0 and none", length, encoding)
	}
}

func TestRefusalIsReportedWithoutTheURL(t *testing.T) {
	// What S3 answers to a signature it refuses: the document repeats the
	// signature and the key ID it was made with.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>SignatureDoesNotMatch</Code>`+
			`<Message>The request signature we calculated does not match</Message>`+
			`<AWSAccessKeyId>AKIDs3cretid</AWSAccessKeyId><SignatureProvided>s3cretsig</SignatureProvided></Error>`)
	}))
	defer srv.Close()
	c := newTestClient()
	url := srv.URL + "/k?X-Amz-Signature=s3cret"

	err := c.create(context.Background(), "index", url, strings.NewReader("x"), 1, silent{})
	if err == nil || !strings.Contains(err.Error(), "403") || !strings.Contains(err.Error(), "SignatureDoesNotMatch") ||
		!strings.Contains(err.Error(), "does not match") {
		t.Fatalf("create: %v, want the status, the code and the message", err)
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("create: the error repeats a signature or a key ID: %v", err)
	}
	if _, err = c.get(context.Background(), "data", url); err == nil || strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("get: %v, want an error that does not quote the URL", err)
	}
}

func TestTransportErrorDoesNotQuoteTheURL(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL + "/k?X-Amz-Signature=s3cret"
	srv.Close() // nothing listens there any more
	_, err := newTestClient().get(context.Background(), "data", url)
	if err == nil || strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("get: %v, want an error that does not quote the URL", err)
	}
}

func TestGetAllHoldsTheObjectToItsSize(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "0123456789")
	}))
	defer srv.Close()
	c := newTestClient()
	b, err := c.getAll(context.Background(), "index", srv.URL, 10, silent{})
	if err != nil || string(b) != "0123456789" {
		t.Fatalf("getAll = %q, %v", b, err)
	}
	for _, size := range []uint64{9, 11, 0} {
		if _, err := c.getAll(context.Background(), "index", srv.URL, size, silent{}); err == nil {
			t.Errorf("getAll accepted 10 bytes as an object of %d", size)
		}
	}
}

// fakeMultipart is the bucket's side of a multipart upload.
type fakeMultipart struct {
	mu       sync.Mutex
	parts    map[string][]byte
	inFlight atomic.Int32
	peak     atomic.Int32
	complete completeMultipartUpload
	answer   string // the body of the completion's 200
	noETag   bool
}

func (f *fakeMultipart) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		n := f.inFlight.Add(1)
		for {
			p := f.peak.Load()
			if n <= p || f.peak.CompareAndSwap(p, n) {
				break
			}
		}
		defer f.inFlight.Add(-1)
		body, _ := io.ReadAll(r.Body)
		part := r.URL.Query().Get("partNumber")
		f.mu.Lock()
		f.parts[part] = body
		f.mu.Unlock()
		if !f.noETag {
			w.Header().Set("ETag", fmt.Sprintf(`"etag-%s"`, part))
		}
	case http.MethodPost:
		body, _ := io.ReadAll(r.Body)
		if err := xml.Unmarshal(body, &f.complete); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		io.WriteString(w, f.answer)
	}
}

func partsFor(base string, n int, partSize uint64) *wire.Parts {
	p := &wire.Parts{PartSize: partSize, CompleteURL: base + "/k?uploadId=u"}
	for i := 1; i <= n; i++ {
		p.URLs = append(p.URLs, fmt.Sprintf("%s/k?uploadId=u&partNumber=%d", base, i))
	}
	return p
}

const completed = `<CompleteMultipartUploadResult><Key>k</Key></CompleteMultipartUploadResult>`

func TestPutPartsUploadsEveryPartAndCompletes(t *testing.T) {
	f := &fakeMultipart{parts: map[string][]byte{}, answer: completed}
	srv := httptest.NewServer(f)
	defer srv.Close()

	data := bytes.Repeat([]byte("abcdefghij"), 25) // 250 bytes: parts of 100, 100, 50
	err := newTestClient().putParts(context.Background(), bytes.NewReader(data), int64(len(data)), partsFor(srv.URL, 3, 100), 2, silent{})
	if err != nil {
		t.Fatal(err)
	}
	var joined []byte
	for _, n := range []string{"1", "2", "3"} {
		joined = append(joined, f.parts[n]...)
	}
	if !bytes.Equal(joined, data) || len(f.parts["3"]) != 50 {
		t.Fatalf("the parts do not join to the data: %d, %d, %d bytes", len(f.parts["1"]), len(f.parts["2"]), len(f.parts["3"]))
	}
	if peak := f.peak.Load(); peak > 2 {
		t.Fatalf("%d parts were in flight at once, the limit was 2", peak)
	}
	if len(f.complete.Parts) != 3 {
		t.Fatalf("the completion lists %d parts, want 3", len(f.complete.Parts))
	}
	for i, p := range f.complete.Parts {
		if p.PartNumber != i+1 || p.ETag != fmt.Sprintf(`"etag-%d"`, i+1) {
			t.Fatalf("completion part %d is %+v", i, p)
		}
	}
}

func TestPutPartsTakesAnErrorBodyForAFailure(t *testing.T) {
	f := &fakeMultipart{parts: map[string][]byte{}, answer: `<Error><Code>InternalError</Code><Message>try again</Message></Error>`}
	srv := httptest.NewServer(f)
	defer srv.Close()
	err := newTestClient().putParts(context.Background(), strings.NewReader("0123456789"), 10, partsFor(srv.URL, 1, 100), 1, silent{})
	if err == nil || !strings.Contains(err.Error(), "InternalError") {
		t.Fatalf("putParts = %v, want the failure the body reports", err)
	}
}

func TestPutPartsNeedsAnETagOfEveryPart(t *testing.T) {
	f := &fakeMultipart{parts: map[string][]byte{}, answer: completed, noETag: true}
	srv := httptest.NewServer(f)
	defer srv.Close()
	err := newTestClient().putParts(context.Background(), strings.NewReader("0123456789"), 10, partsFor(srv.URL, 1, 100), 1, silent{})
	if err == nil || !strings.Contains(err.Error(), "ETag") {
		t.Fatalf("putParts = %v, want a complaint about the missing ETag", err)
	}
}

func TestPutPartsRefusesALayoutThatDoesNotFitTheData(t *testing.T) {
	c := newTestClient()
	data := strings.NewReader("0123456789")
	if err := c.putParts(context.Background(), data, 10, partsFor("http://unused.invalid", 3, 100), 1, silent{}); err == nil {
		t.Error("putParts accepted three URLs for one part")
	}
	if err := c.putParts(context.Background(), data, 10, partsFor("http://unused.invalid", 1, 0), 1, silent{}); err == nil {
		t.Error("putParts accepted a part size of zero")
	}
}

func TestABodyThatIsNotAnErrorDocumentIsNotRepeated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "proxy says: s3cret", http.StatusBadGateway)
	}))
	defer srv.Close()
	_, err := newTestClient().get(context.Background(), "data", srv.URL)
	if err == nil || !strings.Contains(err.Error(), "502") || strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("get: %v, want the status alone", err)
	}
}

// busy answers the first n PUTs of every part with 503.
type busy struct {
	fakeMultipart
	mu       sync.Mutex
	failures map[string]int
	n        int
	status   int
}

func (b *busy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPut {
		part := r.URL.Query().Get("partNumber")
		b.mu.Lock()
		b.failures[part]++
		fail := b.failures[part] <= b.n
		b.mu.Unlock()
		if fail {
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(b.status)
			return
		}
	}
	b.fakeMultipart.ServeHTTP(w, r)
}

func TestAPartIsSentAgainWhenTheBucketIsBusy(t *testing.T) {
	b := &busy{fakeMultipart: fakeMultipart{parts: map[string][]byte{}, answer: completed}, failures: map[string]int{}, n: 2, status: http.StatusServiceUnavailable}
	srv := httptest.NewServer(b)
	defer srv.Close()
	data := bytes.Repeat([]byte("abcdefghij"), 25)
	if err := newTestClient().putParts(context.Background(), bytes.NewReader(data), int64(len(data)), partsFor(srv.URL, 3, 100), 2, silent{}); err != nil {
		t.Fatal(err)
	}
	var joined []byte
	for _, n := range []string{"1", "2", "3"} {
		joined = append(joined, b.parts[n]...)
	}
	if !bytes.Equal(joined, data) {
		t.Fatal("the parts that were sent again do not join to the data")
	}
}

func TestAPartIsGivenUpAfterItsAttemptsAndAtOnceWhenRefused(t *testing.T) {
	b := &busy{fakeMultipart: fakeMultipart{parts: map[string][]byte{}, answer: completed}, failures: map[string]int{}, n: 1000, status: http.StatusServiceUnavailable}
	srv := httptest.NewServer(b)
	defer srv.Close()
	err := newTestClient().putParts(context.Background(), strings.NewReader("0123456789"), 10, partsFor(srv.URL, 1, 100), 1, silent{})
	if err == nil || b.failures["1"] != partAttempts {
		t.Fatalf("putParts = %v after %d attempts, want a failure after %d", err, b.failures["1"], partAttempts)
	}

	// A refusal is final: asking again changes nothing.
	b = &busy{fakeMultipart: fakeMultipart{parts: map[string][]byte{}, answer: completed}, failures: map[string]int{}, n: 1000, status: http.StatusForbidden}
	srv2 := httptest.NewServer(b)
	defer srv2.Close()
	err = newTestClient().putParts(context.Background(), strings.NewReader("0123456789"), 10, partsFor(srv2.URL, 1, 100), 1, silent{})
	if err == nil || b.failures["1"] != 1 {
		t.Fatalf("putParts = %v after %d attempts, want a failure after 1", err, b.failures["1"])
	}
}
