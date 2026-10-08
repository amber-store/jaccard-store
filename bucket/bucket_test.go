package bucket_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/core/key"

	"github.com/amber-store/jaccard-store/bucket"
	"github.com/amber-store/jaccard-store/bucket/buckettest"
)

var ctx = context.Background()

// send performs a request the way the client does: plain net/http, nothing
// set but the body and with it the content length.
func send(t *testing.T, method, rawURL string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, rawURL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		answer, _ := io.ReadAll(resp.Body)
		t.Fatalf("%s %s: %s: %s", method, rawURL, resp.Status, answer)
	}
	return resp
}

func get(t *testing.T, b *bucket.Bucket, objectKey string) []byte {
	t.Helper()
	body, err := b.Get(ctx, objectKey)
	if err != nil {
		t.Fatalf("Get %s: %v", objectKey, err)
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read %s: %v", objectKey, err)
	}
	return data
}

func query(t *testing.T, rawURL string) (*url.URL, url.Values) {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	return u, u.Query()
}

func TestPutSizeGetDelete(t *testing.T) {
	b := buckettest.New(t)
	objectKey := b.Key(key.Key{0: 1}, "u1", "idx")
	want := []byte("an index")

	if err := b.Put(ctx, objectKey, want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	size, err := b.Size(ctx, objectKey)
	if err != nil || size != int64(len(want)) {
		t.Fatalf("Size = %d, %v; want %d", size, err, len(want))
	}
	if got := get(t, b, objectKey); !bytes.Equal(got, want) {
		t.Fatalf("Get = %q, want %q", got, want)
	}
	if err := b.Delete(ctx, objectKey); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := b.Size(ctx, objectKey); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("Size after Delete: %v, want ErrNotFound", err)
	}
}

func TestPutEmptyObject(t *testing.T) {
	b := buckettest.New(t)
	objectKey := b.Key(key.Key{0: 1}, "u1", "links")

	if err := b.Put(ctx, objectKey, nil); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if size, err := b.Size(ctx, objectKey); err != nil || size != 0 {
		t.Fatalf("Size = %d, %v; want 0", size, err)
	}
}

func TestAbsentKey(t *testing.T) {
	b := buckettest.New(t)
	objectKey := b.Key(key.Key{0: 1}, "nobody", "data")

	if _, err := b.Size(ctx, objectKey); !errors.Is(err, bucket.ErrNotFound) {
		t.Errorf("Size: %v, want ErrNotFound", err)
	}
	if body, err := b.Get(ctx, objectKey); !errors.Is(err, bucket.ErrNotFound) {
		t.Errorf("Get: %v, want ErrNotFound", err)
		if err == nil {
			body.Close()
		}
	}
	if err := b.Delete(ctx, objectKey); err != nil {
		t.Errorf("Delete: %v, want success", err)
	}
}

// A pre-signed PUT creates the object and can never replace it: whoever
// holds the URL must not be able to change an object after the server has
// read it.
func TestPresignPutCreatesAndNeverReplaces(t *testing.T) {
	b := buckettest.New(t)
	objectKey := b.Key(key.Key{0: 2}, "u2", "data")
	want := []byte("compressed data")

	rawURL, err := b.PresignPut(ctx, objectKey, time.Hour)
	if err != nil {
		t.Fatalf("PresignPut: %v", err)
	}
	// The condition is under the signature, so a request without the header,
	// or with another value, is not the request that was signed.
	if _, q := query(t, rawURL); q.Get("X-Amz-SignedHeaders") != "host;if-none-match" || q.Get("X-Amz-Expires") != "3600" {
		t.Fatalf("PresignPut signs %q for %q seconds, want host;if-none-match and 3600: %s",
			q.Get("X-Amz-SignedHeaders"), q.Get("X-Amz-Expires"), rawURL)
	}
	if status := conditionalPut(t, rawURL, want); status != http.StatusOK {
		t.Fatalf("the first PUT answered %d", status)
	}
	if got := get(t, b, objectKey); !bytes.Equal(got, want) {
		t.Fatalf("object = %q, want %q", got, want)
	}

	if status := conditionalPut(t, rawURL, []byte("something else")); status != http.StatusPreconditionFailed {
		t.Fatalf("a second PUT to the same URL answered %d, want 412", status)
	}
	if got := get(t, b, objectKey); !bytes.Equal(got, want) {
		t.Fatalf("the object was replaced: %q", got)
	}
}

// conditionalPut sends body the way a client must use a PresignPut URL.
func conditionalPut(t *testing.T, rawURL string, body []byte) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, rawURL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("If-None-Match", bucket.PutCondition)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func TestPresignGetReturnsObject(t *testing.T) {
	b := buckettest.New(t)
	objectKey := b.Key(key.Key{0: 3}, "u3", "idx")
	want := []byte("an index")
	if err := b.Put(ctx, objectKey, want); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rawURL, err := b.PresignGet(ctx, objectKey, time.Hour)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	if _, q := query(t, rawURL); q.Get("X-Amz-SignedHeaders") != "host" {
		t.Fatalf("PresignGet signs %q, want host: %s", q.Get("X-Amz-SignedHeaders"), rawURL)
	}
	resp, err := http.Get(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, want) {
		t.Fatalf("GET = %s, %q, %v; want %q", resp.Status, got, err, want)
	}
}

// The parts are far under S3's minimum of 5 MiB for every part but the last;
// gofakes3 does not enforce it.
func TestMultipartThroughPresignedURLs(t *testing.T) {
	b := buckettest.New(t)
	objectKey := b.Key(key.Key{0: 4}, "u4", "data")
	parts := [][]byte{
		bytes.Repeat([]byte("a"), 4096),
		bytes.Repeat([]byte("b"), 4096),
		[]byte("the short last part"),
	}

	uploadID, err := b.CreateMultipart(ctx, objectKey)
	if err != nil || uploadID == "" {
		t.Fatalf("CreateMultipart = %q, %v", uploadID, err)
	}

	var completion strings.Builder
	completion.WriteString("<CompleteMultipartUpload>")
	for i, part := range parts {
		number := int32(i + 1)
		rawURL, err := b.PresignPart(ctx, objectKey, uploadID, number, time.Hour)
		if err != nil {
			t.Fatalf("PresignPart %d: %v", number, err)
		}
		if _, q := query(t, rawURL); q.Get("X-Amz-SignedHeaders") != "host" {
			t.Fatalf("PresignPart signs %q, want host: %s", q.Get("X-Amz-SignedHeaders"), rawURL)
		}
		etag := send(t, http.MethodPut, rawURL, part).Header.Get("ETag")
		if etag == "" {
			t.Fatalf("part %d: no ETag in the response", number)
		}
		fmt.Fprintf(&completion, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", number, etag)
	}
	completion.WriteString("</CompleteMultipartUpload>")

	if _, err := b.Size(ctx, objectKey); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("Size before completion: %v, want ErrNotFound", err)
	}
	rawURL, err := b.PresignComplete(ctx, objectKey, uploadID, time.Hour)
	if err != nil {
		t.Fatalf("PresignComplete: %v", err)
	}
	send(t, http.MethodPost, rawURL, []byte(completion.String()))

	want := bytes.Join(parts, nil)
	if got := get(t, b, objectKey); !bytes.Equal(got, want) {
		t.Fatalf("object is %d bytes, want the %d of the joined parts", len(got), len(want))
	}
	if size, err := b.Size(ctx, objectKey); err != nil || size != int64(len(want)) {
		t.Fatalf("Size = %d, %v; want %d", size, err, len(want))
	}
	if err := b.AbortMultipart(ctx, objectKey, uploadID); err != nil {
		t.Fatalf("AbortMultipart of a finished upload: %v", err)
	}
	if got := get(t, b, objectKey); !bytes.Equal(got, want) {
		t.Fatal("AbortMultipart of a finished upload changed the object")
	}
}

// The fake checks no signature, so this is the test of what the completion
// URL carries: the object's address as the SDK writes it, the upload, and a
// SigV4 query signature over nothing but the host.
func TestPresignCompleteShape(t *testing.T) {
	b := buckettest.New(t)
	objectKey := b.Key(key.Key{0: 5}, "u5", "data")

	putURL, err := b.PresignPut(ctx, objectKey, time.Hour)
	if err != nil {
		t.Fatalf("PresignPut: %v", err)
	}
	rawURL, err := b.PresignComplete(ctx, objectKey, "upload/5 +", 90*time.Minute)
	if err != nil {
		t.Fatalf("PresignComplete: %v", err)
	}
	put, _ := query(t, putURL)
	complete, q := query(t, rawURL)

	if complete.Scheme != put.Scheme || complete.Host != put.Host || complete.EscapedPath() != put.EscapedPath() {
		t.Errorf("completion URL addresses %s://%s%s, PresignPut %s://%s%s",
			complete.Scheme, complete.Host, complete.EscapedPath(), put.Scheme, put.Host, put.EscapedPath())
	}
	if !strings.HasSuffix(complete.Path, "/"+objectKey) {
		t.Errorf("path %q does not end in the key %q", complete.Path, objectKey)
	}
	for name, want := range map[string]string{
		"uploadId":            "upload/5 +",
		"X-Amz-Algorithm":     "AWS4-HMAC-SHA256",
		"X-Amz-Expires":       "5400",
		"X-Amz-SignedHeaders": "host",
	} {
		if got := q.Get(name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	if got := q.Get("X-Amz-Credential"); !strings.HasSuffix(got, "/us-east-1/s3/aws4_request") {
		t.Errorf("X-Amz-Credential = %q, want the scope us-east-1/s3", got)
	}
	if _, err := time.Parse("20060102T150405Z", q.Get("X-Amz-Date")); err != nil {
		t.Errorf("X-Amz-Date = %q: %v", q.Get("X-Amz-Date"), err)
	}
	if got := q.Get("X-Amz-Signature"); len(got) != 64 || strings.Trim(got, "0123456789abcdef") != "" {
		t.Errorf("X-Amz-Signature = %q, want 64 hex digits", got)
	}
	if q.Has("X-Amz-Security-Token") {
		t.Error("X-Amz-Security-Token present without a session token")
	}
}

func TestAbortMultipart(t *testing.T) {
	b := buckettest.New(t)
	objectKey := b.Key(key.Key{0: 6}, "u6", "data")

	if err := b.AbortMultipart(ctx, objectKey, "no-such-upload"); err != nil {
		t.Errorf("AbortMultipart of an unknown upload: %v", err)
	}

	uploadID, err := b.CreateMultipart(ctx, objectKey)
	if err != nil {
		t.Fatalf("CreateMultipart: %v", err)
	}
	rawURL, err := b.PresignPart(ctx, objectKey, uploadID, 1, time.Hour)
	if err != nil {
		t.Fatalf("PresignPart: %v", err)
	}
	send(t, http.MethodPut, rawURL, []byte("a part nobody will complete"))

	if err := b.AbortMultipart(ctx, objectKey, uploadID); err != nil {
		t.Fatalf("AbortMultipart of an open upload: %v", err)
	}
	if err := b.AbortMultipart(ctx, objectKey, uploadID); err != nil {
		t.Errorf("AbortMultipart of an aborted upload: %v", err)
	}
	if _, err := b.Size(ctx, objectKey); !errors.Is(err, bucket.ErrNotFound) {
		t.Errorf("Size after abort: %v, want ErrNotFound", err)
	}

	req, err := http.NewRequest(http.MethodPut, rawURL, strings.NewReader("too late"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("part PUT after abort: %s, want 404", resp.Status)
	}
}

func TestNewNeedsBucketName(t *testing.T) {
	if _, err := bucket.New(ctx, bucket.Config{Region: "us-east-1"}); err == nil {
		t.Fatal("New without a bucket name succeeded")
	}
}

func TestNewNeedsRegion(t *testing.T) {
	absent := filepath.Join(t.TempDir(), "absent")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", absent)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", absent)

	if _, err := bucket.New(ctx, bucket.Config{Bucket: "example-bucket"}); err == nil {
		t.Fatal("New succeeded with no region given and none in the environment")
	}
}
