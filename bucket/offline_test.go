package bucket

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/amber-store/core/key"
)

// These tests run without the fake of buckettest. A key is formatted and a
// URL pre-signed without a request, so they can cover the ways of addressing
// a bucket that the fake does not have; and the two that send a request want
// to see it, or to answer it as the fake would not.

// static returns a Bucket with fixed credentials instead of the default
// chain's. Like a loaded configuration, the one it is built on asks for a
// checksum on every upload.
func static(cfg Config, sessionToken string) *Bucket {
	return newFromConfig(aws.Config{
		Region:                     cfg.Region,
		Credentials:                credentials.NewStaticCredentialsProvider("AKIDEXAMPLE", "secret", sessionToken),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenSupported,
	}, cfg)
}

func TestKey(t *testing.T) {
	root := key.Key{0: 0xab, 1: 0x01, key.Size - 1: 0xff}
	rootHex := "ab01" + strings.Repeat("00", key.Size-3) + "ff"

	for _, tc := range []struct {
		name, prefix, want string
	}{
		{"no prefix", "", "packs/" + rootHex + "/u1.idx"},
		{"prefix with a slash", "stores/a/", "stores/a/packs/" + rootHex + "/u1.idx"},
		{"prefix without a slash", "stores/a", "stores/a/packs/" + rootHex + "/u1.idx"},
	} {
		b := static(Config{Bucket: "example-bucket", Region: "us-east-1", Prefix: tc.prefix}, "")
		if got := b.Key(root, "u1", "idx"); got != tc.want {
			t.Errorf("%s: Key = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// signCases are the ways of addressing a bucket that New can be asked for,
// most with a prefix that URI escaping has work to do on.
var signCases = []struct {
	name         string
	cfg          Config
	sessionToken string
	// base is the URL up to the "packs/" of a key.
	base string
	// signedFor is the region of the credential scope, where the endpoint
	// rules name another than the configured one.
	signedFor string
}{
	{
		name: "AWS",
		cfg:  Config{Bucket: "example-bucket", Region: "eu-central-1", Prefix: "a b/ü+&=~$/"},
		base: "https://example-bucket.s3.eu-central-1.amazonaws.com/a%20b/%C3%BC%2B%26%3D~%24/",
	},
	{
		name:         "AWS with a session token",
		cfg:          Config{Bucket: "example-bucket", Region: "eu-central-1"},
		sessionToken: "token/with+signs=",
		base:         "https://example-bucket.s3.eu-central-1.amazonaws.com/",
	},
	{
		name:      "AWS, the global pseudo-region",
		cfg:       Config{Bucket: "example-bucket", Region: "aws-global"},
		base:      "https://example-bucket.s3.amazonaws.com/",
		signedFor: "us-east-1",
	},
	{
		name: "AWS, a prefix that starts with a slash",
		cfg:  Config{Bucket: "example-bucket", Region: "eu-central-1", Prefix: "/lead"},
		base: "https://example-bucket.s3.eu-central-1.amazonaws.com//lead/",
	},
	{
		name: "AWS, path style",
		cfg:  Config{Bucket: "example-bucket", Region: "us-west-2", Prefix: "a b/", PathStyle: true},
		base: "https://s3.us-west-2.amazonaws.com/example-bucket/a%20b/",
	},
	{
		name: "endpoint, path style",
		cfg: Config{
			Bucket: "example-bucket", Region: "us-east-1", Prefix: "a b/ü+&=~$/",
			Endpoint: "http://minio.example.com:9000", PathStyle: true,
		},
		base: "http://minio.example.com:9000/example-bucket/a%20b/%C3%BC%2B%26%3D~%24/",
	},
	{
		name: "endpoint with a path, virtual host",
		cfg: Config{
			Bucket: "example-bucket", Region: "auto", Prefix: "a b/",
			Endpoint: "https://objects.example.com/s3",
		},
		base: "https://example-bucket.objects.example.com/s3/a%20b/",
	},
}

// The SDK cannot pre-sign a completion, but it can pre-sign a part, so the
// signing that PresignComplete relies on is checked by signing a part's PUT
// by hand, at the SDK's own timestamp, and expecting the SDK's URL to the
// byte. The fake could not tell a good signature from a bad one.
func TestSignMatchesSDK(t *testing.T) {
	ctx := context.Background()
	for _, tc := range signCases {
		b := static(tc.cfg, tc.sessionToken)
		objectKey := b.Key(key.Key{0: 7}, "u7", "data")
		const uploadID = "upload/7 +="

		want, err := b.PresignPart(ctx, objectKey, uploadID, 3, time.Hour)
		if err != nil {
			t.Fatalf("%s: PresignPart: %v", tc.name, err)
		}
		u, err := url.Parse(want)
		if err != nil {
			t.Fatal(err)
		}
		at, err := time.Parse("20060102T150405Z", u.Query().Get("X-Amz-Date"))
		if err != nil {
			t.Fatalf("%s: X-Amz-Date of %s: %v", tc.name, want, err)
		}

		got, err := b.sign(ctx, http.MethodPut, objectKey, url.Values{
			"partNumber": {"3"},
			"uploadId":   {uploadID},
			"x-id":       {"UploadPart"},
		}, time.Hour, at)
		if err != nil {
			t.Fatalf("%s: sign: %v", tc.name, err)
		}
		if got != want {
			t.Errorf("%s: signed by hand\n  %s\nthe SDK\n  %s", tc.name, got, want)
		}
	}
}

func TestPresignCompleteAddressesTheObject(t *testing.T) {
	ctx := context.Background()
	for _, tc := range signCases {
		b := static(tc.cfg, tc.sessionToken)
		objectKey := b.Key(key.Key{}, "u8", "data")

		putURL, err := b.PresignPut(ctx, objectKey, time.Hour)
		if err != nil {
			t.Fatalf("%s: PresignPut: %v", tc.name, err)
		}
		completeURL, err := b.PresignComplete(ctx, objectKey, "upload-8", time.Hour)
		if err != nil {
			t.Fatalf("%s: PresignComplete: %v", tc.name, err)
		}
		put, err := url.Parse(putURL)
		if err != nil {
			t.Fatal(err)
		}
		complete, err := url.Parse(completeURL)
		if err != nil {
			t.Fatal(err)
		}

		object := tc.base + "packs/" + strings.Repeat("00", key.Size) + "/u8.data?"
		if !strings.HasPrefix(completeURL, object) {
			t.Errorf("%s: PresignComplete = %s, want it to start with %s", tc.name, completeURL, object)
		}
		if complete.Scheme != put.Scheme || complete.Host != put.Host || complete.EscapedPath() != put.EscapedPath() {
			t.Errorf("%s: completion URL addresses %s://%s%s, PresignPut %s://%s%s", tc.name,
				complete.Scheme, complete.Host, complete.EscapedPath(), put.Scheme, put.Host, put.EscapedPath())
		}
		q := complete.Query()
		if q.Get("uploadId") != "upload-8" || q.Get("X-Amz-SignedHeaders") != "host" || q.Get("X-Amz-Expires") != "3600" {
			t.Errorf("%s: completion URL query: %s", tc.name, complete.RawQuery)
		}
		if got := q.Get("X-Amz-Security-Token"); got != tc.sessionToken {
			t.Errorf("%s: X-Amz-Security-Token = %q, want %q", tc.name, got, tc.sessionToken)
		}
		region := tc.signedFor
		if region == "" {
			region = tc.cfg.Region
		}
		wantScope := "/" + region + "/s3/aws4_request"
		if got := q.Get("X-Amz-Credential"); !strings.HasPrefix(got, "AKIDEXAMPLE/") || !strings.HasSuffix(got, wantScope) {
			t.Errorf("%s: X-Amz-Credential = %q, want AKIDEXAMPLE/<date>%s", tc.name, got, wantScope)
		}
	}
}

func TestPresignRefusesTTLOutOfRange(t *testing.T) {
	ctx := context.Background()
	b := static(Config{Bucket: "example-bucket", Region: "us-east-1"}, "")
	presigners := map[string]func(time.Duration) (string, error){
		"PresignGet": func(ttl time.Duration) (string, error) { return b.PresignGet(ctx, "k", ttl) },
		"PresignPut": func(ttl time.Duration) (string, error) { return b.PresignPut(ctx, "k", ttl) },
		"PresignPart": func(ttl time.Duration) (string, error) {
			return b.PresignPart(ctx, "k", "upload", 1, ttl)
		},
		"PresignComplete": func(ttl time.Duration) (string, error) {
			return b.PresignComplete(ctx, "k", "upload", ttl)
		},
	}
	for name, presign := range presigners {
		for _, ttl := range []time.Duration{-time.Hour, 0, time.Second - 1, 7*24*time.Hour + time.Second} {
			if rawURL, err := presign(ttl); err == nil {
				t.Errorf("%s with a lifetime of %v gave %s, want an error", name, ttl, rawURL)
			}
		}
		for _, ttl := range []time.Duration{time.Second, 7 * 24 * time.Hour} {
			if _, err := presign(ttl); err != nil {
				t.Errorf("%s with a lifetime of %v: %v", name, ttl, err)
			}
		}
	}
}

// Put goes to services that may not know the checksums the SDK adds to
// uploads unless told otherwise.
func TestPutSendsNoChecksum(t *testing.T) {
	var sent http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent = r.Header.Clone()
	}))
	defer srv.Close()

	b := static(Config{Bucket: "example-bucket", Region: "us-east-1", Endpoint: srv.URL, PathStyle: true}, "")
	if err := b.Put(context.Background(), "packs/00/u1.idx", []byte("an index")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if sent.Get("Content-Length") != "8" {
		t.Errorf("Content-Length = %q, want 8", sent.Get("Content-Length"))
	}
	for name := range sent {
		switch lower := strings.ToLower(name); {
		case strings.HasPrefix(lower, "x-amz-checksum-"), lower == "x-amz-sdk-checksum-algorithm", lower == "x-amz-trailer":
			t.Errorf("Put sent the header %s", name)
		}
	}
}

// S3 and the fake answer the DELETE of a missing object with 204. Some
// S3-compatible services answer 404 NoSuchKey, which is played here.
func TestDeleteTakesNoSuchKeyForSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("request is a %s, want DELETE", r.Method)
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>no such key</Message></Error>`)
	}))
	defer srv.Close()

	b := static(Config{Bucket: "example-bucket", Region: "us-east-1", Endpoint: srv.URL, PathStyle: true}, "")
	if err := b.Delete(context.Background(), "packs/00/u1.idx"); err != nil {
		t.Fatalf("Delete answered with NoSuchKey: %v, want success", err)
	}
}
