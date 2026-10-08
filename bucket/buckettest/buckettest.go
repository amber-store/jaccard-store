// Package buckettest gives tests a bucket.Bucket on an S3 that lives in the
// test's memory, so the packages that store or serve packs can be tested
// without a network.
package buckettest

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/amber-store/jaccard-store/bucket"
)

const (
	name   = "jaccard-test"
	region = "us-east-1"
)

// New starts a gofakes3 server on the s3mem backend, creates a bucket in it
// and returns a Bucket for that bucket: path style, region us-east-1, no key
// prefix. The server stops when the test ends.
//
// bucket.New takes its credentials from the default chain, so New sets static
// ones in the environment with t.Setenv, and points the SDK away from the
// configuration files of whoever runs the test. A test that calls New can
// therefore not be parallel.
//
// The fake does not check signatures or expiry times, and it does not hold
// multipart parts to S3's minimum of 5 MiB.
func New(t testing.TB) *bucket.Bucket {
	t.Helper()

	backend := s3mem.New()
	if err := backend.CreateBucket(name); err != nil {
		t.Fatalf("buckettest: create bucket: %v", err)
	}
	srv := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(srv.Close)

	absent := filepath.Join(t.TempDir(), "absent")
	t.Setenv("AWS_ACCESS_KEY_ID", "buckettest")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "buckettest-secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", absent)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", absent)

	b, err := bucket.New(context.Background(), bucket.Config{
		Bucket:    name,
		Endpoint:  srv.URL,
		Region:    region,
		PathStyle: true,
	})
	if err != nil {
		t.Fatalf("buckettest: %v", err)
	}
	return b
}
