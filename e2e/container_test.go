package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/jaccard-store/bucket"
	"github.com/amber-store/jaccard-store/server"
	"github.com/amber-store/jaccard-store/wire"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// service is an S3 implementation that the tests run in a container. Unlike
// the fake in memory it checks what S3 checks: the signature of every
// request, the headers that were signed, when a URL expires, and how small
// a part may be. One container serves the whole test run; every test gets a
// bucket of its own in it.
type service struct {
	name  string
	image string
	// userVar and passwordVar name the variables the image takes its root
	// credentials from.
	userVar, passwordVar string
	cmd                  []string
	// health is a path that answers 200 once the service takes requests.
	health string

	once     sync.Once
	ctr      testcontainers.Container
	endpoint string
	err      error
}

const (
	serviceUser     = "jaccardtest"
	servicePassword = "jaccardtest-secret"
	serviceRegion   = "us-east-1"
)

// rustfs is the service the tests use. The image can be changed through the
// environment, to try another release.
var rustfs = &service{
	name:        "rustfs",
	image:       envOr("JACCARD_TEST_RUSTFS_IMAGE", "rustfs/rustfs:1.0.1"),
	userVar:     "RUSTFS_ACCESS_KEY",
	passwordVar: "RUSTFS_SECRET_KEY",
	health:      "/health",
}

// minio is a second service, run only when JACCARD_TEST_MINIO_IMAGE names
// an image for it. MinIO's own images have left the public registries, so
// there is none to pin here; builds by others exist, for example
// cgr.dev/chainguard/minio:latest.
var minio = &service{
	name:        "minio",
	image:       os.Getenv("JACCARD_TEST_MINIO_IMAGE"),
	userVar:     "MINIO_ROOT_USER",
	passwordVar: "MINIO_ROOT_PASSWORD",
	cmd:         []string{"server", "/data"},
	health:      "/minio/health/live",
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// start runs the container and waits until the service answers.
func (s *service) start() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithEnv(map[string]string{s.userVar: serviceUser, s.passwordVar: servicePassword}),
		testcontainers.WithWaitStrategy(wait.ForHTTP(s.health).WithPort("9000/tcp").WithStartupTimeout(3 * time.Minute)),
	}
	if len(s.cmd) > 0 {
		opts = append(opts, testcontainers.WithCmd(s.cmd...))
	}
	s.ctr, s.err = testcontainers.Run(ctx, s.image, opts...)
	if s.err != nil {
		return
	}
	s.endpoint, s.err = s.ctr.PortEndpoint(ctx, "9000/tcp", "http")
}

// stop removes the container, if one was started.
func (s *service) stop() {
	if s.ctr != nil {
		if err := testcontainers.TerminateContainer(s.ctr); err != nil {
			fmt.Fprintf(os.Stderr, "removing the %s container: %v\n", s.name, err)
		}
	}
}

// bucket returns a Bucket on a new bucket of the service. It skips the test
// in short mode and where there is no Docker to run the container.
//
// bucket.New takes its credentials from the default chain, so they are set
// in the environment with t.Setenv, and the SDK is pointed away from the
// configuration files of whoever runs the test. A test that uses a service
// can therefore not be parallel.
func (s *service) bucket(t *testing.T) *bucket.Bucket {
	t.Helper()
	if testing.Short() {
		t.Skipf("%s runs in a container; not in short mode", s.name)
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	s.once.Do(s.start)
	if s.err != nil {
		t.Fatalf("starting %s from %s: %v", s.name, s.image, s.err)
	}

	absent := filepath.Join(t.TempDir(), "absent")
	t.Setenv("AWS_ACCESS_KEY_ID", serviceUser)
	t.Setenv("AWS_SECRET_ACCESS_KEY", servicePassword)
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", absent)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", absent)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	name := "jaccard-" + hex.EncodeToString(suffix[:])
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(serviceRegion))
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s.endpoint)
		o.UsePathStyle = true
	})
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
		t.Fatalf("creating a bucket on %s: %v", s.name, err)
	}
	b, err := bucket.New(ctx, bucket.Config{Bucket: name, Endpoint: s.endpoint, Region: serviceRegion, PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestMain(m *testing.M) {
	if minio.image != "" {
		backends = append(backends, backend{name: minio.name, bucket: minio.bucket, partSize: 5 << 20, strict: true})
	}
	if externalConfigured() {
		backends = append(backends, backend{name: "external", bucket: externalBucket, partSize: 5 << 20, strict: true})
	}
	code := m.Run()
	rustfs.stop()
	minio.stop()
	os.Exit(code)
}

// onStrictBuckets runs a scenario against the backends that check what S3
// checks. The fake would pass these tests for the wrong reason or fail them
// for no reason: it looks at no signature and no expiry.
func onStrictBuckets(t *testing.T, scenario func(t *testing.T, s3 backend)) {
	for _, b := range backends {
		if b.strict {
			t.Run(b.name, func(t *testing.T) { scenario(t, b) })
		}
	}
}

// send makes one request to a pre-signed URL and returns the status.
func send(t *testing.T, method, rawURL string, header map[string]string, body []byte) int {
	t.Helper()
	req, err := http.NewRequest(method, rawURL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range header {
		req.Header.Set(name, value)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	io.Copy(io.Discard, res.Body)
	return res.StatusCode
}

// refused fails the test unless status is a refusal by the service.
func refused(t *testing.T, what string, status int) {
	t.Helper()
	if status < 400 || status >= 500 {
		t.Errorf("%s answered %d, want a refusal", what, status)
	}
}

// The URL of an upload is signed for a PUT that carries the condition. The
// service has to hold a client to it: the condition is what keeps a pack
// from being replaced after the server verified it, and a client that could
// leave it out would be free to do just that.
func TestTheBucketHoldsAnUploadToItsCondition(t *testing.T) {
	onStrictBuckets(t, func(t *testing.T, s3 backend) {
		w := newWorld(t, s3, nil)
		mallory := w.peer("mallory")
		root := mallory.ingest(version1())
		first, second := []byte("the bytes the server verifies"), []byte("the bytes put in their place")

		up := mallory.call(wire.Request{Op: wire.OpPushUpload, Name: "x", Root: root[:], DataSize: uint64(len(first)), Objects: 1, Bytes: 100})
		if up.Error != nil {
			t.Fatal(up.Error)
		}
		u, err := w.db.UploadByID(w.ctx, up.UploadID)
		if err != nil {
			t.Fatal(err)
		}
		condition := map[string]string{"If-None-Match": bucket.PutCondition}

		refused(t, "a PUT without the condition", send(t, http.MethodPut, up.DataURL, nil, first))
		refused(t, "a PUT with another condition", send(t, http.MethodPut, up.DataURL, map[string]string{"If-None-Match": `"0123"`}, first))
		refused(t, "a GET through a URL signed for a PUT", send(t, http.MethodGet, up.DataURL, nil, nil))
		elsewhere := strings.Replace(up.DataURL, up.UploadID, "0123456789abcdef0123456789abcdef", 1)
		if elsewhere == up.DataURL {
			t.Fatal("the upload ID is not in the URL")
		}
		refused(t, "a PUT to another key under the same signature", send(t, http.MethodPut, elsewhere, condition, first))
		if w.present(u.DataKey) {
			t.Fatal("a refused request left an object behind")
		}

		if status := send(t, http.MethodPut, up.DataURL, condition, first); status != http.StatusOK {
			t.Fatalf("the PUT the URL was signed for answered %d", status)
		}
		if status := send(t, http.MethodPut, up.DataURL, condition, second); status != http.StatusPreconditionFailed {
			t.Errorf("a second PUT answered %d, want 412", status)
		}
		refused(t, "a second PUT without the condition", send(t, http.MethodPut, up.DataURL, nil, second))

		body, err := w.bucket.Get(w.ctx, u.DataKey)
		if err != nil {
			t.Fatal(err)
		}
		defer body.Close()
		if got, _ := io.ReadAll(body); !bytes.Equal(got, first) {
			t.Fatalf("the object was replaced: %q", got)
		}
	})
}

// An upload's URLs stop working at its deadline. What is cleared after the
// deadline stays cleared only if they do.
func TestAnUploadURLEndsWithItsDeadline(t *testing.T) {
	onStrictBuckets(t, func(t *testing.T, s3 backend) {
		const lifetime = 2 * time.Second
		w := newWorld(t, s3, func(c *server.Config) { c.UploadTimeout = lifetime })
		alice := w.peer("alice")
		root := alice.ingest(version1())
		up := alice.call(wire.Request{Op: wire.OpPushUpload, Name: "x", Root: root[:], DataSize: 4, Objects: 1, Bytes: 100})
		if up.Error != nil {
			t.Fatal(up.Error)
		}
		u, err := w.db.UploadByID(w.ctx, up.UploadID)
		if err != nil {
			t.Fatal(err)
		}
		condition := map[string]string{"If-None-Match": bucket.PutCondition}

		time.Sleep(lifetime + 2*time.Second)
		refused(t, "a PUT after the deadline", send(t, http.MethodPut, up.DataURL, condition, []byte("late")))
		refused(t, "a PUT of the index after the deadline", send(t, http.MethodPut, up.IndexURL, condition, []byte("late")))
		if w.present(u.DataKey) || w.present(u.IndexKey) {
			t.Fatal("an expired URL still wrote an object")
		}
	})
}

// The URL that completes a multipart upload is signed by hand, because the
// SDK signs none. Completing a real upload through it is part of
// TestALargePackGoesUpInParts; here it is what the URL must not do.
func TestACompletionURLCompletesItsOwnUploadOnly(t *testing.T) {
	onStrictBuckets(t, func(t *testing.T, s3 backend) {
		w := newWorld(t, s3, func(c *server.Config) { c.PartSize = 1000 })
		alice := w.peer("alice")
		rootA, rootB := alice.ingest(version1()), alice.ingest(unrelated())

		a := alice.call(wire.Request{Op: wire.OpPushUpload, Name: "a", Root: rootA[:], DataSize: 2500, Objects: 1, Bytes: 100})
		b := alice.call(wire.Request{Op: wire.OpPushUpload, Name: "b", Root: rootB[:], DataSize: 2500, Objects: 1, Bytes: 100})
		if a.Error != nil || b.Error != nil || a.Parts == nil || b.Parts == nil {
			t.Fatalf("push-upload answered %+v and %+v", a, b)
		}
		ua, err := w.db.UploadByID(w.ctx, a.UploadID)
		if err != nil {
			t.Fatal(err)
		}
		ub, err := w.db.UploadByID(w.ctx, b.UploadID)
		if err != nil {
			t.Fatal(err)
		}
		// One part each, so that there is something to complete.
		etag := partETag(t, a.Parts.URLs[0], bytes.Repeat([]byte{1}, 1000))
		partETag(t, b.Parts.URLs[0], bytes.Repeat([]byte{2}, 1000))
		done := []byte(fmt.Sprintf("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>", etag))

		// The signature of a's URL does not cover b's upload.
		theirs, err := url.Parse(a.Parts.CompleteURL)
		if err != nil {
			t.Fatal(err)
		}
		q := theirs.Query()
		q.Set("uploadId", ub.MultipartID)
		theirs.RawQuery = q.Encode()
		theirs.Path = strings.Replace(theirs.Path, a.UploadID, b.UploadID, 1)
		refused(t, "a completion of another upload under this one's signature", send(t, http.MethodPost, theirs.String(), nil, done))
		refused(t, "a PUT through the completion URL", send(t, http.MethodPut, a.Parts.CompleteURL, nil, done))
		if w.present(ua.DataKey) || w.present(ub.DataKey) {
			t.Fatal("a refused completion made an object")
		}

		if status := send(t, http.MethodPost, a.Parts.CompleteURL, nil, done); status != http.StatusOK {
			t.Fatalf("the completion the URL was signed for answered %d", status)
		}
		if size, err := w.bucket.Size(w.ctx, ua.DataKey); err != nil || size != 1000 {
			t.Fatalf("the completed object: %d bytes, %v", size, err)
		}
	})
}

// partETag uploads one part and returns the ETag the service names it by.
func partETag(t *testing.T, rawURL string, body []byte) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, rawURL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	io.Copy(io.Discard, res.Body)
	if res.StatusCode != http.StatusOK || res.Header.Get("ETag") == "" {
		t.Fatalf("a part answered %d with ETag %q", res.StatusCode, res.Header.Get("ETag"))
	}
	return res.Header.Get("ETag")
}
