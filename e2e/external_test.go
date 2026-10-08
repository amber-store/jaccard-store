package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/amber-store/jaccard-store/bucket"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// The variables that name an S3 service which exists already, such as a
// bucket on AWS or on Cloudflare R2, for the tests to run against as well.
// The bucket has to exist. Every test works under a prefix of its own in it
// and removes what it left there.
const (
	externalEndpointVar  = "JACCARD_TEST_S3_ENDPOINT"
	externalBucketVar    = "JACCARD_TEST_S3_BUCKET"
	externalRegionVar    = "JACCARD_TEST_S3_REGION" // "auto" on R2
	externalKeyIDVar     = "JACCARD_TEST_S3_ACCESS_KEY_ID"
	externalSecretVar    = "JACCARD_TEST_S3_SECRET_ACCESS_KEY"
	externalPathStyleVar = "JACCARD_TEST_S3_PATH_STYLE" // "false" for virtual-hosted addressing
	externalTestsPrefix  = "jaccard-store-test/"
)

// externalConfigured reports whether an external service is named.
func externalConfigured() bool {
	return os.Getenv(externalEndpointVar) != "" && os.Getenv(externalBucketVar) != ""
}

// externalBucket returns a Bucket on the external service, under a prefix
// that is the test's own and is emptied when the test ends. The credentials
// are handed to bucket.New through the environment, as a server gets them.
func externalBucket(t *testing.T) *bucket.Bucket {
	t.Helper()
	absent := filepath.Join(t.TempDir(), "absent")
	t.Setenv("AWS_ACCESS_KEY_ID", os.Getenv(externalKeyIDVar))
	t.Setenv("AWS_SECRET_ACCESS_KEY", os.Getenv(externalSecretVar))
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", absent)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", absent)

	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	cfg := bucket.Config{
		Bucket:    os.Getenv(externalBucketVar),
		Prefix:    externalTestsPrefix + hex.EncodeToString(suffix[:]) + "/",
		Endpoint:  os.Getenv(externalEndpointVar),
		Region:    envOr(externalRegionVar, "us-east-1"),
		PathStyle: os.Getenv(externalPathStyleVar) != "false",
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sdk, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(sdk, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = cfg.PathStyle
	})
	t.Cleanup(func() { emptyPrefix(t, client, cfg.Bucket, cfg.Prefix) })

	b, err := bucket.New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// emptyPrefix removes every object and every unfinished multipart upload
// under prefix. A test leaves both behind on purpose: uploads that are never
// committed are what some of them are about.
func emptyPrefix(t *testing.T, client *s3.Client, name, prefix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	objects := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(name), Prefix: aws.String(prefix)})
	for objects.HasMorePages() {
		page, err := objects.NextPage(ctx)
		if err != nil {
			t.Errorf("listing what the test left under %s: %v", prefix, err)
			return
		}
		for _, o := range page.Contents {
			if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(name), Key: o.Key}); err != nil {
				t.Errorf("removing %s: %v", aws.ToString(o.Key), err)
			}
		}
	}
	uploads, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(name), Prefix: aws.String(prefix)})
	if err != nil {
		t.Errorf("listing the multipart uploads the test left under %s: %v", prefix, err)
		return
	}
	for _, u := range uploads.Uploads {
		_, err := client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(name), Key: u.Key, UploadId: u.UploadId})
		if err != nil {
			t.Errorf("aborting the upload of %s: %v", aws.ToString(u.Key), err)
		}
	}
}
