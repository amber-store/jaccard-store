// Package bucket is the server's whole contact with S3: it names the objects
// of a pack, reads, writes and deletes them, and issues the pre-signed URLs
// through which clients move the bytes themselves, with plain HTTP and no SDK.
//
// Of a request's headers, only the host is part of a URL's signature, so a
// client needs to know nothing but the URL and the method that goes with it.
//
// GET, PUT and part URLs come from the SDK's presign client. That client has
// no call for CompleteMultipartUpload, so PresignComplete builds the request
// itself, POST <object URL>?uploadId=<id>, on the endpoint the SDK's own
// resolver gives for the bucket, and signs it with the SDK's SigV4 signer,
// set up the way the S3 client sets up its own.
package bucket

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyauth "github.com/aws/smithy-go/auth"
	"github.com/aws/smithy-go/encoding/httpbinding"
	"github.com/aws/smithy-go/logging"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/amber-store/core/key"
)

// ErrNotFound is returned by Size and Get for a key that has no object. S3
// says so only to credentials that may list the bucket; to others a missing
// object is a 403, and that is an ordinary error here.
var ErrNotFound = errors.New("bucket: no such object")

// maxTTL is the longest life SigV4 gives a pre-signed URL.
const maxTTL = 7 * 24 * time.Hour

// Config says which bucket to use and how to reach it. Credentials are not
// part of it: they come from the SDK's default chain.
type Config struct {
	// Bucket is the bucket's name.
	Bucket string
	// Prefix is put in front of every key that Key returns.
	Prefix string
	// Endpoint is the URL of an S3-compatible service, or "" for AWS.
	Endpoint string
	// Region is the bucket's region, or "" for the one the SDK finds in the
	// environment or the shared configuration.
	Region string
	// PathStyle addresses the bucket as <endpoint>/<bucket>/<key> instead of
	// <bucket>.<endpoint>/<key>.
	PathStyle bool
}

// Bucket is one S3 bucket, and a key prefix within it. It is safe for
// concurrent use.
type Bucket struct {
	name      string
	prefix    string
	client    *s3.Client
	presigner *s3.PresignClient
	signer    *v4.Signer
}

// New returns the Bucket that cfg describes. It loads the SDK's default
// configuration, and so takes credentials from the default chain: the
// environment, the shared files, the instance or task role. It does not
// contact S3, so a bucket that does not exist or cannot be reached shows in
// the first call that does.
func New(ctx context.Context, cfg Config) (*Bucket, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("bucket: no bucket name")
	}
	var opts []func(*config.LoadOptions) error
	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("bucket: load the AWS configuration: %w", err)
	}
	if awsCfg.Region == "" {
		return nil, errors.New("bucket: no region: none was given, and the AWS configuration has none")
	}
	return newFromConfig(awsCfg, cfg), nil
}

// newFromConfig builds the Bucket on a loaded SDK configuration.
//
// The SDK's default is to add a checksum to every upload. A pre-signed URL
// must not ask the client for a checksum header, and Put should work on
// services that do not know the newer checksums, so the client computes one
// only where S3 requires it. The presign client of this SDK version does the
// same for PUT and part URLs on its own; the option keeps that from being a
// matter of the version.
//
// The signer is the S3 client's own kind: S3 wants the path in the signature
// as it is sent, not escaped a second time.
//
// What the SDK has to say goes to slog at the debug level. Left to itself it
// writes a line to standard error for every download from a service that
// keeps no checksum of the object.
func newFromConfig(awsCfg aws.Config, cfg Config) *Bucket {
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.Logger = logging.LoggerFunc(func(_ logging.Classification, format string, v ...any) {
			slog.Debug("s3: " + fmt.Sprintf(format, v...))
		})
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.PathStyle
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})
	prefix := cfg.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &Bucket{
		name:      cfg.Bucket,
		prefix:    prefix,
		client:    client,
		presigner: s3.NewPresignClient(client),
		signer: v4.NewSigner(func(o *v4.SignerOptions) {
			o.DisableURIPathEscaping = true
		}),
	}
}

// Key returns "<prefix>packs/<root hex>/<uploadID>.<ext>", the name of one
// object of a pack. The prefix is the configured one as given, except that a
// prefix which is not empty and does not end in a slash gets one.
func (b *Bucket) Key(root key.Key, uploadID, ext string) string {
	return b.prefix + "packs/" + hex.EncodeToString(root[:]) + "/" + uploadID + "." + ext
}

// Size returns the size of the object in bytes, or ErrNotFound if there is no
// object under the key.
func (b *Bucket) Size(ctx context.Context, objectKey string) (int64, error) {
	out, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.name),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return 0, objectError("head", objectKey, err)
	}
	return aws.ToInt64(out.ContentLength), nil
}

// Get opens the object for reading, or returns ErrNotFound if there is no
// object under the key. The caller closes the reader.
func (b *Bucket) Get(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.name),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return nil, objectError("get", objectKey, err)
	}
	return out.Body, nil
}

// Put stores body under the key, replacing any object that is there.
func (b *Bucket) Put(ctx context.Context, objectKey string, body []byte) error {
	_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(b.name),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return fmt.Errorf("bucket: put %s: %w", objectKey, err)
	}
	return nil
}

// Delete removes the object. A key without an object is a success.
func (b *Bucket) Delete(ctx context.Context, objectKey string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.name),
		Key:    aws.String(objectKey),
	})
	if err != nil && !hasCode(err, "NoSuchKey", "NotFound") {
		return fmt.Errorf("bucket: delete %s: %w", objectKey, err)
	}
	return nil
}

// CreateMultipart opens a multipart upload for the key and returns S3's ID
// for it. The object appears when the upload is completed through the URL
// that PresignComplete gives. S3 keeps the parts it has received until the
// upload is completed or aborted.
func (b *Bucket) CreateMultipart(ctx context.Context, objectKey string) (uploadID string, err error) {
	out, err := b.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(b.name),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return "", fmt.Errorf("bucket: create a multipart upload of %s: %w", objectKey, err)
	}
	if aws.ToString(out.UploadId) == "" {
		return "", fmt.Errorf("bucket: create a multipart upload of %s: the answer has no upload ID", objectKey)
	}
	return *out.UploadId, nil
}

// AbortMultipart drops a multipart upload and the parts it has received. An
// upload that S3 does not know, because it was completed, aborted or never
// opened, is a success. An object the upload was completed into stays.
func (b *Bucket) AbortMultipart(ctx context.Context, objectKey, uploadID string) error {
	_, err := b.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(b.name),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadID),
	})
	if err != nil && !hasCode(err, "NoSuchUpload") {
		return fmt.Errorf("bucket: abort the multipart upload %s of %s: %w", uploadID, objectKey, err)
	}
	return nil
}

// PresignGet returns a URL that answers a plain GET with the object, for ttl
// from now.
func (b *Bucket) PresignGet(ctx context.Context, objectKey string, ttl time.Duration) (string, error) {
	if err := checkTTL(ttl); err != nil {
		return "", err
	}
	req, err := b.presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.name),
		Key:    aws.String(objectKey),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("bucket: presign a GET of %s: %w", objectKey, err)
	}
	return req.URL, nil
}

// PutCondition is the value of the If-None-Match header that a PUT to a
// PresignPut URL has to carry.
const PutCondition = "*"

// PresignPut returns a URL that stores the body of a plain PUT as the object
// if there is no such object yet, for ttl from now. The request has to carry
// the header "If-None-Match: *" (PutCondition) and needs nothing else beyond
// its content length.
//
// The condition is part of what is signed. A server verifies an object
// after it was uploaded, and the URL stays valid after that: without the
// condition its holder could put other bytes in the place of the verified
// ones. With it the object is written once; S3 answers a second PUT with
// 412. The service behind the bucket has to honor conditional writes.
func (b *Bucket) PresignPut(ctx context.Context, objectKey string, ttl time.Duration) (string, error) {
	if err := checkTTL(ttl); err != nil {
		return "", err
	}
	req, err := b.presigner.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(b.name),
		Key:         aws.String(objectKey),
		IfNoneMatch: aws.String(PutCondition),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("bucket: presign a PUT of %s: %w", objectKey, err)
	}
	return req.URL, nil
}

// PresignPart returns a URL that stores the body of a plain PUT as the given
// part of a multipart upload, for ttl from now. Parts are numbered from 1.
// The ETag header of the answer is what the completion has to name the part
// by.
func (b *Bucket) PresignPart(ctx context.Context, objectKey, uploadID string, part int32, ttl time.Duration) (string, error) {
	if err := checkTTL(ttl); err != nil {
		return "", err
	}
	req, err := b.presigner.PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(b.name),
		Key:        aws.String(objectKey),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(part),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("bucket: presign part %d of %s: %w", part, objectKey, err)
	}
	return req.URL, nil
}

// PresignComplete returns a URL that completes a multipart upload, for ttl
// from now. It takes a plain POST whose body is S3's CompleteMultipartUpload
// document: every part's number and ETag, in ascending order. S3 can answer
// that POST with 200 and an error document, so the status alone does not say
// that the object exists.
func (b *Bucket) PresignComplete(ctx context.Context, objectKey, uploadID string, ttl time.Duration) (string, error) {
	rawURL, err := b.sign(ctx, http.MethodPost, objectKey, url.Values{"uploadId": {uploadID}}, ttl, time.Now())
	if err != nil {
		return "", fmt.Errorf("bucket: presign the completion of %s: %w", objectKey, err)
	}
	return rawURL, nil
}

// sign pre-signs a request on the object with a SigV4 query signature that is
// valid for ttl from at. The signature covers the method, the path, the query
// and the host, and neither the body nor any other header.
func (b *Bucket) sign(ctx context.Context, method, objectKey string, query url.Values, ttl time.Duration, at time.Time) (string, error) {
	if err := checkTTL(ttl); err != nil {
		return "", err
	}
	u, region, err := b.objectURL(ctx, objectKey)
	if err != nil {
		return "", err
	}
	query.Set("X-Amz-Expires", strconv.FormatInt(int64(ttl/time.Second), 10))
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return "", err
	}
	credentials, err := b.client.Options().Credentials.Retrieve(ctx)
	if err != nil {
		return "", fmt.Errorf("retrieve credentials: %w", err)
	}
	signed, _, err := b.signer.PresignHTTP(ctx, credentials, req, "UNSIGNED-PAYLOAD", "s3", region, at)
	return signed, err
}

// objectURL returns the URL of the object without a query, and the region to
// sign requests on it for. Both come from the client's endpoint resolver,
// asked what the client asks it, and the key is escaped and joined to the
// endpoint the way the SDK's serializer does it, so the URL is the one the
// SDK itself would send a request for the object to.
func (b *Bucket) objectURL(ctx context.Context, objectKey string) (*url.URL, string, error) {
	o := b.client.Options()
	endpoint, err := o.EndpointResolverV2.ResolveEndpoint(ctx, s3.EndpointParameters{
		Bucket:                         aws.String(b.name),
		Key:                            aws.String(objectKey),
		Region:                         aws.String(o.Region),
		Endpoint:                       o.BaseEndpoint,
		ForcePathStyle:                 aws.Bool(o.UsePathStyle),
		Accelerate:                     aws.Bool(o.UseAccelerate),
		UseFIPS:                        aws.Bool(o.EndpointOptions.UseFIPSEndpoint == aws.FIPSEndpointStateEnabled),
		UseDualStack:                   aws.Bool(o.EndpointOptions.UseDualStackEndpoint == aws.DualStackEndpointStateEnabled),
		DisableMultiRegionAccessPoints: aws.Bool(o.DisableMultiRegionAccessPoints),
		UseArnRegion:                   aws.Bool(o.UseARNRegion),
	})
	if err != nil {
		return nil, "", fmt.Errorf("resolve the endpoint: %w", err)
	}

	u := endpoint.URI
	u.RawPath = smithyhttp.JoinPath(u.EscapedPath(), "/"+httpbinding.EscapePath(objectKey, false))
	u.Path = smithyhttp.JoinPath(u.Path, "/"+objectKey)

	region := o.Region
	schemes, _ := smithyauth.GetAuthOptions(&endpoint.Properties)
	for _, scheme := range schemes {
		if r, ok := smithyhttp.GetSigV4SigningRegion(&scheme.SignerProperties); ok {
			region = r
		}
	}
	return &u, region, nil
}

// checkTTL refuses a life that SigV4 cannot express: under a second, which
// the SDK would quietly turn into its default of fifteen minutes, or over
// seven days.
func checkTTL(ttl time.Duration) error {
	if ttl < time.Second || ttl > maxTTL {
		return fmt.Errorf("bucket: a pre-signed URL cannot be valid for %v: the range is one second to %v", ttl, maxTTL)
	}
	return nil
}

// objectError wraps the failure of a read, turning S3's answers for a missing
// object into ErrNotFound. A HEAD has no body to carry a code, and the SDK
// reports its 404 as NotFound.
func objectError(op, objectKey string, err error) error {
	if hasCode(err, "NoSuchKey", "NotFound") {
		return fmt.Errorf("%w: %s", ErrNotFound, objectKey)
	}
	return fmt.Errorf("bucket: %s %s: %w", op, objectKey, err)
}

// hasCode reports whether err is an answer of S3 with one of the error codes.
func hasCode(err error, codes ...string) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && slices.Contains(codes, apiErr.ErrorCode())
}
