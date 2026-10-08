package main

import (
	"bytes"
	"context"
	"io"
	"net/netip"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/jaccard-store/bucket"
)

var variables = []string{
	"JACCARD_DATA", "JACCARD_S3_BUCKET", "JACCARD_S3_PREFIX", "JACCARD_S3_ENDPOINT",
	"JACCARD_S3_REGION", "JACCARD_S3_PATH_STYLE", "JACCARD_ADMIN_ADDR",
	"JACCARD_UPLOAD_TIMEOUT", "JACCARD_URL_TTL", "JACCARD_PART_SIZE", "JACCARD_VERIFY_JOBS",
	"JACCARD_MAX_PACK_SIZE", "JACCARD_BIND",
}

// settingsFor runs the command with args under env alone and returns the
// settings it would have served with.
func settingsFor(t *testing.T, env map[string]string, args ...string) (settings, error) {
	t.Helper()
	for _, name := range variables {
		// Setenv registers the restore; a variable set to nothing would
		// still count as given, so it is then removed.
		t.Setenv(name, "")
		os.Unsetenv(name)
	}
	for name, value := range env {
		t.Setenv(name, value)
	}
	var got settings
	app := newApp(io.Discard, func(_ context.Context, _ io.Writer, s settings) error {
		got = s
		return nil
	})
	err := app.Run(append([]string{"jaccard-stored"}, args...))
	return got, err
}

func TestDefaults(t *testing.T) {
	got, err := settingsFor(t, nil, "--data", "/d", "--s3-bucket", "b")
	if err != nil {
		t.Fatal(err)
	}
	want := settings{
		data:          "/d",
		bucket:        bucket.Config{Bucket: "b"},
		adminAddr:     "127.0.0.1:8080",
		uploadTimeout: time.Hour,
		urlTTL:        time.Hour,
		partSize:      64 << 20,
		verifyJobs:    2,
		maxPackBytes:  16 << 30,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestEveryVariableIsRead(t *testing.T) {
	got, err := settingsFor(t, map[string]string{
		"JACCARD_DATA":           "/env/data",
		"JACCARD_S3_BUCKET":      "env-bucket",
		"JACCARD_S3_PREFIX":      "env/prefix/",
		"JACCARD_S3_ENDPOINT":    "http://env:9000",
		"JACCARD_S3_REGION":      "eu-env-1",
		"JACCARD_S3_PATH_STYLE":  "true",
		"JACCARD_ADMIN_ADDR":     "0.0.0.0:9999",
		"JACCARD_UPLOAD_TIMEOUT": "20m",
		"JACCARD_URL_TTL":        "30m",
		"JACCARD_PART_SIZE":      "16MiB",
		"JACCARD_VERIFY_JOBS":    "5",
		"JACCARD_MAX_PACK_SIZE":  "3GiB",
		"JACCARD_BIND":           "0.0.0.0:4435",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := settings{
		data: "/env/data",
		bucket: bucket.Config{
			Bucket: "env-bucket", Prefix: "env/prefix/", Endpoint: "http://env:9000", Region: "eu-env-1", PathStyle: true,
		},
		adminAddr:     "0.0.0.0:9999",
		uploadTimeout: 20 * time.Minute,
		urlTTL:        30 * time.Minute,
		partSize:      16 << 20,
		verifyJobs:    5,
		maxPackBytes:  3 << 30,
		bind:          netip.MustParseAddrPort("0.0.0.0:4435"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestFlagWinsOverVariable(t *testing.T) {
	got, err := settingsFor(t, map[string]string{
		"JACCARD_DATA":           "/env/data",
		"JACCARD_S3_BUCKET":      "env-bucket",
		"JACCARD_S3_PREFIX":      "env/",
		"JACCARD_S3_ENDPOINT":    "http://env:9000",
		"JACCARD_S3_REGION":      "eu-env-1",
		"JACCARD_S3_PATH_STYLE":  "true",
		"JACCARD_ADMIN_ADDR":     "0.0.0.0:9999",
		"JACCARD_UPLOAD_TIMEOUT": "20m",
		"JACCARD_URL_TTL":        "30m",
		"JACCARD_PART_SIZE":      "16MiB",
		"JACCARD_VERIFY_JOBS":    "5",
		"JACCARD_MAX_PACK_SIZE":  "3GiB",
		"JACCARD_BIND":           "0.0.0.0:4435",
	},
		"--data", "/flag/data", "--s3-bucket", "flag-bucket", "--s3-prefix", "flag/",
		"--s3-endpoint", "http://flag:9000", "--s3-region", "eu-flag-1", "--s3-path-style=false",
		"--admin-addr", "127.0.0.1:1", "--upload-timeout", "2h", "--url-ttl", "3h",
		"--part-size", "8MiB", "--verify-jobs", "7", "--max-pack-size", "1GiB", "--bind", "[::]:5000")
	if err != nil {
		t.Fatal(err)
	}
	want := settings{
		data:          "/flag/data",
		bucket:        bucket.Config{Bucket: "flag-bucket", Prefix: "flag/", Endpoint: "http://flag:9000", Region: "eu-flag-1"},
		adminAddr:     "127.0.0.1:1",
		uploadTimeout: 2 * time.Hour,
		urlTTL:        3 * time.Hour,
		partSize:      8 << 20,
		verifyJobs:    7,
		maxPackBytes:  1 << 30,
		bind:          netip.MustParseAddrPort("[::]:5000"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestRefusals(t *testing.T) {
	for name, args := range map[string][]string{
		"no data directory":     {"--s3-bucket", "b"},
		"no bucket":             {"--data", "/d"},
		"part below S3's floor": {"--data", "/d", "--s3-bucket", "b", "--part-size", "1MiB"},
		"part size not a size":  {"--data", "/d", "--s3-bucket", "b", "--part-size", "big"},
		"no upload time":        {"--data", "/d", "--s3-bucket", "b", "--upload-timeout", "0s"},
		"no URL lifetime":       {"--data", "/d", "--s3-bucket", "b", "--url-ttl", "-1s"},
		// What a URL cannot be signed for would fail every request later.
		"upload time under 1s":  {"--data", "/d", "--s3-bucket", "b", "--upload-timeout", "500ms"},
		"upload time over 7d":   {"--data", "/d", "--s3-bucket", "b", "--upload-timeout", "200h"},
		"URL lifetime under 1s": {"--data", "/d", "--s3-bucket", "b", "--url-ttl", "500ms"},
		"URL lifetime over 7d":  {"--data", "/d", "--s3-bucket", "b", "--url-ttl", "169h"},
		"pack size not a size":  {"--data", "/d", "--s3-bucket", "b", "--max-pack-size", "lots"},
		"bind without a port":   {"--data", "/d", "--s3-bucket", "b", "--bind", "0.0.0.0"},
		"bind without an IP":    {"--data", "/d", "--s3-bucket", "b", "--bind", ":4435"},
		"bind to a host name":   {"--data", "/d", "--s3-bucket", "b", "--bind", "localhost:4435"},
		"no verifier":           {"--data", "/d", "--s3-bucket", "b", "--verify-jobs", "0"},
		"an argument":           {"--data", "/d", "--s3-bucket", "b", "extra"},
	} {
		if _, err := settingsFor(t, nil, args...); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]int64{
		"1":       1,
		"5242880": 5 << 20,
		"4KiB":    4 << 10,
		"64MiB":   64 << 20,
		"64 MiB":  64 << 20,
		"2GiB":    2 << 30,
	} {
		if got, err := parseSize(in); err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "MiB", "-1", "0", "1.5MiB", "64MB", "9223372036854775807GiB"} {
		if got, err := parseSize(in); err == nil {
			t.Errorf("parseSize(%q) = %d, want an error", in, got)
		}
	}
}

func TestVersionIsPrinted(t *testing.T) {
	var out bytes.Buffer
	app := newApp(io.Discard, func(context.Context, io.Writer, settings) error {
		t.Fatal("--version started the server")
		return nil
	})
	app.Writer = &out
	if err := app.Run([]string{"jaccard-stored", "--version"}); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, "jaccard-stored") || !strings.Contains(got, version) {
		t.Fatalf("--version printed %q, want the name and %q", got, version)
	}
}
