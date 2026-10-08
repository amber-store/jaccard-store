package client

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/amber-store/jaccard-store/wire"
	"golang.org/x/sync/errgroup"
)

// errorBody is how much of a refusal's body is read to find out why.
const errorBody = 4096

// httpError describes an answer that is not the success asked for, by its
// status and, when the body is S3's error document, by the code and the
// message in it. Nothing else of the body is repeated: the document of a
// refused signature spells out the signature and the key ID it was made
// with. The URL is left out as well: it is pre-signed, and its signature is
// a credential.
func httpError(what string, res *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(res.Body, errorBody))
	var doc struct {
		XMLName xml.Name
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	if xml.Unmarshal(body, &doc) == nil && doc.XMLName.Local == "Error" && doc.Code != "" {
		return &statusError{what: what, status: res.StatusCode, text: fmt.Sprintf("%s: the bucket answered %s: %s: %s", what, res.Status, doc.Code, doc.Message)}
	}
	return &statusError{what: what, status: res.StatusCode, text: fmt.Sprintf("%s: the bucket answered %s", what, res.Status)}
}

// statusError is an answer of the bucket that is not a success.
type statusError struct {
	what   string
	status int
	text   string
}

func (e *statusError) Error() string { return e.text }

// transient reports whether err is worth another attempt: the request did
// not get through, or the bucket said it was busy or broken.
func transient(err error) bool {
	var se *statusError
	if errors.As(err, &se) {
		return se.status >= 500 || se.status == http.StatusTooManyRequests || se.status == http.StatusRequestTimeout
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// transportError strips the URL from an error of the HTTP client, which
// quotes it whole.
func transportError(what string, err error) error {
	var uerr interface{ Unwrap() error }
	if errors.As(err, &uerr) {
		if inner := uerr.Unwrap(); inner != nil {
			err = inner
		}
	}
	return fmt.Errorf("%s: %w", what, err)
}

// get starts a GET of a pre-signed URL and returns the body of a 200.
func (c *Client) get(ctx context.Context, what, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: malformed URL", what)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return nil, transportError(what, err)
	}
	if res.StatusCode != http.StatusOK {
		defer res.Body.Close()
		return nil, httpError(what, res)
	}
	return res.Body, nil
}

// getAll fetches an object that is known to be size bytes and reports the
// bytes to p as they arrive.
func (c *Client) getAll(ctx context.Context, what, url string, size uint64, p Progress) ([]byte, error) {
	body, err := c.get(ctx, what, url)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	b, err := io.ReadAll(io.LimitReader(&meter{r: body, p: p}, int64(size)+1))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	if uint64(len(b)) != size {
		return nil, fmt.Errorf("%s: the object is not the %d bytes the server announced", what, size)
	}
	return b, nil
}

// create stores the first size bytes of data as a new object: the PUT of an
// index or of data in one piece. The server signed the URL for a request
// that carries the condition "If-None-Match: *", which lets the object be
// written once and never replaced.
func (c *Client) create(ctx context.Context, what, url string, data io.ReaderAt, size int64, p Progress) error {
	_, err := c.send(ctx, what, url, data, 0, size, true, p)
	return err
}

// putPart stores size bytes of data from off as one part of a multipart
// upload and returns its ETag.
func (c *Client) putPart(ctx context.Context, what, url string, data io.ReaderAt, off, size int64, p Progress) (string, error) {
	return c.send(ctx, what, url, data, off, size, false, p)
}

// send PUTs size bytes of data from off to a pre-signed URL and returns the
// ETag of the answer. The bytes are reported to p as they leave; those of a
// request that fails are taken back, so p counts what has arrived.
func (c *Client) send(ctx context.Context, what, url string, data io.ReaderAt, off, size int64, once bool, p Progress) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, http.NoBody)
	if err != nil {
		return "", fmt.Errorf("%s: malformed URL", what)
	}
	// The body is a section of something that can be read again, so the
	// HTTP client is given the means to: it sends a request a second time
	// when it is redirected, or when a connection it reused turns out to
	// be closed.
	var sent *meter
	body := func() (io.ReadCloser, error) {
		sent.undo()
		sent = &meter{r: io.NewSectionReader(data, off, size), p: p}
		return io.NopCloser(sent), nil
	}
	if size > 0 {
		req.Body, _ = body()
		req.GetBody = body
	}
	req.ContentLength = size
	if once {
		req.Header.Set("If-None-Match", "*")
	}
	res, err := c.http.Do(req)
	if err != nil {
		sent.undo()
		return "", transportError(what, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		sent.undo()
		return "", httpError(what, res)
	}
	io.Copy(io.Discard, res.Body)
	return res.Header.Get("ETag"), nil
}

// completeMultipartUpload is the body of the request that completes a
// multipart upload.
type completeMultipartUpload struct {
	XMLName xml.Name       `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUpload"`
	Parts   []completePart `xml:"Part"`
}

type completePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// partAttempts is how often one part is sent before the upload is given up.
const partAttempts = 4

// putParts uploads size bytes of data in the parts the server laid out, a
// few at a time, and completes the upload. The bytes are reported to p.
func (c *Client) putParts(ctx context.Context, data io.ReaderAt, size int64, parts *wire.Parts, parallel int, p Progress) error {
	partSize := int64(parts.PartSize)
	if partSize <= 0 {
		return errors.New("data: the server named a part size of zero")
	}
	if want := (size + partSize - 1) / partSize; int64(len(parts.URLs)) != want {
		return fmt.Errorf("data: the server sent %d part URLs for %d parts", len(parts.URLs), want)
	}
	if parallel <= 0 {
		parallel = 4
	}
	done := make([]completePart, len(parts.URLs))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(parallel)
	for i, url := range parts.URLs {
		g.Go(func() error {
			off := int64(i) * partSize
			n := min(partSize, size-off)
			what := fmt.Sprintf("data part %d", i+1)
			// A part is a section of the file and can be sent again; a
			// pack of hundreds of parts should not be lost to one that
			// met a busy bucket.
			var etag string
			var err error
			for attempt := 1; ; attempt++ {
				etag, err = c.putPart(gctx, what, url, data, off, n, p)
				if err == nil || attempt == partAttempts || !transient(err) || gctx.Err() != nil {
					break
				}
				select {
				case <-time.After(c.retryWait * time.Duration(attempt)):
				case <-gctx.Done():
				}
			}
			if err != nil {
				return err
			}
			if etag == "" {
				return fmt.Errorf("%s: the bucket answered without an ETag", what)
			}
			done[i] = completePart{PartNumber: i + 1, ETag: etag}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return c.complete(ctx, parts.CompleteURL, done)
}

// complete POSTs the list of parts. S3 can answer 200 and still report a
// failure in the body, so the body decides.
func (c *Client) complete(ctx context.Context, url string, parts []completePart) error {
	const what = "completing the data upload"
	body, err := xml.Marshal(completeMultipartUpload{Parts: parts})
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%s: malformed URL", what)
	}
	req.Header.Set("Content-Type", "application/xml")
	res, err := c.http.Do(req)
	if err != nil {
		return transportError(what, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return httpError(what, res)
	}
	answer, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	var root struct {
		XMLName xml.Name
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	if err := xml.Unmarshal(answer, &root); err != nil {
		return fmt.Errorf("%s: the bucket's answer is not XML: %w", what, err)
	}
	if root.XMLName.Local == "Error" {
		return fmt.Errorf("%s: the bucket answered %s: %s", what, root.Code, root.Message)
	}
	return nil
}
