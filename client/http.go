package client

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/amber-store/jaccard-store/wire"
	"golang.org/x/sync/errgroup"
)

// errorBody is how much of a refusal's body is quoted in an error.
const errorBody = 512

// httpError describes an answer that is not the success asked for. The URL
// is left out: it is pre-signed, and its signature is a credential.
func httpError(what string, res *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(res.Body, errorBody))
	return fmt.Errorf("%s: the bucket answered %s: %s", what, res.Status, bytes.TrimSpace(body))
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

// getAll fetches an object that is known to be size bytes.
func (c *Client) getAll(ctx context.Context, what, url string, size uint64) ([]byte, error) {
	body, err := c.get(ctx, what, url)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	b, err := io.ReadAll(io.LimitReader(body, int64(size)+1))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	if uint64(len(b)) != size {
		return nil, fmt.Errorf("%s: the object is not the %d bytes the server announced", what, size)
	}
	return b, nil
}

// put sends size bytes of body to a pre-signed URL and returns the ETag of
// the answer.
func (c *Client) put(ctx context.Context, what, url string, body io.Reader, size int64) (string, error) {
	if size == 0 {
		body = http.NoBody
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return "", fmt.Errorf("%s: malformed URL", what)
	}
	req.ContentLength = size
	res, err := c.http.Do(req)
	if err != nil {
		return "", transportError(what, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
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

// putParts uploads size bytes of data in the parts the server laid out, a
// few at a time, and completes the upload.
func (c *Client) putParts(ctx context.Context, data io.ReaderAt, size int64, parts *wire.Parts, parallel int) error {
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
			etag, err := c.put(gctx, what, url, io.NewSectionReader(data, off, n), n)
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
