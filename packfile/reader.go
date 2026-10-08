package packfile

import (
	"fmt"
	"io"
	"iter"

	"github.com/klauspost/compress/zstd"
)

// expandChunk is how much of the stream Expand holds in memory at a time.
const expandChunk = 1 << 20

// source remembers the error its reader failed with, so that a failure to
// read the data can be told from data that is wrong.
type source struct {
	r   io.Reader
	err error
}

func (s *source) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if err != nil && err != io.EOF {
		s.err = err
	}
	return n, err
}

// expander decompresses the data of a pack and holds it to the size its
// index gives: it reads that many bytes and then looks once for more.
type expander struct {
	src  source
	dec  *zstd.Decoder
	size uint64
	read uint64
}

func newExpander(x *Index, data io.Reader) (*expander, error) {
	e := &expander{src: source{r: data}, size: x.DataSize()}
	dec, err := zstd.NewReader(&e.src, zstd.WithDecoderMaxWindow(MaxWindow), zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("packfile: starting the zstd decoder: %w", err)
	}
	e.dec = dec
	return e, nil
}

func (e *expander) close() { e.dec.Close() }

// fill reads the next len(p) bytes of the stream into p.
func (e *expander) fill(p []byte) error {
	n, err := io.ReadFull(e.dec, p)
	e.read += uint64(n)
	if err != nil {
		return e.fail(err)
	}
	return nil
}

// end checks that the stream ends where the index says.
func (e *expander) end() error {
	var one [1]byte
	n, err := io.ReadFull(e.dec, one[:])
	switch {
	case n > 0:
		return fmt.Errorf("%w: data goes on past the %d bytes the index gives", ErrMalformed, e.size)
	case err == io.EOF:
		return nil
	}
	return e.fail(err)
}

// fail turns an error of the decoder into the error to report: the source's
// own when the source failed, and otherwise one that wraps ErrMalformed.
func (e *expander) fail(err error) error {
	if e.src.err != nil {
		return e.src.err
	}
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return fmt.Errorf("%w: data ends after %d bytes, the index gives %d", ErrMalformed, e.read, e.size)
	}
	return fmt.Errorf("%w: decompressing the data after %d of %d bytes: %v", ErrMalformed, e.read, e.size, err)
}

// Objects decompresses data and yields the pack's objects in offset order. A
// stream that is not zstd, asks for a window above MaxWindow, or is shorter
// or longer than x says ends the sequence with an error wrapping
// ErrMalformed; an error from data itself ends it as it is. No more than
// x.DataSize() bytes are decompressed, but for the read that finds bytes
// left over. The yielded Data is the caller's.
func Objects(x *Index, data io.Reader) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		e, err := newExpander(x, data)
		if err != nil {
			yield(Object{}, err)
			return
		}
		defer e.close()
		for _, i := range x.byOffset {
			entry := x.entries[i]
			object := make([]byte, entry.Length)
			if err := e.fill(object); err != nil {
				yield(Object{}, err)
				return
			}
			if !yield(Object{Key: entry.Key, Data: object}, nil) {
				return
			}
		}
		if err := e.end(); err != nil {
			yield(Object{}, err)
		}
	}
}

// Expand decompresses data into w and fails, wrapping ErrMalformed, unless
// it is a zstd stream of exactly x.DataSize() bytes with a window of at most
// MaxWindow. It never writes more than x.DataSize() bytes. An error from
// data or from w is returned as it is.
func Expand(x *Index, data io.Reader, w io.Writer) error {
	e, err := newExpander(x, data)
	if err != nil {
		return err
	}
	defer e.close()
	buf := make([]byte, min(e.size, expandChunk))
	for left := e.size; left > 0; {
		p := buf[:min(left, uint64(len(buf)))]
		if err := e.fill(p); err != nil {
			return err
		}
		if _, err := w.Write(p); err != nil {
			return err
		}
		left -= uint64(len(p))
	}
	return e.end()
}
