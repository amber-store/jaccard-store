package packfile

import (
	"errors"
	"fmt"
	"io"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/key"
	"github.com/klauspost/compress/zstd"
)

// Writer builds one pack: it compresses the objects added to it into one
// zstd stream and keeps the index that stream implies. A pack to which no
// bytes were added writes no data at all. It is not safe for concurrent use.
type Writer struct {
	data io.Writer
	// enc is started with the first byte. What an encoder that was given
	// nothing writes when it is closed has differed between releases of
	// the zstd package, from nothing to an empty frame; a pack without
	// bytes is to be no data whatever the release, so no encoder is asked.
	enc     *zstd.Encoder
	entries []Entry
	bytes   uint64
	done    bool
}

// NewWriter starts a pack whose compressed data goes to data.
func NewWriter(data io.Writer) (*Writer, error) {
	return &Writer{data: data}, nil
}

// Add appends one object, k and its serialized bytes, to the stream. It
// fails, wrapping ErrMalformed and writing nothing, for a key that is not
// canonical, an object above amberpack.MaxPayload and a pack that is full. A
// key added twice is found by Finish. Whether object is what k names is not
// checked.
func (w *Writer) Add(k key.Key, object []byte) error {
	if w.done {
		return errors.New("packfile: Add after Finish")
	}
	if err := k.Validate(); err != nil {
		return fmt.Errorf("%w: key %s is not canonical: %v", ErrMalformed, k, err)
	}
	if len(object) > amberpack.MaxPayload {
		return fmt.Errorf("%w: object %s of %d bytes is above the limit of %d", ErrMalformed, k, len(object), amberpack.MaxPayload)
	}
	if len(w.entries) == MaxEntries {
		return fmt.Errorf("%w: a pack holds at most %d objects", ErrMalformed, MaxEntries)
	}
	if len(object) > 0 {
		if w.enc == nil {
			enc, err := zstd.NewWriter(w.data)
			if err != nil {
				return fmt.Errorf("packfile: starting the zstd stream: %w", err)
			}
			w.enc = enc
		}
		if _, err := w.enc.Write(object); err != nil {
			return err
		}
	}
	w.entries = append(w.entries, Entry{Key: k, Offset: w.bytes, Length: uint32(len(object))})
	w.bytes += uint64(len(object))
	return nil
}

// Len returns the number of objects added so far.
func (w *Writer) Len() int { return len(w.entries) }

// Bytes returns the uncompressed bytes added so far.
func (w *Writer) Bytes() uint64 { return w.bytes }

// Finish ends the zstd stream and returns the index. The writer given to
// NewWriter is not closed. Finish fails, wrapping ErrMalformed, when a key
// was added twice. The Writer cannot be used afterwards.
func (w *Writer) Finish() (*Index, error) {
	if w.done {
		return nil, errors.New("packfile: Finish called twice")
	}
	w.done = true
	if w.enc != nil {
		if err := w.enc.Close(); err != nil {
			return nil, err
		}
	}
	return NewIndex(w.entries)
}
