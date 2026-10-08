package packfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/keyset"
)

// testKey returns the key of a blob holding data.
func testKey(t testing.TB, data []byte) key.Key {
	t.Helper()
	k, err := key.New(key.Blob, uint64(len(data)), data)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// sortedKeys returns n distinct keys in ascending order.
func sortedKeys(t testing.TB, n int) []key.Key {
	t.Helper()
	keys := make([]key.Key, n)
	for i := range keys {
		keys[i] = testKey(t, fmt.Appendf(nil, "object %d", i))
	}
	return keyset.Normalize(keys)
}

// rawIndex encodes an index as given, valid or not.
func rawIndex(count uint64, entries ...Entry) []byte {
	b := make([]byte, 0, IndexHeaderSize+len(entries)*EntrySize)
	b = append(b, IndexMagic...)
	b = binary.BigEndian.AppendUint64(b, count)
	for _, e := range entries {
		b = append(b, e.Key[:]...)
		b = binary.BigEndian.AppendUint64(b, e.Offset)
		b = binary.BigEndian.AppendUint32(b, e.Length)
	}
	return b
}

// backToBack lays the keys out in the given order of positions, each with the
// length at its position.
func backToBack(keys []key.Key, lengths []uint32, order []int) []Entry {
	entries := make([]Entry, len(keys))
	var offset uint64
	for _, i := range order {
		entries[i] = Entry{Key: keys[i], Offset: offset, Length: lengths[i]}
		offset += uint64(lengths[i])
	}
	return entries
}

func TestIndexSize(t *testing.T) {
	if got := IndexSize(0); got != 16 {
		t.Errorf("IndexSize(0) = %d, want 16", got)
	}
	if got := IndexSize(3); got != 16+3*44 {
		t.Errorf("IndexSize(3) = %d, want %d", got, 16+3*44)
	}
	if got := IndexSize(MaxEntries); got != 16+44<<24 {
		t.Errorf("IndexSize(MaxEntries) = %d, want %d", got, 16+44<<24)
	}
	// 44 * (2^62 + 1) is 44 modulo 2^64: a count that would wrap around to
	// the size of an index of one entry.
	for _, n := range []uint64{1<<62 + 1, 1 << 60, math.MaxUint64 / 44, math.MaxUint64} {
		if got := IndexSize(n); got != math.MaxUint64 {
			t.Errorf("IndexSize(%d) = %d, want MaxUint64", n, got)
		}
	}
}

func TestIndexRoundTrip(t *testing.T) {
	keys := sortedKeys(t, 5)
	sorted := backToBack(keys, []uint32{7, 0, 300, 1, 12}, []int{3, 0, 4, 2, 1})
	shuffled := []Entry{sorted[2], sorted[4], sorted[0], sorted[3], sorted[1]}
	given := slices.Clone(shuffled)

	x, err := NewIndex(shuffled)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(shuffled, given) {
		t.Error("NewIndex reordered the caller's entries")
	}
	if x.Len() != 5 {
		t.Fatalf("Len = %d, want 5", x.Len())
	}
	for i, want := range sorted {
		if got := x.Entry(i); got != want {
			t.Errorf("Entry(%d) = %+v, want %+v", i, got, want)
		}
	}
	if got := x.DataSize(); got != 320 {
		t.Errorf("DataSize = %d, want 320", got)
	}

	b := x.Encode()
	if uint64(len(b)) != IndexSize(5) {
		t.Errorf("encoded index is %d bytes, want %d", len(b), IndexSize(5))
	}
	if !bytes.Equal(b, rawIndex(5, sorted...)) {
		t.Error("encoded index differs from the format written by hand")
	}
	y, err := ParseIndex(b)
	if err != nil {
		t.Fatal(err)
	}
	if y.Len() != x.Len() || y.DataSize() != x.DataSize() {
		t.Fatalf("parsed index has %d entries of %d bytes, want %d of %d", y.Len(), y.DataSize(), x.Len(), x.DataSize())
	}
	for i := range sorted {
		if y.Entry(i) != x.Entry(i) {
			t.Errorf("parsed Entry(%d) = %+v, want %+v", i, y.Entry(i), x.Entry(i))
		}
	}
	if !bytes.Equal(y.Encode(), b) {
		t.Error("a parsed index encodes to other bytes")
	}
}

func TestIndexEmpty(t *testing.T) {
	x, err := NewIndex(nil)
	if err != nil {
		t.Fatal(err)
	}
	if x.Len() != 0 || x.DataSize() != 0 || len(x.ByOffset()) != 0 {
		t.Errorf("empty index: Len %d, DataSize %d, ByOffset %v", x.Len(), x.DataSize(), x.ByOffset())
	}
	b := x.Encode()
	if !bytes.Equal(b, rawIndex(0)) {
		t.Errorf("empty index encodes to % x", b)
	}
	if _, err := ParseIndex(b); err != nil {
		t.Errorf("parsing an empty index: %v", err)
	}
	if x.Has(testKey(t, []byte("absent"))) {
		t.Error("an empty index has a key")
	}
}

func TestIndexFind(t *testing.T) {
	keys := sortedKeys(t, 9)
	var present, absent []key.Key
	for i, k := range keys {
		if i%2 == 1 {
			present = append(present, k)
		} else {
			absent = append(absent, k)
		}
	}
	lengths := []uint32{4, 4, 4, 4}
	x, err := NewIndex(backToBack(present, lengths, []int{0, 1, 2, 3}))
	if err != nil {
		t.Fatal(err)
	}
	for i, k := range present {
		got, ok := x.Find(k)
		if !ok || got != i {
			t.Errorf("Find(present %d) = %d, %v", i, got, ok)
		}
		if !x.Has(k) {
			t.Errorf("Has(present %d) = false", i)
		}
	}
	for i, k := range absent {
		if _, ok := x.Find(k); ok {
			t.Errorf("Find(absent %d) found it", i)
		}
		if x.Has(k) {
			t.Errorf("Has(absent %d) = true", i)
		}
	}
}

func TestIndexByOffset(t *testing.T) {
	keys := sortedKeys(t, 6)
	// Positions 1 and 4 are empty objects that share offset 5 with position
	// 0, the object that follows them; position 5 is an empty object at the
	// very end.
	order := []int{2, 1, 4, 0, 3, 5}
	x, err := NewIndex(backToBack(keys, []uint32{9, 0, 5, 2, 0, 0}, order))
	if err != nil {
		t.Fatal(err)
	}
	if got := x.ByOffset(); !slices.Equal(got, order) {
		t.Errorf("ByOffset = %v, want %v", got, order)
	}
	var end uint64
	for _, i := range x.ByOffset() {
		e := x.Entry(i)
		if e.Offset != end {
			t.Fatalf("position %d starts at %d, want %d", i, e.Offset, end)
		}
		end += uint64(e.Length)
	}
	if end != x.DataSize() {
		t.Errorf("entries end at %d, DataSize is %d", end, x.DataSize())
	}
	y, err := ParseIndex(x.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if got := y.ByOffset(); !slices.Equal(got, order) {
		t.Errorf("parsed ByOffset = %v, want %v", got, order)
	}
}

func TestParseIndexRefuses(t *testing.T) {
	keys := sortedKeys(t, 3)
	a, b, c := keys[0], keys[1], keys[2]
	reserved := b
	reserved[key.Size-1] |= 0x08
	if reserved.Validate() == nil {
		t.Fatal("a key with the reserved bit set validates")
	}
	valid := rawIndex(2, Entry{a, 0, 10}, Entry{b, 10, 5})
	if _, err := ParseIndex(valid); err != nil {
		t.Fatalf("the index the cases are derived from: %v", err)
	}

	cases := []struct {
		name string
		b    []byte
		want string
	}{
		{"empty input", nil, "header"},
		{"short header", valid[:IndexHeaderSize-1], "header"},
		{"bad magic", append([]byte("JACIDX\x00\x02"), valid[8:]...), "magic"},
		{"links magic", append([]byte(LinksMagic), valid[8:]...), "magic"},
		{"count above the entries", rawIndex(3, Entry{a, 0, 10}, Entry{b, 10, 5}), "do not fill"},
		{"count below the entries", rawIndex(1, Entry{a, 0, 10}, Entry{b, 10, 5}), "do not fill"},
		{"a cut entry", valid[:len(valid)-1], "do not fill"},
		{"a byte too many", append(slices.Clone(valid), 0), "do not fill"},
		{"more than MaxEntries", rawIndex(MaxEntries + 1), "entries is above the limit"},
		{"2^60 entries in 16 bytes", rawIndex(1 << 60), "entries is above the limit"},
		{"2^64-1 entries in 16 bytes", rawIndex(math.MaxUint64), "entries is above the limit"},
		{"MaxEntries in 16 bytes", rawIndex(MaxEntries), "do not fill"},
		{"unsorted keys", rawIndex(2, Entry{b, 0, 10}, Entry{a, 10, 5}), "out of order"},
		{"duplicate keys", rawIndex(2, Entry{a, 0, 10}, Entry{a, 10, 5}), "twice"},
		{"non-canonical key", rawIndex(2, Entry{a, 0, 10}, Entry{reserved, 10, 5}), "not canonical"},
		{"length above MaxPayload", rawIndex(1, Entry{a, 0, amberpack.MaxPayload + 1}), "bytes is above the limit"},
		{"gap", rawIndex(2, Entry{a, 0, 10}, Entry{b, 12, 5}), "gap"},
		{"gap out of key order", rawIndex(3, Entry{a, 15, 5}, Entry{b, 0, 10}, Entry{c, 10, 4}), "gap"},
		{"overlap", rawIndex(2, Entry{a, 0, 10}, Entry{b, 5, 10}), "overlaps"},
		{"two objects at one offset", rawIndex(2, Entry{a, 0, 10}, Entry{b, 0, 10}), "overlaps"},
		{"first offset above 0", rawIndex(2, Entry{a, 3, 10}, Entry{b, 13, 5}), "not 0"},
		{"end overflows", rawIndex(2, Entry{a, 0, 10}, Entry{b, math.MaxUint64 - 4, 5}), "overflows"},
		{"end overflows, single", rawIndex(1, Entry{a, math.MaxUint64, 1}), "overflows"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			x, err := ParseIndex(tc.b)
			if err == nil {
				t.Fatalf("parsed, with %d entries", x.Len())
			}
			if !errors.Is(err, ErrMalformed) {
				t.Errorf("error does not wrap ErrMalformed: %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// An index that declares more entries than it has bytes for must be refused
// before anything is allocated for the entries it declares.
func TestParseIndexHostileCountAllocatesNothing(t *testing.T) {
	for _, count := range []uint64{MaxEntries, MaxEntries + 1, 1 << 60, math.MaxUint64} {
		b := rawIndex(count)
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		_, err := ParseIndex(b)
		runtime.ReadMemStats(&after)
		if !errors.Is(err, ErrMalformed) {
			t.Errorf("count %d: err = %v, want ErrMalformed", count, err)
		}
		if spent := after.TotalAlloc - before.TotalAlloc; spent > 1<<20 {
			t.Errorf("count %d: refusing allocated %d bytes", count, spent)
		}
	}
}

func TestParseIndexMaxPayloadAllowed(t *testing.T) {
	a := sortedKeys(t, 1)[0]
	x, err := ParseIndex(rawIndex(1, Entry{a, 0, amberpack.MaxPayload}))
	if err != nil {
		t.Fatal(err)
	}
	if x.DataSize() != amberpack.MaxPayload {
		t.Errorf("DataSize = %d", x.DataSize())
	}
}

func TestNewIndexRefuses(t *testing.T) {
	keys := sortedKeys(t, 2)
	a, b := keys[0], keys[1]
	reserved := b
	reserved[key.Size-1] |= 0x08

	cases := []struct {
		name    string
		entries []Entry
		want    string
	}{
		{"duplicate keys", []Entry{{b, 0, 10}, {a, 10, 5}, {b, 15, 1}}, "twice"},
		{"non-canonical key", []Entry{{reserved, 0, 10}}, "not canonical"},
		{"length above MaxPayload", []Entry{{a, 0, amberpack.MaxPayload + 1}}, "bytes is above the limit"},
		{"gap", []Entry{{b, 11, 5}, {a, 0, 10}}, "gap"},
		{"overlap", []Entry{{b, 9, 5}, {a, 0, 10}}, "overlaps"},
		{"first offset above 0", []Entry{{a, 1, 10}}, "not 0"},
		{"end overflows", []Entry{{a, math.MaxUint64 - 9, 10}}, "overflows"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewIndex(tc.entries)
			if !errors.Is(err, ErrMalformed) {
				t.Fatalf("err = %v, want ErrMalformed", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}
