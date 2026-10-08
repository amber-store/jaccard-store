package packfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"strings"
	"testing"
)

// rawLinks encodes links as given, valid or not.
func rawLinks(count uint64, starts []uint64, children []uint32) []byte {
	b := []byte(LinksMagic)
	b = binary.BigEndian.AppendUint64(b, count)
	for _, s := range starts {
		b = binary.BigEndian.AppendUint64(b, s)
	}
	for _, c := range children {
		b = binary.BigEndian.AppendUint32(b, c)
	}
	return b
}

func TestLinksRoundTrip(t *testing.T) {
	children := [][]uint32{{1, 2, 3}, nil, {3}, {}, {0, 4}}
	l := NewLinks(children)
	if l.Len() != 5 {
		t.Fatalf("Len = %d, want 5", l.Len())
	}
	for i, want := range children {
		if got := l.Children(i); !slices.Equal(got, want) {
			t.Errorf("Children(%d) = %v, want %v", i, got, want)
		}
	}
	b := l.Encode()
	want := rawLinks(5, []uint64{0, 3, 3, 4, 4, 6}, []uint32{1, 2, 3, 3, 0, 4})
	if !bytes.Equal(b, want) {
		t.Errorf("encoded links are\n% x, want\n% x", b, want)
	}
	parsed, err := ParseLinks(b, 5)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Len() != 5 {
		t.Fatalf("parsed Len = %d, want 5", parsed.Len())
	}
	for i, want := range children {
		if got := parsed.Children(i); !slices.Equal(got, want) {
			t.Errorf("parsed Children(%d) = %v, want %v", i, got, want)
		}
	}
	if !bytes.Equal(parsed.Encode(), b) {
		t.Error("parsed links encode to other bytes")
	}
}

func TestLinksEmpty(t *testing.T) {
	l := NewLinks(nil)
	if l.Len() != 0 {
		t.Errorf("Len = %d", l.Len())
	}
	b := l.Encode()
	if !bytes.Equal(b, rawLinks(0, []uint64{0}, nil)) {
		t.Errorf("empty links encode to % x", b)
	}
	if _, err := ParseLinks(b, 0); err != nil {
		t.Errorf("parsing empty links: %v", err)
	}
}

func TestNewLinksSortsAndDropsRepeats(t *testing.T) {
	given := [][]uint32{{2, 0, 2, 1, 0, 2}, {1, 1, 1}, {0}}
	kept := [][]uint32{slices.Clone(given[0]), slices.Clone(given[1]), slices.Clone(given[2])}
	l := NewLinks(given)
	for i, want := range [][]uint32{{0, 1, 2}, {1}, {0}} {
		if got := l.Children(i); !slices.Equal(got, want) {
			t.Errorf("Children(%d) = %v, want %v", i, got, want)
		}
	}
	for i := range given {
		if !slices.Equal(given[i], kept[i]) {
			t.Errorf("NewLinks changed the caller's list %d to %v", i, given[i])
		}
	}
	if _, err := ParseLinks(l.Encode(), 3); err != nil {
		t.Errorf("parsing: %v", err)
	}
}

func TestParseLinksRefuses(t *testing.T) {
	valid := rawLinks(3, []uint64{0, 2, 2, 3}, []uint32{1, 2, 0})
	if _, err := ParseLinks(valid, 3); err != nil {
		t.Fatalf("the links the cases are derived from: %v", err)
	}
	cases := []struct {
		name string
		b    []byte
		n    int
		want string
	}{
		{"empty input", nil, 3, "header"},
		{"short header", valid[:15], 3, "header"},
		{"bad magic", append([]byte("JACLNK\x00\x02"), valid[8:]...), 3, "magic"},
		{"index magic", append([]byte(IndexMagic), valid[8:]...), 3, "magic"},
		{"count above n", valid, 2, "the index has"},
		{"count below n", valid, 4, "the index has"},
		{"2^60 objects in 16 bytes", rawLinks(1<<60, nil, nil), 1 << 30, "the index has"},
		{"starts cut short", rawLinks(3, []uint64{0, 2, 2}, nil), 3, "starts"},
		{"MaxEntries objects in 16 bytes", rawLinks(MaxEntries, nil, nil), MaxEntries, "starts"},
		{"child at n", rawLinks(3, []uint64{0, 2, 2, 3}, []uint32{1, 3, 0}), 3, "position 3"},
		{"child above n", rawLinks(3, []uint64{0, 2, 2, 3}, []uint32{1, 2, 1 << 31}), 3, "position 2147483648"},
		{"starts decrease", rawLinks(3, []uint64{0, 2, 1, 3}, []uint32{1, 2, 0}), 3, "decrease"},
		{"first start above 0", rawLinks(3, []uint64{1, 2, 2, 3}, []uint32{1, 2, 0}), 3, "start at 0"},
		{"a start past the children", rawLinks(3, []uint64{0, 2, 4, 3}, []uint32{1, 2, 0}), 3, "past the"},
		{"the last start past the children", rawLinks(3, []uint64{0, 2, 2, 4}, []uint32{1, 2, 0}), 3, "past the"},
		{"a start near 2^64", rawLinks(3, []uint64{0, 2, 1<<64 - 1, 3}, []uint32{1, 2, 0}), 3, "past the"},
		{"children left over", rawLinks(3, []uint64{0, 2, 2, 2}, []uint32{1, 2, 0}), 3, "left over"},
		{"half a child", append(slices.Clone(valid), 0, 0), 3, "whole number"},
		{"a child listed twice", rawLinks(3, []uint64{0, 2, 2, 3}, []uint32{1, 1, 0}), 3, "ascending"},
		{"children out of order", rawLinks(3, []uint64{0, 2, 2, 3}, []uint32{2, 1, 0}), 3, "ascending"},
		{"negative n", rawLinks(0, []uint64{0}, nil), -1, "the index has"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseLinks(tc.b, tc.n)
			if !errors.Is(err, ErrMalformed) {
				t.Fatalf("err = %v, want ErrMalformed", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}
