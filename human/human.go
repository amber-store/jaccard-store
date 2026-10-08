// Package human formats numbers for people: byte counts, object counts and
// durations, as the commands and the progress reports print them.
package human

import (
	"fmt"
	"strconv"
	"time"
)

// scale returns the binary unit a byte count is written in, and how many
// bytes one of that unit is.
func scale(n uint64) (unit string, size uint64) {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	u, size := 0, uint64(1)
	for n/size >= 1024 && u < len(units)-1 {
		size *= 1024
		u++
	}
	return units[u], size
}

// Bytes formats a byte count with a binary unit.
func Bytes(n uint64) string {
	unit, size := scale(n)
	if size == 1 {
		return fmt.Sprintf("%d B", n)
	}
	return fmt.Sprintf("%.2f %s", float64(n)/float64(size), unit)
}

// BytesIn formats n in the unit Bytes writes of in, and leaves the unit
// out: it is the first half of "0.44 / 2.00 GiB", a part beside its whole.
func BytesIn(n, of uint64) string {
	_, size := scale(of)
	if size == 1 {
		return fmt.Sprintf("%d", n)
	}
	return fmt.Sprintf("%.2f", float64(n)/float64(size))
}

// Count formats a count with a comma between every three digits.
func Count(n uint64) string {
	s := strconv.FormatUint(n, 10)
	out := make([]byte, 0, len(s)+len(s)/3)
	for i := range len(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, s[i])
	}
	return string(out)
}

// Seconds formats a duration to the second: 5s, 1m05s, 2h03m09s. It is the
// form of a time that is still running, which tenths would only blur.
func Seconds(d time.Duration) string {
	s := int64(d.Round(time.Second) / time.Second)
	if s < 0 {
		s = 0
	}
	switch {
	case s < 60:
		return fmt.Sprintf("%ds", s)
	case s < 3600:
		return fmt.Sprintf("%dm%02ds", s/60, s%60)
	default:
		return fmt.Sprintf("%dh%02dm%02ds", s/3600, s/60%60, s%60)
	}
}

// Duration formats a time that was taken: to the tenth of a second below
// ten seconds, where the tenths say something, and to the second above.
func Duration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if tenths := d.Round(100 * time.Millisecond); tenths < 10*time.Second {
		return fmt.Sprintf("%.1fs", tenths.Seconds())
	}
	return Seconds(d)
}
