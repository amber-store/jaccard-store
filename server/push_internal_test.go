package server

import "testing"

func TestPartSizeFor(t *testing.T) {
	const mib = 1 << 20
	for _, tc := range []struct {
		size, configured, want int64
	}{
		{0, 64 * mib, 64 * mib},
		{64 * mib, 64 * mib, 64 * mib},
		{maxParts * 64 * mib, 64 * mib, 64 * mib}, // exactly 10,000 parts
		{maxParts*64*mib + 1, 64 * mib, 65 * mib}, // one byte more needs larger parts
		{maxDataSize, 64 * mib, 525 * mib},        // 5 TiB in 10,000 parts is 524.3 MiB a part
		{maxDataSize, 5 * mib, 525 * mib},
		{1000, 100, 100}, // tests lay out tiny parts
	} {
		got := partSizeFor(tc.size, tc.configured)
		if got != tc.want {
			t.Errorf("partSizeFor(%d, %d) = %d, want %d", tc.size, tc.configured, got, tc.want)
		}
		if parts := (tc.size + got - 1) / got; parts > maxParts {
			t.Errorf("partSizeFor(%d, %d) = %d leaves %d parts", tc.size, tc.configured, got, parts)
		}
	}
}
