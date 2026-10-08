package human

import (
	"testing"
	"time"
)

func TestBytes(t *testing.T) {
	for n, want := range map[uint64]string{
		0:             "0 B",
		1023:          "1023 B",
		1024:          "1.00 KiB",
		1536:          "1.50 KiB",
		5 << 20:       "5.00 MiB",
		3 << 30:       "3.00 GiB",
		2 << 40:       "2.00 TiB",
		2048 << 40:    "2048.00 TiB",
		1<<30 + 1<<29: "1.50 GiB",
	} {
		if got := Bytes(n); got != want {
			t.Errorf("Bytes(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestBytesIn(t *testing.T) {
	for _, tc := range []struct {
		n, of uint64
		want  string
	}{
		{0, 1000, "0"},
		{500, 1000, "500"},
		{0, 2 << 30, "0.00"},
		{450 << 20, 2 << 30, "0.44"},
		{2 << 30, 2 << 30, "2.00"},
		{512, 2048, "0.50"},
	} {
		if got := BytesIn(tc.n, tc.of); got != tc.want {
			t.Errorf("BytesIn(%d, %d) = %q, want %q", tc.n, tc.of, got, tc.want)
		}
	}
}

func TestCount(t *testing.T) {
	for n, want := range map[uint64]string{
		0:                    "0",
		7:                    "7",
		999:                  "999",
		1000:                 "1,000",
		12345:                "12,345",
		123456:               "123,456",
		1234567:              "1,234,567",
		18446744073709551615: "18,446,744,073,709,551,615",
	} {
		if got := Count(n); got != want {
			t.Errorf("Count(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestSeconds(t *testing.T) {
	for d, want := range map[time.Duration]string{
		-time.Second:                          "0s",
		0:                                     "0s",
		499 * time.Millisecond:                "0s",
		500 * time.Millisecond:                "1s",
		59 * time.Second:                      "59s",
		59*time.Second + 600*time.Millisecond: "1m00s",
		65 * time.Second:                      "1m05s",
		time.Hour:                             "1h00m00s",
		2*time.Hour + 3*time.Minute + 9*time.Second: "2h03m09s",
	} {
		if got := Seconds(d); got != want {
			t.Errorf("Seconds(%v) = %q, want %q", d, got, want)
		}
	}
}

func TestDuration(t *testing.T) {
	for d, want := range map[time.Duration]string{
		-time.Second:             "0.0s",
		0:                        "0.0s",
		40 * time.Millisecond:    "0.0s",
		440 * time.Millisecond:   "0.4s",
		9940 * time.Millisecond:  "9.9s",
		9960 * time.Millisecond:  "10s",
		27300 * time.Millisecond: "27s",
		65 * time.Second:         "1m05s",
	} {
		if got := Duration(d); got != want {
			t.Errorf("Duration(%v) = %q, want %q", d, got, want)
		}
	}
}
