package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/amber-store/jaccard-store/client"
)

// clock is a time that moves when it is told to.
type clock struct{ at time.Time }

func (c *clock) now() time.Time       { return c.at }
func (c *clock) pass(d time.Duration) { c.at = c.at.Add(d) }

// testProgress returns a progress that writes to a buffer and reads a clock
// of the test's. Its own ticking never comes: the test calls tick.
func testProgress(live bool) (*progress, *bytes.Buffer, *clock) {
	var out bytes.Buffer
	c := &clock{at: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	p := &progress{out: &out, live: live, now: c.now, every: time.Hour, width: func() int { return 100 }}
	return p, &out, c
}

var (
	uploading = view{name: "uploading", unit: client.Bytes, total: 2 << 30, done: 1 << 30, rate: 40 << 20, elapsed: 27 * time.Second}
	packing   = view{name: "packing shared objects", unit: client.Objects, total: 1234567, done: 456789, rate: 12345, elapsed: 65 * time.Second}
	reading   = view{name: "reading the tree", unit: client.Objects, done: 4321, rate: 800, elapsed: 5 * time.Second}
	verifying = view{name: "verifying on the server", elapsed: 75 * time.Second}
)

func TestLiveLine(t *testing.T) {
	for _, tc := range []struct {
		v     view
		width int
		want  string
	}{
		// Wide: everything, and the bar at its longest.
		{uploading, 140, "⠙ uploading                     27s  ███████████████░░░░░░░░░░░░░░░   50.0%  1.00 / 2.00 GiB   40.00 MiB/s  eta 26s"},
		// Narrower: the bar gives way first,
		{uploading, 100, "⠙ uploading                     27s  █████▌░░░░░   50.0%  1.00 / 2.00 GiB   40.00 MiB/s  eta 26s"},
		// then the amounts go, then the rate.
		{uploading, 90, "⠙ uploading                     27s  █████████░░░░░░░░░   50.0%   40.00 MiB/s  eta 26s"},
		{uploading, 80, "⠙ uploading                     27s  ████░░░░   50.0%   40.00 MiB/s  eta 26s"},
		{uploading, 70, "⠙ uploading                     27s  ██████░░░░░░   50.0%  eta 26s"},
		// Too narrow for a bar.
		{uploading, 60, "⠙ uploading                     27s   50.0%  eta 26s"},
		{packing, 140, "⠙ packing shared objects      1m05s  ███████████░░░░░░░░░░░░░░░░░░░   37.0%    456,789 / 1,234,567 objects     12,345 objects/s  eta 1m03s"},
		// No total: what is done and how fast.
		{reading, 100, "⠙ reading the tree               5s  4,321 objects  800 objects/s"},
		// Nothing to count: the time alone.
		{verifying, 100, "⠙ verifying on the server     1m15s"},
	} {
		if got := liveLine(tc.v, 1, tc.width); got != tc.want {
			t.Errorf("at %d columns:\n got %q\nwant %q", tc.width, got, tc.want)
		}
	}
}

func TestLiveLineNeverReachesTheLastColumn(t *testing.T) {
	for _, v := range []view{uploading, packing, reading, verifying} {
		for width := 1; width <= 200; width++ {
			if n := utf8.RuneCountInString(liveLine(v, 0, width)); n >= width {
				t.Fatalf("%s at %d columns is %d wide", v.name, width, n)
			}
		}
	}
}

func TestTheSpinnerTurns(t *testing.T) {
	seen := map[rune]bool{}
	for frame := range 3 * len(spinner) {
		r, _ := utf8.DecodeRuneInString(liveLine(verifying, frame, 100))
		seen[r] = true
	}
	if len(seen) != len(spinner) {
		t.Fatalf("%d marks shown, want the %d of the spinner", len(seen), len(spinner))
	}
}

func TestPlainLine(t *testing.T) {
	for _, tc := range []struct {
		v    view
		want string
	}{
		{uploading, "uploading: 50.0%, 1.00 GiB of 2.00 GiB, 40.00 MiB/s, elapsed 27s, eta 26s"},
		{packing, "packing shared objects: 37.0%, 456,789 of 1,234,567 objects, 12,345 objects/s, elapsed 1m05s, eta 1m03s"},
		{reading, "reading the tree: 4,321 objects, 800 objects/s, elapsed 5s"},
		{verifying, "verifying on the server: elapsed 1m15s"},
	} {
		if got := plainLine(tc.v); got != tc.want {
			t.Errorf("got %q\nwant %q", got, tc.want)
		}
	}
}

func TestDoneLine(t *testing.T) {
	for _, tc := range []struct {
		mark, name string
		took       time.Duration
		summary    string
		want       string
	}{
		{"✓", "connecting to the server", 420 * time.Millisecond, "", "✓ connecting to the server     0.4s"},
		{"✓", "packing", 27300 * time.Millisecond, "base pack of 12 objects", "✓ packing                       27s  base pack of 12 objects"},
		{"✗", "uploading", 12 * time.Second, "interrupted", "✗ uploading                     12s  interrupted"},
		{" ", "total", 145 * time.Second, "", "  total                       2m25s"},
	} {
		if got := doneLine(tc.mark, tc.name, tc.took, tc.summary); got != tc.want {
			t.Errorf("got %q\nwant %q", got, tc.want)
		}
	}
}

func TestBar(t *testing.T) {
	for _, tc := range []struct {
		fraction float64
		want     string
	}{
		{-1, "░░░░░░░░░░"},
		{0, "░░░░░░░░░░"},
		{0.5, "█████░░░░░"},
		{0.55, "█████▌░░░░"},
		{0.999, "█████████▉"},
		{1, "██████████"},
		{2, "██████████"},
	} {
		got := bar(tc.fraction, 10)
		if got != tc.want {
			t.Errorf("bar(%v) = %q, want %q", tc.fraction, got, tc.want)
		}
		if n := utf8.RuneCountInString(got); n != 10 {
			t.Errorf("bar(%v) is %d columns wide, want 10", tc.fraction, n)
		}
	}
}

func TestEta(t *testing.T) {
	const minute = time.Minute
	for _, tc := range []struct {
		v    view
		want string
	}{
		{view{unit: client.Bytes, total: 1000, done: 400, rate: 100, elapsed: minute}, "eta 6s"},
		{view{unit: client.Bytes, total: 1000, done: 400, rate: 0, elapsed: minute}, "eta --"},
		{view{unit: client.Bytes, total: 1000, done: 1000, rate: 0, elapsed: minute}, "eta 0s"},
		// More was counted than there was said to be.
		{view{unit: client.Bytes, total: 1000, done: 1200, rate: 100, elapsed: minute}, "eta 0s"},
		// The first second's rate is no ground for a forecast.
		{view{unit: client.Bytes, total: 1000, done: 400, rate: 100, elapsed: 999 * time.Millisecond}, "eta --"},
		{view{unit: client.Bytes, total: 1000, done: 400, rate: 100, elapsed: time.Second}, "eta 6s"},
		// At this rate it never ends.
		{view{unit: client.Bytes, total: 1 << 50, done: 0, rate: 0.001, elapsed: minute}, "eta --"},
		{view{unit: client.Bytes, total: 360_000, done: 0, rate: 1, elapsed: minute}, "eta --"},
		// Hours away: to the minute.
		{view{unit: client.Bytes, total: 359_999, done: 0, rate: 1, elapsed: minute}, "eta 99h59m"},
		{view{unit: client.Bytes, total: 3600 + 5*60 + 12, done: 0, rate: 1, elapsed: minute}, "eta 1h05m"},
		{view{unit: client.Bytes, total: 3599, done: 0, rate: 1, elapsed: minute}, "eta 59m59s"},
	} {
		if got := tc.v.eta(); got != tc.want {
			t.Errorf("%+v: %q, want %q", tc.v, got, tc.want)
		}
	}
}

// The time left depends on the rate now, not on that of a start that was
// faster or slower.
func TestTheRateIsThatOfTheLastWindow(t *testing.T) {
	c := &clock{at: time.Unix(0, 0)}
	s := &step{name: "uploading", unit: client.Bytes, total: 100_000, start: c.at, marks: []mark{{at: c.at}}}
	done := int64(0)
	// Twenty seconds at 1000 a second, then twenty at 100.
	var v view
	for range 20 {
		c.pass(time.Second)
		done += 1000
		v = s.view(c.at, done)
	}
	if v.rate != 1000 {
		t.Fatalf("rate %v after twenty steady seconds, want 1000", v.rate)
	}
	for range 20 {
		c.pass(time.Second)
		done += 100
		v = s.view(c.at, done)
	}
	if v.rate != 100 {
		t.Fatalf("rate %v after it slowed down, want 100", v.rate)
	}
	if v.elapsed != 40*time.Second || v.done != 22_000 {
		t.Fatalf("view %+v", v)
	}
	// Work taken back does not make a rate below nothing.
	c.pass(time.Second)
	if v = s.view(c.at, 0); v.rate != 0 || v.done != 0 {
		t.Fatalf("view %+v after everything was taken back", v)
	}
}

func TestProgressOffATerminal(t *testing.T) {
	p, out, c := testProgress(false)
	p.Begin("connecting to the server", 0, client.NoUnit)
	c.pass(400 * time.Millisecond)
	p.End("")

	p.Begin("uploading", 1000, client.Bytes)
	c.pass(2 * time.Second)
	p.Advance(200)
	p.tick() // too soon for a line
	c.pass(3 * time.Second)
	p.Advance(300)
	p.tick()
	c.pass(time.Second)
	p.tick() // too soon after the last
	c.pass(4 * time.Second)
	p.Advance(500)
	p.End("1000 B")
	p.Close(nil)

	want := strings.Join([]string{
		"connecting to the server (0.4s)",
		"uploading: 50.0%, 500 B of 1000 B, 100 B/s, elapsed 5s, eta 5s",
		"uploading: 1000 B (10s)",
		"done in 10s",
		"",
	}, "\n")
	if out.String() != want {
		t.Fatalf("got:\n%s\nwant:\n%s", out.String(), want)
	}
}

func TestProgressOnATerminal(t *testing.T) {
	p, out, c := testProgress(true)
	p.Begin("uploading", 1000, client.Bytes)
	c.pass(5 * time.Second)
	p.Advance(500)
	p.tick()
	c.pass(5 * time.Second)
	p.End("1000 B")
	p.Close(nil)

	// The line of the running step is drawn over, never followed by a
	// new line; the finished step and the total each end theirs.
	want := "\r\033[K⠙ uploading                      0s  ░░░░░░░░░░░░░    0.0%     0 / 1000 B         0 B/s  eta --" +
		"\r\033[K⠹ uploading                      5s  ██████▌░░░░░░   50.0%   500 / 1000 B       100 B/s  eta 5s" +
		"\r\033[K✓ uploading                     10s  1000 B\n" +
		"  total                         10s\n"
	if out.String() != want {
		t.Fatalf("got:\n%q\nwant:\n%q", out.String(), want)
	}
}

func TestColourMarksTheStepsOnly(t *testing.T) {
	p, out, c := testProgress(true)
	p.color = true
	p.Begin("uploading", 0, client.NoUnit)
	c.pass(time.Second)
	p.End("done")
	if got, want := out.String(), "\r\033[K\033[36m⠙\033[0m uploading                      0s"+
		"\r\033[K\033[32m✓\033[0m uploading                    1.0s  done\n"; got != want {
		t.Fatalf("got:\n%q\nwant:\n%q", got, want)
	}
}

func TestAFailedStepIsMarked(t *testing.T) {
	for _, tc := range []struct {
		live bool
		err  error
		want string
	}{
		{false, errors.New("the bucket answered 403"), "uploading: failed after 3.0s\n"},
		{false, fmt.Errorf("push: %w", context.Canceled), "uploading: interrupted after 3.0s\n"},
		{true, errors.New("the bucket answered 403"), "\r\033[K✗ uploading                    3.0s  failed\n"},
		{true, fmt.Errorf("push: %w", context.Canceled), "\r\033[K✗ uploading                    3.0s  interrupted\n"},
	} {
		p, out, c := testProgress(tc.live)
		p.Begin("uploading", 0, client.NoUnit)
		out.Reset()
		c.pass(3 * time.Second)
		p.Close(tc.err)
		if out.String() != tc.want {
			t.Errorf("live %v, %v:\n got %q\nwant %q", tc.live, tc.err, out.String(), tc.want)
		}
	}
}

func TestNothingIsShownAfterClose(t *testing.T) {
	p, out, _ := testProgress(false)
	p.Begin("uploading", 0, client.NoUnit)
	p.End("")
	p.Close(nil)
	shown := out.String()
	p.Begin("uploading", 0, client.NoUnit)
	p.Advance(1)
	p.End("again")
	p.tick()
	p.Close(nil)
	p.Close(errors.New("late"))
	if out.String() != shown {
		t.Fatalf("after Close: %q", strings.TrimPrefix(out.String(), shown))
	}
}

func TestACommandWithoutStepsShowsNothing(t *testing.T) {
	p, out, _ := testProgress(true)
	p.End("nothing began")
	p.tick()
	p.Close(nil)
	if out.Len() != 0 {
		t.Fatalf("shown: %q", out.String())
	}
}

func TestQuietShowsNothing(t *testing.T) {
	var out bytes.Buffer
	p := newProgress(&out, true)
	p.Begin("uploading", 10, client.Bytes)
	p.Advance(10)
	p.End("10 B")
	p.Begin("verifying on the server", 0, client.NoUnit)
	p.Close(errors.New("refused"))
	if out.Len() != 0 {
		t.Fatalf("shown: %q", out.String())
	}
}

func TestABufferIsNotATerminal(t *testing.T) {
	var out bytes.Buffer
	if p := newProgress(&out, false); p.live || p.color {
		t.Fatalf("a buffer taken for a terminal: %+v", p)
	}
}

// The drawing runs beside the command, and Advance comes from the parts of
// an upload that go at once: under the race detector this is the test of
// that.
func TestProgressIsDrawnWhileItAdvances(t *testing.T) {
	var out syncBuffer
	p := &progress{out: &out, live: true, now: time.Now, every: time.Millisecond, width: func() int { return 100 }}
	for _, name := range []string{"packing", "uploading"} {
		p.Begin(name, 8000, client.Bytes)
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				for range 1000 {
					p.Advance(1)
				}
			})
		}
		wg.Wait()
		time.Sleep(5 * time.Millisecond)
		p.End("")
	}
	p.Close(nil)
	if got := out.String(); !strings.Contains(got, "✓ uploading") || !strings.HasSuffix(got, "\n") {
		t.Fatalf("shown: %q", got)
	}
	if n := strings.Count(out.String(), "\n"); n != 3 {
		t.Fatalf("%d lines ended, want one for each step and the total", n)
	}
}

// syncBuffer is a buffer a test may read while the progress writes.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(b)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// A figure that grows a digit must not push the ones after it about: the
// bar and what follows it stay where they are from the first percent to
// the last.
func TestTheLiveLineHoldsStill(t *testing.T) {
	for _, unit := range []client.Unit{client.Bytes, client.Objects} {
		v := view{name: "uploading", unit: unit, total: 5_000_000, elapsed: time.Second}
		var widths, bars = map[int]bool{}, map[int]bool{}
		for done := uint64(0); done <= v.total; done += 1234 {
			v.done, v.rate = done, float64(done%700_000)
			line := liveLine(v, 0, 140)
			widths[utf8.RuneCountInString(line[:strings.Index(line, "eta")])] = true
			bars[strings.Count(line, "█")+strings.Count(line, "░")+strings.Count(line, "▏")+strings.Count(line, "▎")+
				strings.Count(line, "▍")+strings.Count(line, "▌")+strings.Count(line, "▋")+strings.Count(line, "▊")+strings.Count(line, "▉")] = true
		}
		if len(widths) != 1 || len(bars) != 1 {
			t.Errorf("unit %v: the time left begins in %d different columns and the bar has %d different widths", unit, len(widths), len(bars))
		}
	}
}
