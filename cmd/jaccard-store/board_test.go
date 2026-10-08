package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/amber-store/jaccard-store/client"
)

// testBoard returns a board that writes to a buffer and reads a clock of
// the test's, on a terminal of 100 columns and 24 lines when it is live.
// Its own ticking never comes: the test calls tick.
func testBoard(live bool, total int) (*board, *bytes.Buffer, *clock) {
	var out bytes.Buffer
	c := &clock{at: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	b := &board{out: &out, live: live, total: total, now: c.now, every: time.Hour,
		size: func() (int, int) { return 100, 24 }}
	return b, &out, c
}

// screen is what a terminal shows of what was written to it: enough of one
// to see what the board leaves on it, which is the point of its cursor
// movements.
func screen(t *testing.T, written string) []string {
	t.Helper()
	lines := [][]rune{nil}
	row, col := 0, 0
	for i := 0; i < len(written); {
		switch {
		case written[i] == '\r':
			col = 0
			i++
		case written[i] == '\n':
			row, col = row+1, 0
			if row == len(lines) {
				lines = append(lines, nil)
			}
			i++
		case strings.HasPrefix(written[i:], "\033["):
			end := i + 2
			for end < len(written) && (written[end] < '@' || written[end] > '~') {
				end++
			}
			arg := written[i+2 : end]
			switch written[end] {
			case 'A':
				n, err := strconv.Atoi(arg)
				if err != nil || n > row {
					t.Fatalf("the cursor goes up %q lines from line %d", arg, row)
				}
				row -= n
			case 'K':
				lines[row] = lines[row][:min(col, len(lines[row]))]
			case 'J':
				lines[row] = lines[row][:min(col, len(lines[row]))]
				lines = lines[:row+1]
			case 'm': // a colour
			default:
				t.Fatalf("an escape this test does not know: %q", written[i:end+1])
			}
			i = end + 1
		default:
			r, n := utf8.DecodeRuneInString(written[i:])
			for len(lines[row]) < col {
				lines[row] = append(lines[row], ' ')
			}
			if col < len(lines[row]) {
				lines[row][col] = r
			} else {
				lines[row] = append(lines[row], r)
			}
			col++
			i += n
		}
	}
	var out []string
	for _, l := range lines {
		out = append(out, string(l))
	}
	// The cursor rests on an empty line under what was drawn.
	if n := len(out); out[n-1] == "" {
		out = out[:n-1]
	}
	return out
}

func sameScreen(t *testing.T, when string, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("%s the terminal shows:\n%s\nwant:\n%s", when, strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestTheBoardOnATerminal(t *testing.T) {
	b, out, c := testBoard(true, 3)
	alpha := b.add("alpha")
	alpha.Begin("importing", 1000, client.Bytes)
	beta := b.add("beta")
	beta.Begin("scanning the directory", 0, client.NoUnit)
	c.pass(2 * time.Second)
	alpha.Advance(500)
	b.tick()
	sameScreen(t, "with two under way", screen(t, out.String()),
		"⠸ alpha                          2s  importing  ████████▌░░░░░░░░   50.0%       250 B/s  eta 2s",
		"⠸ beta                           2s  scanning",
		"  3 directories                  2s  ░░░░░░░░░░░░░░░░░░░░░░░░░    0.0%  0 pushed, 2 running  eta --",
	)

	// One that is done leaves its line above the rows; the next takes a
	// row of its own.
	c.pass(time.Second)
	b.finish(alpha, "base pack, 4.00 KiB", nil)
	gamma := b.add("gamma")
	gamma.Begin("uploading", 0, client.Bytes)
	gamma.Advance(2048)
	c.pass(time.Second)
	b.tick()
	sameScreen(t, "with one done", screen(t, out.String()),
		"✓ alpha                        3.0s  base pack, 4.00 KiB",
		"⠦ beta                           4s  scanning",
		"⠦ gamma                          1s  uploading  2.00 KiB  2.00 KiB/s",
		"  3 directories                  4s  ████████▎░░░░░░░░░░░░░░░░   33.3%  1 pushed, 2 running  eta 8s",
	)

	// One that fails says so, and as much of why as the line holds.
	b.finish(beta, "", errors.New("scanning /somewhere/far/away/that/takes/a/whole/line/to/name/in/full/beta: permission denied"))
	c.pass(time.Second)
	b.finish(gamma, "patch pack, 12 B", nil)
	sameScreen(t, "with all done", screen(t, out.String()),
		"✓ alpha                        3.0s  base pack, 4.00 KiB",
		"✗ beta                         4.0s  failed: scanning /somewhere/far/away/that/takes/a/whole/line/t",
		"✓ gamma                        2.0s  patch pack, 12 B",
		"  3 directories                  5s  ███████████████  100.0%  2 pushed, 1 failed, 0 running  eta 0s",
	)

	// The count of the end takes the place of the one that was running.
	b.Close(0)
	sameScreen(t, "at the end", screen(t, out.String()),
		"✓ alpha                        3.0s  base pack, 4.00 KiB",
		"✗ beta                         4.0s  failed: scanning /somewhere/far/away/that/takes/a/whole/line/t",
		"✓ gamma                        2.0s  patch pack, 12 B",
		"✗ 3 directories                5.0s  2 pushed, 1 failed",
	)
}

// No line the board draws over may be longer than the terminal is wide,
// and it may not draw more of them than the terminal is high: either would
// put the cursor somewhere else than the board thinks it is.
func TestTheBoardStaysWithinTheTerminal(t *testing.T) {
	for _, size := range [][2]int{{120, 40}, {80, 24}, {60, 8}, {40, 5}, {20, 3}} {
		b, out, c := testBoard(true, 30)
		b.size = func() (int, int) { return size[0], size[1] }
		var rows []*row
		for i := range 12 {
			r := b.add(fmt.Sprintf("a-directory-with-quite-a-long-name-%d", i))
			r.Begin("comparing nearby packs", 1<<30, client.Bytes)
			r.Advance(int64(i) << 20)
			rows = append(rows, r)
			c.pass(time.Second)
			b.tick()
		}
		for i, r := range rows[:6] {
			var err error
			if i%2 == 1 {
				err = errors.New(strings.Repeat("a long story of what went wrong ", 10))
			}
			b.finish(r, "base pack, 1.00 GiB", err)
			c.pass(time.Second)
			b.tick()
		}
		shown := screen(t, out.String())
		for _, line := range shown {
			if n := utf8.RuneCountInString(line); n >= size[0] {
				t.Errorf("%dx%d: a line of %d columns: %q", size[0], size[1], n, line)
			}
		}
		// Six lines stay; under them the rows there is room for, and
		// the count.
		if rows := len(shown) - 6; rows > size[1]-1 || rows < 1 {
			t.Errorf("%dx%d: %d lines are drawn over", size[0], size[1], rows)
		}
		b.Close(0)
		if got := screen(t, out.String()); len(got) != 7 || !strings.Contains(got[6], "30 directories") {
			t.Errorf("%dx%d: at the end the terminal shows:\n%s", size[0], size[1], strings.Join(got, "\n"))
		}
	}
}

func TestTheBoardOffATerminal(t *testing.T) {
	b, out, c := testBoard(false, 4)
	alpha, beta := b.add("alpha"), b.add("beta")
	alpha.Begin("importing", 1000, client.Bytes)
	alpha.Advance(500)
	c.pass(2 * time.Second)
	b.tick() // too soon for a line
	b.finish(alpha, "base pack, 4.00 KiB", nil)
	c.pass(3 * time.Second)
	b.tick()
	c.pass(time.Second)
	b.tick() // too soon after the last
	b.finish(beta, "", fmt.Errorf("push: %w", context.Canceled))
	gamma := b.add("gamma")
	c.pass(4 * time.Second)
	b.finish(gamma, "", errors.New("the bucket answered 403"))
	b.Close(1)

	want := strings.Join([]string{
		"alpha: base pack, 4.00 KiB (2.0s)",
		"1 of 4 directories done, 1 running, elapsed 5s, eta 15s",
		"beta: interrupted after 6.0s: push: context canceled",
		"gamma: failed after 4.0s: the bucket answered 403",
		"4 directories: 1 pushed, 2 failed, 1 not begun (10s)",
		"",
	}, "\n")
	if out.String() != want {
		t.Fatalf("got:\n%s\nwant:\n%s", out.String(), want)
	}
}

func TestAQuietBoardShowsNothing(t *testing.T) {
	var out bytes.Buffer
	b := newBoard(&out, true, 2)
	r := b.add("alpha")
	r.Begin("uploading", 10, client.Bytes)
	r.Advance(10)
	r.End("10 B")
	r.Fail(errors.New("refused"))
	b.finish(r, "", errors.New("refused"))
	b.tick()
	b.Close(1)
	if out.Len() != 0 {
		t.Fatalf("shown: %q", out.String())
	}
	if b := newBoard(&out, false, 2); b.live || b.color {
		t.Fatalf("a buffer taken for a terminal: %+v", b)
	}
}

func TestRowAndCountLines(t *testing.T) {
	uploading.name = "uploading"
	for _, tc := range []struct {
		got, want string
	}{
		// The name of the directory, the time since its push began, what
		// is being done, and the figures the line of a command's step has.
		{rowLine("alpha", 31*time.Second, &uploading, 1, 140),
			"⠙ alpha                         31s  uploading  ███████████████░░░░░░░░░░░░░░░   50.0%  1.00 / 2.00 GiB   40.00 MiB/s  eta 26s"},
		{rowLine("alpha", 31*time.Second, &uploading, 1, 100),
			"⠙ alpha                         31s  uploading  ████████▌░░░░░░░░   50.0%   40.00 MiB/s  eta 26s"},
		{rowLine("alpha", 31*time.Second, &uploading, 1, 80),
			"⠙ alpha                         31s  uploading  █████▌░░░░░   50.0%  eta 26s"},
		// Of a step's name the first word is shown: what is being done.
		{rowLine("a-name-that-is-longer-than-its-column", 5*time.Second, &verifying, 1, 100),
			"⠙ a-name-that-is-longer-t…       5s  verifying"},
		{rowLine("alpha", 0, nil, 1, 100),
			"⠙ alpha                          0s"},
		{countLine(count{total: 12, pushed: 2, failed: 1, running: 5, elapsed: 30 * time.Second}, 100),
			"  12 directories                30s  ███░░░░░░░░░   25.0%  2 pushed, 1 failed, 5 running  eta 1m30s"},
		{countLine(count{total: 1, running: 1}, 100),
			"  1 directory                    0s  ░░░░░░░░░░░░░░░░░░░░░░░░░    0.0%  0 pushed, 1 running  eta --"},
		// Narrower: how many are running goes, then the counts.
		{countLine(count{total: 12, pushed: 2, failed: 1, running: 5, elapsed: 30 * time.Second}, 80),
			"  12 directories                30s   25.0%  2 pushed, 1 failed  eta 1m30s"},
		{countLine(count{total: 12, pushed: 2, failed: 1, running: 5, elapsed: 30 * time.Second}, 60),
			"  12 directories                30s   25.0%  eta 1m30s"},
	} {
		if tc.got != tc.want {
			t.Errorf("got %q\nwant %q", tc.got, tc.want)
		}
	}
}

// The drawing runs beside the pushes, which begin, advance and end as they
// please: under the race detector this is the test of that.
func TestTheBoardIsDrawnWhileDirectoriesComeAndGo(t *testing.T) {
	var out syncBuffer
	b := &board{out: &out, live: true, total: 40, now: time.Now, every: time.Millisecond,
		size: func() (int, int) { return 100, 24 }}
	var wg sync.WaitGroup
	free := make(chan struct{}, 5)
	for i := range 40 {
		free <- struct{}{}
		wg.Go(func() {
			defer func() { <-free }()
			r := b.add(fmt.Sprintf("dir-%02d", i))
			for _, step := range []string{"importing", "packing", "uploading"} {
				r.Begin(step, 300, client.Bytes)
				for range 300 {
					r.Advance(1)
				}
				r.End("")
			}
			var err error
			if i%7 == 0 {
				err = errors.New("refused")
			}
			b.finish(r, "base pack, 300 B", err)
		})
	}
	wg.Wait()
	b.Close(0)
	shown := screen(t, out.String())
	if len(shown) != 41 || !strings.Contains(shown[40], "40 directories") || !strings.Contains(shown[40], "34 pushed, 6 failed") {
		t.Fatalf("the terminal shows %d lines, the last of them %q", len(shown), shown[len(shown)-1])
	}
}
