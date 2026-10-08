package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/human"
)

// board shows a command that does several things at once: push-subdirs,
// which pushes directories side by side. A directory that is being pushed
// has a row: its name, how long it has been at it, the step it is in and,
// for a step that knows how much there is to do, a bar, the rate and the
// time left. A line under the rows counts the directories and says when
// the last of them should be through. A directory that is done leaves a
// line above the rows: what came of it, or that it failed.
//
// On a terminal the rows are redrawn in place. Elsewhere a directory is a
// plain line when it is done, and the count gets one every few seconds.
type board struct {
	out   io.Writer
	quiet bool // --no-progress: nothing is written
	live  bool // out is a terminal
	color bool
	size  func() (width, height int)
	now   func() time.Time
	every time.Duration

	mu      sync.Mutex
	total   int       // directories there are
	pushed  int       // of them done
	failed  int       // of them failed
	rows    []*row    // the ones running, in the order they began
	drawn   int       // lines of rows and count that are on the terminal
	began   time.Time // when the first directory began; zero before it
	counted time.Time // when the count last got a line off a terminal
	frame   int
	closed  bool
	stop    chan struct{} // closed to end the drawing; nil when none runs
	stopped chan struct{} // closed when the drawing has ended
}

// row is one directory on the board. It is told the steps of that
// directory's push, as a progress is told those of a command.
type row struct {
	board *board
	name  string
	start time.Time
	// done is how far the running step is. Advance comes from many
	// goroutines, and often: it does not wait for the lock.
	done atomic.Int64
	// step is the one last begun. It stays when it has ended, until the
	// next begins: the steps of a push follow one another without a gap
	// worth showing. Under board.mu.
	step *step
}

// stepWidth is the column of a row that says what step it is in: the first
// word of the step's name, which is what is being done. The rest of the
// name is room a row has better use for.
const stepWidth = len("comparing")

// doing is the first word of a step's name.
func doing(step string) string {
	word, _, _ := strings.Cut(step, " ")
	return word
}

// newBoard returns the board of a command that writes to out and has total
// directories to push. With quiet it shows nothing.
func newBoard(out io.Writer, quiet bool, total int) *board {
	b := &board{out: out, quiet: quiet, total: total, now: time.Now, every: time.Second,
		size: func() (int, int) { return 0, 0 }}
	if quiet {
		return b
	}
	if size, colour, ok := terminal(out); ok {
		b.live, b.every, b.color, b.size = true, liveEvery, colour, size
	}
	return b
}

// add puts a directory on the board: its push begins.
func (b *board) add(name string) *row {
	r := &row{board: b, name: name}
	if b.quiet {
		return r
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	r.start = now
	if b.began.IsZero() {
		b.began, b.counted = now, now
	}
	b.rows = append(b.rows, r)
	if b.live {
		b.paint(now)
	}
	if b.stop == nil && !b.closed {
		b.stop, b.stopped = make(chan struct{}), make(chan struct{})
		go b.run(b.stop, b.stopped)
	}
	return r
}

// finish takes a directory off the board and leaves its line: outcome says
// what came of the push, and err, when there is one, that it failed.
func (b *board) finish(r *row, outcome string, err error) {
	if b.quiet {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if i := slices.Index(b.rows, r); i >= 0 {
		b.rows = slices.Delete(b.rows, i, i+1)
	}
	now := b.now()
	took := now.Sub(r.start)
	if err == nil {
		b.pushed++
	} else {
		b.failed++
	}
	if !b.live {
		if err != nil {
			fmt.Fprintf(b.out, "%s: %s after %s: %v\n", r.name, failure(err), human.Duration(took), err)
		} else {
			fmt.Fprintf(b.out, "%s: %s (%s)\n", r.name, outcome, human.Duration(took))
		}
		return
	}
	width, _ := b.size()
	mark, colour := "✓", green
	if err != nil {
		// The line says that it failed and how it began to; all of the
		// error is in what the command says at its end.
		mark, colour, outcome = "✗", red, failure(err)+": "+err.Error()
	}
	line := clip(doneLine(mark, shorten(r.name), took, outcome), width-1)
	b.paint(now, b.paintMark(line, colour))
}

// failure is the word for how a push ended that did not end well.
func failure(err error) string {
	if errors.Is(err, context.Canceled) {
		return "interrupted"
	}
	return "failed"
}

// Close ends the show with a line that counts what became of the
// directories. skipped is how many were never begun, which is what an
// interrupt does to them.
func (b *board) Close(skipped int) {
	if b.quiet {
		return
	}
	b.mu.Lock()
	stop, stopped := b.stop, b.stopped
	b.stop, b.closed = nil, true
	b.mu.Unlock()
	if stop != nil {
		close(stop)
		<-stopped
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.rows = nil
	var took time.Duration
	if !b.began.IsZero() {
		took = b.now().Sub(b.began)
	}
	counts := fmt.Sprintf("%s pushed", human.Count(uint64(b.pushed)))
	if b.failed > 0 {
		counts += fmt.Sprintf(", %s failed", human.Count(uint64(b.failed)))
	}
	if skipped > 0 {
		counts += fmt.Sprintf(", %s not begun", human.Count(uint64(skipped)))
	}
	if !b.live {
		fmt.Fprintf(b.out, "%s: %s (%s)\n", directories(b.total), counts, human.Duration(took))
		return
	}
	var out strings.Builder
	if b.drawn > 0 {
		fmt.Fprintf(&out, "\033[%dA", b.drawn)
	}
	b.drawn = 0
	mark, colour := "✓", green
	if b.failed > 0 || skipped > 0 {
		mark, colour = "✗", red
	}
	fmt.Fprintf(&out, "\r\033[K%s\n\033[J", b.paintMark(doneLine(mark, directories(b.total), took, counts), colour))
	io.WriteString(b.out, out.String())
}

// run looks at the board now and then until stop is closed.
func (b *board) run(stop <-chan struct{}, stopped chan<- struct{}) {
	defer close(stopped)
	t := time.NewTicker(b.every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			b.tick()
		}
	}
}

// tick shows where the directories are: the rows redrawn on a terminal, a
// line of the count elsewhere when the last one is plainEvery old.
func (b *board) tick() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.began.IsZero() {
		return
	}
	now := b.now()
	if b.live {
		b.paint(now)
		return
	}
	if now.Sub(b.counted) < plainEvery {
		return
	}
	b.counted = now
	c := b.count(now)
	line := fmt.Sprintf("%s of %s done", human.Count(uint64(c.pushed+c.failed)), directories(c.total))
	if c.failed > 0 {
		line += fmt.Sprintf(", %s failed", human.Count(uint64(c.failed)))
	}
	fmt.Fprintf(b.out, "%s, %d running, elapsed %s, %s\n", line, c.running, human.Seconds(c.elapsed), c.eta())
}

// paint draws the board on a terminal: the lines above, which are there to
// stay, and under them the rows and the count, over what was drawn of them
// the last time. b.mu is held.
//
// The rows are drawn from where the last drawing began: the cursor goes up
// by as many lines as that one had, every line is written over what is
// there, and what is left under them of a longer drawing is cleared.
func (b *board) paint(now time.Time, above ...string) {
	var out strings.Builder
	if b.drawn > 0 {
		fmt.Fprintf(&out, "\033[%dA", b.drawn)
	}
	for _, line := range above {
		out.WriteString("\r\033[K" + line + "\n")
	}
	b.frame++
	width, height := b.size()
	// No more rows than the terminal is high, less the count and the line
	// the cursor is left on: a drawing that scrolls cannot be drawn over.
	rows := b.rows[:max(0, min(len(b.rows), height-2))]
	for _, r := range rows {
		var running *view
		if r.step != nil {
			v := r.step.view(now, r.done.Load())
			running = &v
		}
		out.WriteString("\r\033[K" + b.paintMark(rowLine(r.name, now.Sub(r.start), running, b.frame, width), cyan) + "\n")
	}
	out.WriteString("\r\033[K" + countLine(b.count(now), width) + "\n\033[J")
	b.drawn = len(rows) + 1
	io.WriteString(b.out, out.String())
}

// paintMark colours the mark a line begins with.
func (b *board) paintMark(line, colour string) string {
	if !b.color {
		return line
	}
	_, n := utf8.DecodeRuneInString(line)
	return colour + line[:n] + "\033[0m" + line[n:]
}

// count is the directories at one moment. b.mu is held.
func (b *board) count(now time.Time) count {
	return count{total: b.total, pushed: b.pushed, failed: b.failed, running: len(b.rows), elapsed: now.Sub(b.began)}
}

// count is how the directories of a board stand at one moment.
type count struct {
	total, pushed, failed, running int
	elapsed                        time.Duration
}

// eta is the time left for all the directories at the pace of those that
// are done: "--" while none is, for then there is no telling.
func (c count) eta() string {
	done := c.pushed + c.failed
	switch {
	case done >= c.total:
		return "eta 0s"
	case done == 0:
		return "eta --"
	}
	left := c.elapsed.Seconds() / float64(done) * float64(c.total-done)
	if left >= 3600 {
		return fmt.Sprintf("eta %dh%02dm", int(left)/3600, int(left)/60%60)
	}
	return "eta " + human.Seconds(time.Duration(left*float64(time.Second)))
}

// directories is a number of directories in words.
func directories(n int) string {
	if n == 1 {
		return "1 directory"
	}
	return human.Count(uint64(n)) + " directories"
}

// shorten cuts a name to the column of names, and says with its last
// character that it did.
func shorten(name string) string {
	if utf8.RuneCountInString(name) <= nameWidth {
		return name
	}
	return string([]rune(name)[:nameWidth-1]) + "…"
}

// rowLine is the row of a directory that is being pushed, on a terminal
// width columns wide: its name, the time since its push began, the step it
// is in and the figures of that step, as the line of a command's running
// step has them.
func rowLine(name string, elapsed time.Duration, running *view, frame, width int) string {
	line := lead(string(spinner[frame%len(spinner)]), shorten(name), human.Seconds(elapsed))
	if running != nil {
		line = withFigures(fmt.Sprintf("%s  %-*s", line, stepWidth, doing(running.name)), *running, width)
	}
	return strings.TrimRight(clip(line, width-1), " ")
}

// countLine is the line under the rows: how many directories there are,
// the time since the first began, how many are done as a bar and in
// figures, and the time left for the rest.
func countLine(c count, width int) string {
	line := lead(" ", directories(c.total), human.Seconds(c.elapsed))
	done := c.pushed + c.failed
	fraction := 1.0
	if c.total > 0 {
		fraction = float64(done) / float64(c.total)
	}
	counts := fmt.Sprintf("%d pushed", c.pushed)
	if c.failed > 0 {
		counts += fmt.Sprintf(", %d failed", c.failed)
	}
	percent := fmt.Sprintf("%5.1f%%", 100*fraction)
	// A narrow terminal loses how many are running first, then the bar,
	// then the counts.
	room := width - 1 - utf8.RuneCountInString(line) - 2
	choices := []string{
		fmt.Sprintf("%s  %s, %d running  %s", percent, counts, c.running, c.eta()),
		fmt.Sprintf("%s  %s  %s", percent, counts, c.eta()),
		fmt.Sprintf("%s  %s", percent, c.eta()),
	}
	figures := choices[len(choices)-1]
	for _, choice := range choices {
		if utf8.RuneCountInString(choice) <= room {
			figures = choice
			break
		}
	}
	if n := room - utf8.RuneCountInString(figures) - 2; n >= minBar {
		line += "  " + bar(fraction, min(n, maxBar))
	}
	return clip(line+"  "+figures, width-1)
}

// Begin starts a step of the directory's push.
func (r *row) Begin(name string, total uint64, unit client.Unit) {
	b := r.board
	if b.quiet {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	r.done.Store(0)
	r.step = &step{name: name, total: total, unit: unit, start: now, drawn: now, marks: []mark{{at: now}}}
}

// Advance reports n more of the running step's unit done.
func (r *row) Advance(n int64) { r.done.Add(n) }

// End finishes a step. The row goes on showing it until the next begins;
// what came of a step is too much for a row, and the directory's own line
// says what came of all of them.
func (r *row) End(string) {}

// Fail marks a step as the one that failed. The directory's line says so
// when its push is over.
func (r *row) Fail(error) {}
