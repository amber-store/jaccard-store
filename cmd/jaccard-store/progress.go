package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/human"
	"golang.org/x/term"
)

// progress shows on standard error what a push or a pull is doing. Every
// step gets a line: its name, the time it took and what came of it. On a
// terminal the line of the running step is redrawn in place, with a bar,
// the rate and the time left when the step knows how much there is to do.
// Elsewhere the running step is reported on a line of its own every few
// seconds, so that a log shows a long transfer moving.
//
// It is the client.Progress of a command, which adds steps of its own.
type progress struct {
	out   io.Writer
	quiet bool // --no-progress: nothing is written
	live  bool // out is a terminal
	color bool
	width func() int
	now   func() time.Time
	every time.Duration // between two looks at the running step

	// done is how far the running step is. Advance comes from many
	// goroutines, and often: it does not wait for the lock.
	done atomic.Int64

	mu      sync.Mutex
	began   time.Time // when the first step began; zero before it
	step    *step     // the running one; nil between steps
	frame   int
	closed  bool
	stop    chan struct{} // closed to end the drawing; nil when none runs
	stopped chan struct{} // closed when the drawing has ended
}

// step is the running step.
type step struct {
	name  string
	total uint64
	unit  client.Unit
	start time.Time
	drawn time.Time // when a line was last written for it off a terminal
	marks []mark    // what was done at recent moments: the rate is read off them
}

// mark is how far a step was at one moment.
type mark struct {
	at   time.Time
	done int64
}

const (
	// liveEvery is how often the running step is redrawn on a terminal,
	// and plainEvery how often it gets a line elsewhere.
	liveEvery  = 100 * time.Millisecond
	plainEvery = 5 * time.Second
	// rateWindow is the stretch of time the rate is taken over: long
	// enough to be steady, short enough to follow a transfer that slows.
	rateWindow = 10 * time.Second
	// fallbackWidth is taken for a terminal that does not tell its own.
	fallbackWidth = 80
)

// newProgress returns the progress of a command that writes to out. With
// quiet it shows nothing.
func newProgress(out io.Writer, quiet bool) *progress {
	p := &progress{out: out, quiet: quiet, now: time.Now, every: time.Second, width: func() int { return 0 }}
	f, ok := out.(*os.File)
	if !ok || quiet || !term.IsTerminal(int(f.Fd())) || os.Getenv("TERM") == "dumb" {
		return p
	}
	p.live = true
	p.every = liveEvery
	p.color = os.Getenv("NO_COLOR") == ""
	p.width = func() int {
		w, _, err := term.GetSize(int(f.Fd()))
		if err != nil || w <= 0 {
			return fallbackWidth
		}
		return w
	}
	return p
}

// Begin starts a step.
func (p *progress) Begin(name string, total uint64, unit client.Unit) {
	if p.quiet {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	now := p.now()
	if p.began.IsZero() {
		p.began = now
	}
	p.done.Store(0)
	p.step = &step{name: name, total: total, unit: unit, start: now, drawn: now, marks: []mark{{at: now}}}
	if p.live {
		p.draw(now)
	}
	if p.stop == nil {
		p.stop, p.stopped = make(chan struct{}), make(chan struct{})
		go p.run(p.stop, p.stopped)
	}
}

// Advance reports n more of the running step's unit done.
func (p *progress) Advance(n int64) { p.done.Add(n) }

// End finishes the running step and leaves its line: the time it took and
// what came of it.
func (p *progress) End(summary string) {
	if p.quiet {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.step
	if s == nil {
		return
	}
	p.step = nil
	took := p.now().Sub(s.start)
	if p.live {
		fmt.Fprintf(p.out, "\r\033[K%s\n", p.paint(doneLine("✓", s.name, took, summary), green))
		return
	}
	line := fmt.Sprintf("%s (%s)", s.name, human.Duration(took))
	if summary != "" {
		line = fmt.Sprintf("%s: %s (%s)", s.name, summary, human.Duration(took))
	}
	fmt.Fprintln(p.out, line)
}

// Close ends the show. err is how the command went: with an error the step
// that was running is marked as the one that failed, and without one the
// time of all steps together is added. Nothing is shown after Close.
func (p *progress) Close(err error) {
	if p.quiet {
		return
	}
	p.mu.Lock()
	stop, stopped := p.stop, p.stopped
	p.stop, p.closed = nil, true
	p.mu.Unlock()
	if stop != nil {
		close(stop)
		<-stopped
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if s := p.step; s != nil {
		p.step = nil
		took := now.Sub(s.start)
		how := "failed"
		if errors.Is(err, context.Canceled) {
			how = "interrupted"
		}
		switch {
		case err == nil && p.live:
			fmt.Fprint(p.out, "\r\033[K")
		case err == nil:
		case p.live:
			fmt.Fprintf(p.out, "\r\033[K%s\n", p.paint(doneLine("✗", s.name, took, how), red))
		default:
			fmt.Fprintf(p.out, "%s: %s after %s\n", s.name, how, human.Duration(took))
		}
	}
	if err == nil && !p.began.IsZero() {
		if total := now.Sub(p.began); p.live {
			fmt.Fprintln(p.out, doneLine(" ", "total", total, ""))
		} else {
			fmt.Fprintf(p.out, "done in %s\n", human.Duration(total))
		}
	}
	p.began = time.Time{}
}

// run looks at the running step now and then until stop is closed.
func (p *progress) run(stop <-chan struct{}, stopped chan<- struct{}) {
	defer close(stopped)
	t := time.NewTicker(p.every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			p.tick()
		}
	}
}

// tick shows where the running step is.
func (p *progress) tick() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.step != nil {
		p.draw(p.now())
	}
}

// draw writes the running step: over its line on a terminal, and elsewhere
// as a new line when the last one is plainEvery old. p.mu is held.
func (p *progress) draw(now time.Time) {
	s := p.step
	if !p.live && now.Sub(s.drawn) < plainEvery {
		return
	}
	v := s.view(now, p.done.Load())
	if p.live {
		p.frame++
		fmt.Fprintf(p.out, "\r\033[K%s", p.paint(liveLine(v, p.frame, p.width()), cyan))
		return
	}
	s.drawn = now
	fmt.Fprintln(p.out, plainLine(v))
}

const (
	green = "\033[32m"
	red   = "\033[31m"
	cyan  = "\033[36m"
)

// paint colours the mark a line begins with.
func (p *progress) paint(line, colour string) string {
	if !p.color {
		return line
	}
	_, n := utf8.DecodeRuneInString(line)
	return colour + line[:n] + "\033[0m" + line[n:]
}

// view is a step at one moment: everything a line says of it.
type view struct {
	name    string
	unit    client.Unit
	total   uint64
	done    uint64
	rate    float64 // of the unit, a second
	elapsed time.Duration
}

// view notes how far the step is now and returns what there is to show.
// The rate is that of the last rateWindow, not of the whole step: it is
// what the time left depends on.
func (s *step) view(now time.Time, done int64) view {
	done = max(done, 0)
	s.marks = append(s.marks, mark{at: now, done: done})
	for len(s.marks) > 2 && now.Sub(s.marks[1].at) >= rateWindow {
		s.marks = s.marks[1:]
	}
	v := view{name: s.name, unit: s.unit, total: s.total, done: uint64(done), elapsed: now.Sub(s.start)}
	if since := now.Sub(s.marks[0].at).Seconds(); since > 0 {
		v.rate = max(float64(done-s.marks[0].done)/since, 0)
	}
	return v
}

// known reports whether the step says how much there is to do.
func (v view) known() bool { return v.unit != client.NoUnit && v.total > 0 }

// fraction is the part of the step that is done, from 0 to 1.
func (v view) fraction() float64 {
	if v.done >= v.total {
		return 1
	}
	return float64(v.done) / float64(v.total)
}

// percent is the part of the step that is done, as a column of six.
func (v view) percent() string { return fmt.Sprintf("%5.1f%%", 100*v.fraction()) }

// amount is what is done, in the step's unit.
func (v view) amount() string {
	if v.unit == client.Bytes {
		return human.Bytes(v.done)
	}
	return human.Count(v.done) + " objects"
}

// amounts is what is done beside what there is to do. The part is written
// in the unit of the whole and set to the right in the room of the whole's
// figure, so the two take the same room from the first byte to the last.
func (v view) amounts() string {
	if v.unit == client.Bytes {
		return fmt.Sprintf("%*s / %s", len(human.BytesIn(v.total, v.total)), human.BytesIn(v.done, v.total), human.Bytes(v.total))
	}
	total := human.Count(v.total)
	return fmt.Sprintf("%*s / %s objects", len(total), human.Count(v.done), total)
}

// speed is the rate, in the step's unit.
func (v view) speed() string {
	if v.unit == client.Bytes {
		return human.Bytes(uint64(v.rate)) + "/s"
	}
	return human.Count(uint64(v.rate)) + " objects/s"
}

// eta is the time left at the present rate: "--" while nothing moves, for
// then there is no telling, and in a step's first second, when the rate is
// that of a start and says little of what follows.
func (v view) eta() string {
	switch {
	case v.done >= v.total:
		return "eta 0s"
	case v.rate <= 0 || v.elapsed < time.Second:
		return "eta --"
	}
	left := float64(v.total-v.done) / v.rate
	switch {
	case left >= 100*3600:
		// A rate next to nothing gives a time no clock holds.
		return "eta --"
	case left >= 3600:
		// Hours away, the seconds are noise.
		return fmt.Sprintf("eta %dh%02dm", int(left)/3600, int(left)/60%60)
	}
	return "eta " + human.Seconds(time.Duration(left*float64(time.Second)))
}

const (
	// nameWidth and timeWidth are the columns every line begins with: what
	// is being done, and for how long.
	nameWidth = 24
	timeWidth = 8
	// A bar is drawn from minBar columns up and grows to maxBar.
	minBar = 8
	maxBar = 30
	// etaWidth is the room of the time left: "eta 12m05s", "eta 99h59m".
	etaWidth = 10
)

// spinner is the mark of the running step, one frame after the other.
var spinner = []rune("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")

// lead is what every line of a step begins with: its mark, its name and
// its time, in columns.
func lead(mark, name, clock string) string {
	return fmt.Sprintf("%s %-*s %*s", mark, nameWidth, name, timeWidth, clock)
}

// doneLine is the line a step leaves behind on a terminal.
func doneLine(mark, name string, took time.Duration, summary string) string {
	line := lead(mark, name, human.Duration(took))
	if summary != "" {
		line += "  " + summary
	}
	return line
}

// liveLine is the line of the running step on a terminal width columns
// wide. A step that knows its total gets a bar, the percentage, the
// amounts, the rate and the time left; a narrow terminal loses the amounts
// first, then the rate, then the bar. Every figure has the room of its
// longest value and is set to the right in it, so that the line holds
// still while the figures change. The line never reaches the last column:
// a terminal that wraps it could not have it redrawn.
func liveLine(v view, frame, width int) string {
	line := lead(string(spinner[frame%len(spinner)]), v.name, human.Seconds(v.elapsed))
	switch {
	case v.known():
		// The rate of a transfer is all but always below "1000.00 MiB/s",
		// and that of objects below all of them a second.
		speed := fmt.Sprintf("%*s", len("999.99 MiB/s"), v.speed())
		if v.unit == client.Objects {
			speed = fmt.Sprintf("%*s", len(human.Count(v.total))+len(" objects/s"), v.speed())
		}
		eta := fmt.Sprintf("%-*s", etaWidth, v.eta())
		choices := [][]string{
			{v.percent(), v.amounts(), speed, eta},
			{v.percent(), speed, eta},
			{v.percent(), eta},
		}
		room := width - 1 - utf8.RuneCountInString(line) - 2
		stats, barWidth := choices[len(choices)-1], 0
		for _, c := range choices {
			if n := room - utf8.RuneCountInString(strings.Join(c, "  ")) - 2; n >= minBar {
				stats, barWidth = c, min(n, maxBar)
				break
			}
		}
		if barWidth > 0 {
			line += "  " + bar(v.fraction(), barWidth)
		}
		line += "  " + strings.Join(stats, "  ")
	case v.unit != client.NoUnit:
		line += "  " + v.amount() + "  " + v.speed()
	}
	return strings.TrimRight(clip(line, width-1), " ")
}

// plainLine is the line of the running step where there is no terminal.
func plainLine(v view) string {
	elapsed := "elapsed " + human.Seconds(v.elapsed)
	switch {
	case v.known():
		of := human.Bytes(v.done) + " of " + human.Bytes(v.total)
		if v.unit == client.Objects {
			of = human.Count(v.done) + " of " + human.Count(v.total) + " objects"
		}
		return fmt.Sprintf("%s: %s, %s, %s, %s, %s", v.name, strings.TrimSpace(v.percent()), of, v.speed(), elapsed, v.eta())
	case v.unit != client.NoUnit:
		return fmt.Sprintf("%s: %s, %s, %s", v.name, v.amount(), v.speed(), elapsed)
	}
	return fmt.Sprintf("%s: %s", v.name, elapsed)
}

// bar draws fraction of width columns as filled, to the eighth of a column.
func bar(fraction float64, width int) string {
	eighths := []string{"", "▏", "▎", "▍", "▌", "▋", "▊", "▉"}
	filled := int(fraction * float64(width*8))
	filled = max(0, min(filled, width*8))
	full, part := filled/8, eighths[filled%8]
	empty := width - full
	if part != "" {
		empty--
	}
	return strings.Repeat("█", full) + part + strings.Repeat("░", empty)
}

// clip cuts line to n columns.
func clip(line string, n int) string {
	if n < 0 || utf8.RuneCountInString(line) <= n {
		return line
	}
	return string([]rune(line)[:n])
}
