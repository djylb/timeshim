// Package timeshim provides injectable time for deterministic tests: code
// takes a Clock instead of calling time.Now and time.AfterFunc, production
// passes Real, and tests pass a Fake that moves only when told to. The
// package uses only the standard library.
package timeshim

import (
	"context"
	"slices"
	"sync"
	"time"
)

// Clock tells the time and runs functions after a delay.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// AfterFunc calls f in its own goroutine, or for Fake in Advance,
	// once d has passed, as time.AfterFunc does.
	AfterFunc(d time.Duration, f func()) Timer
}

// Timer is a pending call of AfterFunc. A *time.Timer is one.
type Timer interface {
	// Stop prevents the call and reports whether it was still pending.
	Stop() bool
	// Reset makes the call happen d from now and reports whether it was
	// still pending.
	Reset(d time.Duration) bool
}

// Real is the system clock.
var Real Clock = realClock{}

type realClock struct{}

func (realClock) Now() time.Time                            { return time.Now() }
func (realClock) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }

// Or returns c, or Real when c is nil.
func Or(c Clock) Clock {
	if c == nil {
		return Real
	}
	return c
}

// Fake is a clock that moves only by Advance. Its zero value is not
// ready; use NewFake. It is safe for concurrent use.
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
	armed  chan struct{} // closed when a timer is armed; nil if none waits
}

// NewFake returns a Fake clock at t.
func NewFake(t time.Time) *Fake { return &Fake{now: t} }

// Now returns the clock's time.
func (c *Fake) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// AfterFunc arranges for f to be called by the Advance that reaches d from
// now, in the goroutine of Advance. A d of zero or less is due at once: f
// is called by the next Advance, AdvanceTo or AdvanceToNext, Advance(0)
// included, rather than in a goroutine of its own as time.AfterFunc does,
// so that tests stay deterministic and f never runs under the caller's
// locks.
//
// Timer.Reset re-arms in the same way; a negative d counts as zero, so
// Deadlines never reports a time before Now.
func (c *Fake) AfterFunc(d time.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{c: c, f: f, at: c.now.Add(max(d, 0))}
	c.arm(t)
	return t
}

// Advance moves the clock d forward and calls, in order of their times and
// outside the clock's lock, the functions that are due; a function may set
// new timers, which are called too when due. Functions due at the same time
// are called in the order they were armed by AfterFunc or Reset. Time never
// goes back: a negative d panics, and of concurrent calls, each leaves the
// clock at least where it took it.
//
// The order holds within one call. Concurrent calls take due functions
// from the same list, so their functions may run at the same time in
// different goroutines; drive a Fake from one goroutine to keep a single
// global order.
func (c *Fake) Advance(d time.Duration) {
	if d < 0 {
		panic("timeshim: negative Advance")
	}
	c.mu.Lock()
	end := c.now.Add(d)
	c.mu.Unlock()
	c.run(end)
}

// AdvanceTo moves the clock forward to t as Advance does. A t that is not
// after the clock's time only calls the functions already due.
func (c *Fake) AdvanceTo(t time.Time) { c.run(t) }

// AdvanceToNext moves the clock to the earliest pending time, calls the
// functions due then as Advance does, and returns that time. It reports
// false, and leaves the clock alone, when no function is pending.
func (c *Fake) AdvanceToNext() (time.Time, bool) {
	c.mu.Lock()
	i := c.next(time.Time{}, false)
	if i < 0 {
		c.mu.Unlock()
		return time.Time{}, false
	}
	at := c.timers[i].at
	c.mu.Unlock()
	c.run(at)
	return at, true
}

// Deadlines returns the times of the pending functions, earliest first.
func (c *Fake) Deadlines() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ts := make([]time.Time, len(c.timers))
	for i, t := range c.timers {
		ts[i] = t.at
	}
	slices.SortStableFunc(ts, time.Time.Compare)
	return ts
}

// run calls the functions due by end and leaves the clock at least at end.
func (c *Fake) run(end time.Time) {
	for {
		c.mu.Lock()
		i := c.next(end, true)
		if i < 0 {
			if end.After(c.now) {
				c.now = end
			}
			c.mu.Unlock()
			return
		}
		t := c.timers[i]
		c.timers = slices.Delete(c.timers, i, i+1)
		if t.at.After(c.now) {
			c.now = t.at
		}
		c.mu.Unlock()
		t.f()
	}
}

// next returns the index of the earliest timer, the first armed of equal
// times, that is not after end when bounded, or -1; the lock is held.
func (c *Fake) next(end time.Time, bounded bool) int {
	i := -1
	for j, t := range c.timers {
		if (!bounded || !t.at.After(end)) && (i < 0 || t.at.Before(c.timers[i].at)) {
			i = j
		}
	}
	return i
}

// Pending returns the number of functions waiting for their time.
func (c *Fake) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

// WaitPending waits until at least n functions wait for their time, such as
// once code under test running in another goroutine has armed its timers,
// and returns nil, or ctx's error if ctx ends first. It does not move the
// clock.
func (c *Fake) WaitPending(ctx context.Context, n int) error {
	for {
		c.mu.Lock()
		if len(c.timers) >= n {
			c.mu.Unlock()
			return nil
		}
		if c.armed == nil {
			c.armed = make(chan struct{})
		}
		armed := c.armed
		c.mu.Unlock()
		select {
		case <-armed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// arm adds t to the clock's list and wakes WaitPending; the clock's lock is
// held.
func (c *Fake) arm(t *fakeTimer) {
	c.timers = append(c.timers, t)
	if c.armed != nil {
		close(c.armed)
		c.armed = nil
	}
}

type fakeTimer struct {
	c  *Fake
	f  func()
	at time.Time
}

func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	return t.remove()
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	pending := t.remove()
	t.at = t.c.now.Add(max(d, 0))
	t.c.arm(t)
	return pending
}

// remove takes t off the clock's list; the clock's lock is held.
func (t *fakeTimer) remove() bool {
	i := slices.Index(t.c.timers, t)
	if i < 0 {
		return false
	}
	t.c.timers = slices.Delete(t.c.timers, i, i+1)
	return true
}
