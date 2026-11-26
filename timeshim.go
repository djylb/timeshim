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
const Real realClock = 0

type realClock int

func (realClock) Now() time.Time                            { return time.Now() }
func (realClock) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }

// Or returns c, or Real when c is nil.
func Or(c Clock) Clock {
	if c == nil {
		return Real
	}
	return c
}

// Fake is a clock that moves only by Advance. Its zero value starts at the zero time.
// It is safe for concurrent use and must not be copied after first use.
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
// so that tests stay deterministic and f never runs under the Fake's lock.
//
// Timer.Reset re-arms in the same way. A negative d counts as zero, so
// Deadlines never reports a time before Now. An unrepresentable target
// panics before the registration changes.
func (c *Fake) AfterFunc(d time.Duration, f func()) Timer {
	t := &fakeTimer{c: c, fire: func(time.Time) { f() }}
	t.schedule(d, nil)
	return t
}

func later(now time.Time, d time.Duration) time.Time {
	at, ok := addTime(now, d)
	if !ok {
		panic("timeshim: time out of range")
	}
	return at
}

func addTime(now time.Time, d time.Duration) (time.Time, bool) {
	at := now.Add(d)
	return at, at.Sub(now) == d
}

func (c *Fake) deadlineTimer(at time.Time, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !at.After(c.now) {
		return nil
	}
	t := &fakeTimer{c: c, at: at, fire: func(time.Time) { f() }}
	c.arm(t)
	return t
}

// Advance moves the clock d forward and calls, in order of their times and
// outside the clock's lock, the functions that are due; a function may set
// new timers, which are called too when due. The channels of a ChanTimer
// or Ticker created directly on c receive the scheduled time when Advance
// ends, so a goroutine woken by one finds the clock at its new time,
// and no timer it sets fires in the same Advance. Functions due at the same time
// are called in the order they were armed by AfterFunc or Reset. Time never
// goes back. A negative d or an unrepresentable target panics. Of concurrent
// calls, each leaves the clock at least where it took it.
//
// The order holds within one call. Concurrent calls take due functions
// from the same list, so their functions may run at the same time in
// different goroutines; drive a Fake from one goroutine to keep a single
// global order.
func (c *Fake) Advance(d time.Duration) {
	if d < 0 {
		panic("timeshim: negative Advance")
	}
	end := later(c.Now(), d)
	c.run(end)
}

// AdvanceTo moves the clock forward to t as Advance does. A t that is not
// after the clock's time only calls the functions already due.
func (c *Fake) AdvanceTo(t time.Time) { c.run(t) }

// AdvanceToNext moves the clock to the earliest pending time, calls the
// functions due then as Advance does, and returns that time. It reports
// false, and leaves the clock alone, when no function is pending.
func (c *Fake) AdvanceToNext() (time.Time, bool) {
	at, ok := c.Next()
	if !ok {
		return at, false
	}
	c.run(at)
	return at, true
}

// Next returns the earliest pending time, and false when no function is
// pending.
func (c *Fake) Next() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := c.next(nil)
	if i < 0 {
		return time.Time{}, false
	}
	return c.timers[i].at, true
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
	var sends []func()
	defer func() {
		for _, send := range sends {
			send()
		}
	}()
	// A repeating timer contributes at most one delivery to this advance.
	seen := make(map[*fakeTimer]bool)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.now.After(end) {
		end = c.now
	}
	for {
		t, at := c.step(end, seen)
		if t == nil {
			return
		}
		if t.deferred {
			sends = append(sends, func() { t.fire(at) })
			continue
		}
		func() {
			c.mu.Unlock()
			defer c.mu.Lock()
			t.fire(at)
		}()
	}
}

// step moves to the next observable event while holding the clock lock.
func (c *Fake) step(end time.Time, seen map[*fakeTimer]bool) (*fakeTimer, time.Time) {
	var due *fakeTimer
	target := end
	if i := c.next(seen); i >= 0 && !c.timers[i].at.After(end) {
		due = c.timers[i]
		target = due.at
		c.timers = slices.Delete(c.timers, i, i+1)
	}
	if target.After(c.now) {
		c.now = target
	}
	if len(seen) > 0 {
		c.timers = slices.DeleteFunc(c.timers, func(t *fakeTimer) bool {
			if !seen[t] || t.at.After(c.now) {
				return false
			}
			c.notify()
			return !t.repeat(c.now)
		})
	}
	if due != nil && due.period > 0 && due.repeat(c.now) {
		c.arm(due)
		seen[due] = true
	}
	return due, target
}

func (c *Fake) next(skip map[*fakeTimer]bool) int {
	i := -1
	for j, t := range c.timers {
		if !skip[t] && (i < 0 || t.at.Before(c.timers[i].at)) {
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
	return c.wait(ctx, func() bool { return len(c.timers) >= n })
}

// WaitDeadline waits until a function is pending at exactly t, such as
// once code under test has armed the one timer a test is about to pass,
// and returns nil, or ctx's error if ctx ends first. Unlike WaitPending it
// does not depend on how many other timers are pending. It does not move
// the clock.
func (c *Fake) WaitDeadline(ctx context.Context, t time.Time) error {
	return c.wait(ctx, func() bool { return slices.ContainsFunc(c.timers, func(x *fakeTimer) bool { return x.at.Equal(t) }) })
}

func (c *Fake) wait(ctx context.Context, ready func() bool) error {
	for {
		c.mu.Lock()
		if ready() {
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
	c.notify()
}

func (c *Fake) notify() {
	if c.armed != nil {
		close(c.armed)
		c.armed = nil
	}
}

type fakeTimer struct {
	c        *Fake
	fire     func(time.Time)
	deferred bool
	period   time.Duration
	at       time.Time
}

// repeat retires the ticker when its next tick cannot be represented.
func (t *fakeTimer) repeat(now time.Time) bool {
	at, ok := nextTick(t.at, now, t.period)
	if ok {
		t.at = at
	}
	return ok
}

func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	return t.remove()
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	return t.schedule(d, t)
}

// schedule validates the target and replaces old in one queue operation.
func (t *fakeTimer) schedule(d time.Duration, old *fakeTimer) bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	at := later(t.c.now, max(d, 0))
	pending := old != nil && old.remove()
	t.at = at
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
