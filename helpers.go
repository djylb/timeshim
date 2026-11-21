package timeshim

import (
	"context"
	"sync"
	"time"
)

// Since returns the time elapsed on c since t, as time.Since.
func Since(c Clock, t time.Time) time.Duration {
	if _, ok := c.(realClock); ok {
		return time.Since(t)
	}
	return c.Now().Sub(t)
}

// Until returns the duration on c until t, as time.Until.
func Until(c Clock, t time.Time) time.Duration {
	if _, ok := c.(realClock); ok {
		return time.Until(t)
	}
	return t.Sub(c.Now())
}

// Sleep waits until d has passed on c and returns nil, or ctx's error if ctx
// ends first. A d of zero or less returns at once, with ctx's error if any.
func Sleep(ctx context.Context, c Clock, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := NewTimer(c, d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// After returns a channel that receives c's time once d has passed, as
// time.After.
func After(c Clock, d time.Duration) <-chan time.Time {
	return NewTimer(c, d).C
}

// ChanTimer is a timer that sends c's time on C once its time has passed,
// as a *time.Timer made by time.NewTimer. As with time.Timer since Go 1.23,
// C is unbuffered in effect: once Stop or Reset returns, no stale time is
// received from C.
type ChanTimer struct {
	C <-chan time.Time

	real *time.Timer
	fake *chanState
}

// chanState is the channel and AfterFunc timer behind a ChanTimer or Ticker
// of a clock other than Real. gen counts Stop and Reset calls, so that a
// send of a timer that fired before one of them is dropped.
type chanState struct {
	mu     sync.Mutex
	c      chan time.Time
	gen    uint64
	clock  Clock
	timer  Timer
	period time.Duration // for a Ticker; 0 for a ChanTimer
}

func newChanState(c Clock, d, period time.Duration) *chanState {
	s := &chanState{c: make(chan time.Time, 1), clock: c, period: period}
	s.mu.Lock()
	s.arm(d)
	s.mu.Unlock()
	return s
}

// arm makes the current generation fire after d; s.mu is held. A timer
// (not a Ticker) with d <= 0 has its value ready at once, as with
// time.NewTimer: there is no function to run, so nothing waits for an
// Advance.
func (s *chanState) arm(d time.Duration) {
	if d <= 0 && s.period == 0 {
		s.timer = nil
		select {
		case s.c <- s.clock.Now():
		default:
		}
		return
	}
	s.timer = s.clock.AfterFunc(d, s.fire(s.gen))
}

// fire returns the function of the timer of generation gen.
func (s *chanState) fire(gen uint64) func() {
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.gen != gen {
			return
		}
		select {
		case s.c <- s.clock.Now():
		default: // a Ticker drops the tick a slow receiver missed
		}
		if s.period > 0 {
			s.timer.Reset(s.period)
		}
	}
}

// rearm stops the timer, drains the channel and, when d > 0, arms it again.
func (s *chanState) rearm(d time.Duration, period time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gen++
	pending := s.timer != nil && s.timer.Stop()
	s.timer = nil
	select {
	case <-s.c: // fired but not received: pending, as for time.Timer
		pending = true
	default:
	}
	if d > 0 || period == 0 && d >= 0 {
		s.period = period
		s.arm(d)
	}
	return pending
}

// NewTimer returns a timer that sends on its channel once d has passed on
// c, as time.NewTimer. On Real it is a *time.Timer underneath. On other
// clocks a d of zero or less has the value ready on C at once, as does
// Reset with such a d.
func NewTimer(c Clock, d time.Duration) *ChanTimer {
	if _, ok := c.(realClock); ok {
		t := time.NewTimer(d)
		return &ChanTimer{C: t.C, real: t}
	}
	s := newChanState(c, d, 0)
	return &ChanTimer{C: s.c, fake: s}
}

// Stop prevents the timer from firing and reports whether it was pending,
// as (*time.Timer).Stop.
func (t *ChanTimer) Stop() bool {
	if t.real != nil {
		return t.real.Stop()
	}
	return t.fake.rearm(-1, 0)
}

// Reset makes the timer fire d from now and reports whether it was
// pending, as (*time.Timer).Reset.
func (t *ChanTimer) Reset(d time.Duration) bool {
	if t.real != nil {
		return t.real.Reset(d)
	}
	return t.fake.rearm(max(d, 0), 0)
}

// Ticker sends c's time on C every period, as a *time.Ticker: a tick that a
// slow receiver misses is dropped, and once Stop or Reset returns no stale
// tick is received.
type Ticker struct {
	C <-chan time.Time

	real *time.Ticker
	fake *chanState
}

// NewTicker returns a ticker of period d on c, as time.NewTicker; d must be
// positive.
func NewTicker(c Clock, d time.Duration) *Ticker {
	if d <= 0 {
		panic("timeshim: non-positive interval for NewTicker")
	}
	if _, ok := c.(realClock); ok {
		t := time.NewTicker(d)
		return &Ticker{C: t.C, real: t}
	}
	s := newChanState(c, d, d)
	return &Ticker{C: s.c, fake: s}
}

// Stop turns the ticker off, as (*time.Ticker).Stop.
func (t *Ticker) Stop() {
	if t.real != nil {
		t.real.Stop()
		return
	}
	t.fake.rearm(-1, 0)
}

// Reset stops the ticker and restarts it with period d, as
// (*time.Ticker).Reset; d must be positive.
func (t *Ticker) Reset(d time.Duration) {
	if d <= 0 {
		panic("timeshim: non-positive interval for Ticker.Reset")
	}
	if t.real != nil {
		t.real.Reset(d)
		return
	}
	t.fake.rearm(d, d)
}

// WithDeadline returns a copy of ctx that ends with
// context.DeadlineExceeded once c reaches deadline, as context.WithDeadline
// on the system clock. On a Fake the context ends within the Advance that
// reaches deadline. A ctx that has ended, or a deadline that has passed,
// gives a context that has ended already, with ctx's error first. It also ends when ctx does, or when the returned cancel
// is called; children made by the context package propagate without a
// goroutine of their own.
func WithDeadline(ctx context.Context, c Clock, deadline time.Time) (context.Context, context.CancelFunc) {
	if _, ok := c.(realClock); ok {
		return context.WithDeadline(ctx, deadline)
	}
	dc := &deadlineCtx{parent: ctx, deadline: deadline, done: make(chan struct{})}
	if err := ctx.Err(); err != nil {
		dc.end(err)
		return dc, func() {}
	}
	d := deadline.Sub(c.Now())
	if d <= 0 {
		dc.end(context.DeadlineExceeded)
		return dc, func() {}
	}
	dc.mu.Lock()
	dc.timer = c.AfterFunc(d, func() { dc.end(context.DeadlineExceeded) })
	dc.stopParent = context.AfterFunc(ctx, func() { dc.end(ctx.Err()) })
	dc.mu.Unlock()
	return dc, func() { dc.end(context.Canceled) }
}

// WithTimeout returns WithDeadline(ctx, c, c.Now().Add(d)).
func WithTimeout(ctx context.Context, c Clock, d time.Duration) (context.Context, context.CancelFunc) {
	return WithDeadline(ctx, c, c.Now().Add(d))
}

// deadlineCtx is a context that ends at a deadline of a clock other than
// Real.
type deadlineCtx struct {
	parent   context.Context //nolint:containedctx // a context whose deadline follows a clock; the parent is what it wraps
	deadline time.Time
	done     chan struct{}

	mu         sync.Mutex
	err        error
	timer      Timer
	stopParent func() bool
	funcs      map[*func()]struct{} // of AfterFunc, until the end
}

// end ends the context with err unless it has ended; it calls no user code
// under the lock.
func (c *deadlineCtx) end(err error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.err = err
	close(c.done)
	timer, stop, funcs := c.timer, c.stopParent, c.funcs
	c.timer, c.stopParent, c.funcs = nil, nil, nil
	c.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	if stop != nil {
		stop()
	}
	for f := range funcs {
		go (*f)()
	}
}

func (c *deadlineCtx) Deadline() (time.Time, bool) {
	if d, ok := c.parent.Deadline(); ok && d.Before(c.deadline) {
		return d, true
	}
	return c.deadline, true
}

func (c *deadlineCtx) Done() <-chan struct{} { return c.done }

func (c *deadlineCtx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *deadlineCtx) Value(key any) any { return c.parent.Value(key) }

// AfterFunc lets the context package propagate the end of c to its
// children without a goroutine each, as context.AfterFunc does.
func (c *deadlineCtx) AfterFunc(f func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		go f()
		return func() bool { return false }
	}
	if c.funcs == nil {
		c.funcs = make(map[*func()]struct{})
	}
	p := &f
	c.funcs[p] = struct{}{}
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		_, ok := c.funcs[p]
		delete(c.funcs, p)
		return ok
	}
}
