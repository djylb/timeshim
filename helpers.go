package timeshim

import (
	"context"
	"math"
	"math/big"
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
	if d <= 0 || ctx.Err() != nil {
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

// chanState owns a non-Real timer's channel and registration. gen changes
// when a callback is replaced or stopped, invalidating its pending sends.
type chanState struct {
	mu        sync.Mutex
	c         chan time.Time
	gen       uint64
	last      time.Time
	at        time.Time // current target on a wrapped clock
	delivered bool
	pending   bool // armed or awaiting channel delivery
	clock     Clock
	timer     Timer
	period    time.Duration // for a Ticker; 0 for a ChanTimer
}

func newChanState(c Clock, d, period time.Duration) *chanState {
	s := &chanState{c: make(chan time.Time, 1), clock: c}
	s.rearm(max(d, 0), period)
	return s
}

// nextTick keeps the original phase even when the elapsed duration saturates.
func nextTick(at, now time.Time, period time.Duration) (time.Time, bool) {
	elapsed := now.Sub(at)
	rem := elapsed % period
	if elapsed == math.MaxInt64 {
		// Unix seconds can wrap once across the full time.Time range.
		delta := new(big.Int).Sub(big.NewInt(now.Unix()), big.NewInt(at.Unix()))
		if delta.Sign() < 0 {
			const wordBits = 64
			delta.Add(delta, new(big.Int).Lsh(big.NewInt(1), wordBits))
		}
		delta.Mul(delta, big.NewInt(int64(time.Second)))
		delta.Add(delta, big.NewInt(int64(now.Nanosecond()-at.Nanosecond())))
		rem = time.Duration(delta.Mod(delta, big.NewInt(int64(period))).Int64())
	}
	return addTime(now, period-rem)
}

// A wrapped clock uses one underlying timer across positive resets. A callback
// already in flight at Reset may run alongside the next one. Only the first
// callback at or after the current target may deliver that target.
func (s *chanState) fire(gen uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gen != gen || !s.pending {
		return
	}
	now := s.clock.Now()
	if now.Before(s.at) {
		return
	}
	s.sendLocked(s.at)
	if s.period > 0 {
		if next, ok := nextTick(s.at, now, s.period); ok {
			s.timer.Reset(next.Sub(now))
			s.at, s.pending = next, true
			return
		}
	}
	// A late callback can have delivered a target whose callback is still armed.
	s.timer.Stop()
}

// send makes the send of generation gen that a Fake deferred.
func (s *chanState) send(gen uint64, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gen != gen {
		return
	}

	if s.delivered && !now.After(s.last) {
		return
	}
	s.sendLocked(now)
	s.delivered, s.last = true, now
}

// sendLocked sends now unless a value waits; s.mu is held.
func (s *chanState) sendLocked(now time.Time) {
	s.pending = false
	select {
	case s.c <- now:
	default: // a Ticker drops the tick a slow receiver missed
	}
}

// rearm replaces or stops the registration and drains any unread value.
// A zero delay delivers immediately, and a negative delay stops the timer.
func (s *chanState) rearm(d time.Duration, period time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := s.pending
	var now time.Time
	switch {
	case d > 0:
		s.arm(d, period)
	case d == 0:
		now = s.clock.Now()
		fallthrough
	default:
		if s.timer != nil {
			s.timer.Stop()
		}
		s.gen++
		s.timer = nil
	}
	s.pending, s.period, s.delivered = d > 0, period, false
	select {
	case <-s.c: // fired but not received: pending, as for time.Timer
		pending = true
	default:
	}
	if d == 0 {
		s.sendLocked(now)
	}
	return pending
}

// arm replaces a registration while s.mu is held. A direct Fake replaces
// the callback as well, so deliveries owned by earlier advances stay stale.
func (s *chanState) arm(d, period time.Duration) {
	if c, ok := s.clock.(*Fake); ok {
		gen := s.gen + 1
		t := &fakeTimer{c: c, period: period, deferred: true, fire: func(at time.Time) { s.send(gen, at) }}
		old, _ := s.timer.(*fakeTimer)
		t.schedule(d, old)
		s.timer, s.gen = t, gen
		return
	}
	at := later(s.clock.Now(), d)
	if s.timer == nil {
		gen := s.gen
		s.timer = s.clock.AfterFunc(d, func() { s.fire(gen) })
	} else {
		s.timer.Reset(d)
	}
	s.at = at
}

// NewTimer returns a timer that sends on its channel once d has passed on
// c, as time.NewTimer. On Real it is a *time.Timer underneath. On other
// clocks a d of zero or less has the value ready on C at once, as does
// Reset with such a d. A positive d with an unrepresentable target panics.
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
// pending, as (*time.Timer).Reset. On a Fake an unrepresentable target
// panics and preserves the previous registration and any unread value.
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
// positive. On clocks other than Real an unrepresentable first tick panics.
// A Fake ticker retires after its last representable tick.
// Stop tickers on clocks other than Real to release the clock registration.
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
	if t.fake != nil {
		t.fake.rearm(-1, 0)
	}
}

// Reset stops the ticker and restarts it with period d, as
// (*time.Ticker).Reset; d must be positive. On a Fake an unrepresentable
// first tick panics and leaves the ticker unchanged.
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
// gives a context that has ended already, with ctx's error first. The supplied
// clock drives the requested deadline even when ctx uses another clock.
// The context also ends when ctx does, or when the returned cancel
// is called. A parent with a private Done channel may require a waiting
// goroutine unless it implements AfterFunc.
func WithDeadline(ctx context.Context, c Clock, deadline time.Time) (context.Context, context.CancelFunc) {
	if _, ok := c.(realClock); ok {
		return context.WithDeadline(ctx, deadline)
	}
	return WithDeadlineCause(ctx, c, deadline, nil)
}

// WithDeadlineCause is like context.WithDeadlineCause on c: when the deadline
// passes, Err reports context.DeadlineExceeded and context.Cause reports cause.
func WithDeadlineCause(ctx context.Context, c Clock, deadline time.Time, cause error) (context.Context, context.CancelFunc) {
	if _, ok := c.(realClock); ok {
		return context.WithDeadlineCause(ctx, deadline, cause)
	}
	inner, cancelCause := context.WithCancelCause(context.WithoutCancel(ctx))
	dc := &deadlineCtx{clockContext: clockContext{inner}, deadline: deadline, done: make(chan struct{}), cancelCause: cancelCause}
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		dc.deadline = d
	}
	// Publish through a standard cancelable context. Its descendants, including
	// those behind WithValue and WithCancel, then see the same Err and Cause
	// synchronously when the clock fires.
	child, release := context.WithCancel(dc)
	end := func(err, cause error) {
		if parentErr := ctx.Err(); parentErr != nil {
			err, cause = parentErr, context.Cause(ctx)
		}
		dc.end(err, cause)
		release()
	}
	cancel := func() { end(context.Canceled, nil) }
	expire := func() { end(context.DeadlineExceeded, cause) }

	dc.arm(ctx, c, deadline, expire, cancel)
	return clockContext{child}, cancel
}

// clockContext preserves the AfterFunc hook and the standard child's Done
// channel, so value wrappers also participate in standard cancellation.
type clockContext struct {
	context.Context //nolint:containedctx // Embedding preserves the wrapped context's methods while adding the AfterFunc hook.
}

func (c clockContext) AfterFunc(f func()) func() bool { return context.AfterFunc(c.Context, f) }

// WithTimeoutCause is like WithDeadlineCause with a deadline d from now.
// On clocks other than Real it panics if that time cannot be represented.
func WithTimeoutCause(ctx context.Context, c Clock, d time.Duration, cause error) (context.Context, context.CancelFunc) {
	if _, ok := c.(realClock); ok {
		return context.WithTimeoutCause(ctx, d, cause)
	}
	return WithDeadlineCause(ctx, c, later(c.Now(), d), cause)
}

// WithTimeout is like WithDeadline with a deadline d from now.
// On clocks other than Real it panics if that time cannot be represented.
func WithTimeout(ctx context.Context, c Clock, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := c.(realClock); ok {
		return context.WithTimeout(ctx, d)
	}
	return WithDeadline(ctx, c, later(c.Now(), d))
}

// deadlineCtx is a context that ends at a deadline of a clock other than
// Real.
type deadlineCtx struct {
	clockContext // carries the parent values and the published cancellation cause
	deadline     time.Time
	done         chan struct{}

	mu          sync.Mutex
	cancelCause context.CancelCauseFunc
	err         error
	parentWatch deadlineRegistration
	alarm       *deadlineRegistration
	propagate   func()
	children    map[*parentWatch]struct{}
}

type deadlineKey struct{}

type parentWatch struct {
	done   <-chan struct{}
	notify func() // protected by the enclosing deadline's mutex
}

// Each segment has its own slot: its callback can run, renew, or cancel
// before the clock returns the Timer from AfterFunc.
type deadlineRegistration struct{ stop func() }

// install either retains a new registration or releases it if cancellation
// or another segment overtook it. External clock methods run without dc.mu.
func (dc *deadlineCtx) install(slot *deadlineRegistration, stop func()) {
	dc.mu.Lock()
	if dc.err == nil && (slot == &dc.parentWatch || slot == dc.alarm) {
		// arm passes only the address of parentWatch or a newly allocated slot, both non-nil.
		//goland:noinspection GoMaybeNil
		slot.stop = stop
		stop = nil
	}
	dc.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// arm uses the same failure cleanup on construction and on every renewal.
// Relative clocks need another segment when the remaining duration saturates.
func (dc *deadlineCtx) arm(ctx context.Context, c Clock, at time.Time, expire, cancel func()) {
	if ctx.Err() != nil {
		cancel()
		return
	}
	dc.mu.Lock()
	if dc.err != nil {
		dc.mu.Unlock()
		return
	}
	first := dc.alarm == nil
	slot := new(deadlineRegistration)
	dc.alarm = slot
	dc.mu.Unlock()

	complete := false
	defer func() {
		if !complete {
			cancel()
		}
	}()
	if first {
		dc.install(&dc.parentWatch, watchParent(ctx, cancel))
	}
	var timer Timer
	if fake, ok := c.(*Fake); ok {
		timer = fake.deadlineTimer(at, expire)
	} else if left := at.Sub(c.Now()); left > 0 {
		timer = c.AfterFunc(left, func() { dc.arm(ctx, c, at, expire, cancel) })
	}
	if timer != nil {
		dc.install(slot, func() { timer.Stop() })
	}
	if timer == nil || ctx.Err() != nil {
		expire()
	}
	complete = true
}

// watchParent follows independent cancellation of the immediate parent. When
// an enclosing clock deadline ends, it also checks the immediate parent
// synchronously, after standard contexts have propagated through any wrappers.
func watchParent(parent context.Context, cancel func()) func() {
	done := parent.Done()
	if done == nil {
		return nil
	}
	ancestor, _ := parent.Value(deadlineKey{}).(*deadlineCtx)
	if ancestor == nil {
		stop := context.AfterFunc(parent, cancel)
		return func() { stop() }
	}
	w := &parentWatch{done: done, notify: cancel}
	stop := context.AfterFunc(parent, func() {
		ancestor.mu.Lock()
		notify := w.notify
		ancestor.mu.Unlock()
		if notify != nil {
			notify()
		}
	})
	ancestor.mu.Lock()
	if ancestor.err == nil {
		if ancestor.children == nil {
			ancestor.children = make(map[*parentWatch]struct{})
		}
		ancestor.children[w] = struct{}{}
	}
	ancestor.mu.Unlock()
	return func() {
		stop()
		ancestor.mu.Lock()
		delete(ancestor.children, w)
		ancestor.mu.Unlock()
	}
}

// takeChildren claims notifications for parents closed by standard
// propagation. A detached or independently cancelable parent that is still
// live keeps its ordinary AfterFunc subscription. dc.mu is held.
func (dc *deadlineCtx) takeChildren() []func() {
	var notify []func()
	for w := range dc.children {
		select {
		case <-w.done:
			notify = append(notify, w.notify)
			w.notify = nil
		default:
		}
	}
	dc.children = nil
	return notify
}

// end publishes the first result before calling external cleanup functions.
func (dc *deadlineCtx) end(err, cause error) {
	dc.mu.Lock()
	if dc.err != nil {
		dc.mu.Unlock()
		return
	}
	if cause == nil {
		cause = err
	}
	dc.err = err
	dc.cancelCause(cause)
	close(dc.done)
	dc.propagate()
	timer, stop, children := dc.alarm, dc.parentWatch.stop, dc.takeChildren()
	dc.alarm, dc.parentWatch.stop, dc.propagate = nil, nil, nil
	dc.mu.Unlock()
	for _, check := range children {
		check()
	}
	if timer != nil && timer.stop != nil {
		timer.stop()
	}
	if stop != nil {
		stop()
	}
}

func (dc *deadlineCtx) Deadline() (time.Time, bool) { return dc.deadline, true }
func (dc *deadlineCtx) Done() <-chan struct{}       { return dc.done }

func (dc *deadlineCtx) Err() error {
	select {
	case <-dc.done:
		return dc.err
	default:
		return nil
	}
}

func (dc *deadlineCtx) Value(key any) any {
	if key == (deadlineKey{}) {
		return dc
	}
	return dc.clockContext.Value(key)
}

// AfterFunc is the propagation hook for the single standard child created
// during construction. Only that child is exposed to callers.
func (dc *deadlineCtx) AfterFunc(f func()) func() bool {
	dc.propagate = f
	return func() bool { return false }
}
