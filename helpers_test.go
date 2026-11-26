package timeshim

import (
	"cmp"
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

var epoch = time.Unix(1000, 0)

// TestFakeOrderProperty arms random timers, stops and resets some, and checks
// that Advance calls the rest by time and, for equal times, in arming order.
func TestFakeOrderProperty(t *testing.T) {
	for seed := range uint64(200) {
		r := rand.New(rand.NewPCG(seed, 1))
		c := NewFake(epoch)
		type want struct {
			at  time.Duration
			seq int
		}
		expect := make([]want, 0, 30)
		var got []int
		live := map[int]Timer{}
		seq := 0
		for i := range 30 {
			d := time.Duration(r.IntN(5)) * time.Second
			id := i
			live[id] = c.AfterFunc(d, func() { got = append(got, id) })
			expect = append(expect, want{d, seq})
			seq++
		}
		for id := range 30 {
			switch r.IntN(4) {
			case 0:
				live[id].Stop()
				expect[id].at = -1
			case 1:
				d := time.Duration(r.IntN(5)) * time.Second
				live[id].Reset(d)
				expect[id] = want{d, seq}
				seq++
			}
		}
		var ids []int
		for id, w := range expect {
			if w.at >= 0 {
				ids = append(ids, id)
			}
		}
		slices.SortFunc(ids, func(a, b int) int {
			if expect[a].at != expect[b].at {
				return cmp.Compare(expect[a].at, expect[b].at)
			}
			return expect[a].seq - expect[b].seq
		})
		if dl := c.Deadlines(); len(dl) != len(ids) || !slices.IsSortedFunc(dl, time.Time.Compare) {
			t.Fatalf("seed %d: Deadlines() = %v", seed, dl)
		}
		c.Advance(10 * time.Second)
		if !slices.Equal(got, ids) {
			t.Fatalf("seed %d: fired %v, want %v", seed, got, ids)
		}
	}
}

func TestFakeAdvanceToNext(t *testing.T) {
	c := NewFake(epoch)
	if _, ok := c.AdvanceToNext(); ok {
		t.Fatal("AdvanceToNext with nothing pending")
	}
	n := 0
	c.AfterFunc(3*time.Second, func() { n++ })
	c.AfterFunc(3*time.Second, func() { n++ })
	c.AfterFunc(5*time.Second, func() { n += 10 })
	if at, ok := c.AdvanceToNext(); !ok || !at.Equal(epoch.Add(3*time.Second)) || n != 2 || !c.Now().Equal(at) {
		t.Fatalf("AdvanceToNext() = %v, %v; n = %d", at, ok, n)
	}
	c.AdvanceTo(epoch) // earlier: no move
	if !c.Now().Equal(epoch.Add(3 * time.Second)) {
		t.Fatal("AdvanceTo moved back")
	}
	c.AdvanceTo(epoch.Add(5 * time.Second))
	if n != 12 || c.Pending() != 0 {
		t.Fatalf("n = %d, %d pending", n, c.Pending())
	}
}

func TestChanTimer(t *testing.T) {
	c := NewFake(epoch)
	tm := NewTimer(c, time.Second)
	c.Advance(time.Second)
	// Fired but not received: Reset drops the stale value (Go 1.23 semantics).
	if !tm.Reset(time.Second) {
		t.Error("Reset of a fired, unreceived timer reported not pending")
	}
	select {
	case <-tm.C:
		t.Fatal("stale value after Reset")
	default:
	}
	c.Advance(time.Second)
	if v := <-tm.C; !v.Equal(epoch.Add(2 * time.Second)) {
		t.Fatalf("got %v", v)
	}
	if tm.Reset(time.Second) || !tm.Stop() || tm.Stop() {
		t.Error("Reset/Stop pending reports")
	}
	c.Advance(time.Hour)
	select {
	case <-tm.C:
		t.Fatal("stopped timer fired")
	default:
	}
	if c.Pending() != 0 {
		t.Fatalf("%d pending", c.Pending())
	}
	if Until(c, epoch) != -(time.Hour+2*time.Second) || Since(c, epoch) != time.Hour+2*time.Second {
		t.Error("Since")
	}
}

func TestTicker(t *testing.T) {
	c := NewFake(epoch)
	tk := NewTicker(c, time.Second)
	c.Advance(3500 * time.Millisecond) // three ticks, two dropped
	if v := <-tk.C; !v.Equal(epoch.Add(time.Second)) {
		t.Fatalf("first tick %v", v)
	}
	select {
	case v := <-tk.C:
		t.Fatalf("tick %v not dropped", v)
	default:
	}
	c.Advance(500 * time.Millisecond) // the phase is kept
	if v := <-tk.C; !v.Equal(epoch.Add(4 * time.Second)) {
		t.Fatalf("tick %v", v)
	}
	tk.Reset(2 * time.Second)
	c.Advance(time.Second)
	select {
	case <-tk.C:
		t.Fatal("tick before the new period")
	default:
	}
	c.Advance(time.Second)
	<-tk.C
	tk.Stop()
	c.Advance(time.Hour)
	if c.Pending() != 0 || len(tk.C) != 0 {
		t.Fatal("stopped ticker ticked")
	}
}

func TestSleepAndAfter(t *testing.T) {
	c := NewFake(epoch)
	errc := make(chan error, 1)
	var wg sync.WaitGroup
	t.Cleanup(wg.Wait)
	wg.Go(func() { errc <- Sleep(t.Context(), c, time.Minute) })
	if err := c.WaitPending(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	c.Advance(time.Minute)
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	cctx, ccancel := context.WithCancel(t.Context())
	ccancel()
	if err := Sleep(cctx, c, time.Minute); !errors.Is(err, context.Canceled) || c.Pending() != 0 {
		t.Fatalf("Sleep = %v, %d pending", err, c.Pending())
	}
	ch := After(c, time.Second)
	c.Advance(time.Second)
	<-ch
}

func TestWithDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewFake(epoch)
		ctx, cancel := WithTimeout(t.Context(), c, time.Minute)
		defer cancel()
		child, cancelChild := context.WithCancel(ctx)
		defer cancelChild()
		if d, ok := ctx.Deadline(); !ok || !d.Equal(epoch.Add(time.Minute)) {
			t.Fatal("Deadline")
		}
		c.Advance(59 * time.Second)
		if ctx.Err() != nil {
			t.Fatal("ended early")
		}
		c.Advance(time.Second)
		select {
		case <-ctx.Done():
		default:
			t.Fatal("not ended by Advance")
		}
		<-child.Done()
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) || !errors.Is(child.Err(), context.DeadlineExceeded) || !errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			t.Fatalf("Err = %v, child %v", ctx.Err(), child.Err())
		}
		if c.Pending() != 0 {
			t.Fatal("timer left")
		}

		parent, cancelParent := context.WithCancel(t.Context())
		ctx2, cancel2 := WithDeadline(parent, c, c.Now().Add(time.Hour))
		defer cancel2()
		cancelParent()
		<-ctx2.Done()
		synctest.Wait()
		if !errors.Is(ctx2.Err(), context.Canceled) || c.Pending() != 0 {
			t.Fatalf("Err = %v, %d pending", ctx2.Err(), c.Pending())
		}
		past, cancel3 := WithDeadline(t.Context(), c, epoch)
		defer cancel3()
		if !errors.Is(past.Err(), context.DeadlineExceeded) {
			t.Fatal("past deadline")
		}
	})
}

// TestRealInSynctest checks the Real paths, which a synctest bubble drives
// on its fake time.
func TestRealInSynctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := Real.Now()
		if !start.Equal(time.Now()) {
			t.Fatal("Real.Now differs from the system clock")
		}
		if err := Sleep(t.Context(), Real, time.Hour); err != nil {
			t.Fatal(err)
		}
		if Since(Real, start) != time.Hour || Until(Real, start) != -time.Hour {
			t.Fatal("Since/Until")
		}
		tk := NewTicker(Real, time.Minute)
		<-tk.C
		tk.Stop()
		tm := NewTimer(Real, time.Second)
		if !tm.Stop() {
			t.Fatal("Stop")
		}
		ctx, cancel := WithTimeout(t.Context(), Real, time.Second)
		defer cancel()
		<-ctx.Done()
		<-After(Real, time.Second)
	})
}

// TestFakeInSynctest checks that a Fake works inside a bubble: a goroutine
// blocked on it is durably blocked, so synctest.Wait returns.
func TestFakeInSynctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewFake(epoch)
		var mu sync.Mutex
		done := false
		go func() {
			_ = Sleep(t.Context(), c, time.Minute)
			mu.Lock()
			done = true
			mu.Unlock()
		}()
		synctest.Wait()
		if c.Pending() != 1 {
			t.Fatalf("%d pending", c.Pending())
		}
		c.Advance(time.Minute)
		synctest.Wait()
		mu.Lock()
		defer mu.Unlock()
		if !done {
			t.Fatal("not woken")
		}
	})
}

func TestRealAllocs(t *testing.T) {
	if n := testing.AllocsPerRun(100, func() { _ = Since(Real, epoch); _ = Until(Real, epoch); _ = Or(nil).Now() }); n != 0 {
		t.Errorf("%v allocations", n)
	}
}

// TestStopAfterFireMatchesTime checks that Stop and Reset of a timer that
// fired but whose value was not received report it pending, as time does
// since Go 1.23, on Real and on a Fake.
func TestStopAfterFireMatchesTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, c := range []Clock{Real, NewFake(epoch)} {
			advance := func() { time.Sleep(2 * time.Second) }
			if f, ok := c.(*Fake); ok {
				advance = func() { f.Advance(2 * time.Second) }
			}
			tm := NewTimer(c, time.Second)
			advance()
			if !tm.Stop() {
				t.Errorf("%T: Stop of an unreceived fired timer = false", c)
			}
			tm.Reset(time.Second)
			advance()
			if !tm.Reset(time.Second) {
				t.Errorf("%T: Reset of an unreceived fired timer = false", c)
			}
			select {
			case <-tm.C:
				t.Errorf("%T: stale value after Reset", c)
			default:
			}
			tm.Stop()
		}
	})
}

func TestWithDeadlineEndedParent(t *testing.T) {
	c := NewFake(epoch)
	parent, cancel := context.WithCancel(t.Context())
	cancel()
	for _, d := range []time.Duration{time.Hour, -time.Hour} {
		ctx, stop := WithTimeout(parent, c, d)
		if !errors.Is(ctx.Err(), context.Canceled) || c.Pending() != 0 {
			t.Fatalf("Err = %v, %d pending", ctx.Err(), c.Pending())
		}
		stop()
	}
}

func TestFakeNonPositiveDelay(t *testing.T) {
	c := NewFake(epoch)
	c.Advance(time.Second)
	n := 0
	tm := c.AfterFunc(time.Hour, func() { n++ })
	tm.Reset(-time.Hour)
	c.AfterFunc(0, func() { n++ })
	if dl := c.Deadlines(); len(dl) != 2 || dl[0].Before(c.Now()) {
		t.Fatalf("Deadlines() = %v at %v", dl, c.Now())
	}
	if n != 0 {
		t.Fatal("called before Advance")
	}
	c.Advance(0)
	if n != 2 {
		t.Fatalf("n = %d", n)
	}
}

func TestChanTimerNonPositiveDelay(t *testing.T) {
	c := NewFake(epoch)
	tm := NewTimer(c, 0)
	select {
	case v := <-tm.C:
		if !v.Equal(epoch) {
			t.Fatalf("got %v", v)
		}
	default:
		t.Fatal("NewTimer(0) not ready")
	}
	if tm.Stop() || c.Pending() != 0 {
		t.Fatal("received timer still pending")
	}
	tm.Reset(-time.Second)
	if !tm.Stop() {
		t.Fatal("unreceived value after Reset(<0) not pending")
	}
	select {
	case <-tm.C:
		t.Fatal("stale value after Stop")
	default:
	}
	<-After(c, 0)
}

// A ChanTimer that fires in an Advance delivers when the Advance ends;
// Stop in between reports it pending and drops the value; a timer that a
// function of the Advance sets still fires in it.
func TestChanTimerDeliversAtEnd(t *testing.T) {
	c := NewFake(epoch)
	tm := NewTimer(c, time.Second)
	var stop *ChanTimer
	c.AfterFunc(2*time.Second, func() {
		select {
		case <-tm.C:
			t.Error("value before the Advance ended")
		default:
		}
		stop = NewTimer(c, time.Hour)
		if !stop.Stop() {
			t.Error("Stop of a pending timer")
		}
		stop = NewTimer(c, time.Second)
	})
	late := NewTimer(c, 3*time.Second)
	c.AfterFunc(3*time.Second, func() {
		if !late.Stop() {
			t.Error("Stop of a fired, undelivered timer reported not pending")
		}
	})
	c.Advance(10 * time.Second)
	if v := <-tm.C; !v.Equal(epoch.Add(time.Second)) {
		t.Fatalf("got %v", v)
	}
	select {
	case <-late.C:
		t.Fatal("value of a stopped timer")
	default:
	}
	select {
	case <-stop.C:
	default:
		t.Fatal("timer armed by a function did not fire in the Advance")
	}
}

var (
	errQueueTimeout = errors.New("queue timeout")
	errParentGone   = errors.New("parent")
)

func TestWithDeadlineCause(t *testing.T) {
	cause := errQueueTimeout
	for _, clk := range []Clock{Real, NewFake(time.Unix(0, 0))} {
		ctx, cancel := WithTimeoutCause(context.Background(), clk, time.Millisecond, cause)
		if f, ok := clk.(*Fake); ok {
			f.Advance(time.Second)
		} else {
			<-ctx.Done()
		}
		<-ctx.Done()
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) || !errors.Is(context.Cause(ctx), cause) {
			t.Fatalf("%T: err %v cause %v", clk, ctx.Err(), context.Cause(ctx))
		}
		cancel()
	}
	fake := NewFake(time.Unix(0, 0))
	ctx, cancel := WithTimeoutCause(context.Background(), fake, time.Hour, cause)
	cancel()
	if !errors.Is(ctx.Err(), context.Canceled) || errors.Is(context.Cause(ctx), cause) {
		t.Fatalf("cancel: err %v cause %v", ctx.Err(), context.Cause(ctx))
	}
	parent, stop := context.WithCancelCause(context.Background())
	parentCause := errParentGone
	ctx, cancel = WithTimeoutCause(parent, fake, time.Hour, cause)
	defer cancel()
	stop(parentCause)
	<-ctx.Done()
	if !errors.Is(context.Cause(ctx), parentCause) {
		t.Fatalf("parent cause %v", context.Cause(ctx))
	}
}

type forwardedClock struct{ Clock }

func TestAdvanceDeliveryOwnership(t *testing.T) {
	for _, concurrent := range []bool{false, true} {
		c := NewFake(epoch)
		tm := NewTimer(c, 1)
		immediate := NewTimer(forwardedClock{c}, 1)
		c.AfterFunc(2, func() {
			if concurrent {
				var wg sync.WaitGroup
				wg.Go(func() { c.Advance(0) })
				wg.Wait()
			} else {
				c.Advance(0)
			}
			if len(tm.C) != 0 || len(immediate.C) != 1 {
				t.Error("incorrect delivery during callback")
			}
			panic("callback")
		})
		func() {
			defer func() {
				if recover() != "callback" {
					t.Error("missing callback panic")
				}
			}()
			c.Advance(10)
		}()
		if len(tm.C) != 1 || c.Now() != epoch.Add(2) {
			t.Fatal("incorrect panic cleanup")
		}
	}
}

func TestDeadlineOrdering(t *testing.T) {
	c := NewFake(time.Time{})
	far := c.Now().AddDate(500, 0, 0)
	for _, parentFirst := range []bool{true, false} {
		parent, stop := context.WithCancelCause(t.Context())
		child, cancel := WithDeadline(parent, c, far)
		if at, ok := c.Next(); !ok || !at.Equal(far) {
			t.Fatal("deadline registration", at)
		}
		if parentFirst {
			stop(errParentGone)
		}
		c.AdvanceTo(far)
		stop(errParentGone)
		want := context.DeadlineExceeded
		if parentFirst {
			want = errParentGone
		}
		if !errors.Is(context.Cause(child), want) {
			t.Errorf("cause %v", context.Cause(child))
		}
		cancel()
		far = far.AddDate(500, 0, 0)
	}
}

func TestLargeTickerStep(t *testing.T) {
	c := NewFake(epoch)
	tk := NewTicker(c, 3)
	defer tk.Stop()
	c.Advance(time.Hour)
	if <-tk.C != epoch.Add(3) {
		t.Fatal("first tick")
	}
	at, _ := c.Next()
	if at != c.Now().Add(3) {
		t.Fatal("phase", at)
	}
}

func TestPastAdvanceAndZeroTicker(t *testing.T) {
	var c Fake
	called := false
	c.AfterFunc(0, func() { called = true })
	c.AdvanceTo(c.Now().Add(-1))
	if !called {
		t.Error("due callback skipped")
	}
	var tk Ticker
	tk.Stop()
	defer func() {
		if recover() == nil {
			t.Error("zero ticker Reset succeeded")
		}
	}()
	tk.Reset(1)
}

// A callback that has entered the wrapper has already left the clock queue.
type pausedClock struct {
	Clock
	entered, resume chan struct{}
}

func (c pausedClock) AfterFunc(d time.Duration, f func()) Timer {
	return c.Clock.AfterFunc(d, func() { close(c.entered); <-c.resume; f() })
}

func TestDequeuedTimerPending(t *testing.T) {
	for _, reset := range []bool{false, true} {
		f := NewFake(epoch)
		c := pausedClock{f, make(chan struct{}), make(chan struct{})}
		tm := NewTimer(c, 1)
		var wg sync.WaitGroup
		wg.Go(func() { f.Advance(1) })
		<-c.entered
		if reset {
			if !tm.Reset(2) {
				t.Error("Reset lost pending value")
			}
		} else if !tm.Stop() {
			t.Error("Stop lost pending value")
		}
		close(c.resume)
		wg.Wait()
		tm.Stop()
		if len(tm.C) != 0 || f.Pending() != 0 {
			t.Fatal("stale generation")
		}
	}
}

func TestEarlierParentDeadline(t *testing.T) {
	var f Fake
	parent, stop := WithTimeout(t.Context(), &f, time.Second)
	defer stop()
	child, cancel := WithTimeout(parent, &f, time.Hour)
	defer cancel()
	f.Advance(time.Second)
	select {
	case <-child.Done():
	default:
		t.Fatal("child still active")
	}
	if !errors.Is(child.Err(), context.DeadlineExceeded) {
		t.Error(child.Err())
	}
}

func TestUnrepresentableDelay(t *testing.T) {
	f := NewFake(time.Unix(math.MaxInt64-62135596800, 950000000))
	defer func() {
		if recover() == nil || f.Pending() != 0 {
			t.Error("invalid timer accepted")
		}
	}()
	_ = NewTicker(f, 100*time.Millisecond)
}

type privateParent struct {
	done chan struct{}
}

func (privateParent) Deadline() (time.Time, bool) { return time.Time{}, false }
func (privateParent) Value(any) any               { return nil }

func (p privateParent) Done() <-chan struct{} { return p.done }
func (p privateParent) Err() error {
	select {
	case <-p.done:
		return context.Canceled
	default:
		return nil
	}
}

func TestPrivateParent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, endParent := range []bool{true, false} {
			p := privateParent{make(chan struct{})}
			var f Fake
			child, stop := WithTimeout(p, &f, time.Hour)
			if endParent {
				close(p.done)
			} else {
				stop()
			}
			<-child.Done()
			synctest.Wait()
			if !errors.Is(child.Err(), context.Canceled) || f.Pending() != 0 {
				t.Fatal("incomplete cancellation")
			}
			stop()
		}
	})
}

func TestPrecancelledSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := Sleep(ctx, nil, time.Hour); !errors.Is(err, context.Canceled) {
		t.Error(err)
	}
}

// Delay propagation until the test releases the registered callback.
type delayedParent struct {
	privateParent
	notify func()
}

func (p *delayedParent) AfterFunc(f func()) func() bool {
	p.notify = f
	return func() bool { p.notify = nil; return true }
}

func TestCanceledParentBeforeTimeout(t *testing.T) {
	p := &delayedParent{privateParent: privateParent{make(chan struct{})}}
	var c Fake
	child, cancel := WithTimeout(p, &c, time.Second)
	defer cancel()
	if p.notify == nil {
		t.Fatal("parent callback missing")
	}
	close(p.done)
	c.Advance(time.Second)
	if !errors.Is(child.Err(), context.Canceled) || !errors.Is(context.Cause(child), context.Canceled) {
		t.Fatal(child.Err(), context.Cause(child))
	}
}

func TestLateCallbackKeepsScheduledTime(t *testing.T) {
	f := NewFake(epoch)
	c := pausedClock{f, make(chan struct{}), make(chan struct{})}
	tk := NewTicker(c, 3)
	defer tk.Stop()
	var wg sync.WaitGroup
	wg.Go(func() { f.Advance(3) })
	<-c.entered
	f.Advance(7)
	close(c.resume)
	wg.Wait()
	at, ok := f.Next()
	if !ok || at != epoch.Add(12) || <-tk.C != epoch.Add(3) {
		t.Fatal("ticker phase", at)
	}
}

type observingClock struct {
	*Fake
	calls  int
	onStop func()
}

func (c *observingClock) Now() time.Time { return c.Fake.Now().Add(time.Hour) }
func (c *observingClock) AfterFunc(d time.Duration, f func()) Timer {
	c.calls++
	if c.onStop == nil {
		panic("registration")
	}
	return observingTimer{c.Fake.AfterFunc(d, f), c.onStop}
}

type observingTimer struct {
	Timer
	observe func()
}

func (t observingTimer) Stop() bool {
	active := t.Timer.Stop()
	t.observe()
	return active
}

func TestClockOverrides(t *testing.T) {
	var child context.Context
	c := &observingClock{Fake: new(Fake), onStop: func() {
		if child != nil {
			select {
			case <-child.Done():
			default:
				t.Error("Done still open during cleanup")
			}
			if child.Err() == nil || context.Cause(child) == nil {
				t.Error("missing cancellation")
			}
		}
	}}
	tm := NewTimer(c, 1)
	c.Advance(1)
	if at := <-tm.C; !at.Equal(c.Now()) {
		t.Error("timer ignored Now", at)
	}
	tm.Stop()
	child, cancel := WithTimeout(t.Context(), c, 1)
	if at, ok := c.Next(); !ok || !at.Equal(c.Fake.Now().Add(1)) {
		t.Error("deadline ignored Now", at)
	}
	if c.calls != 2 {
		t.Error("registration count", c.calls)
	}
	cancel()
}

func TestFailedConstruction(t *testing.T) {
	p := &delayedParent{privateParent: privateParent{make(chan struct{})}}
	c := &observingClock{Fake: new(Fake)}
	defer func() {
		if recover() != "registration" || p.notify != nil {
			t.Error("failed registration cleanup")
		}
	}()
	_, cancel := WithTimeout(p, c, 1)
	cancel()
}

func TestIndependentDeadline(t *testing.T) {
	var p, c Fake
	parent, stop := WithTimeout(t.Context(), &p, 1)
	defer stop()
	child, cancel := WithTimeout(parent, &c, 2)
	defer cancel()
	c.Advance(1)
	if child.Err() != nil {
		t.Fatal("child expired early")
	}
	c.Advance(1)
	if !errors.Is(child.Err(), context.DeadlineExceeded) {
		t.Fatal(child.Err())
	}
}

func TestOvertakenTick(t *testing.T) {
	for _, concurrent := range []bool{false, true} {
		c := NewFake(epoch)
		tk := NewTicker(c, 1)
		c.AfterFunc(2, func() {
			if concurrent {
				var wg sync.WaitGroup
				wg.Go(func() { c.Advance(1) })
				wg.Wait()
			} else {
				c.Advance(1)
			}
			if at := <-tk.C; at != epoch.Add(3) {
				t.Error(at)
			}
		})
		c.Advance(2)
		if len(tk.C) != 0 {
			t.Error("older tick delivered")
		}
		tk.Stop()
	}
}

func TestResetOverflow(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		for _, ticker := range []bool{false, true} {
			for _, unread := range []bool{false, true} {
				testResetOverflow(t, wrapped, ticker, unread)
			}
		}
	}
}

func testResetOverflow(t *testing.T, wrapped, ticker, unread bool) {
	t.Helper()
	f := NewFake(time.Unix(math.MaxInt64-62135596800, 950000000))
	var c Clock = f
	if wrapped {
		c = forwardedClock{f}
	}
	var ch <-chan time.Time
	var reset func()
	if ticker {
		tk := NewTicker(c, 1)
		defer tk.Stop()
		ch = tk.C
		reset = func() { tk.Reset(time.Second) }
	} else {
		tm := NewTimer(c, 1)
		defer tm.Stop()
		ch = tm.C
		reset = func() { tm.Reset(time.Second) }
	}
	if unread {
		f.Advance(1)
	}
	before := f.Deadlines()
	func() {
		defer func() {
			if recover() == nil {
				t.Error("missing panic")
			}
		}()
		reset()
	}()
	if !slices.Equal(before, f.Deadlines()) || unread && len(ch) != 1 {
		t.Error("reset lost state")
	}
}

func TestForwardedFarDeadline(t *testing.T) {
	var f Fake
	far := f.Now().AddDate(500, 0, 0)
	ctx, cancel := WithDeadline(t.Context(), forwardedClock{&f}, far)
	defer cancel()
	first, ok := f.AdvanceToNext()
	if !ok || !first.Before(far) || ctx.Err() != nil {
		t.Fatal("first deadline segment", first)
	}
	at, ok := f.AdvanceToNext()
	if !ok || !at.Equal(far) || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatal("final deadline segment", at)
	}
}

func TestFarDeadlineRenewalPanic(t *testing.T) {
	parent := &delayedParent{privateParent: privateParent{make(chan struct{})}}
	c := &observingClock{Fake: new(Fake), onStop: func() {}}
	child, cancel := WithDeadline(parent, c, c.Now().AddDate(500, 0, 0))
	defer cancel()
	c.onStop = nil // reject the second registration
	func() {
		defer func() {
			if recover() != "registration" {
				t.Error("missing renewal panic")
			}
		}()
		c.AdvanceToNext()
	}()
	if !errors.Is(child.Err(), context.Canceled) || parent.notify != nil || c.Pending() != 0 {
		t.Fatal("failed renewal retained a registration", child.Err())
	}
}

type probeParent struct {
	privateParent
	probe func()
}

func (p *probeParent) Err() error {
	if p.probe != nil {
		p.probe()
	}
	return p.privateParent.Err()
}

func TestParentDeadlineInFlight(t *testing.T) {
	var f Fake
	p := new(probeParent)
	parent, stop := WithTimeoutCause(p, &f, 1, errParentGone)
	defer stop()
	child, cancel := WithTimeoutCause(parent, &f, 100, errQueueTimeout)
	defer cancel()
	p.probe = func() {
		p.probe = nil
		f.Advance(0) // parent callback has left the queue but has not published
		if child.Err() != nil {
			t.Error("child expired on the parent's clock event")
		}
	}
	f.Advance(1)
	if !errors.Is(context.Cause(child), errParentGone) || f.Pending() != 0 {
		t.Fatal("child lost parent cause or retained its timer", context.Cause(child))
	}
}

func TestDeadlineThroughStandardContexts(t *testing.T) {
	type key struct{}
	for _, step := range []time.Duration{1, 100} {
		var f Fake
		root, stop := WithTimeoutCause(t.Context(), &f, 1, errParentGone)
		defer stop()
		value := context.WithValue(root, key{}, "value")
		middle, stopMiddle := context.WithCancel(value)
		defer stopMiddle()
		viaValue, cancelValue := WithTimeoutCause(value, &f, 100, errQueueTimeout)
		defer cancelValue()
		viaCancel, cancelMiddle := WithTimeoutCause(middle, &f, 100, errQueueTimeout)
		defer cancelMiddle()
		f.Advance(step)
		for _, child := range []context.Context{viaValue, viaCancel} {
			if !errors.Is(child.Err(), context.DeadlineExceeded) || !errors.Is(context.Cause(child), errParentGone) || child.Value(key{}) != "value" {
				t.Fatalf("after step %v: err %v, cause %v", step, child.Err(), context.Cause(child))
			}
		}
		if f.Pending() != 0 {
			t.Fatal("child registrations survived parent deadline")
		}
	}
}

func TestDetachedDeadline(t *testing.T) {
	var f Fake
	root, stop := WithTimeout(t.Context(), &f, 1)
	defer stop()
	parent, stopParent := context.WithCancelCause(context.WithoutCancel(root))
	defer stopParent(nil)
	child, cancel := WithTimeoutCause(parent, &f, 2, errQueueTimeout)
	defer cancel()
	f.Advance(1)
	if child.Err() != nil || f.Pending() != 1 {
		t.Fatal("cancellation crossed WithoutCancel")
	}
	stopParent(errParentGone)
	f.Advance(1)
	if !errors.Is(context.Cause(child), errParentGone) || f.Pending() != 0 {
		t.Fatal("detached parent cause", context.Cause(child))
	}
}

type countedClock struct {
	Clock
	observe func()
}

func (c countedClock) AfterFunc(d time.Duration, f func()) Timer {
	t := c.Clock.AfterFunc(d, f)
	c.observe()
	return countedTimer{t, c.observe}
}

type countedTimer struct {
	Timer
	observe func()
}

func (t countedTimer) Reset(d time.Duration) bool {
	pending := t.Timer.Reset(d)
	t.observe()
	return pending
}

func TestDeadlineDuringRegistration(t *testing.T) {
	for _, advanceFirst := range []bool{false, true} {
		var f Fake
		var cancel context.CancelFunc
		first := true
		c := countedClock{Clock: &f, observe: func() {
			if first {
				first = false
				if advanceFirst {
					f.AdvanceToNext() // renew before the first AfterFunc returns
				}
			} else if cancel != nil {
				cancel() // cancel before the renewal returns its timer
			}
		}}
		child, stop := WithDeadline(t.Context(), c, f.Now().AddDate(500, 0, 0))
		cancel = stop
		if child.Err() != nil || f.Pending() != 1 {
			t.Error("missing active deadline after registration")
		}
		if !advanceFirst {
			f.AdvanceToNext()
			if !errors.Is(child.Err(), context.Canceled) {
				t.Error("renewal ignored cancellation")
			}
		}
		stop()
		if f.Pending() != 0 {
			t.Fatal("registration outlived cancellation")
		}
	}
}

func TestResetPendingCount(t *testing.T) {
	var f Fake
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	c := countedClock{Clock: &f, observe: func() {
		if f.Pending() != 1 || !errors.Is(f.WaitPending(ctx, 2), context.Canceled) {
			t.Error("one timer counted as two")
		}
	}}
	tm := NewTimer(c, 1)
	defer tm.Stop()
	if !tm.Reset(2) {
		t.Fatal("active timer not pending")
	}
	f.Advance(2)
	if at := <-tm.C; !at.Equal(f.Now()) {
		t.Error("wrong reset time", at)
	}
}

func TestTickerTimeLimits(t *testing.T) {
	var f Fake
	tk := NewTicker(&f, 10)
	defer tk.Stop()
	far := f.Now().Add(math.MaxInt64).Add(101)
	f.AdvanceTo(far)
	if at, ok := f.Next(); !ok || at != far.Add(2) {
		t.Fatal("saturated phase", at)
	}
	end := NewFake(time.Unix(math.MaxInt64-62135596800, 999999998))
	last := NewTicker(end, 1)
	defer last.Stop()
	end.Advance(1)
	if len(last.C) != 1 || end.Pending() != 0 {
		t.Fatal("final tick")
	}
}
