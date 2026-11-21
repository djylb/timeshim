package timeshim

import (
	"context"
	"errors"
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
				return int(expect[a].at - expect[b].at)
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
	errc := make(chan error)
	go func() { errc <- Sleep(t.Context(), c, time.Minute) }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := c.WaitPending(ctx, 1); err != nil {
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
	if !errors.Is(ctx2.Err(), context.Canceled) || c.Pending() != 0 {
		t.Fatalf("Err = %v, %d pending", ctx2.Err(), c.Pending())
	}
	past, cancel3 := WithDeadline(t.Context(), c, epoch)
	defer cancel3()
	if !errors.Is(past.Err(), context.DeadlineExceeded) {
		t.Fatal("past deadline")
	}
}

// TestRealInSynctest checks the Real paths, which a synctest bubble drives
// on its fake time.
func TestRealInSynctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := Real.Now()
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
