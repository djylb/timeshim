package wallclock_test

import (
	"encoding/json"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/timeshim"
	"github.com/djylb/timeshim/wallclock"
)

var referenceDate = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

func TestFloorCarriesForward(t *testing.T) {
	var f timeshim.Fake
	c := wallclock.New(&f, referenceDate.Unix())
	if got := c.Now(); !got.Equal(referenceDate) {
		t.Fatalf("now %v, want initial floor %v", got, referenceDate)
	}
	f.Advance(time.Hour)
	if got := c.Now(); !got.Equal(referenceDate.Add(time.Hour)) {
		t.Fatalf("now %v, want initial floor + 1h", got)
	}
	if c.Measured() {
		t.Fatal("a floor alone must leave the clock unmeasured")
	}
}

func TestMedianOffset(t *testing.T) {
	f := timeshim.NewFake(referenceDate)
	c := wallclock.New(f, 0)
	reading := referenceDate.Add(10 * time.Hour)
	c.Observe(reading.Unix())
	c.Observe(reading.Unix() + 1)
	c.Observe(reading.AddDate(0, 0, -30).Unix())
	if off, ok := c.Skew(); !ok || off != 10*time.Hour {
		t.Fatalf("skew (%v, %v), want (%v, true)", off, ok, 10*time.Hour)
	}
	f.Advance(time.Minute)
	if got := c.Now(); !got.Equal(reading.Add(time.Minute)) {
		t.Fatalf("now %v, want %v", got, reading.Add(time.Minute))
	}

	q := wallclock.New(timeshim.NewFake(referenceDate.Add(500*time.Millisecond)), 0)
	q.Observe(referenceDate.Unix())
	if off, ok := q.Skew(); !ok || off != 0 {
		t.Fatalf("sub-second skew (%v, %v), want (0, true)", off, ok)
	}
}

func TestRestore(t *testing.T) {
	f := timeshim.NewFake(referenceDate)
	c := wallclock.New(f, 0)
	c.Observe(referenceDate.Add(time.Hour).Unix())
	state := c.State()

	retained := wallclock.New(timeshim.NewFake(referenceDate.Add(time.Minute)), 0)
	retained.Restore(state)
	want := referenceDate.Add(time.Hour + time.Minute)
	if !retained.Measured() || !retained.Now().Equal(want) {
		t.Fatalf("restored now %v, measured %v, want %v and measured", retained.Now(), retained.Measured(), want)
	}

	resetBase := timeshim.NewFake(time.Unix(0, 0))
	reset := wallclock.New(resetBase, 0)
	reset.Restore(state)
	if reset.Measured() || reset.Now().Unix() != state.Time {
		t.Fatalf("restored at epoch: now %v, measured %v, want Unix %d and unmeasured", reset.Now(), reset.Measured(), state.Time)
	}
	resetBase.Advance(time.Minute)
	if got := reset.Now(); !got.Equal(want) {
		t.Fatalf("restored floor now %v, want %v", got, want)
	}

	live := wallclock.New(timeshim.NewFake(referenceDate), 0)
	live.Observe(referenceDate.Add(2 * time.Hour).Unix())
	live.Restore(wallclock.State{Offset: 3 * time.Hour, Measured: true, Time: state.Time})
	if got := live.State(); got.Offset != 2*time.Hour || !got.Measured {
		t.Fatalf("after restore got %+v, want the live two-hour offset", got)
	}
}

func TestObservationWindow(t *testing.T) {
	c := wallclock.New(timeshim.NewFake(referenceDate), 0)
	for _, hour := range []int{1, 1, 1, 2, 2} {
		c.Observe(referenceDate.Add(time.Duration(hour) * time.Hour).Unix())
	}
	if got, _ := c.Skew(); got != time.Hour {
		t.Fatalf("skew = %v, want 1h", got)
	}
	c.Observe(referenceDate.Add(2 * time.Hour).Unix())
	if got, _ := c.Skew(); got != 2*time.Hour {
		t.Fatalf("skew after eviction = %v, want 2h", got)
	}
}

func TestCorrections(t *testing.T) {
	f := timeshim.NewFake(referenceDate)
	c := wallclock.New(f, referenceDate.Add(time.Hour).Unix())
	first := c.Changes()
	c.Observe(referenceDate.Unix())
	checkChange(t, first, true)
	if got := c.Now(); !got.Equal(referenceDate) {
		t.Fatalf("now %v, want the measured time without the initial floor %v", got, referenceDate)
	}

	second := c.Changes()
	c.Observe(referenceDate.Unix())
	checkChange(t, second, false)
	c.Raise(referenceDate.Add(time.Minute).Unix())
	checkChange(t, second, true)

	third := c.Changes()
	f.Advance(time.Second)
	c.Raise(referenceDate.Add(time.Minute).Unix())
	c.Restore(wallclock.State{})
	checkChange(t, third, false)
	if got := c.Now(); !got.Equal(referenceDate.Add(time.Minute + time.Second)) {
		t.Fatalf("now %v, want advanced trusted floor", got)
	}

	fourth := referenceDate.Add(time.Hour)
	c.Restore(wallclock.State{Time: fourth.Unix()})
	checkChange(t, third, true)
	if got := c.Now(); !got.Equal(fourth) {
		t.Fatalf("now %v, want restored trusted floor %v", got, fourth)
	}
	idempotent := c.Changes()
	c.Restore(wallclock.State{Time: fourth.Unix()})
	checkChange(t, idempotent, false)
}

func checkChange(t *testing.T, changed <-chan struct{}, want bool) {
	t.Helper()
	select {
	case <-changed:
		if !want {
			t.Fatal("received an unexpected correction")
		}
	default:
		if want {
			t.Fatal("missed a correction")
		}
	}
}

func TestOnSample(t *testing.T) {
	c := wallclock.New(timeshim.NewFake(referenceDate), 0)
	calls := 0
	c.OnSample(func() {
		calls++
		if state := c.State(); !state.Measured {
			t.Fatalf("sample callback sees unmeasured state: %+v", state)
		}
		c.OnSample(nil)
	})
	c.Observe(0)
	c.Observe(math.MaxInt64)
	if calls != 0 || c.Measured() {
		t.Fatal("invalid observations changed the clock")
	}
	c.Observe(referenceDate.Unix())
	c.Observe(referenceDate.Unix())
	if calls != 1 {
		t.Fatalf("callback count %d, want 1", calls)
	}
}

func TestConcurrentObservations(t *testing.T) {
	c := wallclock.New(timeshim.NewFake(referenceDate), 0)
	var calls atomic.Int64
	f := func() { calls.Add(1) }
	var wg sync.WaitGroup
	c.OnSample(f)
	const observers = 4
	for range observers {
		wg.Go(func() {
			c.OnSample(f)
			c.Observe(referenceDate.Unix())
		})
	}
	wg.Wait()
	if got := calls.Load(); got != observers {
		t.Fatalf("callback count %d, want %d", got, observers)
	}
}

func TestExtremeSkew(t *testing.T) {
	var f timeshim.Fake
	c := wallclock.New(&f, 0)
	c.Observe(referenceDate.Unix())
	if got := c.Now(); !got.Equal(referenceDate) {
		t.Fatalf("now %v, want %v", got, referenceDate)
	}
	if off, ok := c.Skew(); !ok || off != math.MaxInt64 {
		t.Fatalf("skew (%v, %v), want saturated positive offset", off, ok)
	}
	changed := c.Changes()
	c.Observe(referenceDate.Add(time.Hour).Unix())
	checkChange(t, changed, true)
	if got := c.Now(); !got.Equal(referenceDate.Add(time.Hour)) {
		t.Fatalf("now %v, want upper median", got)
	}
	restored := wallclock.New(timeshim.NewFake(referenceDate), 0)
	restored.Restore(c.State())
	if got := restored.Now(); restored.Measured() || !got.Equal(referenceDate.Add(time.Hour)) {
		t.Fatalf("restored now %v, measured %v, want the restored floor and no offset", got, restored.Measured())
	}
}

func TestZeroClock(t *testing.T) {
	var c wallclock.Clock
	// A zero snapshot restores neither a floor nor a measurement.
	c.Restore(wallclock.State{})
	if c.Measured() || c.Local().IsZero() || c.Now().IsZero() {
		t.Fatalf("unexpected zero-clock state: %+v", c.State())
	}
}

func TestBaseTimerAndWallTime(t *testing.T) {
	// Start with a base reading that can include a monotonic component.
	baseTime := timeshim.Real.Now()
	f := timeshim.NewFake(baseTime)
	c := wallclock.New(f, 0)
	if got := c.Local(); got != baseTime {
		t.Fatalf("local %v, want unmodified base time %v", got, baseTime)
	}
	fired := false
	timer := c.AfterFunc(time.Minute, func() { fired = true })
	defer timer.Stop()
	reading := baseTime.Add(time.Hour).Truncate(time.Second)
	c.Observe(reading.Unix())
	if got := c.Now(); !got.Equal(reading) || got != got.Round(0) || got.Location() != baseTime.Location() {
		t.Fatalf("now %v, want wall-only %v in the base location", got, reading)
	}
	if fired || f.Pending() != 1 {
		t.Fatal("a wall correction affected the base timer")
	}
	f.Advance(time.Minute)
	if !fired || f.Pending() != 0 {
		t.Fatal("the original base deadline did not fire")
	}
}

type countedClock struct {
	*timeshim.Fake
	reads int
}

func (c *countedClock) Now() time.Time {
	c.reads++
	return c.Fake.Now()
}

func TestStateSnapshot(t *testing.T) {
	f := &countedClock{Fake: timeshim.NewFake(referenceDate)}
	c := wallclock.New(f, 0)
	c.Restore(wallclock.State{Offset: time.Hour, Measured: true, Time: referenceDate.Unix()})
	if f.reads != 1 {
		t.Fatalf("restore took %d base readings, want 1", f.reads)
	}
	state := c.State()
	if f.reads != 2 {
		t.Fatalf("state snapshot took %d base readings, want 1", f.reads-1)
	}
	if want := (wallclock.State{Offset: time.Hour, Measured: true, Time: referenceDate.Add(time.Hour).Unix()}); state != want {
		t.Fatalf("state = %+v, want %+v", state, want)
	}
}

func TestStateJSON(t *testing.T) {
	const data = `{"offset_ns":1000000000,"measured":true,"time":10000}`
	var state wallclock.State
	if err := json.Unmarshal([]byte(data), &state); err != nil {
		t.Fatal(err)
	}
	if want := (wallclock.State{Offset: time.Second, Measured: true, Time: 10_000}); state != want {
		t.Fatalf("state = %+v, want %+v", state, want)
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != data {
		t.Fatalf("encoded state = %s, want %s", encoded, data)
	}
}
