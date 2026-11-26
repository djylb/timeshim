package timeshim

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestFake(t *testing.T) {
	c := NewFake(time.Unix(1000, 0))
	var got []int
	c.AfterFunc(2*time.Second, func() { got = append(got, 2) })
	t1 := c.AfterFunc(time.Second, func() {
		got = append(got, 1)
		c.AfterFunc(500*time.Millisecond, func() { got = append(got, 15) })
	})
	stopped := c.AfterFunc(time.Second, func() { got = append(got, -1) })
	if !stopped.Stop() || stopped.Stop() {
		t.Fatal("Stop")
	}
	c.Advance(1500 * time.Millisecond)
	if len(got) != 2 || got[0] != 1 || got[1] != 15 || !c.Now().Equal(time.Unix(1001, 5e8)) {
		t.Fatalf("%v at %v", got, c.Now())
	}
	if t1.Reset(time.Second) {
		t.Error("Reset of a timer that fired reported it pending")
	}
	c.Advance(time.Second)
	if len(got) != 4 || got[2] != 2 || got[3] != 1 || c.Pending() != 1 {
		t.Fatalf("%v, %d pending", got, c.Pending())
	}
	c.Advance(time.Second)
	if c.Pending() != 0 {
		t.Fatal("timer left")
	}
	if Or(nil) != Real || Or(c) != c {
		t.Error("Or")
	}
}

func TestFakeWaitPending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewFake(time.Unix(0, 0))
		go c.AfterFunc(time.Second, func() {})
		if err := c.WaitPending(t.Context(), 1); err != nil || c.Pending() != 1 {
			t.Fatalf("WaitPending() = %v with %d pending", err, c.Pending())
		}
		short, cancelShort := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancelShort()
		if err := c.WaitPending(short, 2); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("WaitPending() = %v, want the context's error", err)
		}
		c.Advance(time.Second)
		if c.Pending() != 0 {
			t.Fatal("timer left")
		}
	})
}

func TestFakeAdvanceNeverGoesBack(t *testing.T) {
	c := NewFake(time.Time{})
	// A call inside the first moves the clock past where the first ends.
	c.AfterFunc(5*time.Second, func() { c.Advance(20 * time.Second) })
	c.Advance(10 * time.Second)
	if want := (time.Time{}).Add(25 * time.Second); !c.Now().Equal(want) {
		t.Fatalf("Now() = %v, want %v", c.Now(), want)
	}
	defer func() {
		if recover() == nil {
			t.Error("Advance(-1) did not panic")
		}
	}()
	c.Advance(-1)
}

func TestFakeNextAndWaitDeadline(t *testing.T) {
	c := NewFake(time.Unix(0, 0))
	if _, ok := c.Next(); ok {
		t.Fatal("Next on an empty clock")
	}
	at := c.Now().Add(2 * time.Second)
	done := make(chan error, 1)
	go func() { done <- c.WaitDeadline(t.Context(), at) }()
	tm1 := c.AfterFunc(time.Second, func() {})
	tm2 := c.AfterFunc(2*time.Second, func() {})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n, ok := c.Next(); !ok || !n.Equal(c.Now().Add(time.Second)) {
		t.Fatalf("Next() = %v, %v", n, ok)
	}
	tm1.Stop()
	tm2.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.WaitDeadline(ctx, at); err == nil {
		t.Fatal("WaitDeadline after cancel")
	}
}
