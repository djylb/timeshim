package timeshim_test

import (
	"context"
	"fmt"
	"time"

	"github.com/djylb/timeshim"
)

// Cache takes a Clock and treats nil as the system clock.
type Cache struct {
	Clock timeshim.Clock // nil means timeshim.Real
}

func (c *Cache) expireAfter(d time.Duration, f func()) timeshim.Timer {
	return timeshim.Or(c.Clock).AfterFunc(d, f)
}

// A fake clock moves only when told to, so a timer fires exactly when the
// test advances past its deadline.
func ExampleFake() {
	clk := timeshim.NewFake(time.Unix(0, 0))
	c := &Cache{Clock: clk}

	expired := false
	c.expireAfter(time.Minute, func() { expired = true })

	clk.Advance(59 * time.Second)
	fmt.Println(expired, clk.Pending())
	clk.Advance(time.Second)
	fmt.Println(expired, clk.Pending())
	// Output:
	// false 1
	// true 0
}

// The helpers take a clock and keep the semantics of the time package.
func ExampleWithTimeout() {
	clk := timeshim.NewFake(time.Unix(0, 0))
	ctx, cancel := timeshim.WithTimeout(context.Background(), clk, 5*time.Second)
	defer cancel()

	fmt.Println(ctx.Err())
	clk.Advance(5 * time.Second)
	fmt.Println(ctx.Err())
	// Output:
	// <nil>
	// context deadline exceeded
}

// A ticker on a fake clock delivers a tick for each period advanced.
func ExampleNewTicker() {
	clk := timeshim.NewFake(time.Unix(0, 0))
	t := timeshim.NewTicker(clk, time.Second)
	defer t.Stop()

	clk.Advance(time.Second)
	fmt.Println((<-t.C).Unix())
	clk.Advance(time.Second)
	fmt.Println((<-t.C).Unix())
	// Output:
	// 1
	// 2
}
