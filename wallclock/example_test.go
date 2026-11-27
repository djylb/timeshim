package wallclock_test

import (
	"fmt"
	"time"

	"github.com/djylb/timeshim"
	"github.com/djylb/timeshim/wallclock"
)

// A clock whose base wall time is wrong adopts a trusted observation. The
// fake base stands in for a system clock that was never set.
func ExampleClock_Observe() {
	base := timeshim.NewFake(time.Unix(1000, 0))
	clk := wallclock.New(base, 0)

	clk.Observe(1_700_000_000)
	fmt.Println(clk.Now().Unix())

	// The correction keeps following the base clock.
	base.Advance(30 * time.Second)
	fmt.Println(clk.Now().Unix())
	// Output:
	// 1700000000
	// 1700000030
}

// State is saved by the caller and restored at the next start, so wall time
// never falls below what was already known.
func ExampleClock_Restore() {
	base := timeshim.NewFake(time.Unix(1000, 0))
	first := wallclock.New(base, 0)
	first.Observe(1_700_000_000)
	saved := first.State()

	// A new process starts with the same unset system clock.
	restarted := wallclock.New(timeshim.NewFake(time.Unix(1000, 0)), 0)
	restarted.Restore(saved)
	fmt.Println(restarted.Now().Unix() >= saved.Time)
	// Output:
	// true
}
