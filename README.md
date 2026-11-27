# timeshim

Injectable clocks for timers, deadlines, and wall time. Production can use
the system clock, and tests can advance a fake clock explicitly. Requires
Go 1.26 and uses only the standard library.

| Package | Contents |
| --- | --- |
| `github.com/djylb/timeshim` | Clock interface, real and fake clocks, timers, tickers, and context deadlines |
| `github.com/djylb/timeshim/wallclock` | Wall time estimated from trusted observations, with lower bounds and restorable state |

## Install

```sh
go get github.com/djylb/timeshim
```

## Production code

Accept a `Clock` and default to `Real`:

```go
type Cache struct {
	Clock timeshim.Clock // nil means timeshim.Real
}

func (c *Cache) expireAfter(d time.Duration, f func()) timeshim.Timer {
	return timeshim.Or(c.Clock).AfterFunc(d, f)
}
```

A `*time.Timer` is a `timeshim.Timer`, so `Real` adds no wrapper. To switch clock implementations, declare
`var clk timeshim.Clock = timeshim.Real`.

## API

The `Clock` interface is two methods, `Now` and `AfterFunc`, so any clock
is easy to supply. The rest of the `time` package is built on it as
functions that take a clock, with the names and semantics of `time`:

| Function                        | Like                                   |
|---------------------------------|----------------------------------------|
| `Since(c, t)`, `Until(c, t)`    | `time.Since`, `time.Until`             |
| `Sleep(ctx, c, d)`              | `time.Sleep`, ending early with `ctx`  |
| `After(c, d)`                   | `time.After`                           |
| `NewTimer(c, d)`                | `time.NewTimer`                        |
| `NewTicker(c, d)`               | `time.NewTicker`                       |
| `WithDeadline`, `WithTimeout`   | `context.WithDeadline`, `WithTimeout`  |
| `WithDeadlineCause`, `WithTimeoutCause` | `context.WithDeadlineCause`, `WithTimeoutCause` |

On `Real` these helpers use system timers and contexts directly.
`Sleep` waits on a stoppable timer so cancellation can wake it.
On other clocks, stop tickers when finished to release their registrations.
Only a direct `*Fake` defers channel delivery until an advance ends.
Wrappers, including types embedding `*Fake`, use their own `Now` and
`AfterFunc` methods and deliver the scheduled time when the callback runs.
Standard cancelable parents and parents implementing
`AfterFunc` support cancellation propagation without a waiting goroutine.
For other parents with a private `Done` channel, the context package may
start a waiter until the parent or child ends.

## Corrected wall time

Use a `wallclock.Clock` when the system wall clock may be unset or
inaccurate. The zero value and `New(nil, 0)` use the system clock as the
base. Pass a `timeshim.Fake` as the base in tests.

```go
import (
	"time"

	"github.com/djylb/timeshim/wallclock"
)

func correctedNow(knownPast, observed time.Time) time.Time {
	clk := wallclock.New(nil, 0)
	clk.Raise(knownPast.Unix())
	clk.Observe(observed.Unix())
	return clk.Now()
}
```

- Validate the source before calling `Observe` or `Raise`. The clock
  computes the median of up to five recent observations and carries the
  observations and lower bounds forward using elapsed time on the base
  clock. `New` accepts an initial lower bound used until an offset is
  available, while a bound recorded by `Raise` continues to apply.
- `Now` returns wall time and may step after a correction. Use `Local`
  or the base clock to measure elapsed time. `AfterFunc` delegates to
  the base clock, so a correction leaves an armed timer's delay intact.
- Store the snapshot from `State` and pass it to `Restore` at startup.
  The caller owns serialization and storage. `OnSample` registers a
  synchronous callback that runs outside the clock's lock. Concurrent
  observations can run callbacks concurrently, so serialize snapshot
  and write together if your store needs ordered updates.
- Obtain `Changes` before computing a wall-clock deadline. A correction
  closes the channel and lets waiters recompute the deadline. Get a new
  channel after each close. The clock starts no background work. Call
  `Stop` on timers you no longer need and use your own cancellation when
  waiting on `Changes`.

## Tests

```go
clk := timeshim.NewFake(time.Unix(0, 0))
c := &Cache{Clock: clk}
go c.Run()

// Wait until the code under test has armed its timer, then fire it.
if err := clk.WaitPending(ctx, 1); err != nil {
	t.Fatal(err)
}
clk.Advance(time.Minute)
```

| `Fake` method          | Does                                                 |
|------------------------|------------------------------------------------------|
| `Advance(d)`           | moves forward by d, calling what comes due           |
| `AdvanceTo(t)`         | moves forward to t; an earlier t does not move back  |
| `AdvanceToNext()`      | jumps to the earliest pending time and fires it      |
| `Pending()`            | counts the pending functions                         |
| `Next()`               | reports the earliest pending time                    |
| `Deadlines()`          | lists their times, earliest first                    |
| `WaitPending(ctx, n)`  | waits until n functions are pending                  |
| `WaitDeadline(ctx, t)` | waits until a function is pending at t               |

## Guarantees

- `Advance` runs the functions that come due in its own goroutine, in order
  of their deadlines, outside the clock's lock, so a function may call the
  clock again or arm new timers; those run in the same `Advance` when due.
- The channel of a `ChanTimer` or `Ticker` created directly on a `*Fake`
  receives the timer's time when the firing `Advance` ends: a goroutine it wakes
  finds the clock at its new time, as after a jump, and a timer that
  goroutine sets does not fire in the same `Advance`, however it is
  scheduled.
- Equal deadlines fire in the order the timers were armed (`AfterFunc` or
  `Reset`).
- Time never goes backwards: a negative `Advance` panics, and of concurrent
  calls each leaves the clock at least where it found it.
- `Timer.Stop` and `Timer.Reset` match `*time.Timer`: each reports whether
  the call was still pending. `ChanTimer` and `Ticker` follow the channel
  semantics of `time` since Go 1.23: a timer that fired but whose value was
  not received still counts as pending, after `Stop` or `Reset` returns no
  stale value is received, and a ticker drops ticks a slow receiver misses.
- For `Fake.AfterFunc` and its timer's `Reset`, a nonpositive delay is due at once: the function runs in
  the next `Advance` (`Advance(0)` included), not in a goroutine of its own,
  and `Deadlines` never reports a time before `Now`. A `ChanTimer` (or
  `After`) with such a delay has its value ready at once, as with
  `time.NewTimer`, since no function has to run.
- The order holds within one `Advance`; concurrent `Advance` calls may run
  due functions at the same time in different goroutines.
- `WithDeadline` on an ended parent, or with a past deadline, returns a
  context that has already ended, with the parent's error first.
- A clock drives the deadline requested on it. When a parent deadline fires,
  its children inherit its error and cause, including through standard value
  and cancellation contexts. An independent clock can still reach a child's
  own requested deadline first. A parent wrapped in `context.WithoutCancel`
  supplies values without propagating cancellation.
- The zero value of `Fake` starts at the zero time. It supports concurrent
  use and must not be copied after first use.
- Each advance processes its own deferred deliveries even if a callback panics.
  Older ticks overtaken by a later delivery are dropped. The panic propagates
  with the clock at the latest time reached, including by nested advances.
- On a fake clock, an unrepresentable target for relative advance, timeout,
  initial timer or ticker registration, or reset panics. A failed reset preserves
  the previous registration and any unread value. A running ticker retires
  after its last representable tick.

## With testing/synctest

`testing/synctest` runs a test in a bubble whose `time` package is
virtual, and advances it when every goroutine is blocked. Inside a bubble,
`Real` is that virtual time, so code that takes a `timeshim.Clock` needs
nothing special. `timeshim` covers what a bubble does not:

- explicit control: move time by an exact step at a chosen point, or
  inspect what is pending, rather than waiting for the bubble to idle;
- code that runs outside a bubble, such as goroutines started before the
  test or shared fixtures;
- tests on real network I/O, which keeps a bubble from ever idling;
- injecting time into production code, such as a clock that follows
  another source.

A `Fake` also works inside a bubble: a goroutine waiting on it is durably
blocked, so `synctest.Wait` returns.

## License

Apache-2.0
