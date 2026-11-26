# timeshim

Injectable time for deterministic tests. Code that times out or expires in
the background takes a `timeshim.Clock` instead of calling `time.Now` and
`time.AfterFunc`; production uses the system clock, and tests move a fake
clock instead of waiting. Standard library only.

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

A `*time.Timer` is a `timeshim.Timer`, so `Real` adds no wrapper.

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

On `Real` each of them calls the `time` or `context` function directly.
On other clocks, timers and tickers are built on `AfterFunc` without a
goroutine of their own, and contexts end in the `Advance` that reaches
their deadline.

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
- The channel of a `ChanTimer` or `Ticker` that fires in an `Advance`
  receives the timer's time when that `Advance` ends: a goroutine it wakes
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
- On a `Fake`, a delay of zero or less is due at once: the function runs in
  the next `Advance` (`Advance(0)` included), not in a goroutine of its own,
  and `Deadlines` never reports a time before `Now`. A `ChanTimer` (or
  `After`) with such a delay has its value ready at once, as with
  `time.NewTimer`, since no function has to run.
- The order holds within one `Advance`; concurrent `Advance` calls may run
  due functions at the same time in different goroutines.
- `WithDeadline` on an ended parent, or with a past deadline, returns a
  context that has already ended, with the parent's error first.
- `Fake` is safe for concurrent use; its zero value is not ready, use
  `NewFake`.

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
