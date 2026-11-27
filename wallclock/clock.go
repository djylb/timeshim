// Package wallclock estimates wall time from trusted observations. It uses
// an injected clock to advance observations and lower bounds, using the base
// clock's monotonic reading when available. The caller authenticates
// observations and manages persistence.
package wallclock

import (
	"math"
	"slices"
	"sync"
	"time"

	"github.com/djylb/timeshim"
)

const (
	// Take the median over a bounded window to limit the influence of outliers.
	sampleWindow = 5
	// Whole-second observations do not warrant a small correction.
	deadband = 2 * time.Second
)

// Clock is a [timeshim.Clock] that estimates wall time. Its zero value uses
// [timeshim.Real] with no lower bound or observations. It is safe for concurrent
// use and must not be copied after first use. An injected clock must support
// concurrent use. Clock reads the base time under its lock, so the base's Now
// must run independently of Clock. Clock starts no goroutines and owns no timers.
type Clock struct {
	base timeshim.Clock
	mu   sync.Mutex

	observations   []sample // oldest first
	restoredOffset time.Duration
	restored       bool
	onSample       func()
	changes        chan struct{}
	initialFloor   sample
	floor          sample
}

type sample struct {
	local     time.Time
	reference time.Time
}

func (s sample) project(now time.Time) time.Time {
	return s.reference.Add(now.Sub(s.local))
}

// New returns a clock over base. A nil base uses [timeshim.Real]. The initial
// lower bound is given in Unix seconds and advances with base. It applies
// until an observation or a restored offset provides a measurement. A
// nonpositive or unrepresentable bound is ignored.
func New(base timeshim.Clock, initialFloorUnix int64) *Clock {
	c := &Clock{base: base}
	if floor := unixTime(initialFloorUnix); !floor.IsZero() {
		c.initialFloor = sample{local: c.Local(), reference: floor}
	}
	return c
}

// Observe records a trusted source's wall time in Unix seconds at the current
// base time. It ignores nonpositive or unrepresentable times. The estimate
// uses the median of the latest five observations (the upper median if the
// window has an even number of observations). An absolute skew below two
// seconds uses the local wall time.
func (c *Clock) Observe(unix int64) {
	at := unixTime(unix)
	if at.IsZero() {
		return
	}

	if f := c.observe(at); f != nil {
		f()
	}
}

func (c *Clock) observe(at time.Time) func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.Local()
	before, measured := c.estimateLocked(now)
	if len(c.observations) == sampleWindow {
		copy(c.observations, c.observations[1:])
		c.observations = c.observations[:sampleWindow-1]
	}
	c.observations = append(c.observations, sample{local: now, reference: at})
	after, _ := c.estimateLocked(now)
	if !measured || !after.Equal(before) {
		c.signalLocked()
	}
	return c.onSample
}

// OnSample replaces the function called after each accepted observation. Pass
// nil to clear it. Observe calls the selected function synchronously and
// outside the clock's lock, so it may call Clock methods. Concurrent observations
// may call it concurrently. Replacement does not wait for in-flight calls.
// If the callback persists state, serialize the snapshot and write as a unit.
func (c *Clock) OnSample(f func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onSample = f
}

// Raise lifts the trusted lower bound to a time known to have passed, in Unix
// seconds. The bound advances with the base clock and remains in effect after
// measurements. A nonpositive, unrepresentable, or earlier time is ignored.
func (c *Clock) Raise(unix int64) {
	at := unixTime(unix)
	if at.IsZero() {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.raiseLocked(c.Local(), at) {
		c.signalLocked()
	}
}

func (c *Clock) raiseLocked(now, at time.Time) bool {
	if !c.floor.reference.IsZero() && !at.After(c.floor.project(now)) {
		return false
	}
	c.floor = sample{local: now, reference: at}
	return true
}

// Measured reports whether a live observation or restored offset is available.
func (c *Clock) Measured() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.observations) > 0 || c.restored
}

// Skew returns the measured wall time minus the local wall time before applying
// any lower bound. The duration saturates at the limits of [time.Duration]. The
// boolean reports whether a measurement is available.
func (c *Clock) Skew() (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.Local()
	estimate, measured := c.estimateLocked(now)
	return estimate.Sub(now.Round(0)), measured
}

// estimateLocked returns a wall-only time before applying either bound. Use
// base elapsed time to carry observations forward. Comparing the projected
// times directly preserves the median even when the skew exceeds a Duration.
func (c *Clock) estimateLocked(now time.Time) (time.Time, bool) {
	local := now.Round(0)
	if len(c.observations) == 0 {
		return local.Add(c.restoredOffset), c.restored
	}
	var scratch [sampleWindow]time.Time
	estimates := scratch[:len(c.observations)]
	for i, obs := range c.observations {
		estimates[i] = obs.project(now)
	}
	slices.SortFunc(estimates, time.Time.Compare)
	estimate := estimates[len(estimates)/2]
	if skew := estimate.Sub(local); skew > -deadband && skew < deadband {
		return local, true
	}
	return estimate, true
}

// Now returns the estimated wall time, subject to the lower bounds, in the
// base clock's location. It omits the monotonic reading and can move backward
// as observations correct the estimate. Use Local or the base clock to measure
// elapsed time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.Local()
	estimate, measured := c.estimateLocked(now)
	return c.applyFloorLocked(now, estimate, measured).In(now.Location())
}

func (c *Clock) applyFloorLocked(now, estimate time.Time, measured bool) time.Time {
	if !measured && !c.initialFloor.reference.IsZero() {
		if floor := c.initialFloor.project(now); floor.After(estimate) {
			estimate = floor
		}
	}
	if !c.floor.reference.IsZero() {
		if floor := c.floor.project(now); floor.After(estimate) {
			estimate = floor
		}
	}
	return estimate
}

// Local returns the uncorrected base reading, including any monotonic reading
// supplied by the base clock.
func (c *Clock) Local() time.Time { return timeshim.Or(c.base).Now() }

// AfterFunc delegates to the base clock. Corrections do not affect its delay.
// The caller owns the returned timer and can stop it to cancel a pending call.
func (c *Clock) AfterFunc(d time.Duration, f func()) timeshim.Timer {
	return timeshim.Or(c.base).AfterFunc(d, f)
}

// State is a snapshot for persistence by the caller. Its zero value carries no
// information to restore. Offset at either duration limit marks a potentially
// saturated skew and is only diagnostic.
type State struct {
	Offset   time.Duration `json:"offset_ns"` // skew before applying bounds
	Measured bool          `json:"measured"`  // whether Offset represents a measurement
	Time     int64         `json:"time"`      // bounded wall time in Unix seconds
}

// State returns the offset, measurement status, and bounded wall time from
// one base reading.
func (c *Clock) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.Local()
	estimate, measured := c.estimateLocked(now)
	return State{
		Offset:   estimate.Sub(now.Round(0)),
		Measured: measured,
		Time:     c.applyFloorLocked(now, estimate, measured).Unix(),
	}
}

// Restore raises the trusted lower bound to the snapshot's time. It accepts
// the snapshot's measured offset only if there are no live observations and
// the local wall time plus that offset is at or after the snapshot's time.
// An offset at a duration limit may have saturated, so it is not restored.
// Use a trusted snapshot. A nonpositive or unrepresentable time is ignored.
func (c *Clock) Restore(s State) {
	at := unixTime(s.Time)
	if at.IsZero() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.Local()
	changed := c.raiseLocked(now, at)
	if len(c.observations) == 0 && s.reusableAt(now, at) {
		changed = changed || !c.restored || s.Offset != c.restoredOffset
		c.restoredOffset, c.restored = s.Offset, true
	}
	if changed {
		c.signalLocked()
	}
}

func (s State) reusableAt(local, at time.Time) bool {
	return s.Measured && s.Offset != math.MinInt64 && s.Offset != math.MaxInt64 && !local.Round(0).Add(s.Offset).Before(at)
}

// Changes returns a channel that closes when Observe, Raise, or Restore first
// makes a measurement available, changes the correction, or raises the trusted
// lower bound. Obtain the channel before reading Now to subscribe without
// missing a correction between the read and the wait. After a close, call
// Changes again. A subscriber can stop waiting without unregistering. Base
// time may advance or step without a notification. Recompute any absolute
// deadlines on a correction, since existing timers keep their original delays.
func (c *Clock) Changes() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.changes == nil {
		c.changes = make(chan struct{})
	}
	return c.changes
}

func (c *Clock) signalLocked() {
	if c.changes != nil {
		close(c.changes)
		c.changes = nil
	}
}

func unixTime(sec int64) time.Time {
	if sec <= 0 {
		return time.Time{}
	}
	at := time.Unix(sec, 0)
	if at.Before(time.Time{}) { // the internal time representation overflowed
		return time.Time{}
	}
	return at
}
