package flow

import (
	"time"
)

// Timepoint is a future for a point in time, which may be
// awaited (via Ready) and, once resolved, offers a time.Time
// as well as a Next future Timepoint which may be awaited.
type Timepoint struct {
	readyCh chan struct{}
	// Time of a resolved Timepoint.
	// May not be read until Ready selects.
	Time time.Time
	// Next Timepoint future which will resolve after this one.
	// May not be read until Ready selects.
	Next *Timepoint
}

// NewTimepoint returns a resolved Timepoint at the given time.
func NewTimepoint(time time.Time) *Timepoint {
	var readyCh = make(chan struct{})
	close(readyCh)

	return &Timepoint{
		readyCh: readyCh,
		Time:    time,
		Next:    &Timepoint{readyCh: make(chan struct{})},
	}
}

// Ready selects when the Timepoint is ready.
func (t *Timepoint) Ready() <-chan struct{} { return t.readyCh }

// Resolve the Timepoint at the given |time|.
func (t *Timepoint) Resolve(time time.Time) {
	t.Time = time
	t.Next = &Timepoint{readyCh: make(chan struct{})}
	close(t.readyCh)
}
