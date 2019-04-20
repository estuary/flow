package task

import (
	"context"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Group is a group of tasks which should each be executed concurrently,
// and which should be collectively blocked on until all are complete.
// Tasks should be preemptable, and the first task to return a non-nil
// error cancels the entire Group. While Group is used to invoke and
// wait on multiple goroutines, it is not itself thread-safe.
type Group struct {
	// Context of the Group, which is cancelled by:
	//  * Any function of the Group returning non-nil error, or
	//  * An explicit call to Cancel, or
	//  * A cancellation of the parent Context of the Group.
	//
	// Tasks queued to the Group should monitor Context and return
	// upon its cancellation.
	ctx context.Context
	// Cancels Context.
	cancelFn context.CancelFunc

	tasks   []task
	eg      *errgroup.Group
	started bool
}

// task composes a runnable and its description.
type task struct {
	desc string
	fn   func() error
}

// NewGroup returns a new, empty Group with the given Context.
func NewGroup(ctx context.Context) *Group {
	ctx, cancel := context.WithCancel(ctx)
	eg, ctx := errgroup.WithContext(ctx)
	return &Group{ctx: ctx, eg: eg, cancelFn: cancel}
}

// Context returns the Group Context.
func (g *Group) Context() context.Context { return g.ctx }

// Cancel the Group Context.
func (g *Group) Cancel() { g.cancelFn() }

// Queue a function for execution with the Group.
// Cannot be called after GoRun is invoked or Queue panics.
func (g *Group) Queue(desc string, fn func() error) {
	if g.started {
		panic("Queue called after GoRun")
	}
	g.tasks = append(g.tasks, task{desc: desc, fn: fn})
}

// GoRun all queued functions. GoRun may be called only once:
// the second invocation will panic.
func (g *Group) GoRun() {
	if g.started {
		panic("GoRun already called")
	}
	g.started = true

	for i := range g.tasks {
		var t = g.tasks[i]
		g.eg.Go(func() error { return errors.WithMessage(t.fn(), t.desc) })
	}
}

// Wait for started functions, returning only after all complete.
// The first encountered non-nil error is returned.
// GoRun must have been called or Wait panics.
func (g *Group) Wait() error {
	if !g.started {
		panic("Wait called before GoRun")
	}
	return g.eg.Wait()
}
