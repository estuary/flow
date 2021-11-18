package capture

import (
	"fmt"
	"io"
	"math"

	pf "github.com/estuary/protocols/flow"
	"go.gazette.dev/core/broker/client"
)

// coordinator encapsulates common coordination of PushServer and PullClient.
type coordinator struct {
	// logCommittedDone marks that logCommittedOp has already signaled and been
	// cleared during the current transaction.
	logCommittedDone bool
	// logCommittedOp resolves on the prior transaction's commit to the recovery log.
	// When resolved, the PullClient notifies the driver by sending Acknowledge,
	// and will notify the caller it may start to commit if further data is ready.
	// Nil if there isn't an ongoing recovery log commit.
	logCommittedOp client.OpFuture
	// logCommittedOpCh is sent to from SetLogCommittedOp(), and reads from Serve()
	// to reset the current logCommittedOp.
	logCommittedOpCh chan client.OpFuture
	// Serve loop exit status.
	loopOp *client.AsyncOperation
	// Next transaction which is being accumulated.
	next captureTxn
	// Prior transaction which is being committed.
	prior captureTxn
	// Specification of this Pull RPC.
	spec *pf.CaptureSpec
	// Version of the client's CaptureSpec.
	version string
}

// captureTxn is the state of a transaction.
type captureTxn struct {
	combiners []pf.Combiner
	// Merged checkpoint of the capture.
	merged pf.DriverCheckpoint
	// Number of checkpoints accumulated into the transaction.
	numCheckpoints int
	// Are we awaiting a Checkpoint before we may commit ?
	pending bool
}

func newCoordinator(
	newCombinerFn func(*pf.CaptureSpec_Binding) (pf.Combiner, error),
	range_ pf.RangeSpec,
	spec *pf.CaptureSpec,
	version string,
) (coordinator, error) {

	if range_.RClockBegin != 0 || range_.RClockEnd != math.MaxUint32 {
		return coordinator{}, fmt.Errorf("captures cannot split on r-clock: " + range_.String())
	}

	var combiners [2][]pf.Combiner
	for i := range combiners {
		for _, b := range spec.Bindings {
			var combiner, err = newCombinerFn(b)
			if err != nil {
				return coordinator{}, fmt.Errorf("creating %s combiner: %w", b.Collection.Collection, err)
			}
			combiners[i] = append(combiners[i], combiner)
		}
	}
	return coordinator{
		logCommittedDone: true, // Initialize as ready-to-commit.
		logCommittedOp:   nil,
		logCommittedOpCh: make(chan client.OpFuture),
		loopOp:           client.NewAsyncOperation(),
		next:             captureTxn{combiners: combiners[0], pending: true},
		prior:            captureTxn{combiners: combiners[1]},
		spec:             spec,
		version:          version,
	}, nil
}

// Close the Capture. The primary loop must have already initiated a close
// (due to an error, or context cancellation, or EOF).
// Close blocks until the error has propagated through the coordinator's
// loop and has fully stopped.
func (c *coordinator) Close() error {
	<-c.loopOp.Done()

	for _, c := range c.prior.combiners {
		c.Destroy()
	}
	for _, c := range c.next.combiners {
		c.Destroy()
	}

	// EOF is a graceful shutdown.
	if err := c.loopOp.Err(); err != io.EOF {
		return err
	}
	return nil
}

// Combiners returns the Combiners of a transaction which is ready to commit.
// It's safe to call only after a callback notification from Serve(),
// and only until a call to SetLogCommitOp().
// The caller is responsible for fully draining the combiners.
func (c *coordinator) Combiners() []pf.Combiner { return c.prior.combiners }

// DriverCheckpoint returns the DriverCheckpoint of a transaction which is ready
// to commit. It's safe to call only after a callback notification from Serve(),
// and only until a call to SetLogCommitOp().
func (c *coordinator) DriverCheckpoint() pf.DriverCheckpoint { return c.prior.merged }

// SetLogCommitOp tells the PullClient of a future recovery log commit operation
// which will commit a transaction previously started via a Serve() callback.
func (c *coordinator) SetLogCommitOp(op client.OpFuture) error {
	select {
	case c.logCommittedOpCh <- op:
		return nil
	case <-c.loopOp.Done():
		return c.loopOp.Err()
	}
}

func (c *coordinator) onLogCommitted() error {
	if err := c.logCommittedOp.Err(); err != nil {
		return fmt.Errorf("recovery log commit: %w", err)
	}
	c.logCommittedOp = nil // Don't receive again.
	c.logCommittedDone = true

	return nil
}

func (c *coordinator) onLogCommittedOpCh(op client.OpFuture) error {
	if c.logCommittedOp != nil || c.logCommittedDone {
		return fmt.Errorf("protocol error: a commit operation is already running")
	}
	c.logCommittedOp = op
	return nil
}

func (c *coordinator) onDocuments(docs Documents) error {
	var b = int(docs.Binding)
	if b >= len(c.next.combiners) {
		return fmt.Errorf("protocol error (binding %d out of range)", b)
	}
	var combiner = c.next.combiners[b]

	// Feed documents into the combiner as combine-right operations.
	for _, slice := range docs.DocsJson {
		if err := combiner.CombineRight(docs.Arena.Bytes(slice)); err != nil {
			return fmt.Errorf("combiner.CombineRight: %w", err)
		}
	}
	c.next.pending = true // Mark that we're awaiting a Checkpoint.

	return nil
}

func (c *coordinator) onCheckpoint(checkpoint pf.DriverCheckpoint) error {
	if c.next.numCheckpoints == 0 {
		c.next.merged = checkpoint
	} else if err := c.next.merged.Reduce(checkpoint); err != nil {
		return fmt.Errorf("reducing driver checkpoint: %w", err)
	}
	c.next.numCheckpoints++
	c.next.pending = false

	return nil
}

func (c *coordinator) maybeLogCommittedOp() <-chan struct{} {
	if c.logCommittedOp != nil {
		return c.logCommittedOp.Done()
	}
	return nil
}

func (c *coordinator) loop(
	startCommitFn func(error),
	nextFn func() (drained bool, _ error),
) (__out error) {
	defer func() {
		// loopOp must resolve first to avoid a deadlock if
		// startCommitFn calls back into SetLogCommitOp.
		c.loopOp.Resolve(__out)
		startCommitFn(__out)
	}()

	var drained bool
	for !drained || !c.next.pending || !c.logCommittedDone {
		// Is the next transaction ready to start committing ?
		//  * It must have at least one checkpoint (though it may have no documents).
		//  * It must not have documents awaiting a checkpoint.
		//  * The prior transaction must have fully committed.
		if !c.next.pending && c.logCommittedDone {
			c.prior, c.next = c.next, captureTxn{
				combiners:      c.prior.combiners,
				merged:         pf.DriverCheckpoint{},
				numCheckpoints: 0,
				pending:        true,
			}
			c.logCommittedDone = false

			startCommitFn(nil)
			continue
		}

		var err error
		if drained, err = nextFn(); err != nil {
			return err
		}
	}

	return io.EOF
}
