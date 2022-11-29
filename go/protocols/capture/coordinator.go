package capture

import (
	"errors"
	"fmt"
	"io"
	"math"

	pf "github.com/estuary/flow/go/protocols/flow"
	"go.gazette.dev/core/broker/client"
	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
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
	// Are we no longer accepting further documents & checkpoints ?
	full bool
	// Merged checkpoint of the capture.
	merged pf.DriverCheckpoint
	// Number of bytes accumulated into the transaction.
	numBytes int
	// Number of checkpoints accumulated into the transaction.
	numCheckpoints int
	// Are we awaiting a Checkpoint before we may commit ?
	pending bool
	// Was this ready transaction already popped?
	popped bool
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

// PopTransaction returns the Combiners and DriverCheckpoint of a transaction
// which is ready to commit. It's safe to call only after a commit callback
// notification from Serve(), and must be called exactly once prior to
// SetLogCommitOp(). The caller is responsible for fully draining the combiners.
func (c *coordinator) PopTransaction() ([]pf.Combiner, pf.DriverCheckpoint) {
	if c.prior.popped {
		panic("PopTransaction was called more than once")
	}
	c.prior.popped = true

	return c.prior.combiners, c.prior.merged
}

// SetLogCommitOp tells the PullClient of a future recovery log commit operation
// which will commit a transaction previously started via a Serve() callback.
func (c *coordinator) SetLogCommitOp(op client.OpFuture) error {
	if !c.prior.popped {
		panic("PopTransaction was not called before SetLogCommitOp")
	}

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
		c.next.numBytes += int(slice.End - slice.Begin)
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
	c.next.full = c.next.numBytes > combinerByteThreshold

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
	nextFn func(full bool) (drained bool, _ error),
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
				numBytes:       0,
				numCheckpoints: 0,
				pending:        true,
				full:           false,
			}
			c.logCommittedDone = false

			startCommitFn(nil)
			continue
		}

		var err error
		if drained, err = nextFn(c.next.full); err != nil {
			if status, ok := status.FromError(err); ok && status.Code() == codes.Internal {
				err = errors.New(status.Message())
			}
			return err
		}
	}

	return io.EOF
}

// combinerByteThreshold is a coarse target on the documents which can be
// optimistically combined within a capture transaction, while awaiting
// the commit of a previous transaction. Upon reaching this threshold,
// further documents and checkpoints will not be folded into the
// transaction.
var combinerByteThreshold = (1 << 27) // 128MB.
