package capture

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"

	pf "github.com/estuary/protocols/flow"
	"go.gazette.dev/core/broker/client"
)

// PullClient is a client of a driver's Pull RPC.
type PullClient struct {
	// logCommittedDone marks that logCommittedOp has already signaled and been
	// cleared during the current transaction.
	logCommittedDone bool
	// logCommittedOp resolves on the prior transaction's commit to the recovery log.
	// When resolved, the PullClient notifies the driver by sending Acknowledge,
	// and will notify the caller it may start to commit if further data is ready.
	// Nil if there isn't an ongoing recovery log commit.
	logCommittedOp client.OpFuture
	// logCommittedOpCh is sent to from SetLogCommittedOp(), and reads from Read()
	// to reset the current logCommittedOp.
	logCommittedOpCh chan client.OpFuture
	// Read loop exit status.
	loopOp *client.AsyncOperation
	// Next transaction which is being accumulated.
	next pullTxn
	// Prior transaction which is being committed.
	prior pullTxn
	// rpc is the long-lived Pull RPC, and is accessed only from Read.
	rpc Driver_PullClient
	// Specification of this Pull RPC.
	spec *pf.CaptureSpec
	// Version of the client's CaptureSpec.
	version string
}

// pullTxn is the state of a pull transaction.
type pullTxn struct {
	combiners []pf.Combiner
	// Merged checkpoint of the capture.
	merged pf.DriverCheckpoint
	// Number of checkpoints accumulated into the transaction.
	numCheckpoints int
	// Are we awaiting a Checkpoint before we may commit ?
	pending bool
}

// OpenPull opens a Pull RPC.
// It returns a *PullClient which provides a high-level API for executing
// the pull-based capture transaction workflow.
func OpenPull(
	ctx context.Context,
	driver DriverClient,
	driverCheckpoint json.RawMessage,
	newCombinerFn func(*pf.CaptureSpec_Binding) (pf.Combiner, error),
	range_ pf.RangeSpec,
	spec *pf.CaptureSpec,
	version string,
	tail bool,
) (*PullClient, error) {

	if range_.RClockBegin != 0 || range_.RClockEnd != math.MaxUint32 {
		return nil, fmt.Errorf("captures cannot split on r-clock: " + range_.String())
	}

	var combiners [2][]pf.Combiner
	for i := range combiners {
		for _, b := range spec.Bindings {
			var combiner, err = newCombinerFn(b)
			if err != nil {
				return nil, fmt.Errorf("creating %s combiner: %w", b.Collection.Collection, err)
			}
			combiners[i] = append(combiners[i], combiner)
		}
	}

	rpc, err := driver.Pull(ctx)
	if err != nil {
		return nil, fmt.Errorf("driver.Pull: %w", err)
	}
	// Close RPC if remaining initialization fails.
	defer func() {
		if rpc != nil {
			_ = rpc.CloseSend()
		}
	}()

	if err = rpc.Send(&PullRequest{
		Open: &PullRequest_Open{
			Capture:              spec,
			Version:              version,
			KeyBegin:             range_.KeyBegin,
			KeyEnd:               range_.KeyEnd,
			DriverCheckpointJson: driverCheckpoint,
			Tail:                 tail,
		}}); err != nil {
		return nil, fmt.Errorf("sending Open: %w", err)
	}

	// Read Opened response.
	opened, err := rpc.Recv()
	if err != nil {
		return nil, fmt.Errorf("reading Opened: %w", err)
	} else if opened.Opened == nil {
		return nil, fmt.Errorf("expected Opened, got %#v", opened.String())
	}

	var out = &PullClient{
		logCommittedDone: true, // Initialize as ready-to-commit.
		logCommittedOp:   nil,
		logCommittedOpCh: make(chan client.OpFuture),
		loopOp:           client.NewAsyncOperation(),
		next:             pullTxn{combiners: combiners[0], pending: true},
		prior:            pullTxn{combiners: combiners[1]},
		rpc:              rpc,
		spec:             spec,
		version:          version,
	}

	rpc = nil // Don't run deferred CloseSend.
	return out, nil
}

// Close the PullClient. The driver must have already initiated a close of the
// RPC, or errorred, or the RPC context must be cancelled.
// Close blocks until the error has propagated through the PullClient's Read()
// loop and has fully stopped.
func (c *PullClient) Close() error {
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

// Read is a long-lived routine which processes transactions from the Pull RPC.
// When captured documents are ready to commit, it invokes the startCommitFn
// callback.
//
// On callback, the caller must drain documents from Combiners() and track
// the associated DriverCheckpoint(), and then notify the PullClient of a
// pending commit via SetLogCommittedOp().
//
// While this drain and commit is ongoing, Read() will accumulate further
// captured documents and checkpoints. It will then notify the caller of
// the next transaction only after the resolution of the prior transaction's
// commit.
//
// Read will call into startCommitFn with a non-nil error exactly once,
// as its very last invocation.
func (c *PullClient) Read(startCommitFn func(error)) (__out error) {
	defer func() {
		// loopOp must resolve first to avoid a deadlock if
		// startCommitFn calls back into SetLogCommitOp.
		c.loopOp.Resolve(__out)
		startCommitFn(__out)
	}()

	var respCh = PullResponseChannel(c.rpc)

	for respCh != nil || !c.next.pending {

		// Is the next transaction ready to start committing ?
		//  * It must have at least one checkpoint (though it may have no documents).
		//  * It must not have documents awaiting a checkpoint.
		//  * The prior transaction must have fully committed.
		if !c.next.pending && c.logCommittedDone {
			c.prior, c.next = c.next, pullTxn{
				combiners:      c.prior.combiners,
				merged:         pf.DriverCheckpoint{},
				numCheckpoints: 0,
				pending:        true,
			}
			c.logCommittedDone = false

			startCommitFn(nil)
			continue
		}

		var maybeLogCommittedOp <-chan struct{}
		if c.logCommittedOp != nil {
			maybeLogCommittedOp = c.logCommittedOp.Done()
		}

		select {
		case <-maybeLogCommittedOp:
			if err := c.onLogCommitted(); err != nil {
				return fmt.Errorf("onLogCommitted: %w", err)
			}

		case op := <-c.logCommittedOpCh:
			if c.logCommittedOp != nil || c.logCommittedDone {
				return fmt.Errorf("protocol error: a commit operation is already running")
			}
			c.logCommittedOp = op

		case rx, ok := <-respCh:
			if !ok {
				respCh = nil // Don't select again.
				continue
			} else if rx.Error != nil {
				return rx.Error
			} else if err := rx.Validate(); err != nil {
				return err
			}

			switch {
			case rx.Documents != nil:
				if err := c.onDocuments(*rx.Documents); err != nil {
					return fmt.Errorf("onDocuments: %w", err)
				}
			case rx.Checkpoint != nil:
				if err := c.onCheckpoint(*rx.Checkpoint); err != nil {
					return fmt.Errorf("onCheckpoint: %w", err)
				}
			default:
				return fmt.Errorf("read unexpected response: %v", rx)
			}
		}
	}

	return io.EOF
}

// Combiners returns the Combiners of a transaction which is ready to commit.
// It's safe to call only after a callback notification from Read(),
// and only until a call to SetLogCommitOp().
// The caller is responsible for fully draining the combiners.
func (c *PullClient) Combiners() []pf.Combiner { return c.prior.combiners }

// DriverCheckpoint returns the DriverCheckpoint of a transaction which is ready
// to commit. It's safe to call only after a callback notification from Read(),
// and only until a call to SetLogCommitOp().
func (c *PullClient) DriverCheckpoint() pf.DriverCheckpoint { return c.prior.merged }

// SetLogCommitOp tells the PullClient of a future recovery log commit operation
// which will commit a transaction previously started via a Read() callback.
func (c *PullClient) SetLogCommitOp(op client.OpFuture) error {
	select {
	case c.logCommittedOpCh <- op:
		return nil
	case <-c.loopOp.Done():
		return c.loopOp.Err()
	}
}

func (c *PullClient) onLogCommitted() error {
	if err := c.logCommittedOp.Err(); err != nil {
		return fmt.Errorf("recovery log commit: %w", err)
	}
	c.logCommittedOp = nil // Don't receive again.
	c.logCommittedDone = true

	// Notify the driver of the commit.
	for i := 0; i != c.prior.numCheckpoints; i++ {
		// We ignore a failure to send Acknowledge for two reasons:
		// * The server controls stream shutdown. It could have gracefully closed
		//   the stream already, and we have no way of knowing that here.
		// * Send errors are only ever nil or EOF.
		//   If it's EOF then a read of the stream will return a more descriptive error.
		_ = c.rpc.Send(&PullRequest{Acknowledge: &Acknowledge{}})
	}

	return nil
}

func (c *PullClient) onDocuments(docs Documents) error {
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

func (c *PullClient) onCheckpoint(checkpoint pf.DriverCheckpoint) error {
	if c.next.numCheckpoints == 0 {
		c.next.merged = checkpoint
	} else if err := c.next.merged.Reduce(checkpoint); err != nil {
		return fmt.Errorf("reducing driver checkpoint: %w", err)
	}
	c.next.numCheckpoints++
	c.next.pending = false

	return nil
}
