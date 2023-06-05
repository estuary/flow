package capture

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	math "math"

	pf "github.com/estuary/flow/go/protocols/flow"
	"go.gazette.dev/core/broker/client"
)

// Client of a connector's Capture RPC. It provides a high-level
// API for executing the pull-based capture transaction workflow.
type Client struct {
	// Should Acknowledgements be sent?
	explicitAcks bool
	// logCommittedDone marks that logCommittedOp has already signaled and been
	// cleared during the current transaction.
	logCommittedDone bool
	// logCommittedOp resolves on the prior transaction's commit to the recovery log.
	// When resolved, the Client notifies the connector by sending Acknowledge,
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
	// rpc is the long-lived Pull RPC, and is accessed only from Serve.
	rpc Connector_CaptureClient
	// Specification of this Pull RPC.
	spec *pf.CaptureSpec
	// Version of the client's CaptureSpec.
	version string
}

// Open a Capture RPC using the provided ConnectorClient and CaptureSpec.
//
// When captured documents are ready to commit, the startCommitFn callback
// is invoked. Upon this callback, the caller must PopTransaction() to
// drain documents from combiners and track the associated ConnectorState,
// and must then notify the Client of a pending commit via SetLogCommittedOp().
//
// While this drain and commit is ongoing, the Client will accumulate further
// captured documents and checkpoints. It will then notify the caller of
// the next transaction only after the resolution of the prior transaction's
// commit.
//
// startCommitFn() will be called with a non-nil error exactly once,
// as its very last invocation.
func Open(
	ctx context.Context,
	connector ConnectorClient,
	connectorState json.RawMessage,
	newCombinerFn func(*pf.CaptureSpec_Binding) (pf.Combiner, error),
	range_ pf.RangeSpec,
	spec *pf.CaptureSpec,
	version string,
	startCommitFn func(error),
) (*Client, error) {

	if range_.RClockBegin != 0 || range_.RClockEnd != math.MaxUint32 {
		return nil, fmt.Errorf("captures cannot split on r-clock: %s", range_)
	}

	var combiners [2][]pf.Combiner
	for i := range combiners {
		for _, b := range spec.Bindings {
			var combiner, err = newCombinerFn(b)
			if err != nil {
				return nil, fmt.Errorf("creating %s combiner: %w", b.Collection.Name, err)
			}
			combiners[i] = append(combiners[i], combiner)
		}
	}

	rpc, err := connector.Capture(ctx)
	if err != nil {
		return nil, fmt.Errorf("driver.Pull: %w", err)
	}
	// Close RPC if remaining initialization fails.
	defer func() {
		if rpc != nil {
			_ = rpc.CloseSend()
		}
	}()

	if err = rpc.Send(&Request{
		Open: &Request_Open{
			Capture:   spec,
			Version:   version,
			Range:     &range_,
			StateJson: connectorState,
		}}); err != nil {
		return nil, fmt.Errorf("sending Open: %w", err)
	}

	// Read Opened response.
	opened, err := rpc.Recv()
	if err != nil {
		return nil, fmt.Errorf("reading Opened: %w", pf.UnwrapGRPCError(err))
	} else if opened.Opened == nil {
		return nil, fmt.Errorf("expected Opened, got %#v", opened.String())
	}

	// We will send no more input into the RPC.
	if !opened.Opened.ExplicitAcknowledgements {
		_ = rpc.CloseSend()
	}

	var out = &Client{
		explicitAcks:     opened.Opened.ExplicitAcknowledgements,
		logCommittedDone: true, // Initialize as ready-to-commit.
		logCommittedOp:   nil,
		logCommittedOpCh: make(chan client.OpFuture),
		loopOp:           client.NewAsyncOperation(),
		next:             captureTxn{combiners: combiners[0], pending: true},
		prior:            captureTxn{combiners: combiners[1]},
		rpc:              rpc,
		spec:             spec,
		version:          version,
	}
	go out.serve(ctx, startCommitFn)

	rpc = nil // Don't run deferred CloseSend.
	return out, nil
}

// Close the client. The connector must have already initiated a close
// (due to an error, or context cancellation, or EOF).
// Close blocks until the Client's loop and has fully stopped.
func (c *Client) Close() {
	<-c.loopOp.Done()

	for _, c := range c.prior.combiners {
		c.Destroy()
	}
	for _, c := range c.next.combiners {
		c.Destroy()
	}
}

// captureTxn is the state of a transaction.
type captureTxn struct {
	combiners []pf.Combiner
	// Are we no longer accepting further documents & checkpoints ?
	full bool
	// Merged checkpoint of the capture.
	merged *pf.ConnectorState
	// Number of bytes accumulated into the transaction.
	numBytes int
	// Number of checkpoints accumulated into the transaction.
	numCheckpoints int
	// Are we awaiting a Checkpoint before we may commit ?
	pending bool
	// Was this ready transaction already popped?
	popped bool
}

// serve is a long-lived routine which processes transactions from the RPC.
func (c *Client) serve(ctx context.Context, startCommitFn func(error)) (__out error) {
	defer func() {
		// loopOp must resolve first to avoid a deadlock if
		// startCommitFn calls back into SetLogCommitOp.
		c.loopOp.Resolve(__out)
		startCommitFn(__out)
	}()
	if c.explicitAcks {
		defer c.rpc.CloseSend()
	}

	var respCh = make(chan responseOrError, 8)
	go readResponses(c.rpc, respCh)

	for respCh != nil || !c.next.pending || !c.logCommittedDone {

		// Is the next transaction ready to start committing ?
		//  * It must have at least one checkpoint (though it may have no documents).
		//  * It must not have documents awaiting a checkpoint.
		//  * The prior transaction must have fully committed.
		if !c.next.pending && c.logCommittedDone {
			c.prior, c.next = c.next, captureTxn{
				combiners:      c.prior.combiners,
				full:           false,
				merged:         nil,
				numBytes:       0,
				numCheckpoints: 0,
				pending:        true,
				popped:         false,
			}
			c.logCommittedDone = false

			startCommitFn(nil)
			continue
		}

		// If we have a commit operation we're waiting for, listen for it.
		var maybeLogCommittedOp <-chan struct{}
		if c.logCommittedOp != nil {
			maybeLogCommittedOp = c.logCommittedOp.Done()
		}

		// Only read more responses if we haven't filled the current transaction.
		var maybeRespCh <-chan responseOrError
		if !c.next.full {
			maybeRespCh = respCh
		}

		// We prefer to gracefully `respCh` to close, and then drain any
		// final transaction. But, if not reading from `respCh`, we *must*
		// monitor for context cancellation as there's no guarantee that
		// log commit operations will resolve (the client may have gone away).
		var maybeDoneCh <-chan struct{}
		if maybeRespCh == nil {
			maybeDoneCh = ctx.Done()
		}

		select {

		// Runtime is informing us of a started commit operation.
		case op := <-c.logCommittedOpCh:
			if c.logCommittedOp != nil || c.logCommittedDone {
				return fmt.Errorf("protocol error: a commit operation is already running")
			}
			c.logCommittedOp = op

		// A previously started commit operation has completed.
		case <-maybeLogCommittedOp:
			if err := c.logCommittedOp.Err(); err != nil {
				return fmt.Errorf("recovery log commit: %w", err)
			}
			c.logCommittedOp = nil // Don't receive again.
			c.logCommittedDone = true

			if c.explicitAcks {
				c.sendAcks() // Notify connector of commit.
			}

		// We're not currently reading from `respCh` and the runtime went away.
		case <-maybeDoneCh:
			return ctx.Err()

		// There's a ready connector Response.
		case rx, ok := <-maybeRespCh:
			if !ok {
				respCh = nil // Don't select again.
				continue
			} else if rx.Error != nil {
				__out = rx.Error
				continue
			} else if err := rx.Validate(); err != nil {
				return err
			}

			switch {
			// Connector captured another document.
			case rx.Captured != nil:
				var b = int(rx.Captured.Binding)
				if b >= len(c.next.combiners) {
					return fmt.Errorf("protocol error (binding %d out of range)", b)
				}
				var combiner = c.next.combiners[b]

				if err := combiner.CombineRight(rx.Captured.DocJson); err != nil {
					return fmt.Errorf("combiner.CombineRight: %w", err)
				}
				c.next.numBytes += int(len(rx.Captured.DocJson))
				c.next.pending = true // Mark that we're awaiting a Checkpoint.

			// Connector checkpoint of captured documents.
			case rx.Checkpoint != nil:
				if rx.Checkpoint.State == nil {
					// No-op.
				} else if c.next.merged == nil {
					c.next.merged = rx.Checkpoint.State
				} else if err := c.next.merged.Reduce(*rx.Checkpoint.State); err != nil {
					return fmt.Errorf("reducing driver checkpoint: %w", err)
				}
				c.next.numCheckpoints++
				c.next.pending = false
				c.next.full = c.next.numBytes > combinerByteThreshold

			default:
				return fmt.Errorf("read unexpected response: %v", rx)
			}
		}
	}

	return
}

// PopTransaction returns the Combiners and DriverCheckpoint of a transaction
// which is ready to commit. It's safe to call only after a commit callback
// notification from Serve(), and must be called exactly once prior to
// SetLogCommitOp(). The caller is responsible for fully draining the combiners.
func (c *Client) PopTransaction() ([]pf.Combiner, *pf.ConnectorState) {
	if c.prior.popped {
		panic("PopTransaction was called more than once")
	}
	c.prior.popped = true

	return c.prior.combiners, c.prior.merged
}

// SetLogCommitOp tells the Client of a future recovery log commit operation
// which will commit a transaction previously started via a Serve() callback.
func (c *Client) SetLogCommitOp(op client.OpFuture) error {
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

func (c *Client) sendAcks() {
	// Notify the driver of the commit.
	for i := 0; i != c.prior.numCheckpoints; i++ {
		// We ignore a failure to send Acknowledge for two reasons:
		// * The server controls stream shutdown. It could have gracefully closed
		//   the stream already, and we have no way of knowing that here.
		// * Send errors are only ever nil or EOF.
		//   If it's EOF then a read of the stream will return a more descriptive error.
		_ = c.rpc.Send(&Request{Acknowledge: &Request_Acknowledge{}})
	}
}

// combinerByteThreshold is a coarse target on the documents which can be
// optimistically combined within a capture transaction, while awaiting
// the commit of a previous transaction. Upon reaching this threshold,
// further documents and checkpoints will not be folded into the
// transaction.
var combinerByteThreshold = (1 << 25) // 32MB.

// responseError is a channel-oriented wrapper of Response.
type responseOrError struct {
	*Response
	Error error
}

// ResponseChannel spawns a goroutine which receives
// from the stream and sends responses into the returned channel,
// which is closed after the first encountered read error.
func readResponses(stream Connector_CaptureClient, ch chan<- responseOrError) {
	for {
		// Use Recv because ownership of |m| is transferred to |ch|,
		// and |m| cannot be reused.
		var m, err = stream.Recv()

		if err == nil {
			ch <- responseOrError{Response: m}
			continue
		}
		err = pf.UnwrapGRPCError(err)

		if err == context.Canceled {
			err = io.EOF // Treat as EOF.
		}

		ch <- responseOrError{Error: err}
		close(ch)
		return
	}
}
