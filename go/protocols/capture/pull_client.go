package capture

import (
	"context"
	"encoding/json"
	"fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
)

// PullClient is a client of a driver's Pull RPC. It provides a high-level
// API for executing the pull-based/ capture transaction workflow.
type PullClient struct {
	coordinator
	// rpc is the long-lived Pull RPC, and is accessed only from Serve.
	rpc Driver_PullClient
	// Should Acknowledgements be sent?
	explicitAcks bool
}

// OpenPull opens a Pull RPC using the provided DriverClient and CaptureSpec.
func OpenPull(
	ctx context.Context,
	driver DriverClient,
	driverCheckpoint json.RawMessage,
	newCombinerFn func(*pf.CaptureSpec_Binding) (pf.Combiner, error),
	range_ pf.RangeSpec,
	spec *pf.CaptureSpec,
	version string,
	tail bool,
	startCommitFn func(error),
) (*PullClient, error) {

	var coordinator, err = newCoordinator(newCombinerFn, range_, spec, version)
	if err != nil {
		return nil, err
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

	// We will send no more input into the RPC.
	if !opened.Opened.ExplicitAcknowledgements {
		_ = rpc.CloseSend()
	}

	var out = &PullClient{
		coordinator:  coordinator,
		rpc:          rpc,
		explicitAcks: opened.Opened.ExplicitAcknowledgements,
	}
	go out.serve(ctx, startCommitFn)

	rpc = nil // Don't run deferred CloseSend.
	return out, nil
}

// serve is a long-lived routine which processes transactions from the Pull RPC.
// When captured documents are ready to commit, it invokes the startCommitFn
// callback.
//
// On callback, the caller must drain documents from Combiners() and track
// the associated DriverCheckpoint(), and then notify the PullClient of a
// pending commit via SetLogCommittedOp().
//
// While this drain and commit is ongoing, serve() will accumulate further
// captured documents and checkpoints. It will then notify the caller of
// the next transaction only after the resolution of the prior transaction's
// commit.
//
// serve will call into startCommitFn with a non-nil error exactly once,
// as its very last invocation.
func (c *PullClient) serve(ctx context.Context, startCommitFn func(error)) {
	if c.explicitAcks {
		defer c.rpc.CloseSend()
	}
	var respCh = PullResponseChannel(c.rpc)

	var onResp = func(rx PullResponseError, ok bool) (drained bool, err error) {
		if !ok {
			respCh = nil // Don't select again.
			return true, nil
		} else if rx.Error != nil {
			return false, rx.Error
		} else if err := rx.Validate(); err != nil {
			return false, err
		}

		switch {
		case rx.Documents != nil:
			if err := c.onDocuments(*rx.Documents); err != nil {
				return false, fmt.Errorf("onDocuments: %w", err)
			}
		case rx.Checkpoint != nil:
			if err := c.onCheckpoint(*rx.Checkpoint); err != nil {
				return false, fmt.Errorf("onCheckpoint: %w", err)
			}
		default:
			return false, fmt.Errorf("read unexpected response: %v", rx)
		}

		return respCh == nil, nil
	}

	c.loop(startCommitFn,
		func(full bool) (drained bool, err error) {
			var maybeRespCh <-chan PullResponseError

			// If we're not full, prefer to include more
			// ready responses in the current transaction.
			if !full {
				select {
				case rx, ok := <-respCh:
					return onResp(rx, ok)
				default:
					maybeRespCh = respCh
				}
			}

			// We prefer to gracefully `respCh` to close, and then drain any
			// final transaction. But, if not reading from `respCh`, we *must*
			// monitor for context cancellation as there's no guarantee that
			// log commit operations will resolve (the client may have gone away).
			var maybeDoneCh <-chan struct{}
			if maybeRespCh == nil {
				maybeDoneCh = ctx.Done()
			}

			// We don't have a ready response.
			// Block for a response OR a commit operation.
			select {
			case <-c.maybeLogCommittedOp():
				if err = c.onLogCommitted(); err != nil {
					return false, fmt.Errorf("onLogCommitted: %w", err)
				}
				if c.explicitAcks {
					c.sendAck()
				}

			case op := <-c.logCommittedOpCh:
				if err := c.onLogCommittedOpCh(op); err != nil {
					return false, fmt.Errorf("onLogCommittedOpCh: %w", err)
				}

			case rx, ok := <-maybeRespCh:
				return onResp(rx, ok)

			case <-maybeDoneCh:
				return false, ctx.Err()
			}

			return respCh == nil, nil
		})
}

func (c *PullClient) sendAck() {
	// Notify the driver of the commit.
	for i := 0; i != c.prior.numCheckpoints; i++ {
		// We ignore a failure to send Acknowledge for two reasons:
		// * The server controls stream shutdown. It could have gracefully closed
		//   the stream already, and we have no way of knowing that here.
		// * Send errors are only ever nil or EOF.
		//   If it's EOF then a read of the stream will return a more descriptive error.
		_ = c.rpc.Send(&PullRequest{Acknowledge: &Acknowledge{}})
	}
}
