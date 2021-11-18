package capture

import (
	"context"
	"encoding/json"
	"fmt"

	pf "github.com/estuary/protocols/flow"
)

// PullClient is a client of a driver's Pull RPC. It provides a high-level
// API for executing the pull-based/ capture transaction workflow.
type PullClient struct {
	coordinator
	// rpc is the long-lived Pull RPC, and is accessed only from Serve.
	rpc Driver_PullClient
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

	var out = &PullClient{
		coordinator: coordinator,
		rpc:         rpc,
	}

	rpc = nil // Don't run deferred CloseSend.
	return out, nil
}

// Serve is a long-lived routine which processes transactions from the Pull RPC.
// When captured documents are ready to commit, it invokes the startCommitFn
// callback.
//
// On callback, the caller must drain documents from Combiners() and track
// the associated DriverCheckpoint(), and then notify the PullClient of a
// pending commit via SetLogCommittedOp().
//
// While this drain and commit is ongoing, Serve() will accumulate further
// captured documents and checkpoints. It will then notify the caller of
// the next transaction only after the resolution of the prior transaction's
// commit.
//
// Serve will call into startCommitFn with a non-nil error exactly once,
// as its very last invocation.
func (c *PullClient) Serve(startCommitFn func(error)) {
	var respCh = PullResponseChannel(c.rpc)

	c.loop(startCommitFn,
		func() (drained bool, err error) {
			select {
			case <-c.maybeLogCommittedOp():
				if err = c.onLogCommitted(); err != nil {
					return false, fmt.Errorf("onLogCommitted: %w", err)
				}
				c.sendAck()

			case op := <-c.logCommittedOpCh:
				if err := c.onLogCommittedOpCh(op); err != nil {
					return false, fmt.Errorf("onLogCommittedOpCh: %w", err)
				}

			case rx, ok := <-respCh:
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
