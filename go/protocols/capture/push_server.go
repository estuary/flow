package capture

import (
	"context"
	fmt "fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
)

// PullServer is a server which aides implementations of the Runtime.Push RPC.
type PushServer struct {
	coordinator
	extractors        []pf.Extractor
	ctx               context.Context         // Context of Serve's lifetime.
	pushCh            chan readyPush          // Sent to from Push.
	priorAck, nextAck []client.AsyncOperation // Notifications for awaiting RPCs.
}

// NewPushServer builds a new *PushServer using the provided CaptureSpec.
func NewPushServer(
	ctx context.Context,
	newCombinerFn func(*pf.CaptureSpec_Binding) (pf.Combiner, error),
	newExtractorFn func(*pf.CaptureSpec_Binding) (pf.Extractor, error),
	range_ pf.RangeSpec,
	spec *pf.CaptureSpec,
	version string,
	startCommitFn func(error),
) (*PushServer, error) {

	var coordinator, err = newCoordinator(newCombinerFn, range_, spec, version)
	if err != nil {
		return nil, err
	}

	var extractors []pf.Extractor
	for _, b := range spec.Bindings {
		var extractor, extractor_err = newExtractorFn(b)
		if extractor_err != nil {
			return nil, fmt.Errorf("creating %s extractor: %w", b.Collection.Collection, extractor_err)
		}
		// assuming that bindings are in order...
		extractors = append(extractors, extractor)
	}

	var out = &PushServer{
		coordinator: coordinator,
		extractors:  extractors,
		ctx:         ctx,
		pushCh:      make(chan readyPush),
	}
	go out.serve(startCommitFn)

	return out, nil
}

// Push Documents and an accompanying DriverCheckpoint into the capture.
// Push returns an error if the Serve loop isn't running.
// Otherwise, Push returns immediately and |ackCh| will be signaled one
// time when the Push has fully committed.
// The caller must also monitor ServeOp to determine if the Serve loop
// has exited, in which case |achCh| will never be notified.
func (c *PushServer) Push(
	docs []Documents,
	checkpoint pf.DriverCheckpoint,
	ackCh *client.AsyncOperation,
) error {
	select {
	case c.pushCh <- readyPush{
		docs:       docs,
		checkpoint: checkpoint,
		ackCh:      ackCh,
	}:
		return nil

	case <-c.loopOp.Done():
		return c.loopOp.Err()
	}
}

// ServeOp returns the Serve loop future of this PushServer.
// It resolves with its terminal error when the Serve loop has stopped running.
// An error of io.EOF is expected upon a graceful cancellation.
func (c *PushServer) ServeOp() client.OpFuture { return c.loopOp }

// readyPush is a Push that traverses PushServer.readyCh.
type readyPush struct {
	docs       []Documents
	checkpoint pf.DriverCheckpoint
	ackCh      *client.AsyncOperation
}

// serve is a long-lived routine which processes Push transactions.
// When captured documents are ready to commit, it invokes the startCommitFn
// callback.
//
// On callback, the caller must drain documents from Combiners() and track
// the associated DriverCheckpoint(), and then notify the PushServer of a
// pending commit via SetLogCommittedOp().
//
// While this drain and commit is ongoing, serve() will accumulate further
// pushed documents and checkpoints. It will then notify the caller of
// the next transaction only after the resolution of the prior transaction's
// commit.
//
// serve will call into startCommitFn with a non-nil error exactly once,
// as its very last invocation.
func (c *PushServer) serve(startCommitFn func(error)) {
	var doneCh = c.ctx.Done()

	c.loop(
		func(err error) {
			if err == nil {
				c.priorAck, c.nextAck = c.nextAck, c.priorAck[:0]
			}
			startCommitFn(err)
		},
		func(full bool) (drained bool, err error) {
			var maybePushCh <-chan readyPush
			if !full {
				maybePushCh = c.pushCh
			}

			select {
			case <-doneCh:
				doneCh = nil // Don't select again.
				return true, nil

			case <-c.maybeLogCommittedOp():
				if err = c.onLogCommitted(); err != nil {
					return false, fmt.Errorf("onLogCommitted: %w", err)
				}
				c.sendAck()

			case op := <-c.logCommittedOpCh:
				if err := c.onLogCommittedOpCh(op); err != nil {
					return false, fmt.Errorf("onLogCommittedOpCh: %w", err)
				}

			case rx := <-maybePushCh:
				for _, docs := range rx.docs {
					for _, doc_slice := range docs.DocsJson {
						var doc = docs.Arena.Bytes(doc_slice)
						var extractor = c.extractors[docs.Binding]

						extractor.Document(doc)
						var _, _, validation_err = extractor.Extract()
						if validation_err != nil {
							return false, fmt.Errorf("document validation error: %w", validation_err)
						}
					}
					logrus.Info(fmt.Sprintf("Successfully validated %d document", len(docs.DocsJson)))
					if err := c.onDocuments(docs); err != nil {
						return false, fmt.Errorf("onDocuments: %w", err)
					}
				}
				if err := c.onCheckpoint(rx.checkpoint); err != nil {
					return false, fmt.Errorf("onCheckpoint: %w", err)
				}
				c.nextAck = append(c.nextAck, *rx.ackCh)
			}

			return doneCh == nil, nil
		})
}

func (c *PushServer) sendAck() {
	for _, ch := range c.priorAck {
		ch.Resolve(nil)
	}
	c.priorAck = c.priorAck[:0]
}
