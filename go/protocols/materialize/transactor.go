package materialize

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
)

// Transactor is a store-agnostic interface for a materialization driver
// that implements Flow materialization protocol transactions.
type Transactor interface {
	// Load implements the transaction load phase by consuming Load requests
	// from the LoadIterator and calling the provided `loaded` callback.
	// Load can ignore keys which are not found in the store, and it may
	// defer calls to `loaded` for as long as it wishes, so long as `loaded`
	// is called for every found document prior to returning.
	//
	// If this Transactor chooses to uses concurrency in StartCommit, note
	// that Load may be called while the OpFuture returned by StartCommit
	// is still running. However, absent an error, LoadIterator.Next() will
	// not return false until that OpFuture has resolved.
	//
	// Typically a Transactor that chooses to use concurrency should "stage"
	// loads for later evaluation, and then evaluate all loads upon that
	// commit resolving, or even wait until Next() returns false.
	//
	// Waiting for the prior commit ensures that evaluated loads reflect the
	// updates of that prior transaction, and thus meet the formal "read-committed"
	// guarantee required by the runtime.
	Load(_ *LoadIterator, loaded func(binding int, doc json.RawMessage) error) error
	// Store consumes Store requests from the StoreIterator.
	Store(*StoreIterator) error
	// StartCommit begins to commit the transaction. Upon its return a commit
	// operation may still be running in the background, and the returned
	// OpFuture must resolve with its completion.
	// (Upon its resolution, Acknowledged will be sent to the Runtime).
	//
	// # When using the "Remote Store is Authoritative" pattern:
	//
	// StartCommit must include `runtimeCheckpoint` within its endpoint
	// transaction and either immediately or asynchronously commit.
	// If the Transactor commits synchronously, it may return a nil OpFuture.
	//
	// # When using the "Recovery Log is Authoritative with Idempotent Apply" pattern:
	//
	// StartCommit must return a DriverCheckpoint which encodes the staged
	// application. It must begin an asynchronous application of this staged
	// update, returning its OpFuture.
	//
	// That async application MUST await a future call to RuntimeCommitted before
	// taking action, however, to ensure that the DriverCheckpoint returned by
	// StartCommit has been durably committed to the runtime recovery log.
	//
	// Note it's possible that the DriverCheckpoint may commit to the log,
	// but then the runtime or this Transactor may crash before the application
	// is able to complete. For this reason, on initialization a Transactor must
	// take care to (re-)apply a staged update in the opened DriverCheckpoint.
	StartCommit(_ context.Context, runtimeCheckpoint []byte) (*pf.DriverCheckpoint, pf.OpFuture, error)
	// RuntimeCommitted is called after StartCommit, upon the runtime completing
	// its commit to its recovery log.
	//
	// Most Transactors can ignore this signal, but those using the
	// "Recovery Log is Authoritative with Idempotent Apply" should use it
	// to unblock an apply operation initiated by StartCommit,
	// which may only now proceed.
	RuntimeCommitted(context.Context) error
	// Destroy the Transactor, releasing any held resources.
	Destroy()
}

// RunTransactions processes materialization protocol transactions
// over the established stream against a Driver.
func RunTransactions(
	stream Driver_TransactionsServer,
	newTransactor func(context.Context, TransactionRequest_Open) (Transactor, *TransactionResponse_Opened, error),
) (_err error) {

	var rxRequest, err = ReadOpen(stream)
	if err != nil {
		return err
	}
	transactor, opened, err := newTransactor(stream.Context(), *rxRequest.Open)
	if err != nil {
		return err
	}

	defer func() {
		if _err != nil {
			logrus.WithError(_err).Error("RunTransactions failed")
		} else {
			logrus.Debug("RunTransactions finished")
		}
		transactor.Destroy()
	}()

	txResponse, err := WriteOpened(stream, opened)
	if err != nil {
		return err
	}

	var (
		// awaitErr is the last await() result,
		// and is readable upon its close of its parameter `awaitDoneCh`.
		awaitErr error
		// loadErr is the last loadAll() result,
		// and is readable upon its close of its parameter `loadDoneCh`.
		loadErr error
	)

	// await is a closure which awaits the completion of a previously
	// started commit, and then writes Acknowledged to the runtime.
	// It has an exclusive ability to write to `stream` until it returns.
	var await = func(
		round int,
		commitOp pf.OpFuture, // Resolves when the prior commit completes.
		awaitDoneCh chan<- struct{}, // To be closed upon return.
		loadDoneCh <-chan struct{}, // Signaled when load() has completed.
	) (__out error) {

		defer func() {
			logrus.WithFields(logrus.Fields{
				"round": round,
				"error": __out,
			}).Debug("await commit finished")

			awaitErr = __out
			close(awaitDoneCh)
		}()

		// Wait for commit to complete, with cancellation checks.
		select {
		case <-commitOp.Done():
			if err := commitOp.Err(); err != nil {
				return err
			}
		case <-loadDoneCh:
			// load() must have error'd, as it otherwise cannot
			// complete until we send Acknowledged.
			return nil
		}

		return WriteAcknowledged(stream, &txResponse)
	}

	// load is a closure for async execution of Transactor.Load.
	var load = func(
		round int,
		it *LoadIterator,
		awaitDoneCh <-chan struct{}, // Signaled when await() has completed.
		loadDoneCh chan<- struct{}, // To be closed upon return.
	) (__out error) {

		var loaded int
		defer func() {
			logrus.WithFields(logrus.Fields{
				"round":  round,
				"total":  it.total,
				"loaded": loaded,
				"error":  __out,
			}).Debug("load finished")

			loadErr = __out
			close(loadDoneCh)
		}()

		var err = transactor.Load(it, func(binding int, doc json.RawMessage) error {
			if awaitDoneCh != nil {
				// Wait for await() to complete and then clear our local copy of its channel.
				_, awaitDoneCh = <-awaitDoneCh, nil
			}
			if awaitErr != nil {
				// We cannot write a Loaded response if await() failed, as it would
				// be an out-of-order response (a protocol violation). Bail out.
				return context.Canceled
			}

			loaded++
			return WriteLoaded(stream, &txResponse, binding, doc)
		})

		if awaitDoneCh == nil && awaitErr != nil {
			return nil // Cancelled by await() error.
		} else if it.err != nil {
			// Prefer the iterator's error over `err` as it's earlier in the chain
			// of dependency and is likely causal of (or equal to) `err`.
			return it.err
		}
		return err
	}

	// commitOp is a future for the most-recent started commit.
	var commitOp pf.OpFuture = client.FinishedOperation(nil)

	for round := 0; true; round++ {
		var (
			awaitDoneCh = make(chan struct{}) // Signals await() is done.
			loadDoneCh  = make(chan struct{}) // Signals load() is done.
			loadIt      = LoadIterator{stream: stream, request: &rxRequest}
		)

		if err = ReadAcknowledge(stream, &rxRequest); err != nil {
			return err
		} else if round == 0 {
			// Suppress explicit Acknowledge of the opened commit.
			// newTransactor() is expected to have already taken any required
			// action to apply this commit to the store (where applicable).
		} else if err = transactor.RuntimeCommitted(stream.Context()); err != nil {
			return fmt.Errorf("transactor.RuntimeCommitted: %w", err)
		}

		// Await the commit of the prior transaction, then notify the runtime.
		// On completion, Acknowledged has been written to the stream,
		// and a concurrent load() phase may now begin to close.
		// At exit, `awaitDoneCh` is closed and `awaitErr` is its status.
		go await(round, commitOp, awaitDoneCh, loadDoneCh)

		// Begin an async load of the current transaction.
		// At exit, `loadDoneCh` is closed and `loadErr` is its status.
		go load(round, &loadIt, awaitDoneCh, loadDoneCh)

		// Join over await() and load().
		for awaitDoneCh != nil || loadDoneCh != nil {
			select {
			case <-awaitDoneCh:
				if awaitErr != nil {
					return fmt.Errorf("commit failed: %w", awaitErr)
				}
				awaitDoneCh = nil
			case <-loadDoneCh:
				if loadErr != nil && loadErr != io.EOF {
					return fmt.Errorf("transactor.Load: %w", loadErr)
				}
				loadDoneCh = nil
			}
		}

		if loadErr == io.EOF {
			return nil // Graceful shutdown.
		}

		if err = ReadFlush(&rxRequest); err != nil {
			return err
		} else if err = WriteFlushed(stream, &txResponse); err != nil {
			return err
		}
		logrus.WithField("round", round).Debug("wrote Flushed")

		// Process all Store requests until StartCommit is read.
		var storeIt = StoreIterator{stream: stream, request: &rxRequest}
		if err = transactor.Store(&storeIt); storeIt.err != nil {
			err = storeIt.err // Prefer an iterator error as it's more directly causal.
		}
		if err != nil {
			return fmt.Errorf("transactor.Store: %w", err)
		}
		logrus.WithFields(logrus.Fields{"round": round, "stored": storeIt.total}).Debug("Store finished")

		var runtimeCheckpoint []byte
		var driverCheckpoint *pf.DriverCheckpoint

		if runtimeCheckpoint, err = ReadStartCommit(&rxRequest); err != nil {
			return err
		} else if driverCheckpoint, commitOp, err = transactor.StartCommit(stream.Context(), runtimeCheckpoint); err != nil {
			return fmt.Errorf("transactor.StartCommit: %w", err)
		} else if err = WriteStartedCommit(stream, &txResponse, driverCheckpoint); err != nil {
			return err
		}

		// As a convenience, map a nil OpFuture to a pre-resolved one so the
		// rest of our handling can ignore the nil case.
		if commitOp == nil {
			commitOp = client.FinishedOperation(nil)
		}
	}
	panic("not reached")
}
