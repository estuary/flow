package materialize

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	pf "github.com/estuary/protocols/flow"
	"github.com/sirupsen/logrus"
)

// Transactor is a store-agnostic interface for a materialization driver
// that implements Flow materialization protocol transactions.
type Transactor interface {
	// Load implements the transaction load phase by consuming Load requests
	// from the LoadIterator until a Prepare request is read. Requested keys
	// are not known to exist in the store, and very often they won't.
	// Load can ignore keys which are not found in the store. Before Load returns,
	// though, it must ensure loaded() is called for all found documents.
	//
	// Loads of transaction T+1 will be invoked after Store of T+0, but
	// concurrently with the commit and acknowledgement of T+0. This is an
	// important optimization that allows the driver to immediately begin the work
	// of T+1, for instance by staging Load keys into a temporary table which will
	// be joined and queried once the LoadIterator is drained.
	//
	// But, the driver contract is that documents loaded in T+1 must reflect
	// stores of T+0 (a.k.a. "read committed"). The driver must ensure that the
	// prior transaction is fully reflected in the store before attempting to
	// load any keys.
	//
	// Load is given two channels, priorCommittedCh and priorAcknowledgedCh,
	// to help it understand where the prior transaction is in its lifecycle.
	//  - priorCommittedCh selects (is closed) upon the completion of Transactor.Commit.
	//  - priorAcknowledgedCh selects (is closed) upon completion of Transactor.Acknowledge.
	//
	// * In the simple case where the driver uses implementation pattern
	//   "Recovery Log with Non-Transactional Store", the driver is free
	//   to simply ignore these signals. It may issue lookups to the store and call
	//   back to loaded() when those lookups succeed.
	//
	// * If the driver uses implementation pattern "Remote Store is Authoritative",
	//   it MUST await priorCommittedCh before loading from the store to ensure
	//   read-committed behavior (priorAcknowledgedCh is ignored).
	//   For ease of implementation, it MAY delay reading from the LoadIterator
	//   until priorCommittedCh is signaled to ensure this behavior.
	//
	//   After priorCommittedCh it MAY immediately evaluate loads against the store,
	//   calling back to loaded().
	//
	//   For simplicity it may wish to stage all loads, draining the LoadIterator,
	//   and only then evaluate staged keys against the store. In this case it
	//   can ignore priorCommittedCh, as the LoadIterator will drain only after
	//   it has been signaled.
	//
	// * If the driver uses pattern "Recovery Log with Idempotent Apply",
	//   it MUST await priorAcknowledgedCh before loading from the store,
	//   which indicates that Acknowledge has completed and applied the prior
	//   commit to the store.
	//
	//   The LoadIterator needs to make progress in order for acknowledgement
	//   to occur, and the driver MUST read from LoadIterator and stage those
	//   loads for future evaluation once priorAcknowledgedCh is signaled.
	//   Otherwise the transaction will deadlock.
	//
	//   After priorAcknowledgedCh it MAY immediately evaluate loads against the
	//   store, calling back to loaded().
	//
	//   For simplicity it may wish to stage all loads, draining the LoadIterator,
	//   and only then evaluate staged keys against the store. In this case it
	//   can ignore priorAcknowledgedCh, as the LoadIterator will drain only after
	//   it has been signaled.
	//
	Load(
		it *LoadIterator,
		priorCommittedCh <-chan struct{},
		priorAcknowledgedCh <-chan struct{},
		loaded func(binding int, doc json.RawMessage) error,
	) error
	// Prepare begins the transaction store phase.
	//
	// If the remote store is authoritative, the driver must stage the
	// request's Flow checkpoint for its future driver commit.
	//
	// If the recovery log is authoritative, the driver may wish to provide a
	// driver update which will be included in the log's commit. At this stage
	// the transaction hasn't stored any documents yet, so the driver checkpoint
	// may want to include what the driver _plans_ to do.
	Prepare(context.Context, TransactionRequest_Prepare) (pf.DriverCheckpoint, error)
	// Store consumes Store requests from the StoreIterator.
	Store(*StoreIterator) error
	// Commit is called upon the Flow runtime's request that the driver commit
	// its transaction. Commit does so, and returns a final error status.
	// If the driver doesn't use store transactions, Commit may be a no-op.
	//
	// Note that Commit runs concurrently with Transactor.Load().
	Commit(context.Context) error
	// Acknowledge is called upon the Flow runtime's acknowledgement of its
	// recovery log commit. If the driver stages data for application after
	// commit, it must perform that apply now and return a final error status.
	//
	// Note that Acknowledge may be called multiple times in acknowledgement
	// of a single actual commit. The driver must account for this. If it applies
	// staged data as part of acknowledgement, it must ensure that apply is
	// idempotent.
	//
	// Note that Acknowledge runs concurrently with Transactor.Load().
	Acknowledge(context.Context) error
	// Destroy the Transactor, releasing any held resources.
	Destroy()
}

// RunTransactions processes materialization protocol transactions
// over the established stream against a Driver.
func RunTransactions(
	stream Driver_TransactionsServer,
	transactor Transactor,
	log *logrus.Entry,
) (_err error) {

	defer func() {
		if _err != nil {
			log.WithError(_err).Error("RunTransactions failed")
		} else {
			log.Debug("RunTransactions finished")
		}
		transactor.Destroy()
	}()

	var (
		ctx = stream.Context()
		// tx is an exclusively-owned capability to stage and write
		// TransactionResponses into the |stream|.
		tx struct {
			response   *TransactionResponse // In-progress response.
			sync.Mutex                      // Guards |response| and stream.Send.
		}
		// commitAckErr is the last doCommitAck() result,
		// and is readable upon its close of its parameter |ackCh|.
		commitAckErr error
		// loadErr is the last doLoad() result,
		// and is readable upon its close of its parameter |loadCh|.
		loadErr error
	)

	// doCommitAck is a closure for async execution of Transactor.Commit and Acknowledge.
	// It requires that it owns a lock on |tx| at invocation start.
	var doCommitAck = func(
		round int,
		reqAckCh <-chan struct{}, // Signaled when Acknowledge is recieved.
		commitCh chan<- struct{}, // To be closed when DriverCommitted is done.
		ackCh chan<- struct{}, // To be closed when Acknowledged is done.
	) (__out error) {

		defer func() {
			log.WithFields(logrus.Fields{
				"round": round,
				"error": __out,
			}).Debug("doCommitAck finished")

			commitAckErr = __out

			if commitCh != nil {
				tx.Unlock() // We returned before Unlock. Do it now.
				close(commitCh)
			}
			close(ackCh)
		}()

		if round == 0 {
			// Nothing to commit in the first round.
		} else if err := transactor.Commit(ctx); err != nil {
			return fmt.Errorf("transactor.Commit: %w", err)
		} else if err := WriteDriverCommitted(stream, &tx.response); err != nil {
			return fmt.Errorf("WriteDriverCommitted: %w", err)
		} else {
			log.Debug("Commit finished")
		}

		// A concurrent Transactor.Load may now send Loaded responses.
		tx.Unlock()
		close(commitCh)
		commitCh = nil

		// Wait to read TransactionRequest.Acknowledge.
		select {
		case <-reqAckCh:
		case <-ctx.Done():
			return
		}

		if err := transactor.Acknowledge(ctx); err != nil {
			return fmt.Errorf("Acknowledge: %w", err)
		}

		// Writing Acknowledged may race writes of Loaded responses.
		tx.Lock()
		var err = WriteAcknowledged(stream, &tx.response)
		tx.Unlock()

		if err != nil {
			return fmt.Errorf("writing Acknowledged: %w", err)
		} else {
			log.Debug("Acknowledge finished")
		}

		return nil
	}

	// doLoad is a closure for async execution of Transactor.Load.
	var doLoad = func(
		round int,
		it *LoadIterator,
		commitCh <-chan struct{}, // Signaled when DriverCommitted is done.
		ackCh <-chan struct{}, // Signaled when Acknowledged is done.
		loadCh chan<- struct{}, // To be closed when Load is done.
	) (__out error) {

		var loaded int
		defer func() {
			log.WithFields(logrus.Fields{
				"round":  round,
				"total":  it.total,
				"loaded": loaded,
				"error":  __out,
			}).Debug("doLoad finished")

			loadErr = __out
			close(loadCh)
		}()

		// Process all Load requests until Prepare is read.
		var loadErr = transactor.Load(it, commitCh, ackCh, func(binding int, doc json.RawMessage) error {
			loaded++

			tx.Lock()
			defer tx.Unlock()

			return StageLoaded(stream, &tx.response, binding, doc)
		})

		// Prefer the iterator's error over |loadErr|, as it's earlier in the chain
		// of dependency and is likely causal of (or equal to) |loadErr|.
		if it.err != nil {
			return it.err
		}
		return loadErr
	}

	for round := 0; true; round++ {
		var (
			loadCh   = make(chan struct{}) // Signals Load() is done.
			commitCh = make(chan struct{}) // Signals Commit() is done.
			reqAckCh = make(chan struct{}) // Signals a TransactionRequest.Acknowledge was received.
			ackCh    = make(chan struct{}) // Signals Commit() is done.
			loadIt   = NewLoadIterator(stream, reqAckCh)
		)

		tx.Lock() // doCommitAck expects to own |tx| at its invocation.

		// Begin async commit and acknowledgement of the prior transaction.
		// On completion, |commitCh| and |ackCh| are closed and |commitAckErr| is its status.
		go doCommitAck(round, reqAckCh, commitCh, ackCh)

		// Begin an async load of the current transaction.
		// On completion, |loadCh| is closed and |loadErr| is its status.
		go doLoad(round, loadIt, commitCh, ackCh, loadCh)

		// Join over both tasks.
		for ackCh != nil || loadCh != nil {
			select {
			case <-ackCh:
				if commitAckErr != nil {
					// Bail out to cancel ongoing Load.
					return fmt.Errorf("prior transaction: %w", commitAckErr)
				}
				ackCh = nil
			case <-loadCh:
				if loadErr != nil && loadErr != io.EOF {
					// Bail out to cancel ongoing Commit & Acknowledge.
					return fmt.Errorf("Load: %w", loadErr)
				}
				loadCh = nil
			}
		}

		if loadErr == io.EOF {
			return nil // Graceful shutdown.
		}

		// Prepare, then respond with Prepared.
		if prepared, err := transactor.Prepare(ctx, *loadIt.req.Prepare); err != nil {
			return err
		} else if err = WritePrepared(stream, &tx.response, prepared); err != nil {
			return err
		}
		log.WithField("round", round).Debug("wrote Prepared")

		// Process all Store requests until Commit is read.
		var storeIt = NewStoreIterator(stream)
		if storeIt.poll() {
			if err := transactor.Store(storeIt); err != nil {
				return err
			}
		}
		log.WithFields(logrus.Fields{"round": round, "store": storeIt.total}).Debug("Store finished")

		if storeIt.Err() != nil {
			return storeIt.Err()
		}
	}
	panic("not reached")
}
