package materialize

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	"github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
)

// TxnClient is a client of a driver's Transactions RPC.
type TxnClient struct {
	// Opened response returned by the server while opening.
	opened *TransactionResponse_Opened
	// rx organizes receive state of the TxnClient.
	// Fields are exclusively accessed from readLoop, with the exception of
	// |loopOp| which is inspected for loop status.
	// Fields are guarded by the common mutex, and are accessed
	rx struct {
		commitOps   CommitOps                       // CommitOps being evaluated.
		commitOpsCh <-chan CommitOps                // Reads from StartCommit().
		loopOp      *client.AsyncOperation          // Read loop exit status.
		preparedCh  chan<- pf.DriverCheckpoint      // Sends into Prepare().
		respCh      <-chan TransactionResponseError // Receive from the driver.
		state       txnClientState                  // Receive state (one of rx*).
	}
	// shared state mutated by both transmission and receive.
	shared struct {
		// Combiners of the materialization, one for each binding.
		combiners []pf.Combiner
		// Flighted keys of the current transaction for each binding, plus a bounded number of
		// retained fully-reduced documents of the last transaction.
		flighted   []map[string]json.RawMessage
		sync.Mutex // Guards shared state.
	}
	// tx organizes transmission state of the TxnClient.
	// Fields are guarded by the common mutex, and are accessed and mutated
	// via synchronous public methods of the TxnClient interface.
	tx struct {
		client      Driver_TransactionsClient  // Used to send (never receive).
		commitOpsCh chan<- CommitOps           // Sends from StartCommit().
		preparedCh  <-chan pf.DriverCheckpoint // Reads into Prepare()
		staged      *TransactionRequest        // Staged request to be sent to |client|.
		state       txnClientState             // Transmission state (one of tx*).
		sync.Mutex                             // Guards all transmission state.
	}
	// Specification of this Transactions client.
	spec *pf.MaterializationSpec
	// Version of the client's MaterializationSpec.
	version string
}

// CommitOps are operations which coordinate the mutual commit of a
// transaction between the Flow runtime and materialization driver.
type CommitOps struct {
	// DriverCommitted resolves on reading DriverCommitted of the prior transaction.
	// This operation is a depencency of a staged recovery log write, and its
	// resolution allows the recovery log commit to proceed.
	// Nil if there isn't an ongoing driver commit.
	DriverCommitted *client.AsyncOperation
	// LogCommitted resolves on the prior transactions's commit to the recovery log.
	// When resolved, the TxnClient notifies the driver by sending Acknowledge.
	// Nil if there isn't an ongoing recovery log commit.
	LogCommitted client.OpFuture
	// Acknowledged resolves on reading Acknowledged from the driver, and completes
	// the transaction lifecycle. Once resolved (and not before), a current and
	// concurrent may begin to commit (by sending Prepared).
	Acknowledged *client.AsyncOperation
}

type txnClientState int

const (
	// We've sent Commit, and may send Load.
	txLoadCommit txnClientState = iota
	// We've sent Acknowledge, and may send Load.
	txLoadAcknowledge txnClientState = iota
	// We've sent Prepare, and must Commit next.
	txPrepare txnClientState = iota
	// We've received Prepared, and are about to commit.
	rxPrepared txnClientState = iota
	// We've received CommitOps, and await DriverCommitted.
	rxPendingCommit txnClientState = iota
	// We've received DriverCommitted, and await Acknowledged & Loaded.
	rxDriverCommitted txnClientState = iota
	// We've received Acknowledged, and await Loaded & Prepared.
	rxAcknowledged txnClientState = iota
)

// OpenTransactions opens a Transactions RPC.
// It returns a *TxnClient which provides a high-level API for executing
// the materialization transaction workflow.
func OpenTransactions(
	ctx context.Context,
	driver DriverClient,
	driverCheckpoint json.RawMessage,
	newCombinerFn func(*pf.MaterializationSpec_Binding) (pf.Combiner, error),
	range_ pf.RangeSpec,
	spec *pf.MaterializationSpec,
	version string,
) (*TxnClient, error) {

	if range_.RClockBegin != 0 || range_.RClockEnd != math.MaxUint32 {
		return nil, fmt.Errorf("materializations cannot split on r-clock: " + range_.String())
	}

	var combiners []pf.Combiner
	var flighted []map[string]json.RawMessage
	var txStaged *TransactionRequest

	for _, b := range spec.Bindings {
		var combiner, err = newCombinerFn(b)
		if err != nil {
			return nil, fmt.Errorf("creating %s combiner: %w", b.Collection.Collection, err)
		}
		combiners = append(combiners, combiner)
		flighted = append(flighted, make(map[string]json.RawMessage))
	}

	rpc, err := driver.Transactions(ctx)
	if err != nil {
		return nil, fmt.Errorf("driver.Transactions: %w", err)
	}
	// Close RPC if remaining initialization fails.
	defer func() {
		if rpc != nil {
			_ = rpc.CloseSend()
		}
	}()

	if err = rpc.Send(&TransactionRequest{
		Open: &TransactionRequest_Open{
			Materialization:      spec,
			Version:              version,
			KeyBegin:             range_.KeyBegin,
			KeyEnd:               range_.KeyEnd,
			DriverCheckpointJson: driverCheckpoint,
		},
	}); err != nil {
		return nil, fmt.Errorf("sending Open: %w", err)
	}

	// Read Opened response with driver's optional Flow Checkpoint.
	opened, err := rpc.Recv()
	if err != nil {
		return nil, fmt.Errorf("reading Opened: %w", err)
	} else if opened.Opened == nil {
		return nil, fmt.Errorf("expected Opened, got %#v", opened.String())
	}

	// Write Acknowledge request to re-acknowledge the last commit.
	if err := WriteAcknowledge(rpc, &txStaged); err != nil {
		return nil, err
	}

	// Read Acknowledged response.
	acked, err := rpc.Recv()
	if err != nil {
		return nil, fmt.Errorf("reading Acknowledged: %w", err)
	} else if acked.Acknowledged == nil {
		return nil, fmt.Errorf("expected Acknowledged, got %#v", acked.String())
	}

	var preparedCh = make(chan pf.DriverCheckpoint)
	var commitOpsCh = make(chan CommitOps)

	var out = &TxnClient{
		spec:    spec,
		version: version,
		opened:  opened.Opened,
	}
	out.tx.client = rpc
	out.tx.commitOpsCh = commitOpsCh
	out.tx.preparedCh = preparedCh
	out.tx.staged = txStaged
	out.tx.state = txLoadAcknowledge

	out.rx.commitOps = CommitOps{}
	out.rx.commitOpsCh = commitOpsCh
	out.rx.loopOp = client.NewAsyncOperation()
	out.rx.preparedCh = preparedCh
	out.rx.respCh = TransactionResponseChannel(rpc)
	out.rx.state = rxAcknowledged

	out.shared.combiners = combiners
	out.shared.flighted = flighted

	go out.readLoop()

	rpc = nil // Don't run deferred CloseSend.
	return out, nil
}

// Opened returns the driver's prior Opened response.
func (c *TxnClient) Opened() *TransactionResponse_Opened { return c.opened }

// Close the TxnClient. Close returns an error if the RPC is not in an
// Acknowledged and idle state, or on any other error.
func (f *TxnClient) Close() error {
	f.tx.Lock()
	defer f.tx.Unlock()

	f.tx.client.CloseSend()
	<-f.rx.loopOp.Done()

	for _, c := range f.shared.combiners {
		c.Destroy()
	}

	// EOF is a graceful shutdown.
	if err := f.rx.loopOp.Err(); err != io.EOF {
		return err
	}
	return nil
}

// AddDocument to the current transaction under the given binding and tuple-encoded key.
func (f *TxnClient) AddDocument(binding int, packedKey []byte, doc json.RawMessage) error {
	f.tx.Lock()
	defer f.tx.Unlock()

	switch f.tx.state {
	case txLoadCommit, txLoadAcknowledge:
	default:
		return fmt.Errorf("caller protocol error: AddDocument is invalid in state %v", f.tx.state)
	}

	// Note that combineRight obtains a lock on |f.shared|, but it's not held
	// while we StageLoad to the connector (which could block).
	// This allows for a concurrent handling of a Loaded response.

	if load, err := f.combineRight(binding, packedKey, doc); err != nil {
		return err
	} else if !load {
		// No-op.
	} else if err = StageLoad(f.tx.client, &f.tx.staged, binding, packedKey); err != nil {
		return err
	}

	// f.tx.state is unchanged.
	return nil
}

func (f *TxnClient) combineRight(binding int, packedKey []byte, doc json.RawMessage) (bool, error) {
	f.shared.Lock()
	defer f.shared.Unlock()

	var flighted = f.shared.flighted[binding]
	var combiner = f.shared.combiners[binding]
	var deltaUpdates = f.spec.Bindings[binding].DeltaUpdates
	var load bool

	if doc, ok := flighted[string(packedKey)]; ok && doc == nil {
		// We've already seen this key within this transaction.
	} else if ok {
		// We retained this document from the last transaction.
		if deltaUpdates {
			panic("we shouldn't have retained if deltaUpdates")
		}
		if err := combiner.ReduceLeft(doc); err != nil {
			return false, fmt.Errorf("combiner.ReduceLeft: %w", err)
		}
		flighted[string(packedKey)] = nil // Clear old value & mark as visited.
	} else {
		// This is a novel key.
		load = !deltaUpdates
		flighted[string(packedKey)] = nil // Mark as visited.
	}

	if err := combiner.CombineRight(doc); err != nil {
		return false, fmt.Errorf("combiner.CombineRight: %w", err)
	}

	return load, nil
}

// Prepare to commit with the given Checkpoint.
// Block until the driver's Prepared response is read, with an optional driver
// checkpoint to commit in the Flow recovery log.
func (f *TxnClient) Prepare(flowCheckpoint pf.Checkpoint) (pf.DriverCheckpoint, error) {
	f.tx.Lock()
	defer f.tx.Unlock()

	if f.tx.state != txLoadAcknowledge {
		return pf.DriverCheckpoint{}, fmt.Errorf(
			"client protocol error: SendPrepare is invalid in state %v", f.tx.state)
	}

	if err := WritePrepare(f.tx.client, &f.tx.staged, flowCheckpoint); err != nil {
		return pf.DriverCheckpoint{}, err
	}
	f.tx.state = txPrepare

	// We deliberately hold the |f.tx| lock while awaiting Prepared,
	// because the Prepare => Prepared interaction is synchronous.

	select {
	case prepared := <-f.tx.preparedCh:
		return prepared, nil
	case <-f.rx.loopOp.Done():
		return pf.DriverCheckpoint{}, f.rx.loopOp.Err()
	}
}

// StartCommit of the prepared transaction. The CommitOps must be initialized by the caller.
// The caller must arrange for LogCommitted to be resolved appropriately, after DriverCommitted.
// The *TxnClient will resolve DriverCommitted & Acknowledged.
func (f *TxnClient) StartCommit(ops CommitOps) error {
	f.tx.Lock()
	defer f.tx.Unlock()

	if f.tx.state != txPrepare {
		return fmt.Errorf(
			"client protocol error: StartCommit is invalid in state %v", f.tx.state)
	}

	f.shared.Lock()
	defer f.shared.Unlock()

	// We hold both the |f.tx| and |f.shared| locks during the store phase.
	// The read loop accesses |f.shared| to handled Loaded responses,
	// but those are disallowed at this stage of the protocol.

	// Any remaining flighted keys *not* having `nil` values are retained documents
	// of a prior transaction which were not updated during this one.
	// We garbage collect them here, and achieve the drainBinding() precondition that
	// flighted maps hold only keys of the current transaction with `nil` sentinels.
	for _, flighted := range f.shared.flighted {
		for key, doc := range flighted {
			if doc != nil {
				delete(flighted, key)
			}
		}
	}

	// Drain each binding.
	for i, combiner := range f.shared.combiners {
		if err := drainBinding(
			f.shared.flighted[i],
			combiner,
			f.spec.Bindings[i].DeltaUpdates,
			f.tx.client,
			&f.tx.staged,
			i,
		); err != nil {
			return err
		}
	}

	// Inform read loop of new CommitOps.
	select {
	case f.tx.commitOpsCh <- ops:
	case <-f.rx.loopOp.Done():
		return f.rx.loopOp.Err()
	}

	// Tell the driver to commit.
	if err := WriteCommit(f.tx.client, &f.tx.staged); err != nil {
		return err
	}

	f.tx.state = txLoadCommit
	return nil
}

func (f *TxnClient) onLogCommitted() error {
	if err := f.rx.commitOps.LogCommitted.Err(); err != nil {
		return fmt.Errorf("recovery log commit: %w", err)
	}
	f.rx.commitOps.LogCommitted = nil // Don't receive again.

	// It's technically _possible_ that a deadlock could occur here.
	// It would require either:
	//  a) that the driver isn't reading Load messages at all, which is
	//     explicitly against the API contract, or
	//  b) that both driver and server managed to stuff their channels
	//     at the same time.
	//
	// It's pretty unlikely in practice. If it *does* occur, two solutions:
	//  * Use a TryLock fast-path, and a slow-path that spawns a goroutine
	//    (Mutex.TryLock should be available after Go 1.18+).
	//  * Or, just always spawn a goroutine.
	f.tx.Lock()
	defer f.tx.Unlock()

	if err := WriteAcknowledge(f.tx.client, &f.tx.staged); err != nil {
		return err
	}
	f.tx.state = txLoadAcknowledge
	return nil
}

func (f *TxnClient) readLoop() (__out error) {
	if f.rx.state != rxAcknowledged {
		panic(f.rx.state)
	}

	defer func() {
		f.rx.loopOp.Resolve(__out)

		// These can never succeed, since we're no longer looping.
		if f.rx.commitOps.DriverCommitted != nil {
			f.rx.commitOps.DriverCommitted.Resolve(__out)
		}
		if f.rx.commitOps.Acknowledged != nil {
			f.rx.commitOps.Acknowledged.Resolve(__out)
		}
	}()

	for {
		var maybeLogCommitted <-chan struct{}
		if f.rx.commitOps.LogCommitted != nil {
			maybeLogCommitted = f.rx.commitOps.LogCommitted.Done()
		}

		select {
		case <-maybeLogCommitted:
			if err := f.onLogCommitted(); err != nil {
				return fmt.Errorf("onLogCommitted: %w", err)
			}
			logrus.Debug("read log commit")

		case ops := <-f.rx.commitOpsCh:
			if err := f.onCommitOps(ops); err != nil {
				return fmt.Errorf("onCommitOps: %w", err)
			}
			logrus.Debug("read CommitOps")

		case rx, ok := <-f.rx.respCh:
			if !ok {
				return io.EOF
			} else if rx.Error != nil {
				return rx.Error
			} else if err := rx.Validate(); err != nil {
				return err
			}

			switch {
			case rx.DriverCommitted != nil:
				logrus.Debug("read DriverCommitted")
				if err := f.onDriverCommitted(*rx.DriverCommitted); err != nil {
					return fmt.Errorf("onDriverCommitted: %w", err)
				}
			case rx.Loaded != nil:
				logrus.Debug("read Loaded")
				if err := f.onLoaded(*rx.Loaded); err != nil {
					return fmt.Errorf("onLoaded: %w", err)
				}
			case rx.Acknowledged != nil:
				logrus.Debug("read Acknowledged")
				if err := f.onAcknowledged(*rx.Acknowledged); err != nil {
					return fmt.Errorf("onAcknowledged: %w", err)
				}
			case rx.Prepared != nil:
				logrus.Debug("read Prepared")
				if err := f.onPrepared(*rx.Prepared); err != nil {
					return fmt.Errorf("onPrepared: %w", err)
				}
			default:
				return fmt.Errorf("read unexpected response: %v", rx)
			}
		}
	}
}

func (f *TxnClient) onDriverCommitted(TransactionResponse_DriverCommitted) error {
	if f.rx.state != rxPendingCommit {
		return fmt.Errorf("connector protocol error (DriverCommitted not expected in state %v)", f.rx.state)
	}

	// This future was used as a recovery log write dependency by StartCommit of
	// the last transaction. Resolving it allows that recovery log write to proceed,
	// which we'll observe as a future resolution of fsm.logCommittedOp.
	f.rx.commitOps.DriverCommitted.Resolve(nil)
	f.rx.commitOps.DriverCommitted = nil

	f.rx.state = rxDriverCommitted
	return nil
}

func (f *TxnClient) onLoaded(loaded TransactionResponse_Loaded) error {
	switch f.rx.state {
	case rxDriverCommitted, rxAcknowledged:
		// Pass.
	default:
		return fmt.Errorf("connector protocol error (Loaded not expected in state %v)", f.rx.state)
	}

	f.shared.Lock()
	defer f.shared.Unlock()

	if int(loaded.Binding) > len(f.shared.combiners) {
		return fmt.Errorf("driver error (binding %d out of range)", loaded.Binding)
	}

	// Feed documents into the combiner as reduce-left operations.
	var combiner = f.shared.combiners[loaded.Binding]
	for _, slice := range loaded.DocsJson {
		if err := combiner.ReduceLeft(loaded.Arena.Bytes(slice)); err != nil {
			return fmt.Errorf("combiner.ReduceLeft: %w", err)
		}
	}

	// f.rx.state is unchanged.
	return nil
}

func (f *TxnClient) onAcknowledged(TransactionResponse_Acknowledged) error {
	if f.rx.state != rxDriverCommitted {
		return fmt.Errorf("connector protocol error (Acknowledged not expected in state %v)", f.rx.state)
	}

	// This future was returned by StartCommit of the last transaction.
	// Gazette holds the current transaction open until it resolves.
	f.rx.commitOps.Acknowledged.Resolve(nil)
	f.rx.commitOps.Acknowledged = nil

	f.rx.state = rxAcknowledged
	return nil
}

func (f *TxnClient) onPrepared(prepared pf.DriverCheckpoint) error {
	if f.rx.state != rxAcknowledged {
		return fmt.Errorf("connector protocol error (Prepared not expected in state %v)", f.rx.state)
	}

	// Tell synchronous Client.Prepare() of this response.
	f.rx.preparedCh <- prepared

	f.rx.state = rxPrepared
	return nil
}

func (f *TxnClient) onCommitOps(ops CommitOps) error {
	if f.rx.state != rxPrepared {
		return fmt.Errorf("client protocol error (StartCommit not expected in state %v)", f.rx.state)
	}

	f.rx.commitOps = ops
	f.rx.state = rxPendingCommit
	return nil
}

// drainBinding drains the Combiner of the specified materialization
// binding by sending Store requests for its reduced documents.
func drainBinding(
	flighted map[string]json.RawMessage,
	combiner pf.Combiner,
	deltaUpdates bool,
	driverTx Driver_TransactionsClient,
	request **TransactionRequest,
	binding int,
) error {
	// Precondition: |flighted| contains the precise set of keys for this binding in this transaction.
	var remaining = len(flighted)

	// Drain the combiner into materialization Store requests.
	if err := combiner.Drain(func(full bool, docRaw json.RawMessage, packedKey, packedValues []byte) error {
		// Inlined use of string(packedKey) clues compiler escape analysis to avoid allocation.
		if _, ok := flighted[string(packedKey)]; !ok {
			var key, _ = tuple.Unpack(packedKey)
			return fmt.Errorf(
				"driver implementation error: "+
					"loaded key %v (rawKey: %q) was not requested by Flow in this transaction (document %s)",
				key,
				string(packedKey),
				string(docRaw),
			)
		}

		// We're using |full|, an indicator of whether the document was a full
		// reduction or a partial combine, to track whether the document exists
		// in the store. This works because we only issue reduce-left when a
		// document was provided by Loaded or was retained from a previous
		// transaction's Store.

		if err := StageStore(driverTx, request, binding, packedKey, packedValues, docRaw, full); err != nil {
			return err
		}

		// We can retain a bounded number of documents from this transaction
		// as a performance optimization, so that they may be directly available
		// to the next transaction without issuing a Load.
		if deltaUpdates || remaining >= cachedDocumentBound {
			delete(flighted, string(packedKey)) // Don't retain.
		} else {
			// We cannot reference |rawDoc| beyond this callback, and must copy.
			// Fortunately, StageStore did just that, appending the document
			// to the staged request Arena, which we can reference here because
			// Arena bytes are write-once.
			var s = (*request).Store
			flighted[string(packedKey)] = s.Arena.Bytes(s.DocsJson[len(s.DocsJson)-1])
		}

		remaining--
		return nil

	}); err != nil {
		return fmt.Errorf("combine.Finish: %w", err)
	}

	// We should have seen 1:1 combined documents for each flighted key.
	if remaining != 0 {
		logrus.WithFields(logrus.Fields{
			"remaining": remaining,
			"flighted":  len(flighted),
		}).Panic("combiner drained, but expected documents remainder != 0")
	}

	return nil
}

// TODO(johnny): This is an interesting knob we may want expose.
const cachedDocumentBound = 2048
