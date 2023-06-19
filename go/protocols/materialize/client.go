package materialize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
)

// TxnClient is a client of a driver's Transactions RPC.
type TxnClient struct {
	client Connector_MaterializeClient
	// Combiners of the materialization, one for each binding.
	combiners []pf.Combiner
	// Guards combiners, which are accessed concurrently from readAcknowledgedAndLoaded().
	combinersMu sync.Mutex
	// Flighted keys of the current transaction for each binding, plus a bounded cache of
	// fully-reduced documents of the last transaction.
	flighted []map[string]json.RawMessage
	// OpFuture that's resolved on completion of a current Loaded phase,
	// or nil if readAcknowledgedAndLoaded is not currently running.
	loadedOp   *client.AsyncOperation
	opened     *Response_Opened        // Opened response returned by the server while opening.
	rxResponse Response                // Response which is received into.
	spec       *pf.MaterializationSpec // Specification of this Transactions client.
	txRequest  Request                 // Request which is sent from.
	version    string                  // Version of the client's MaterializationSpec.
}

// OpenTransactions opens a transactions RPC and completes the Open/Opened phase,
// returning a TxnClient prepared for the first transaction of the RPC.
func OpenTransactions(
	ctx context.Context,
	connector ConnectorClient,
	connectorState json.RawMessage,
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

	for _, b := range spec.Bindings {
		var combiner, err = newCombinerFn(b)
		if err != nil {
			return nil, fmt.Errorf("creating %s combiner: %w", b.Collection.Name, err)
		}
		combiners = append(combiners, combiner)
		flighted = append(flighted, make(map[string]json.RawMessage))
	}

	// TODO(johnny): temporary support for in-process sqlite materialization.
	// This can be removed with that implementation.
	ctx = pb.WithDispatchDefault(ctx)

	rpc, err := connector.Materialize(ctx)
	if err != nil {
		return nil, fmt.Errorf("driver.Transactions: %w", err)
	}
	// Close RPC if remaining initialization fails.
	defer func() {
		if rpc != nil {
			_ = rpc.CloseSend()
		}
	}()

	txRequest, err := WriteOpen(rpc,
		&Request_Open{
			Materialization: spec,
			Version:         version,
			Range:           &range_,
			StateJson:       connectorState,
		})
	if err != nil {
		return nil, err
	}

	// Read Opened response with driver's optional Flow Checkpoint.
	rxResponse, err := ReadOpened(rpc)
	if err != nil {
		return nil, err
	}

	// Write Acknowledge request to re-acknowledge the last commit.
	if err := WriteAcknowledge(rpc, &txRequest); err != nil {
		return nil, err
	}

	var c = &TxnClient{
		combiners: combiners,
		//combinersMu: sync.Mutex{},
		flighted:   flighted,
		loadedOp:   client.NewAsyncOperation(),
		opened:     rxResponse.Opened,
		client:     rpc,
		rxResponse: rxResponse,
		spec:       spec,
		txRequest:  txRequest,
		version:    version,
	}

	var initialAcknowledged = client.NewAsyncOperation()
	go c.readAcknowledgedAndLoaded(initialAcknowledged)

	// We must block until the very first Acknowledged is read (or errors).
	// If we didn't do this, then TxnClient.Flush could potentially
	// be called (and write Flush) before the first Acknowledged is read,
	// which is a protocol violation.
	<-initialAcknowledged.Done()

	rpc = nil // Don't run deferred CloseSend.
	return c, nil
}

// Opened returns the driver's prior Opened response.
func (c *TxnClient) Opened() *Response_Opened { return c.opened }

// Close the TxnClient. Close returns an error if the RPC is not in an
// Acknowledged and idle state, or on any other error.
func (c *TxnClient) Close() error {
	c.client.CloseSend()

	var loadedErr error
	if c.loadedOp != nil {
		loadedErr = c.loadedOp.Err()
	}

	for _, c := range c.combiners {
		c.Destroy()
	}

	// EOF is a graceful shutdown.
	if err := loadedErr; err != io.EOF {
		return err
	}
	return nil
}

// AddDocument to the current transaction under the given binding and tuple-encoded key.
func (c *TxnClient) AddDocument(binding int, keyPacked []byte, keyJson json.RawMessage, doc json.RawMessage) error {
	// Note that combineRight obtains a lock on `c.combinerMu`, but it's not held
	// while we WriteLoad to the connector (which could block).
	// This allows for a concurrent handling of a Loaded response.

	// Check `flighted` without locking. Safety:
	// * combineRight() modifies `flighted`, but is called only from this function (below).
	// * Store also modifies `flighted`, but is called only by the same thread
	//   which invokes AddDocument.
	if len(c.flighted[binding]) >= maxFlightedKeys {
		return consumer.ErrDeferToNextTransaction
	}

	if load, err := c.combineRight(binding, keyPacked, doc); err != nil {
		return err
	} else if !load {
		// No-op.
	} else if err = WriteLoad(c.client, &c.txRequest, binding, keyPacked, keyJson); err != nil {
		return c.writeErr(err)
	}

	return nil
}

func (c *TxnClient) combineRight(binding int, packedKey []byte, doc json.RawMessage) (bool, error) {
	c.combinersMu.Lock()
	defer c.combinersMu.Unlock()

	var flighted = c.flighted[binding]
	var combiner = c.combiners[binding]
	var deltaUpdates = c.spec.Bindings[binding].DeltaUpdates
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

// Flush the current transaction, causing the server to respond with any
// remaining Loaded responses before it sends Flushed in response.
func (c *TxnClient) Flush() error {
	if err := WriteFlush(c.client, &c.txRequest); err != nil {
		return c.writeErr(err)
	}
	// Now block until we've read through the remaining `Loaded` responses.
	if c.loadedOp.Err() != nil {
		return c.loadedOp.Err()
	}
	c.loadedOp = nil // readAcknowledgedAndLoaded has completed.

	if err := ReadFlushed(&c.rxResponse); err != nil {
		return err
	}
	return nil
}

// Store documents drained from binding combiners.
func (c *TxnClient) Store() ([]*pf.CombineAPI_Stats, error) {
	// Any remaining flighted keys *not* having `nil` values are retained documents
	// of a prior transaction which were not updated during this one.
	// We garbage collect them here, and achieve the drainBinding() precondition that
	// flighted maps hold only keys of the current transaction with `nil` sentinels.
	for _, flighted := range c.flighted {
		for key, doc := range flighted {
			if doc != nil {
				delete(flighted, key)
			}
		}
	}

	var allStats = make([]*pf.CombineAPI_Stats, 0, len(c.combiners))
	// Drain each binding.
	for i, combiner := range c.combiners {
		if stats, err := c.drainBinding(
			c.flighted[i],
			combiner,
			c.spec.Bindings[i].DeltaUpdates,
			i,
		); err != nil {
			return nil, err
		} else {
			allStats = append(allStats, stats)
		}
	}

	return allStats, nil
}

// StartCommit by synchronously writing StartCommit with the runtime checkpoint
// and reading StartedCommit with the driver checkpoint, then asynchronously
// read an Acknowledged response.
func (c *TxnClient) StartCommit(runtimeCP *pf.Checkpoint) (_ *pf.ConnectorState, acknowledged client.OpFuture, _ error) {
	var connectorCP *pf.ConnectorState

	if err := WriteStartCommit(c.client, &c.txRequest, runtimeCP); err != nil {
		return nil, nil, c.writeErr(err)
	} else if connectorCP, err = ReadStartedCommit(c.client, &c.rxResponse); err != nil {
		return nil, nil, err
	}

	// Future resolved upon reading `Acknowledged` response, which permits the
	// runtime to start closing a subsequent pipelined transaction.
	var acknowledgedOp = client.NewAsyncOperation()
	// Future resolved when all `Loaded` responses have been read and
	// readAcknowledgedAndLoaded() has exited.
	c.loadedOp = client.NewAsyncOperation()

	go c.readAcknowledgedAndLoaded(acknowledgedOp)

	return connectorCP, acknowledgedOp, nil
}

// Acknowledge that the runtime's commit to its recovery log has completed.
func (c *TxnClient) Acknowledge() error {
	if err := WriteAcknowledge(c.client, &c.txRequest); err != nil {
		return c.writeErr(err)
	}
	return nil
}

func (c *TxnClient) writeErr(err error) error {
	// EOF indicates a stream break, which returns a causal error only with RecvMsg.
	if !errors.Is(err, io.EOF) {
		return err
	}
	// If loadedOp != nil then readAcknowledgedAndLoaded is running.
	// It will (or has) read an error, and we should wait for it.
	if c.loadedOp != nil {
		return c.loadedOp.Err()
	}
	// Otherwise we must synchronously read the error.
	for {
		if _, err = c.client.Recv(); err != nil {
			return pf.UnwrapGRPCError(err)
		}
	}
}

// drainBinding drains the Combiner of the specified materialization
// binding by sending Store requests for its reduced documents.
func (c *TxnClient) drainBinding(
	flighted map[string]json.RawMessage,
	combiner pf.Combiner,
	deltaUpdates bool,
	binding int,
) (*pf.CombineAPI_Stats, error) {
	// Precondition: |flighted| contains the precise set of keys for this binding in this transaction.
	var remaining = len(flighted)

	// Drain the combiner into materialization Store requests.
	var stats, err = combiner.Drain(func(full bool, docRaw json.RawMessage, keyPacked, valuesPacked []byte) error {
		// Inlined use of string(packedKey) clues compiler escape analysis to avoid allocation.
		if _, ok := flighted[string(keyPacked)]; !ok {
			var key, _ = tuple.Unpack(keyPacked)
			return fmt.Errorf(
				"driver implementation error: "+
					"loaded key %v (rawKey: %q) was not requested by Flow in this transaction (document %s)",
				key,
				string(keyPacked),
				string(docRaw),
			)
		}

		// We're using |full|, an indicator of whether the document was a full
		// reduction or a partial combine, to track whether the document exists
		// in the store. This works because we only issue reduce-left when a
		// document was provided by Loaded or was retained from a previous
		// transaction's Store.

		// TODO(johnny): Not sent yet. Potentially make part of combiner API scope?
		var keyJSON, valuesJSON json.RawMessage

		if err := WriteStore(c.client, &c.txRequest, binding, keyPacked, keyJSON, valuesPacked, valuesJSON, docRaw, full); err != nil {
			return c.writeErr(err)
		}

		// We can retain a bounded number of documents from this transaction
		// as a performance optimization, so that they may be directly available
		// to the next transaction without issuing a Load.
		if deltaUpdates || remaining >= cachedDocumentBound || len(docRaw) > cachedDocumentMaxSize {
			delete(flighted, string(keyPacked)) // Don't retain.
		} else {
			// We cannot reference `docRaw` beyond this callback, and must copy.
			flighted[string(keyPacked)] = append(json.RawMessage{}, docRaw...)
		}

		remaining--
		return nil

	})
	if err != nil {
		return nil, fmt.Errorf("combine.Finish: %w", err)
	}

	// We should have seen 1:1 combined documents for each flighted key.
	if remaining != 0 {
		logrus.WithFields(logrus.Fields{
			"remaining": remaining,
			"flighted":  len(flighted),
		}).Panic("combiner drained, but expected documents remainder != 0")
	}

	return stats, nil
}

func (c *TxnClient) readAcknowledgedAndLoaded(acknowledgedOp *client.AsyncOperation) (__err error) {
	defer func() {
		if acknowledgedOp != nil {
			acknowledgedOp.Resolve(__err)
		}
		c.loadedOp.Resolve(__err)
	}()

	if err := ReadAcknowledged(c.client, &c.rxResponse); err != nil {
		return err
	}

	acknowledgedOp.Resolve(nil)
	acknowledgedOp = nil // Don't resolve again.

	c.combinersMu.Lock()
	defer c.combinersMu.Unlock()

	for {
		c.combinersMu.Unlock()
		var loaded, err = ReadLoaded(c.client, &c.rxResponse)
		c.combinersMu.Lock()

		if err != nil {
			return err
		} else if loaded == nil {
			return nil
		}

		if int(loaded.Binding) > len(c.combiners) {
			return fmt.Errorf("driver implementation error (binding %d out of range)", loaded.Binding)
		}

		// Feed document into the combiner as a reduce-left operation.
		if err := c.combiners[loaded.Binding].ReduceLeft(loaded.DocJson); err != nil {
			return fmt.Errorf("combiner.ReduceLeft: %w", err)
		}
	}
}

const (
	// Number of documents we'll cache between transactions of a standard materialization,
	// to avoid extra Loads when only a small number of keys are being modified each transaction.
	// TODO(johnny): This is an interesting knob we may want expose.
	cachedDocumentBound = 2048
	// Maximum size of a serialized document that we will cache
	cachedDocumentMaxSize = 1 << 15 // 32K
	// Maximum number of keys we'll manage in a single transaction.
	// TODO(johnny): We'd like to remove this, but cannot so long as we're using an in-memory key map.
	maxFlightedKeys = 10_000_000
)
