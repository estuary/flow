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
	pc "go.gazette.dev/core/consumer/protocol"
)

// WriteOpen writes an Open request into the stream.
func WriteOpen(
	stream interface {
		Send(*TransactionRequest) error
	},
	request **TransactionRequest,
	spec *pf.MaterializationSpec,
	version string,
	range_ *pf.RangeSpec,
	driverCheckpoint json.RawMessage,
) error {
	if *request != nil {
		panic("expected nil request")
	} else if range_.RClockBegin != 0 || range_.RClockEnd != math.MaxUint32 {
		panic("materialization shards cannot split on r-clock: " + range_.String())
	}

	if err := stream.Send(&TransactionRequest{
		Open: &TransactionRequest_Open{
			Materialization:      spec,
			Version:              version,
			KeyBegin:             range_.KeyBegin,
			KeyEnd:               range_.KeyEnd,
			DriverCheckpointJson: driverCheckpoint,
		},
	}); err != nil {
		return fmt.Errorf("sending Open request: %w", err)
	}

	return nil
}

// WriteOpened writes an Open response into the stream.
func WriteOpened(
	stream interface {
		Send(*TransactionResponse) error
	},
	response **TransactionResponse,
	opened *TransactionResponse_Opened,
) error {
	if *response != nil {
		panic("expected nil response")
	}

	if err := stream.Send(&TransactionResponse{
		Opened: opened,
	}); err != nil {
		return fmt.Errorf("sending Opened response: %w", err)
	}

	return nil
}

// StageLoad potentially sends a previously staged Load into the stream,
// and then stages its arguments into request.Load.
func StageLoad(
	stream interface {
		Send(*TransactionRequest) error
	},
	request **TransactionRequest,
	binding int,
	packedKey []byte,
) error {
	// Send current |request| if it uses a different binding or would re-allocate.
	if *request != nil {
		var rem int
		if l := (*request).Load; int(l.Binding) != binding {
			rem = -1 // Must flush this request.
		} else if cap(l.PackedKeys) != len(l.PackedKeys) {
			rem = cap(l.Arena) - len(l.Arena)
		}
		if rem < len(packedKey) {
			if err := stream.Send(*request); err != nil {
				return fmt.Errorf("sending Load request: %w", err)
			}
			*request = nil
		}
	}

	if *request == nil {
		*request = &TransactionRequest{
			Load: &TransactionRequest_Load{
				Binding:    uint32(binding),
				Arena:      make(pf.Arena, 0, arenaSize),
				PackedKeys: make([]pf.Slice, 0, sliceSize),
			},
		}
	}

	var l = (*request).Load
	l.PackedKeys = append(l.PackedKeys, l.Arena.Add(packedKey))

	return nil
}

// StageLoaded potentially sends a previously staged Loaded into the stream,
// and then stages its arguments into response.Loaded.
func StageLoaded(
	stream interface {
		Send(*TransactionResponse) error
	},
	response **TransactionResponse,
	binding int,
	document json.RawMessage,
) error {
	// Send current |response| if we would re-allocate.
	if *response != nil {
		var rem int
		if l := (*response).Loaded; int(l.Binding) != binding {
			rem = -1 // Must flush this response.
		} else if cap(l.DocsJson) != len(l.DocsJson) {
			rem = cap(l.Arena) - len(l.Arena)
		}
		if rem < len(document) {
			if err := stream.Send(*response); err != nil {
				return fmt.Errorf("sending Loaded response: %w", err)
			}
			*response = nil
		}
	}

	if *response == nil {
		*response = &TransactionResponse{
			Loaded: &TransactionResponse_Loaded{
				Binding:  uint32(binding),
				Arena:    make(pf.Arena, 0, arenaSize),
				DocsJson: make([]pf.Slice, 0, sliceSize),
			},
		}
	}

	var l = (*response).Loaded
	l.DocsJson = append(l.DocsJson, l.Arena.Add(document))

	return nil
}

// WritePrepare flushes a pending Load request, and sends a Prepare request
// with the provided Flow checkpoint.
func WritePrepare(
	stream interface {
		Send(*TransactionRequest) error
	},
	request **TransactionRequest,
	checkpoint pc.Checkpoint,
) error {
	// Flush partial Load request, if required.
	if *request != nil {
		if err := stream.Send(*request); err != nil {
			return fmt.Errorf("flushing final Load request: %w", err)
		}
		*request = nil
	}

	var checkpointBytes, err = checkpoint.Marshal()
	if err != nil {
		panic(err) // Cannot fail.
	}

	if err := stream.Send(&TransactionRequest{
		Prepare: &TransactionRequest_Prepare{
			FlowCheckpoint: checkpointBytes,
		},
	}); err != nil {
		return fmt.Errorf("sending Prepare request: %w", err)
	}

	return nil
}

// WritePrepared flushes a pending Loaded response, and sends a Prepared response
// with the provided driver checkpoint.
func WritePrepared(
	stream interface {
		Send(*TransactionResponse) error
	},
	response **TransactionResponse,
	prepared *TransactionResponse_Prepared,
) error {
	// Flush partial Loaded response, if required.
	if *response != nil {
		if err := stream.Send(*response); err != nil {
			return fmt.Errorf("flushing final Loaded response: %w", err)
		}
		*response = nil
	}

	if err := stream.Send(&TransactionResponse{
		Prepared: prepared,
	}); err != nil {
		return fmt.Errorf("sending Prepared response: %w", err)
	}

	return nil
}

// StageStore potentially sends a previously staged Store into the stream,
// and then stages its arguments into request.Store.
func StageStore(
	stream interface {
		Send(*TransactionRequest) error
	},
	request **TransactionRequest,
	binding int,
	packedKey []byte,
	packedValues []byte,
	doc json.RawMessage,
	exists bool,
) error {
	// Send current |request| if we would re-allocate.
	if *request != nil {
		var rem int
		if s := (*request).Store; int(s.Binding) != binding {
			rem = -1 // Must flush this request.
		} else if cap(s.PackedKeys) != len(s.PackedKeys) {
			rem = cap(s.Arena) - len(s.Arena)
		}
		if need := len(packedKey) + len(packedValues) + len(doc); need > rem {
			if err := stream.Send(*request); err != nil {
				return fmt.Errorf("sending Store request: %w", err)
			}
			*request = nil
		}
	}

	if *request == nil {
		*request = &TransactionRequest{
			Store: &TransactionRequest_Store{
				Binding:      uint32(binding),
				Arena:        make(pf.Arena, 0, arenaSize),
				PackedKeys:   make([]pf.Slice, 0, sliceSize),
				PackedValues: make([]pf.Slice, 0, sliceSize),
				DocsJson:     make([]pf.Slice, 0, sliceSize),
			},
		}
	}

	var s = (*request).Store
	s.PackedKeys = append(s.PackedKeys, s.Arena.Add(packedKey))
	s.PackedValues = append(s.PackedValues, s.Arena.Add(packedValues))
	s.DocsJson = append(s.DocsJson, s.Arena.Add(doc))
	s.Exists = append(s.Exists, exists)

	return nil
}

// WriteCommit flushes a pending Store request, and sends a Commit request.
func WriteCommit(
	stream interface {
		Send(*TransactionRequest) error
	},
	request **TransactionRequest,
) error {
	// Flush partial Store request, if required.
	if *request != nil {
		if err := stream.Send(*request); err != nil {
			return fmt.Errorf("flushing final Store request: %w", err)
		}
		*request = nil
	}

	if err := stream.Send(&TransactionRequest{
		Commit: &TransactionRequest_Commit{},
	}); err != nil {
		return fmt.Errorf("sending Commit request: %w", err)
	}

	return nil
}

// WriteDriverCommitted writes a DriverCommitted response to the stream.
func WriteDriverCommitted(
	stream interface {
		Send(*TransactionResponse) error
	},
	response **TransactionResponse,
) error {
	// We must have sent Prepared prior to DriverCommitted.
	if *response != nil {
		panic("expected nil response")
	}

	if err := stream.Send(&TransactionResponse{
		DriverCommitted: &TransactionResponse_DriverCommitted{},
	}); err != nil {
		return fmt.Errorf("sending DriverCommitted response: %w", err)
	}

	return nil
}

// WriteAcknowledged writes an Acknowledged response to the stream.
func WriteAcknowledged(
	stream interface {
		Send(*TransactionResponse) error
	},
	response **TransactionResponse,
) error {
	// We must have sent DriverCommitted prior to Acknowledged.
	// Acknowledged may be intermixed with Loaded.
	if *response != nil && (*response).Loaded == nil {
		panic("expected response to be nil or have staged Loaded responses")
	}

	if err := stream.Send(&TransactionResponse{
		Acknowledged: &TransactionResponse_Acknowledged{},
	}); err != nil {
		return fmt.Errorf("sending Acknowledged response: %w", err)
	}

	return nil
}

// LoadIterator is an iterator over Load requests.
type LoadIterator struct {
	Binding int         // Binding index of this document to load.
	Key     tuple.Tuple // Key of the next document to load.

	stream interface {
		Context() context.Context
		RecvMsg(m interface{}) error // Receives Load, Acknowledge, and Prepare.
	}
	reqAckCh chan<- struct{}    // Closed and nil'd on reading Acknowledge.
	req      TransactionRequest // Last read request.
	index    int                // Last-returned document index within |req|.
	total    int                // Total number of iterated keys.
	err      error              // Final error.
}

// NewLoadIterator returns a *LoadIterator of the stream.
func NewLoadIterator(stream Driver_TransactionsServer, reqAckCh chan<- struct{}) *LoadIterator {
	return &LoadIterator{stream: stream, reqAckCh: reqAckCh}
}

// poll returns true if there is at least one LoadRequest message with at least one key
// remaining to be read. This will read the next message from the stream if required. This will not
// advance the iterator, so it can be used to check whether the LoadIterator contains at least one
// key to load without actually consuming the next key. If false is returned, then there are no
// remaining keys and poll must not be called again. Note that if poll returns
// true, then Next may still return false if the LoadRequest message is malformed.
func (it *LoadIterator) poll() bool {
	if it.err != nil || it.req.Prepare != nil {
		panic("Poll called again after having returned false")
	}

	// Must we read another request?
	if it.req.Load == nil || it.index == len(it.req.Load.PackedKeys) {
		// Use RecvMsg to re-use |it.req| without allocation.
		// Safe because we fully process |it.req| between calls to RecvMsg.
		if err := it.stream.RecvMsg(&it.req); err == io.EOF {
			if it.total != 0 {
				it.err = fmt.Errorf("unexpected EOF when there are loaded keys")
			} else if it.reqAckCh != nil {
				it.err = fmt.Errorf("unexpected EOF before receiving Acknowledge")
			} else {
				it.err = io.EOF // Clean shutdown
			}
			return false
		} else if err != nil {
			it.err = fmt.Errorf("reading Load: %w", err)
			return false
		}

		if it.req.Acknowledge != nil {
			if it.reqAckCh == nil {
				it.err = fmt.Errorf("protocol error (Acknowledge seen twice during load phase)")
				return false
			}
			close(it.reqAckCh) // Signal that Acknowledge should run.
			it.reqAckCh = nil

			return it.poll() // Tail-recurse to read the next message.
		} else if it.req.Prepare != nil {
			if it.reqAckCh != nil {
				it.err = fmt.Errorf("protocol error (Prepare seen before Acknowledge)")
			}
			return false // Prepare ends the Load phase.
		} else if it.req.Load == nil || len(it.req.Load.PackedKeys) == 0 {
			it.err = fmt.Errorf("expected non-empty Load, got %#v", it.req)
			return false
		}
		it.index = 0
		it.Binding = int(it.req.Load.Binding)
	}
	return true
}

// Context returns the Context of this LoadIterator.
func (it *LoadIterator) Context() context.Context { return it.stream.Context() }

// Next returns true if there is another Load and makes it available via Key.
// When a Prepare is read, or if an error is encountered, it returns false
// and must not be called again.
func (it *LoadIterator) Next() bool {
	if !it.poll() {
		return false
	}

	var l = it.req.Load
	it.Key, it.err = tuple.Unpack(it.req.Load.Arena.Bytes(l.PackedKeys[it.index]))

	if it.err != nil {
		it.err = fmt.Errorf("unpacking Load key: %w", it.err)
		return false
	}

	it.index++
	it.total++
	return true
}

// Err returns an encountered error.
func (it *LoadIterator) Err() error {
	if it.err != io.EOF {
		return it.err // Graceful shutdown is not a public Err.
	}
	return nil
}

// Prepare returns a TransactionRequest_Prepare which caused this LoadIterator
// to terminate. It's valid only after Next returns false and Err is nil.
func (it *LoadIterator) Prepare() *TransactionRequest_Prepare {
	return it.req.Prepare
}

// StoreIterator is an iterator over Store requests.
type StoreIterator struct {
	Binding int             // Binding index of this stored document.
	Key     tuple.Tuple     // Key of the next document to store.
	Values  tuple.Tuple     // Values of the next document to store.
	RawJSON json.RawMessage // Document to store.
	Exists  bool            // Does this document exist in the store already?

	stream interface {
		Context() context.Context
		RecvMsg(m interface{}) error // Receives Store and Commit.
	}
	req   TransactionRequest
	index int
	total int
	err   error
}

// NewStoreIterator returns a *StoreIterator of the stream.
func NewStoreIterator(stream Driver_TransactionsServer) *StoreIterator {
	return &StoreIterator{stream: stream}
}

// poll returns true if there is at least one StoreRequest message with at least one document
// remaining to be read. This will read the next message from the stream if required. This will not
// advance the iterator, so it can be used to check whether the StoreIterator contains at least one
// document to store without actually consuming the next document. If false is returned, then there
// are no remaining documents and poll must not be called again. Note that if poll returns true,
// then Next may still return false if the StoreRequest message is malformed.
func (it *StoreIterator) poll() bool {
	if it.err != nil || it.req.Commit != nil {
		panic("Poll called again after having returned false")
	}
	// Must we read another request?
	if it.req.Store == nil || it.index == len(it.req.Store.PackedKeys) {
		// Use RecvMsg to re-use |it.req| without allocation.
		// Safe because we fully process |it.req| between calls to RecvMsg.
		if err := it.stream.RecvMsg(&it.req); err != nil {
			it.err = fmt.Errorf("reading Store: %w", err)
			return false
		}

		if it.req.Commit != nil {
			return false // Prepare ends the Store phase.
		} else if it.req.Store == nil || len(it.req.Store.PackedKeys) == 0 {
			it.err = fmt.Errorf("expected non-empty Store, got %#v", it.req)
			return false
		}
		it.index = 0
		it.Binding = int(it.req.Store.Binding)
	}
	return true
}

// Context returns the Context of this StoreIterator.
func (it *StoreIterator) Context() context.Context { return it.stream.Context() }

// Next returns true if there is another Store and makes it available.
// When a Commit is read, or if an error is encountered, it returns false
// and must not be called again.
func (it *StoreIterator) Next() bool {
	if !it.poll() {
		return false
	}

	var s = it.req.Store

	it.Key, it.err = tuple.Unpack(s.Arena.Bytes(s.PackedKeys[it.index]))
	if it.err != nil {
		it.err = fmt.Errorf("unpacking Store key: %w", it.err)
		return false
	}
	it.Values, it.err = tuple.Unpack(s.Arena.Bytes(s.PackedValues[it.index]))
	if it.err != nil {
		it.err = fmt.Errorf("unpacking Store values: %w", it.err)
		return false
	}
	it.RawJSON = s.Arena.Bytes(s.DocsJson[it.index])
	it.Exists = s.Exists[it.index]

	it.index++
	it.total++
	return true
}

// Err returns an encountered error.
func (it *StoreIterator) Err() error {
	return it.err
}

// Commit returns a TransactionRequest_Commit which caused this StoreIterator
// to terminate. It's valid only after Next returns false and Err is nil.
func (it *StoreIterator) Commit() *TransactionRequest_Commit {
	return it.req.Commit
}

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
	Prepare(context.Context, *TransactionRequest_Prepare) (*TransactionResponse_Prepared, error)
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
			log.WithField("err", _err).Error("RunTransactions failed")
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
		if err := transactor.Load(it, commitCh, ackCh, func(binding int, doc json.RawMessage) error {
			loaded++

			tx.Lock()
			defer tx.Unlock()

			return StageLoaded(stream, &tx.response, binding, doc)
		}); err != nil {
			return err
		}

		return it.Err()
	}

	for round := 0; true; round++ {
		var (
			loadCh   chan struct{}         // Signals Load() is done.
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
		if prepared, err := transactor.Prepare(ctx, loadIt.req.Prepare); err != nil {
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

const (
	arenaSize = 16 * 1024
	sliceSize = 32
)
