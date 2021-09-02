package materialize

import (
	"encoding/json"
	"fmt"
	"io"
	"math"

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

// WriteCommitted writes a Committed response to the stream.
func WriteCommitted(
	stream interface {
		Send(*TransactionResponse) error
	},
	response **TransactionResponse,
) error {
	// We must have sent Prepared prior to Committed.
	if *response != nil {
		panic("expected nil response")
	}

	if err := stream.Send(&TransactionResponse{
		Committed: &TransactionResponse_Committed{},
	}); err != nil {
		return fmt.Errorf("sending Committed response: %w", err)
	}

	return nil
}

// LoadIterator is an iterator over Load requests.
type LoadIterator struct {
	Binding int         // Binding index of this document to load.
	Key     tuple.Tuple // Key of the next document to load.

	stream interface {
		RecvMsg(m interface{}) error
	}
	req   TransactionRequest
	index int
	total int
	err   error
}

// NewLoadIterator returns a *LoadIterator of the stream.
func NewLoadIterator(stream Driver_TransactionsServer) *LoadIterator {
	return &LoadIterator{stream: stream}
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
		if err := it.stream.RecvMsg(&it.req); err != nil {
			if err == io.EOF && it.req.Load == nil {
				it.err = io.EOF // Clean shutdown before first Load.
			} else {
				it.err = fmt.Errorf("reading Load: %w", err)
			}
			return false
		}

		if it.req.Prepare != nil {
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
		RecvMsg(m interface{}) error
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
	// Load implements the transaction "load" phase by:
	// * Consuming Load requests from the LoadIterator.
	// * Awaiting a Prepare request or |commitCh| before reading from the store.
	// * Invoking loaded() with loaded documents.
	//
	// Loads of transaction T+1 will be invoked after Store of T+0, but
	// concurrently with Commit of T+0. This is an important optimization that
	// allows the driver to immediately begin the work of T+1, for instance by
	// staging Load keys into a temporary table which will later be joined and
	// queried upon a future Prepare.
	//
	// But, the driver contract is that documents loaded in T+1 must reflect
	// stores of T+0 (a.k.a. "read committed"). The driver must therefore await
	// either a Prepare or |commitCh| before reading from the store to ensure
	// this contract is met.
	Load(_ *LoadIterator, commitCh <-chan struct{}, loaded func(binding int, doc json.RawMessage) error) error
	// Prepare begins the transaction "store" phase.
	Prepare(*TransactionRequest_Prepare) (*TransactionResponse_Prepared, error)
	// Store consumes Store requests from the StoreIterator.
	Store(*StoreIterator) error
	// Commit the transaction.
	Commit() error
	// Destroy the Transactor, releasing any held resources.
	Destroy()
}

// RunTransactions processes materialization protocol transactions
// over the established stream against a Driver.
func RunTransactions(
	stream Driver_TransactionsServer,
	transactor Transactor,
	log *logrus.Entry,
) (err error) {

	defer func() {
		if err != nil {
			log.WithField("err", err).Error("RunTransactions failed")
		} else {
			log.Debug("RunTransactions finished")
		}
		transactor.Destroy()
	}()

	var (
		response  *TransactionResponse  // In-progress response.
		loadCh    chan struct{}         // Signals Load() is done.
		loadErr   error                 // Readable on |<-commitCh|.
		commitCh  = make(chan struct{}) // Signals Commit() is done.
		commitErr error                 // Readable on |<-commitCh|.
	)
	close(commitCh) // Initialize as already committed.

	for round := 0; true; round++ {
		var log = log.WithField("round", round)

		loadCh = make(chan struct{})
		var loadIt = NewLoadIterator(stream)

		go func(commitCh <-chan struct{}) (err error) {
			var loaded int
			defer func() {
				var log = log.WithFields(logrus.Fields{
					"load":   loadIt.total,
					"loaded": loaded,
				})
				if err != nil {
					log.WithField("err", err).Error("Load failed")
				} else {
					log.Debug("Load finished")
				}

				loadErr = err
				close(loadCh)
			}()

			if !loadIt.poll() {
				return nil
			}

			// Process all Load requests until Prepare is read.
			return transactor.Load(loadIt, commitCh, func(binding int, doc json.RawMessage) error {
				// If a buggy driver implementation calls Loaded before |commitCh| is ready,
				// it's detectable by runtime.Materialize, which will observe / fail on a
				// Loaded response received before an expected Committed response.
				loaded++
				return StageLoaded(stream, &response, binding, doc)
			})
		}(commitCh)

		// Join over current transaction Load and prior transaction Commit.
		for commitCh != nil || loadCh != nil {
			select {
			case <-commitCh:
				if commitErr != nil {
					return commitErr // Bail now, to cancel ongoing load.
				}
				commitCh = nil
			case <-loadCh:
				loadCh = nil
			}
		}

		if loadErr != nil {
			return loadErr
		} else if loadIt.Err() != nil {
			return loadIt.Err()
		} else if loadIt.err == io.EOF {
			return nil // Graceful shutdown.
		}

		// Prepare, then respond with Prepared.
		if prepared, err := transactor.Prepare(loadIt.req.Prepare); err != nil {
			return err
		} else if err = WritePrepared(stream, &response, prepared); err != nil {
			return err
		}
		log.Debug("wrote Prepared")

		// Process all Store requests until Commit is read.
		var storeIt = NewStoreIterator(stream)
		if storeIt.poll() {
			if err := transactor.Store(storeIt); err != nil {
				return err
			}
		}
		log.WithField("store", storeIt.total).Debug("Store finished")

		if storeIt.Err() != nil {
			return storeIt.Err()
		}

		// Begin async commit.
		commitCh = make(chan struct{})
		go func() (err error) {
			defer func() {
				if err != nil {
					log.WithField("err", err).Error("Commit failed")
				} else {
					log.Debug("Commit finished")
				}

				commitErr = err
				close(commitCh)
			}()

			// Commit, then acknowledge our commit.
			if err := transactor.Commit(); err != nil {
				return fmt.Errorf("store.Commit: %w", err)
			}
			return WriteCommitted(stream, &response)
		}()
	}
	panic("not reached")
}

const (
	arenaSize = 16 * 1024
	sliceSize = 32
)
