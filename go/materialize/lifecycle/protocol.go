package lifecycle

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/estuary/flow/go/fdb/tuple"
	"github.com/estuary/flow/go/protocols/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	pc "go.gazette.dev/core/consumer/protocol"
)

// WriteOpen writes an Open request into the stream.
func WriteOpen(
	stream pm.Driver_TransactionsClient,
	request **pm.TransactionRequest,
	endpointType pf.EndpointType,
	endpointConfigJSON string,
	fields *pf.FieldSelection,
	shardFQN string,
	driverCheckpoint []byte,
) error {
	if *request != nil {
		panic("expected nil request")
	}

	if err := stream.Send(&pm.TransactionRequest{
		Open: &pm.TransactionRequest_Open{
			EndpointType:       endpointType,
			EndpointConfigJson: endpointConfigJSON,
			Fields:             fields,
			ShardFqn:           shardFQN,
			DriverCheckpoint:   driverCheckpoint,
		},
	}); err != nil {
		return fmt.Errorf("sending Open request: %w", err)
	}

	return nil
}

// WriteOpened writes an Open response into the stream.
func WriteOpened(
	stream pm.Driver_TransactionsServer,
	response **pm.TransactionResponse,
	flowCheckpoint []byte,
	deltaUpdate bool,
) error {
	if *response != nil {
		panic("expected nil response")
	}

	if err := stream.Send(&pm.TransactionResponse{
		Opened: &pm.TransactionResponse_Opened{
			FlowCheckpoint: flowCheckpoint,
			DeltaUpdates:   deltaUpdate,
		},
	}); err != nil {
		return fmt.Errorf("sending Opened response: %w", err)
	}

	return nil
}

// StageLoad potentially sends a previously staged Load into the stream,
// and then stages its arguments into request.Load.
func StageLoad(
	stream pm.Driver_TransactionsClient,
	request **pm.TransactionRequest,
	packedKey []byte,
) error {
	// Send current |request| if we would re-allocate.
	if *request != nil {
		var rem int
		if l := (*request).Load; cap(l.PackedKeys) != len(l.PackedKeys) {
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
		*request = &pm.TransactionRequest{
			Load: &pm.TransactionRequest_Load{
				Arena:      make(flow.Arena, 0, arenaSize),
				PackedKeys: make([]flow.Slice, 0, sliceSize),
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
	stream pm.Driver_TransactionsServer,
	response **pm.TransactionResponse,
	document json.RawMessage,
) error {
	// Send current |response| if we would re-allocate.
	if *response != nil {
		var rem int
		if l := (*response).Loaded; cap(l.DocsJson) != len(l.DocsJson) {
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
		*response = &pm.TransactionResponse{
			Loaded: &pm.TransactionResponse_Loaded{
				Arena:    make(flow.Arena, 0, arenaSize),
				DocsJson: make([]flow.Slice, 0, sliceSize),
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
	stream pm.Driver_TransactionsClient,
	request **pm.TransactionRequest,
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

	if err := stream.Send(&pm.TransactionRequest{
		Prepare: &pm.TransactionRequest_Prepare{
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
	stream pm.Driver_TransactionsServer,
	response **pm.TransactionResponse,
	driverCheckpoint []byte,
) error {
	// Flush partial Loaded response, if required.
	if *response != nil {
		if err := stream.Send(*response); err != nil {
			return fmt.Errorf("flushing final Loaded response: %w", err)
		}
		*response = nil
	}

	if err := stream.Send(&pm.TransactionResponse{
		Prepared: &pm.TransactionResponse_Prepared{
			DriverCheckpoint: driverCheckpoint,
		},
	}); err != nil {
		return fmt.Errorf("sending Prepared response: %w", err)
	}

	return nil
}

// StageStore potentially sends a previously staged Store into the stream,
// and then stages its arguments into request.Store.
func StageStore(
	stream pm.Driver_TransactionsClient,
	request **pm.TransactionRequest,
	packedKey []byte,
	packedValues []byte,
	doc json.RawMessage,
	exists bool,
) error {
	// Send current |request| if we would re-allocate.
	if *request != nil {
		var rem int
		if s := (*request).Store; cap(s.PackedKeys) != len(s.PackedKeys) {
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
		*request = &pm.TransactionRequest{
			Store: &pm.TransactionRequest_Store{
				Arena:        make(flow.Arena, 0, arenaSize),
				PackedKeys:   make([]flow.Slice, 0, sliceSize),
				PackedValues: make([]flow.Slice, 0, sliceSize),
				DocsJson:     make([]flow.Slice, 0, sliceSize),
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
	stream pm.Driver_TransactionsClient,
	request **pm.TransactionRequest,
) error {
	// Flush partial Store request, if required.
	if *request != nil {
		if err := stream.Send(*request); err != nil {
			return fmt.Errorf("flushing final Store request: %w", err)
		}
		*request = nil
	}

	if err := stream.Send(&pm.TransactionRequest{
		Commit: &pm.TransactionRequest_Commit{},
	}); err != nil {
		return fmt.Errorf("sending Commit request: %w", err)
	}

	return nil
}

// WriteCommitted writes a Committed response to the stream.
func WriteCommitted(
	stream pm.Driver_TransactionsServer,
	response **pm.TransactionResponse,
) error {
	// We must have sent Prepared prior to Committed.
	if *response != nil {
		panic("expected nil response")
	}

	if err := stream.Send(&pm.TransactionResponse{
		Committed: &pm.TransactionResponse_Committed{},
	}); err != nil {
		return fmt.Errorf("sending Committed response: %w", err)
	}

	return nil
}

// LoadIterator is an iterator over Load requests.
type LoadIterator struct {
	Key tuple.Tuple // Key of the next document to load.

	stream interface {
		Recv() (*pm.TransactionRequest, error)
	}
	req   pm.TransactionRequest
	index int
	err   error
}

// NewLoadIterator returns a *LoadIterator of the stream.
func NewLoadIterator(stream pm.Driver_TransactionsServer) *LoadIterator {
	return &LoadIterator{stream: stream}
}

// Poll returns true if there is at least one LoadRequest message with at least one key
// remaining to be read. This will read the next message from the stream if required. This will not
// advance the iterator, so it can be used to check whether the LoadIterator contains at least one
// key to load without actually consuming the next key. If false is returned, then there are no
// remaining keys and Poll must not be called again. Note that if Poll returns
// true, then Next may still return false if the LoadRequest message is malformed.
func (it *LoadIterator) Poll() bool {
	if it.err != nil || it.req.Prepare != nil {
		panic("Poll called again after having returned false")
	}

	// Must we read another request?
	if it.req.Load == nil || it.index == len(it.req.Load.PackedKeys) {
		if next, err := it.stream.Recv(); err != nil {
			if err == io.EOF && it.req.Load == nil {
				it.err = io.EOF // Clean shutdown before first Load.
			} else {
				it.err = fmt.Errorf("reading Load: %w", err)
			}
			return false
		} else {
			it.req = *next
		}

		if it.req.Prepare != nil {
			return false // Prepare ends the Load phase.
		} else if it.req.Load == nil || len(it.req.Load.PackedKeys) == 0 {
			it.err = fmt.Errorf("expected non-empty Load, got %#v", it.req)
			return false
		}
		it.index = 0
	}
	return true
}

// Next returns true if there is another Load and makes it available via Key.
// When a Prepare is read, or if an error is encountered, it returns false
// and must not be called again.
func (it *LoadIterator) Next() bool {
	if !it.Poll() {
		return false
	}
	var slice = it.req.Load.PackedKeys[it.index]
	it.Key, it.err = tuple.Unpack(it.req.Load.Arena.Bytes(slice))

	if it.err != nil {
		it.err = fmt.Errorf("unpacking Load key: %w", it.err)
		return false
	}

	it.index++
	return true
}

// Err returns an encountered error.
func (it *LoadIterator) Err() error {
	return it.err
}

// Prepare returns the Prepare request which caused iteration to terminate.
func (it *LoadIterator) Prepare() *pm.TransactionRequest_Prepare {
	return it.req.Prepare
}

// StoreIterator is an iterator over Store requests.
type StoreIterator struct {
	Key     tuple.Tuple     // Key of the next document to store.
	Values  tuple.Tuple     // Values of the next document to store.
	RawJSON json.RawMessage // Document to store.
	Exists  bool            // Does this document exist in the store already?

	stream interface {
		Recv() (*pm.TransactionRequest, error)
	}
	req   pm.TransactionRequest
	index int
	err   error
}

// NewStoreIterator returns a *StoreIterator of the stream.
func NewStoreIterator(stream pm.Driver_TransactionsServer) *StoreIterator {
	return &StoreIterator{stream: stream}
}

// Poll returns true if there is at least one StoreRequest message with at least one document
// remaining to be read. This will read the next message from the stream if required. This will not
// advance the iterator, so it can be used to check whether the StoreIterator contains at least one
// document to store without actually consuming the next document. If false is returned, then there
// are no remaining documents and Poll must not be called again. Note that if Poll returns true,
// then Next may still return false if the StoreRequest message is malformed.
func (it *StoreIterator) Poll() bool {
	if it.err != nil || it.req.Commit != nil {
		panic("Poll called again after having returned false")
	}
	// Must we read another request?
	if it.req.Store == nil || it.index == len(it.req.Store.PackedKeys) {
		if next, err := it.stream.Recv(); err != nil {
			it.err = fmt.Errorf("reading Store: %w", err)
			return false
		} else {
			it.req = *next
		}

		if it.req.Commit != nil {
			return false // Prepare ends the Store phase.
		} else if it.req.Store == nil || len(it.req.Store.PackedKeys) == 0 {
			it.err = fmt.Errorf("expected non-empty Store, got %#v", it.req)
			return false
		}
		it.index = 0
	}
	return true
}

// Next returns true if there is another Store and makes it available.
// When a Commit is read, or if an error is encountered, it returns false
// and must not be called again.
func (it *StoreIterator) Next() bool {
	if !it.Poll() {
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
	return true
}

// Err returns an encountered error.
func (it *StoreIterator) Err() error {
	return it.err
}

// Commit returns the Commit request which caused iteration to terminate.
func (it *StoreIterator) Commit() *pm.TransactionRequest_Commit {
	return it.req.Commit
}

const (
	arenaSize = 16 * 1024
	sliceSize = 32
)
