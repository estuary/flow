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

// ReadAllLoads reads Load requests from the stream, invoking the callback for
// each contained document key, until it reads and returns a Prepare request.
func ReadAllLoads(
	stream pm.Driver_TransactionsServer,
	loadFn func(key tuple.Tuple) error,
) (*pm.TransactionRequest_Prepare, error) {
	for i := 0; ; i++ {
		var request, err = stream.Recv()
		if i == 0 && err == io.EOF {
			return nil, io.EOF // Clean shutdown of the stream.
		} else if err != nil {
			return nil, fmt.Errorf("stream.Recv: %w", err)
		} else if request.Prepare != nil {
			return request.Prepare, nil
		} else if request.Load == nil {
			return nil, fmt.Errorf("expected Load, got %#v", request)
		}
		var l = request.Load

		for _, slice := range l.PackedKeys {
			if key, err := tuple.Unpack(l.Arena.Bytes(slice)); err != nil {
				return nil, fmt.Errorf("unpacking key: %w", err)
			} else if err = loadFn(key); err != nil {
				return nil, err
			}
		}
	}
}

// ReadAllStores reads Store requests from the stream, invoking the insert
// or update callback for each contained document, until it reads and returns
// a Commit request.
func ReadAllStores(
	stream pm.Driver_TransactionsServer,
	insertFn, updateFn func(key, values tuple.Tuple, doc json.RawMessage) error,
) (*pm.TransactionRequest_Commit, error) {
	for {
		var request, err = stream.Recv()
		if err != nil {
			return nil, fmt.Errorf("stream.Recv: %w", err)
		} else if request.Commit != nil {
			return request.Commit, nil
		} else if request.Store == nil {
			return nil, fmt.Errorf("expected Store, got %#v", request)
		}
		var s = request.Store

		for i := range s.PackedKeys {
			key, err := tuple.Unpack(s.Arena.Bytes(s.PackedKeys[i]))
			if err != nil {
				return nil, fmt.Errorf("unpacking key: %w", err)
			}
			values, err := tuple.Unpack(s.Arena.Bytes(s.PackedValues[i]))
			if err != nil {
				return nil, fmt.Errorf("unpacking value: %w", err)
			}
			var doc = s.Arena.Bytes(s.DocsJson[i])

			if s.Exists[i] {
				if err = updateFn(key, values, doc); err != nil {
					return nil, fmt.Errorf("update: %w", err)
				}
			} else {
				if err = insertFn(key, values, doc); err != nil {
					return nil, fmt.Errorf("insert: %w", err)
				}
			}
		}
	}
}

const (
	arenaSize = 16 * 1024
	sliceSize = 32
)
