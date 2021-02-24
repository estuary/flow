package lifecycle

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/estuary/flow/go/fdb/tuple"
	"github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// WriteLoaded queues |document| into |response|,
// and sends it if it's over threshold.
func WriteLoaded(
	stream pm.Driver_TransactionsServer,
	response **pm.TransactionResponse,
	document json.RawMessage,
) error {
	if *response == nil {
		*response = &pm.TransactionResponse{
			Loaded: &pm.TransactionResponse_Loaded{
				Arena:    make(flow.Arena, 0, 3*LoadedResponseArenaSize/2),
				DocsJson: make([]flow.Slice, 0, 128),
			},
		}
	}

	var l = (*response).Loaded
	l.DocsJson = append(l.DocsJson, l.Arena.Add(document))

	if len(l.Arena) > LoadedResponseArenaSize {
		if err := stream.Send(*response); err != nil {
			return fmt.Errorf("sending Loaded response: %w", err)
		}
		*response = nil
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

// ReadAllLoads reads TransactionRequest.Load messages from the stream
// and invokes the callback for each contained document key.
// It returns an encountered TransactionRequest.Prepare,
// which marks the end of the Load phase of the transaction.
func ReadAllLoads(
	stream pm.Driver_TransactionsServer,
	cb func(key tuple.Tuple) error,
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
			} else if err = cb(key); err != nil {
				return nil, err
			}
		}
	}
}

// ReadAllStores does the thing
func ReadAllStores(
	stream pm.Driver_TransactionsServer,
	insertCallback, updateCallback func(key, values tuple.Tuple, doc json.RawMessage) error,
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
				if err = updateCallback(key, values, doc); err != nil {
					return nil, fmt.Errorf("update: %w", err)
				}
			} else {
				if err = insertCallback(key, values, doc); err != nil {
					return nil, fmt.Errorf("insert: %w", err)
				}
			}
		}
	}
}

// LoadedResponseArenaSize is the lower-bound target size for response arenas.
const LoadedResponseArenaSize = 16 * 1024
