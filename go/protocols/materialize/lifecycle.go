package materialize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/gogo/protobuf/proto"
	pc "go.gazette.dev/core/consumer/protocol"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// Protocol routines for sending TransactionRequest follow:

type TransactionRequestTx interface {
	Send(*TransactionRequest) error
}

func WriteOpen(stream TransactionRequestTx, open *TransactionRequest_Open) (TransactionRequest, error) {
	var request = TransactionRequest{Open: open}

	if err := stream.Send(&request); err != nil {
		return TransactionRequest{}, fmt.Errorf("sending Open: %w", err)
	}
	return request, nil
}

func WriteAcknowledge(stream TransactionRequestTx, request *TransactionRequest) error {
	if request.Open == nil && request.StartCommit == nil {
		panic(fmt.Sprintf("expected prior request is Open or StartCommit, got %#v", request))
	}
	*request = TransactionRequest{
		Acknowledge: &TransactionRequest_Acknowledge{},
	}
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("sending Acknowledge request: %w", err)
	}
	return nil
}

func WriteLoad(
	stream TransactionRequestTx,
	request *TransactionRequest,
	binding int,
	packedKey []byte,
) error {
	if request.Acknowledge == nil && request.Load == nil {
		panic(fmt.Sprintf("expected prior request is Acknowledge or Load, got %#v", request))
	}

	// Send current `request` if it uses a different binding or would re-allocate.
	if request.Load != nil {
		var rem int
		if l := (*request).Load; int(l.Binding) != binding {
			rem = -1 // Must flush this request.
		} else if cap(l.PackedKeys) != len(l.PackedKeys) {
			rem = cap(l.Arena) - len(l.Arena)
		}
		if rem < len(packedKey) {
			if err := stream.Send(request); err != nil {
				return fmt.Errorf("sending Load request: %w", err)
			}
			request.Load = nil
		}
	}

	if request.Load == nil {
		*request = TransactionRequest{
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

func WriteFlush(
	stream TransactionRequestTx,
	request *TransactionRequest,
	deprecatedCheckpoint pc.Checkpoint, // Will be removed.
) error {
	if request.Acknowledge == nil && request.Load == nil {
		panic(fmt.Sprintf("expected prior request is Acknowledge or Load, got %#v", request))
	}
	// Flush partial Load request, if required.
	if request.Load != nil {
		if err := stream.Send(request); err != nil {
			return fmt.Errorf("flushing final Load request: %w", err)
		}
		*request = TransactionRequest{}
	}

	var checkpointBytes, err = deprecatedCheckpoint.Marshal()
	if err != nil {
		panic(err) // Cannot fail.
	}
	*request = TransactionRequest{
		Flush: &TransactionRequest_Flush{
			DeprecatedRuntimeCheckpoint: checkpointBytes,
		},
	}
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("sending Flush request: %w", err)
	}
	return nil
}

func WriteStore(
	stream TransactionRequestTx,
	request *TransactionRequest,
	binding int,
	packedKey []byte,
	packedValues []byte,
	doc json.RawMessage,
	exists bool,
) error {
	if request.Flush == nil && request.Store == nil {
		panic(fmt.Sprintf("expected prior request is Flush or Store, got %#v", request))
	}

	// Send current |request| if we would re-allocate.
	if request.Store != nil {
		var rem int
		if s := (*request).Store; int(s.Binding) != binding {
			rem = -1 // Must flush this request.
		} else if cap(s.PackedKeys) != len(s.PackedKeys) {
			rem = cap(s.Arena) - len(s.Arena)
		}
		if need := len(packedKey) + len(packedValues) + len(doc); need > rem {
			if err := stream.Send(request); err != nil {
				return fmt.Errorf("sending Store request: %w", err)
			}
			*request = TransactionRequest{}
		}
	}

	if request.Store == nil {
		*request = TransactionRequest{
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

func WriteStartCommit(
	stream TransactionRequestTx,
	request *TransactionRequest,
	checkpoint pc.Checkpoint,
) error {
	if request.Flush == nil && request.Store == nil {
		panic(fmt.Sprintf("expected prior request is Flush or Store, got %#v", request))
	}
	// Flush partial Store request, if required.
	if request.Store != nil {
		if err := stream.Send(request); err != nil {
			return fmt.Errorf("flushing final Store request: %w", err)
		}
		*request = TransactionRequest{}
	}

	var checkpointBytes, err = checkpoint.Marshal()
	if err != nil {
		panic(err) // Cannot fail.
	}
	*request = TransactionRequest{
		StartCommit: &TransactionRequest_StartCommit{
			RuntimeCheckpoint: checkpointBytes,
		},
	}
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("sending StartCommit request: %w", err)
	}
	return nil
}

// Protocol routines for receiving TransactionRequest follow:

type TransactionRequestRx interface {
	Context() context.Context
	RecvMsg(interface{}) error
}

func ReadOpen(stream TransactionRequestRx) (TransactionRequest, error) {
	var request TransactionRequest

	if err := recv(stream, &request); err != nil {
		return TransactionRequest{}, fmt.Errorf("reading Open: %w", err)
	} else if request.Open == nil {
		return TransactionRequest{}, fmt.Errorf("protocol error (expected Open, got %#v)", request)
	} else if err = request.Validate(); err != nil {
		return TransactionRequest{}, fmt.Errorf("validation failed: %w", err)
	}
	return request, nil
}

func ReadAcknowledge(stream TransactionRequestRx, request *TransactionRequest) error {
	if request.Open == nil && request.StartCommit == nil {
		panic(fmt.Sprintf("expected prior request is Open or StartCommit, got %#v", request))
	} else if err := recv(stream, request); err != nil {
		return fmt.Errorf("reading Acknowledge: %w", err)
	} else if request.Acknowledge == nil {
		return fmt.Errorf("protocol error (expected Acknowledge, got %#v)", request)
	} else if err = request.Validate(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}
	return nil
}

// LoadIterator is an iterator over Load requests.
type LoadIterator struct {
	Binding   int         // Binding index of this document to load.
	Key       tuple.Tuple // Key of the document to load.
	PackedKey []byte      // PackedKey of the document to load.

	stream  TransactionRequestRx
	request *TransactionRequest // Request read into.
	index   int                 // Last-returned document index within `request`.
	total   int                 // Total number of iterated keys.
	err     error               // Terminal error.
}

// Context returns the Context of this LoadIterator.
func (it *LoadIterator) Context() context.Context { return it.stream.Context() }

// Next returns true if there is another Load and makes it available.
// When no Loads remain, or if an error is encountered, it returns false
// and must not be called again.
func (it *LoadIterator) Next() bool {
	if it.request.Acknowledge == nil && it.request.Load == nil {
		panic(fmt.Sprintf("expected prior request is Acknowledge or Load, got %#v", it.request))
	}
	// Read next `Load` request from `stream`?
	if it.request.Acknowledge != nil || it.index == len(it.request.Load.PackedKeys) {
		if err := recv(it.stream, it.request); err == io.EOF {
			if it.total != 0 {
				it.err = fmt.Errorf("unexpected EOF when there are loaded keys")
			} else {
				it.err = io.EOF // Clean shutdown.
			}
			return false
		} else if err != nil {
			it.err = fmt.Errorf("reading Load: %w", err)
			return false
		} else if it.request.Load == nil {
			return false // No loads remain.
		} else if err = it.request.Validate(); err != nil {
			it.err = fmt.Errorf("validation failed: %w", err)
			return false
		}
		it.index = 0
		it.Binding = int(it.request.Load.Binding)
	}

	var l = it.request.Load

	it.PackedKey = l.Arena.Bytes(l.PackedKeys[it.index])
	it.Key, it.err = tuple.Unpack(it.PackedKey)

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
	return it.err
}

func ReadFlush(request *TransactionRequest) error {
	if request.Flush == nil {
		return fmt.Errorf("protocol error (expected Flush, got %#v)", request)
	} else if err := request.Validate(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}
	return nil
}

// StoreIterator is an iterator over Store requests.
type StoreIterator struct {
	Binding   int             // Binding index of this stored document.
	Exists    bool            // Does this document exist in the store already?
	Key       tuple.Tuple     // Key of the document to store.
	PackedKey []byte          // PackedKey of the document to store.
	RawJSON   json.RawMessage // Document to store.
	Values    tuple.Tuple     // Values of the document to store.

	stream  TransactionRequestRx
	request *TransactionRequest // Request read into.
	index   int                 // Last-returned document index within `request`
	total   int                 // Total number of iterated stores.
	err     error               // Terminal error.
}

// Context returns the Context of this StoreIterator.
func (it *StoreIterator) Context() context.Context { return it.stream.Context() }

// Next returns true if there is another Store and makes it available.
// When no Stores remain, or if an error is encountered, it returns false
// and must not be called again.
func (it *StoreIterator) Next() bool {
	if it.request.Flush == nil && it.request.Store == nil {
		panic(fmt.Sprintf("expected prior request is Flush or Store, got %#v", it.request))
	}
	// Read next `Store` request from `stream`?
	if it.request.Flush != nil || it.index == len(it.request.Store.PackedKeys) {
		if err := recv(it.stream, it.request); err != nil {
			it.err = fmt.Errorf("reading Store: %w", err)
			return false
		} else if it.request.Store == nil {
			return false // No stores remain.
		} else if err = it.request.Validate(); err != nil {
			it.err = fmt.Errorf("validation failed: %w", err)
		}
		it.index = 0
		it.Binding = int(it.request.Store.Binding)
	}

	var s = it.request.Store

	it.PackedKey = s.Arena.Bytes(s.PackedKeys[it.index])
	it.Key, it.err = tuple.Unpack(it.PackedKey)
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

func ReadStartCommit(request *TransactionRequest) (runtimeCheckpoint []byte, _ error) {
	if request.StartCommit == nil {
		return nil, fmt.Errorf("protocol error (expected StartCommit, got %#v)", request)
	} else if err := request.Validate(); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}
	return request.StartCommit.RuntimeCheckpoint, nil
}

// Protocol routines for sending TransactionResponse follow:

type TransactionResponseTx interface {
	Send(*TransactionResponse) error
}

func WriteOpened(stream TransactionResponseTx, opened *TransactionResponse_Opened) (TransactionResponse, error) {
	var response = TransactionResponse{Opened: opened}

	if err := stream.Send(&response); err != nil {
		return TransactionResponse{}, fmt.Errorf("sending Opened: %w", err)
	}
	return response, nil
}

func WriteAcknowledged(stream TransactionResponseTx, response *TransactionResponse) error {
	if response.Opened == nil && response.StartedCommit == nil {
		panic(fmt.Sprintf("expected prior response is Opened or StartedCommit, got %#v", response))
	}
	*response = TransactionResponse{
		Acknowledged: &TransactionResponse_Acknowledged{},
	}
	if err := stream.Send(response); err != nil {
		return fmt.Errorf("sending Acknowledged response: %w", err)
	}
	return nil
}

func WriteLoaded(
	stream TransactionResponseTx,
	response *TransactionResponse,
	binding int,
	document json.RawMessage,
) error {
	if response.Acknowledged == nil && response.Loaded == nil {
		panic(fmt.Sprintf("expected prior response is Acknowledged or Loaded, got %#v", response))
	}

	// Send current `response` if it uses a different binding or would re-allocate.
	if response.Loaded != nil {
		var rem int
		if l := (*response).Loaded; int(l.Binding) != binding {
			rem = -1 // Must flush this response.
		} else if cap(l.DocsJson) != len(l.DocsJson) {
			rem = cap(l.Arena) - len(l.Arena)
		}
		if rem < len(document) {
			if err := stream.Send(response); err != nil {
				return fmt.Errorf("sending Loaded response: %w", err)
			}
			response.Loaded = nil
		}
	}

	if response.Loaded == nil {
		*response = TransactionResponse{
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

func WriteFlushed(stream TransactionResponseTx, response *TransactionResponse) error {
	if response.Acknowledged == nil && response.Loaded == nil {
		panic(fmt.Sprintf("expected prior response is Acknowledged or Loaded, got %#v", response))
	}
	// Flush partial Loaded response, if required.
	if response.Loaded != nil {
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("flushing final Loaded response: %w", err)
		}
		*response = TransactionResponse{}
	}

	*response = TransactionResponse{
		// Flushed as-a DriverCheckpoint is deprecated and will be removed.
		Flushed: &pf.DriverCheckpoint{
			DriverCheckpointJson: []byte("{}"),
			Rfc7396MergePatch:    true,
		},
	}
	if err := stream.Send(response); err != nil {
		return fmt.Errorf("sending Flushed response: %w", err)
	}
	return nil
}

func WriteStartedCommit(
	stream TransactionResponseTx,
	response *TransactionResponse,
	checkpoint *pf.DriverCheckpoint,
) error {
	if response.Flushed == nil {
		panic(fmt.Sprintf("expected prior response is Flushed, got %#v", response))
	}
	*response = TransactionResponse{
		StartedCommit: &TransactionResponse_StartedCommit{
			DriverCheckpoint: checkpoint,
		},
	}
	if err := stream.Send(response); err != nil {
		return fmt.Errorf("sending StartedCommit: %w", err)
	}
	return nil
}

// Protocol routines for reading TransactionResponse follow:

type TransactionResponseRx interface {
	RecvMsg(interface{}) error
}

func ReadOpened(stream TransactionResponseRx) (TransactionResponse, error) {
	var response TransactionResponse

	if err := recv(stream, &response); err != nil {
		return TransactionResponse{}, fmt.Errorf("reading Opened: %w", err)
	} else if response.Opened == nil {
		return TransactionResponse{}, fmt.Errorf("protocol error (expected Opened, got %#v)", response)
	} else if err = response.Validate(); err != nil {
		return TransactionResponse{}, fmt.Errorf("validation failed: %w", err)
	}
	return response, nil
}

func ReadAcknowledged(stream TransactionResponseRx, response *TransactionResponse) error {
	if response.Opened == nil && response.StartedCommit == nil {
		panic(fmt.Sprintf("expected prior response is Opened or StartedCommit, got %#v", response))
	} else if err := recv(stream, response); err != nil {
		return fmt.Errorf("reading Acknowledged: %w", err)
	} else if response.Acknowledged == nil {
		return fmt.Errorf("protocol error (expected Acknowledged, got %#v)", response)
	} else if err = response.Validate(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}
	return nil
}

func ReadLoaded(stream TransactionResponseRx, response *TransactionResponse) (*TransactionResponse_Loaded, error) {
	if response.Acknowledged == nil && response.Loaded == nil {
		panic(fmt.Sprintf("expected prior response is Acknowledged or Loaded, got %#v", response))
	} else if err := recv(stream, response); err == io.EOF && response.Acknowledged != nil {
		return nil, io.EOF // Clean EOF.
	} else if err != nil {
		return nil, fmt.Errorf("reading Loaded: %w", err)
	} else if response.Loaded == nil {
		return nil, nil // No loads remain.
	} else if err = response.Validate(); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}
	return response.Loaded, nil
}

func ReadFlushed(response *TransactionResponse) (deprecatedDriverCP *pf.DriverCheckpoint, _ error) {
	if response.Flushed == nil {
		return nil, fmt.Errorf("protocol error (expected Flushed, got %#v)", response)
	} else if err := response.Validate(); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}
	return response.Flushed, nil
}

func ReadStartedCommit(stream TransactionResponseRx, response *TransactionResponse) (*pf.DriverCheckpoint, error) {
	if response.Flushed == nil {
		panic(fmt.Sprintf("expected prior response is Flushed, got %#v", response))
	} else if err := recv(stream, response); err != nil {
		return nil, fmt.Errorf("reading StartedCommit: %w", err)
	} else if response.StartedCommit == nil {
		return nil, fmt.Errorf("protocol error (expected StartedCommit, got %#v)", response)
	} else if err = response.Validate(); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}
	return response.StartedCommit.DriverCheckpoint, nil
}

func recv(
	stream interface{ RecvMsg(interface{}) error },
	message proto.Message,
) error {
	if err := stream.RecvMsg(message); err == nil {
		return nil
	} else if status, ok := status.FromError(err); ok && status.Code() == codes.Internal {
		return errors.New(status.Message())
	} else if status.Code() == codes.Canceled {
		return context.Canceled
	} else {
		return err
	}
}

const (
	arenaSize = 16 * 1024
	sliceSize = 32
)
