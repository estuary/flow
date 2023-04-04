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

// Protocol routines for sending Request follow:

type RequestTx interface {
	Send(*Request) error
}

func WriteOpen(stream RequestTx, open *Request_Open) (Request, error) {
	var request = Request{
		Open: open,
	}
	if err := stream.Send(&request); err != nil {
		return Request{}, fmt.Errorf("sending Open: %w", err)
	}
	return request, nil
}

func WriteAcknowledge(stream RequestTx, request *Request) error {
	if request.Open == nil && request.StartCommit == nil {
		panic(fmt.Sprintf("expected prior request is Open or StartCommit, got %#v", request))
	}
	*request = Request{
		Acknowledge: &Request_Acknowledge{},
	}
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("sending Acknowledge request: %w", err)
	}
	return nil
}

func WriteLoad(
	stream RequestTx,
	request *Request,
	binding int,
	keyPacked []byte,
	keyJSON json.RawMessage,
) error {
	if request.Acknowledge == nil && request.Load == nil {
		panic(fmt.Sprintf("expected prior request is Acknowledge or Load, got %#v", request))
	}
	*request = Request{
		Load: &Request_Load{
			Binding:   uint32(binding),
			KeyPacked: keyPacked,
			KeyJson:   keyJSON,
		},
	}
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("sending Load request: %w", err)
	}
	return nil
}

func WriteFlush(stream RequestTx, request *Request) error {
	if request.Acknowledge == nil && request.Load == nil {
		panic(fmt.Sprintf("expected prior request is Acknowledge or Load, got %#v", request))
	}
	*request = Request{
		Flush: &Request_Flush{},
	}
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("sending Flush request: %w", err)
	}
	return nil
}

func WriteStore(
	stream RequestTx,
	request *Request,
	binding int,
	keyPacked []byte,
	keyJSON json.RawMessage,
	valuesPacked []byte,
	valuesJSON json.RawMessage,
	doc json.RawMessage,
	exists bool,
) error {
	if request.Flush == nil && request.Store == nil {
		panic(fmt.Sprintf("expected prior request is Flush or Store, got %#v", request))
	}
	*request = Request{
		Store: &Request_Store{
			Binding:      uint32(binding),
			KeyPacked:    keyPacked,
			KeyJson:      keyJSON,
			ValuesPacked: valuesPacked,
			ValuesJson:   valuesJSON,
			DocJson:      doc,
			Exists:       exists,
		},
	}
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("sending Store request: %w", err)
	}
	return nil
}

func WriteStartCommit(
	stream RequestTx,
	request *Request,
	checkpoint *pc.Checkpoint,
) error {
	if request.Flush == nil && request.Store == nil {
		panic(fmt.Sprintf("expected prior request is Flush or Store, got %#v", request))
	}
	*request = Request{
		StartCommit: &Request_StartCommit{
			RuntimeCheckpoint: checkpoint,
		},
	}
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("sending StartCommit request: %w", err)
	}
	return nil
}

// Protocol routines for receiving Request follow:

type RequestRx interface {
	Context() context.Context
	RecvMsg(interface{}) error
}

func ReadAcknowledge(stream RequestRx, request *Request) error {
	if request.Open == nil && request.StartCommit == nil {
		panic(fmt.Sprintf("expected prior request is Open or StartCommit, got %#v", request))
	} else if err := recv(stream, request); err != nil {
		return fmt.Errorf("reading Acknowledge: %w", err)
	} else if request.Acknowledge == nil {
		return fmt.Errorf("protocol error (expected Acknowledge, got %#v)", request)
	} else if err = request.Validate_(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}
	return nil
}

// LoadIterator is an iterator over Load requests.
type LoadIterator struct {
	Binding   int         // Binding index of this document to load.
	Key       tuple.Tuple // Key of the document to load.
	PackedKey []byte      // PackedKey of the document to load.

	stream      RequestRx
	request     *Request        // Request read into.
	awaitDoneCh <-chan struct{} // Signaled when last commit acknowledgment has completed.
	total       int             // Total number of iterated keys.
	err         error           // Terminal error.
}

// Context returns the Context of this LoadIterator.
func (it *LoadIterator) Context() context.Context { return it.stream.Context() }

// WaitForAcknowledged returns once the prior transaction has been fully acknowledged.
// Importantly, upon its return a materialization connector is free to issues loads
// to its backing store (as doing so cannot now violate read-committed semantics).
func (it *LoadIterator) WaitForAcknowledged() {
	if it.awaitDoneCh != nil {
		// Wait for await() to complete and then clear our local copy of its channel.
		_, it.awaitDoneCh = <-it.awaitDoneCh, nil
	}
}

// Next returns true if there is another Load and makes it available.
// When no Loads remain, or if an error is encountered, it returns false
// and must not be called again.
func (it *LoadIterator) Next() bool {
	if it.request.Acknowledge == nil && it.request.Load == nil {
		panic(fmt.Sprintf("expected prior request is Acknowledge or Load, got %#v", it.request))
	}

	// Fully zero the request prior to reading the next, because the
	// client may retain internal buffers that we previously returned.
	*it.request = Request{}

	// Read next `Load` request from `stream`.
	if err := recv(it.stream, it.request); err == io.EOF {
		if it.total != 0 {
			it.err = fmt.Errorf("unexpected EOF when there are loaded keys")
		} else {
			it.err = io.EOF // Clean shutdown.
			// If we didn't wait here, the await loop could see our return
			// as a cancellation (which is not intended).
			it.WaitForAcknowledged()
		}
		return false
	} else if err != nil {
		it.err = fmt.Errorf("reading Load: %w", err)
		return false
	} else if it.request.Load == nil {
		// No loads remain.

		// Block for clients which stage loads during the loop and query on
		// our return, and which don't bother to check WaitForAcknowledged().
		it.WaitForAcknowledged()
		return false
	} else if err = it.request.Validate_(); err != nil {
		it.err = fmt.Errorf("validation failed: %w", err)
		return false
	}
	var l = it.request.Load

	it.Binding = int(l.Binding)
	it.PackedKey = l.KeyPacked
	it.Key, it.err = tuple.Unpack(it.PackedKey)

	if it.err != nil {
		it.err = fmt.Errorf("unpacking Load key: %w", it.err)
		return false
	}

	it.total++
	return true
}

// Err returns an encountered error.
func (it *LoadIterator) Err() error {
	return it.err
}

func ReadFlush(request *Request) error {
	if request.Flush == nil {
		return fmt.Errorf("protocol error (expected Flush, got %#v)", request)
	} else if err := request.Validate_(); err != nil {
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

	stream  RequestRx
	request *Request // Request read into.
	total   int      // Total number of iterated stores.
	err     error    // Terminal error.
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

	// Fully zero the request prior to reading the next, because the
	// client may retain internal buffers that we previously returned.
	*it.request = Request{}

	// Read next `Store` request from `stream`.
	if err := recv(it.stream, it.request); err != nil {
		it.err = fmt.Errorf("reading Store: %w", err)
		return false
	} else if it.request.Store == nil {
		return false // No stores remain.
	} else if err = it.request.Validate_(); err != nil {
		it.err = fmt.Errorf("validation failed: %w", err)
	}
	var s = it.request.Store

	it.Binding = int(s.Binding)
	it.PackedKey = s.KeyPacked
	it.Key, it.err = tuple.Unpack(s.KeyPacked)
	if it.err != nil {
		it.err = fmt.Errorf("unpacking Store key: %w", it.err)
		return false
	}
	it.Values, it.err = tuple.Unpack(s.ValuesPacked)
	if it.err != nil {
		it.err = fmt.Errorf("unpacking Store values: %w", it.err)
		return false
	}
	it.RawJSON = s.DocJson
	it.Exists = s.Exists

	it.total++
	return true
}

// Err returns an encountered error.
func (it *StoreIterator) Err() error {
	return it.err
}

func ReadStartCommit(request *Request) (*pc.Checkpoint, error) {
	if request.StartCommit == nil {
		return nil, fmt.Errorf("protocol error (expected StartCommit, got %#v)", request)
	} else if err := request.Validate_(); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}
	return request.StartCommit.RuntimeCheckpoint, nil
}

// Protocol routines for sending Response follow:

type ResponseTx interface {
	Send(*Response) error
}

func WriteOpened(stream ResponseTx, opened *Response_Opened) (Response, error) {
	var response = Response{Opened: opened}

	if err := stream.Send(&response); err != nil {
		return Response{}, fmt.Errorf("sending Opened: %w", err)
	}
	return response, nil
}

func WriteAcknowledged(stream ResponseTx, response *Response) error {
	if response.Opened == nil && response.StartedCommit == nil {
		panic(fmt.Sprintf("expected prior response is Opened or StartedCommit, got %#v", response))
	}
	*response = Response{
		Acknowledged: &Response_Acknowledged{},
	}
	if err := stream.Send(response); err != nil {
		return fmt.Errorf("sending Acknowledged response: %w", err)
	}
	return nil
}

func WriteLoaded(
	stream ResponseTx,
	response *Response,
	binding int,
	document json.RawMessage,
) error {
	if response.Acknowledged == nil && response.Loaded == nil {
		panic(fmt.Sprintf("expected prior response is Acknowledged or Loaded, got %#v", response))
	}
	*response = Response{
		Loaded: &Response_Loaded{
			Binding: uint32(binding),
			DocJson: document,
		},
	}
	if err := stream.Send(response); err != nil {
		return fmt.Errorf("sending Loaded response: %w", err)
	}
	return nil
}

func WriteFlushed(stream ResponseTx, response *Response) error {
	if response.Acknowledged == nil && response.Loaded == nil {
		panic(fmt.Sprintf("expected prior response is Acknowledged or Loaded, got %#v", response))
	}
	*response = Response{
		Flushed: &Response_Flushed{},
	}
	if err := stream.Send(response); err != nil {
		return fmt.Errorf("sending Flushed response: %w", err)
	}
	return nil
}

func WriteStartedCommit(
	stream ResponseTx,
	response *Response,
	checkpoint *pf.ConnectorState,
) error {
	if response.Flushed == nil {
		panic(fmt.Sprintf("expected prior response is Flushed, got %#v", response))
	}
	*response = Response{
		StartedCommit: &Response_StartedCommit{
			State: checkpoint,
		},
	}
	if err := stream.Send(response); err != nil {
		return fmt.Errorf("sending StartedCommit: %w", err)
	}
	return nil
}

// Protocol routines for reading Response follow:

type ResponseRx interface {
	RecvMsg(interface{}) error
}

func ReadOpened(stream ResponseRx) (Response, error) {
	var response Response

	if err := recv(stream, &response); err != nil {
		return Response{}, fmt.Errorf("reading Opened: %w", err)
	} else if response.Opened == nil {
		return Response{}, fmt.Errorf("protocol error (expected Opened, got %#v)", response)
	} else if err = response.Validate(); err != nil {
		return Response{}, fmt.Errorf("validation failed: %w", err)
	}
	return response, nil
}

func ReadAcknowledged(stream ResponseRx, response *Response) error {
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

func ReadLoaded(stream ResponseRx, response *Response) (*Response_Loaded, error) {
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

func ReadFlushed(response *Response) error {
	if response.Flushed == nil {
		return fmt.Errorf("protocol error (expected Flushed, got %#v)", response)
	} else if err := response.Validate(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}
	return nil
}

func ReadStartedCommit(stream ResponseRx, response *Response) (*pf.ConnectorState, error) {
	if response.Flushed == nil {
		panic(fmt.Sprintf("expected prior response is Flushed, got %#v", response))
	} else if err := recv(stream, response); err != nil {
		return nil, fmt.Errorf("reading StartedCommit: %w", err)
	} else if response.StartedCommit == nil {
		return nil, fmt.Errorf("protocol error (expected StartedCommit, got %#v)", response)
	} else if err = response.Validate(); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}
	return response.StartedCommit.State, nil
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
