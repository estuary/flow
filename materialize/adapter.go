package materialize

// TODO(johnny): While they started a bit different, over time this file and
// capture/adapter.go now look essentially identical in terms of
// structure. They do differ on interfaces, however, making re-use a challenge.
// If contemplating a change here, make it there as well.
// And when generally available, consider using Go generics ?

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// transactionRequestError is a channel-oriented wrapper of TransactionRequest.
type transactionRequestError struct {
	*TransactionRequest
	Error error
}

// TransactionResponseError is a channel-oriented wrapper of TransactionResponse.
type TransactionResponseError struct {
	*TransactionResponse
	Error error
}

// TransactionResponseChannel spawns a goroutine which receives
// from the stream and sends responses into the returned channel,
// which is closed after the first encountered read error.
// As an optimization, it avoids this read loop if the stream
// is an in-process adapter.
func TransactionResponseChannel(stream Driver_TransactionsClient) <-chan TransactionResponseError {
	if adapter, ok := stream.(*adapterStreamClient); ok {
		return adapter.rx
	}

	var ch = make(chan TransactionResponseError, 4)
	go func() {
		for {
			// Use Recv because ownership of |m| is transferred to |ch|,
			// and |m| cannot be reused.
			var m, err = stream.Recv()

			if err == nil {
				ch <- TransactionResponseError{TransactionResponse: m}
				continue
			}

			if err != io.EOF {
				ch <- TransactionResponseError{Error: err}
			}
			close(ch)
			return
		}
	}()

	return ch
}

// Rx receives from a TransactionResponse channel.
// It de-structures TransactionResponse into its parts,
// and also returns an explicit io.EOF for channel closures.
func Rx(ch <-chan TransactionResponseError, block bool) (*TransactionResponse, error) {
	var rx TransactionResponseError
	var ok bool

	if block {
		rx, ok = <-ch
	} else {
		select {
		case rx, ok = <-ch:
		default:
			ok = true
		}
	}

	if !ok {
		return nil, io.EOF
	} else if rx.Error != nil {
		return nil, rx.Error
	} else {
		return rx.TransactionResponse, nil
	}
}

// AdaptServerToClient wraps an in-process DriverServer to provide a DriverClient.
func AdaptServerToClient(srv DriverServer) DriverClient {
	return adapter{srv}
}

// adapter is DriverClient that wraps an in-process DriverServer.
type adapter struct{ DriverServer }

func (a adapter) Spec(ctx context.Context, in *SpecRequest, opts ...grpc.CallOption) (*SpecResponse, error) {
	return a.DriverServer.Spec(ctx, in)
}

func (a adapter) Validate(ctx context.Context, in *ValidateRequest, opts ...grpc.CallOption) (*ValidateResponse, error) {
	return a.DriverServer.Validate(ctx, in)
}

func (a adapter) ApplyUpsert(ctx context.Context, in *ApplyRequest, opts ...grpc.CallOption) (*ApplyResponse, error) {
	return a.DriverServer.ApplyUpsert(ctx, in)
}

func (a adapter) ApplyDelete(ctx context.Context, in *ApplyRequest, opts ...grpc.CallOption) (*ApplyResponse, error) {
	return a.DriverServer.ApplyDelete(ctx, in)
}

func (a adapter) Transactions(ctx context.Context, opts ...grpc.CallOption) (Driver_TransactionsClient, error) {
	var reqCh = make(chan transactionRequestError, 4)
	var respCh = make(chan TransactionResponseError, 4)
	var doneCh = make(chan struct{})

	var clientStream = &adapterStreamClient{
		ctx:  ctx,
		tx:   reqCh,
		rx:   respCh,
		done: doneCh,
	}
	var serverStream = &adapterStreamServer{
		ctx: ctx,
		tx:  respCh,
		rx:  reqCh,
	}

	go func() (err error) {
		defer func() {
			if err != nil {
				respCh <- TransactionResponseError{Error: err}
			}
			close(respCh)
			close(doneCh)
		}()
		return a.DriverServer.Transactions(serverStream)
	}()

	return clientStream, nil
}

type adapterStreamClient struct {
	ctx  context.Context
	tx   chan<- transactionRequestError
	rx   <-chan TransactionResponseError
	done <-chan struct{}
}

func (a *adapterStreamClient) Context() context.Context {
	return a.ctx
}

func (a *adapterStreamClient) Send(m *TransactionRequest) error {
	select {
	case a.tx <- transactionRequestError{TransactionRequest: m}:
		return nil
	case <-a.done:
		// The server already closed the RPC, revoking our ability to transmit.
		// Match gRPC behavior of returning io.EOF on Send, and the real error on Recv.
		return io.EOF
	}
}

func (a *adapterStreamClient) CloseSend() error {
	close(a.tx)
	return nil
}

func (a *adapterStreamClient) Recv() (*TransactionResponse, error) {
	if m, ok := <-a.rx; ok {
		return m.TransactionResponse, m.Error
	}
	return nil, io.EOF
}

// Remaining panic implementations of grpc.ClientStream follow:

func (a *adapterStreamClient) Header() (metadata.MD, error) { panic("not implemented") }
func (a *adapterStreamClient) Trailer() metadata.MD         { panic("not implemented") }
func (a *adapterStreamClient) SendMsg(m interface{}) error  { panic("not implemented") } // Use Send.
func (a *adapterStreamClient) RecvMsg(m interface{}) error  { panic("not implemented") }

type adapterStreamServer struct {
	ctx context.Context
	tx  chan<- TransactionResponseError
	rx  <-chan transactionRequestError
}

var _ Driver_TransactionsServer = new(adapterStreamServer)

func (a *adapterStreamServer) Context() context.Context {
	return a.ctx
}

func (a *adapterStreamServer) Send(m *TransactionResponse) error {
	// Under the gRPC model, the server controls RPC termination. The client cannot
	// revoke the server's ability to send (in the absence of a broken transport,
	// which we don't model here).
	a.tx <- TransactionResponseError{TransactionResponse: m}
	return nil
}

func (a *adapterStreamServer) Recv() (*TransactionRequest, error) {
	if m, ok := <-a.rx; ok {
		return m.TransactionRequest, m.Error
	}
	return nil, io.EOF
}

func (a *adapterStreamServer) RecvMsg(m interface{}) error {
	if mm, ok := <-a.rx; ok && mm.Error == nil {
		*m.(*TransactionRequest) = *mm.TransactionRequest
		return nil
	} else if ok {
		return mm.Error
	}
	return io.EOF
}

// Remaining panic implementations of grpc.ServerStream follow:

func (a *adapterStreamServer) SetHeader(metadata.MD) error  { panic("not implemented") }
func (a *adapterStreamServer) SendHeader(metadata.MD) error { panic("not implemented") }
func (a *adapterStreamServer) SetTrailer(metadata.MD)       { panic("not implemented") }
func (a *adapterStreamServer) SendMsg(m interface{}) error  { panic("not implemented") } // Use Send().
