package driver

import (
	"context"
	"io"

	pm "github.com/estuary/flow/go/protocols/materialize"
	"google.golang.org/grpc"
)

// TransactionRequest is a channel-oriented wrapper of pf.TransactionRequest.
type TransactionRequest struct {
	*pm.TransactionRequest
	Error error
}

// TransactionResponse is a channel-oriented wrapper of pf.TransactionResponse.
type TransactionResponse struct {
	*pm.TransactionResponse
	Error error
}

// TransactionResponseChannel spawns a goroutine which receives
// from the stream and sends responses into the returned channel,
// which is closed after the first encountered read error.
// As an optimization, it avoids this read loop if the stream
// is an in-process adapter.
func TransactionResponseChannel(stream pm.Driver_TransactionsClient) <-chan TransactionResponse {
	if adapter, ok := stream.(*adapterStreamClient); ok {
		return adapter.rx
	}

	var ch = make(chan TransactionResponse, 4)
	go func() {
		for {
			var m, err = stream.Recv()

			if err == nil {
				ch <- TransactionResponse{TransactionResponse: m}
				continue
			}

			if err != io.EOF {
				ch <- TransactionResponse{Error: err}
			}
			close(ch)
			return
		}
	}()

	return ch
}

// adapter is pm.DriverClient that wraps an in-process pm.DriverServer.
type adapter struct{ pm.DriverServer }

func (a adapter) Validate(ctx context.Context, in *pm.ValidateRequest, opts ...grpc.CallOption) (*pm.ValidateResponse, error) {
	return a.DriverServer.Validate(ctx, in)
}

func (a adapter) Apply(ctx context.Context, in *pm.ApplyRequest, opts ...grpc.CallOption) (*pm.ApplyResponse, error) {
	return a.DriverServer.Apply(ctx, in)
}

func (a adapter) Transactions(ctx context.Context, opts ...grpc.CallOption) (pm.Driver_TransactionsClient, error) {
	var reqCh = make(chan TransactionRequest, 4)
	var respCh = make(chan TransactionResponse, 4)
	var doneCh = make(chan struct{})

	var clientStream = &adapterStreamClient{
		ctx:          ctx,
		tx:           reqCh,
		rx:           respCh,
		done:         doneCh,
		ClientStream: nil,
	}
	var serverStream = &adapterStreamServer{
		ctx:          ctx,
		tx:           respCh,
		rx:           reqCh,
		ServerStream: nil,
	}

	go func() (err error) {
		defer func() {
			if err != nil {
				respCh <- TransactionResponse{Error: err}
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
	tx   chan<- TransactionRequest
	rx   <-chan TransactionResponse
	done <-chan struct{}

	// Embed a nil ClientStream to provide remaining methods of the pm.Driver_TransactionClient
	// interface. It's left as nil, so these methods will panic if called!
	grpc.ClientStream
}

func (a *adapterStreamClient) Context() context.Context {
	return a.ctx
}

func (a *adapterStreamClient) Send(m *pm.TransactionRequest) error {
	select {
	case a.tx <- TransactionRequest{TransactionRequest: m}:
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

func (a *adapterStreamClient) Recv() (*pm.TransactionResponse, error) {
	if m, ok := <-a.rx; ok {
		return m.TransactionResponse, m.Error
	}
	return nil, io.EOF
}

type adapterStreamServer struct {
	ctx context.Context
	tx  chan<- TransactionResponse
	rx  <-chan TransactionRequest

	// Embed a nil ServerStream to provide remaining methods of the pm.Driver_TransactionServer
	// interface. It's left as nil, so these methods will panic if called!
	grpc.ServerStream
}

var _ pm.Driver_TransactionsServer = new(adapterStreamServer)

func (a *adapterStreamServer) Context() context.Context {
	return a.ctx
}

func (a *adapterStreamServer) Send(m *pm.TransactionResponse) error {
	// Under the gRPC model, the server controls RPC termination. The client cannot
	// revoke the server's ability to send (in the absence of a broken transport,
	// which we don't model here).
	a.tx <- TransactionResponse{TransactionResponse: m}
	return nil
}

func (a *adapterStreamServer) Recv() (*pm.TransactionRequest, error) {
	if m, ok := <-a.rx; ok {
		return m.TransactionRequest, m.Error
	}
	return nil, io.EOF
}
