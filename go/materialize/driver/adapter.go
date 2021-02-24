package driver

import (
	"context"
	"io"

	pm "github.com/estuary/flow/go/protocols/materialize"
	"google.golang.org/grpc"
)

// adapter is pm.DriverClient that wraps an in-process pm.DriverServer.
type adapter struct{ pm.DriverServer }

func (a adapter) Validate(ctx context.Context, in *pm.ValidateRequest, opts ...grpc.CallOption) (*pm.ValidateResponse, error) {
	return a.DriverServer.Validate(ctx, in)
}

func (a adapter) Apply(ctx context.Context, in *pm.ApplyRequest, opts ...grpc.CallOption) (*pm.ApplyResponse, error) {
	return a.DriverServer.Apply(ctx, in)
}

func (a adapter) Transactions(ctx context.Context, opts ...grpc.CallOption) (pm.Driver_TransactionsClient, error) {
	var reqCh = make(chan *pm.TransactionRequest, 4)
	var respCh = make(chan *pm.TransactionResponse, 4)
	var clientCloseCh = make(chan struct{})
	var serverCloseCh = make(chan struct{})
	var serverErr = new(error)

	var clientStream = &adapterStreamClient{
		ctx:          ctx,
		tx:           reqCh,
		rx:           respCh,
		rxClose:      serverCloseCh,
		finalErr:     serverErr,
		ClientStream: nil,
	}

	var serverStream = &adapterStreamServer{
		ctx:          ctx,
		tx:           respCh,
		rx:           reqCh,
		rxClose:      clientCloseCh,
		ServerStream: nil,
	}

	go func() (err error) {
		defer func() {
			*serverErr = err
			close(serverCloseCh)
		}()
		return a.DriverServer.Transactions(serverStream)
	}()

	return clientStream, nil
}

type adapterStreamClient struct {
	ctx      context.Context
	tx       chan<- *pm.TransactionRequest
	rx       <-chan *pm.TransactionResponse
	rxClose  <-chan struct{}
	finalErr *error

	// Embed a nil ClientStream to provide remaining methods of the pm.Driver_TransactionClient
	// interface. It's left as nil, so these methods will panic if called!
	grpc.ClientStream
}

func (a *adapterStreamClient) Context() context.Context {
	return a.ctx
}

func (a *adapterStreamClient) Send(m *pm.TransactionRequest) error {
	select {
	case a.tx <- m:
		return nil
	case <-a.rxClose:
		// The server may have already closed the RPC, revoking our ability to transmit.
		// Match gRPC behavior of returning io.EOF on Send, and the real error on Recv.
		return io.EOF
	}
}

func (a *adapterStreamClient) CloseSend() error {
	close(a.tx)
	return nil
}

func (a *adapterStreamClient) Recv() (*pm.TransactionResponse, error) {
	select {
	case m := <-a.rx:
		return m, nil
	case <-a.rxClose:
		if a.finalErr == nil {
			return nil, io.EOF
		}
		return nil, *a.finalErr
	}
}

type adapterStreamServer struct {
	ctx     context.Context
	tx      chan<- *pm.TransactionResponse
	rx      <-chan *pm.TransactionRequest
	rxClose <-chan struct{}

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
	a.tx <- m
	return nil
}

func (a *adapterStreamServer) Recv() (*pm.TransactionRequest, error) {
	select {
	case m := <-a.rx:
		return m, nil
	case <-a.rxClose:
		return nil, io.EOF
	}
}
