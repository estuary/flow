package materialize

import (
	"context"
	"io"

	pm "github.com/estuary/protocols/materialize"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// transactionRequest is a channel-oriented wrapper of pf.transactionRequest.
type transactionRequest struct {
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
			// Use Recv because ownership of |m| is transferred to |ch|,
			// and |m| cannot be reused.
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

// Rx receives from a TransactionResponse channel.
// It destructures TransactionResponse into its parts,
// and also returns an explicit io.EOF for channel closures.
func Rx(ch <-chan TransactionResponse, block bool) (*pm.TransactionResponse, error) {
	var rx TransactionResponse
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
func AdaptServerToClient(srv pm.DriverServer) pm.DriverClient {
	return adapter{srv}
}

// adapter is pm.DriverClient that wraps an in-process pm.DriverServer.
type adapter struct{ pm.DriverServer }

func (a adapter) Spec(ctx context.Context, in *pm.SpecRequest, opts ...grpc.CallOption) (*pm.SpecResponse, error) {
	return a.DriverServer.Spec(ctx, in)
}

func (a adapter) Validate(ctx context.Context, in *pm.ValidateRequest, opts ...grpc.CallOption) (*pm.ValidateResponse, error) {
	return a.DriverServer.Validate(ctx, in)
}

func (a adapter) Apply(ctx context.Context, in *pm.ApplyRequest, opts ...grpc.CallOption) (*pm.ApplyResponse, error) {
	return a.DriverServer.Apply(ctx, in)
}

func (a adapter) Transactions(ctx context.Context, opts ...grpc.CallOption) (pm.Driver_TransactionsClient, error) {
	var reqCh = make(chan transactionRequest, 4)
	var respCh = make(chan TransactionResponse, 4)
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
	tx   chan<- transactionRequest
	rx   <-chan TransactionResponse
	done <-chan struct{}
}

func (a *adapterStreamClient) Context() context.Context {
	return a.ctx
}

func (a *adapterStreamClient) Send(m *pm.TransactionRequest) error {
	select {
	case a.tx <- transactionRequest{TransactionRequest: m}:
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

// Remaining panic implementations of grpc.ClientStream follow:

func (a *adapterStreamClient) Header() (metadata.MD, error) { panic("not implemented") }
func (a *adapterStreamClient) Trailer() metadata.MD         { panic("not implemented") }
func (a *adapterStreamClient) SendMsg(m interface{}) error  { panic("not implemented") } // Use Send.
func (a *adapterStreamClient) RecvMsg(m interface{}) error  { panic("not implemented") }

type adapterStreamServer struct {
	ctx context.Context
	tx  chan<- TransactionResponse
	rx  <-chan transactionRequest
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

func (a *adapterStreamServer) RecvMsg(m interface{}) error {
	if mm, ok := <-a.rx; ok && mm.Error == nil {
		*m.(*pm.TransactionRequest) = *mm.TransactionRequest
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
