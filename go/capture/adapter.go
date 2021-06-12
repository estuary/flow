package capture

import (
	"context"
	"io"

	pc "github.com/estuary/flow/go/protocols/capture"
	"google.golang.org/grpc"
)

// CaptureResponse is a channel-oriented wrapper of pc.CaptureResponse.
type CaptureResponse struct {
	*pc.CaptureResponse
	Error error
}

// CaptureResponseChannel spawns a goroutine which receives
// from the stream and sends responses into the returned channel,
// which is closed after the first encountered read error.
// As an optimization, it avoids this read loop if the stream
// is an in-process adapter.
func CaptureResponseChannel(stream pc.Driver_CaptureClient) <-chan CaptureResponse {
	if adapter, ok := stream.(*adapterStreamClient); ok {
		return adapter.rx
	}

	var ch = make(chan CaptureResponse, 4)
	go func() {
		for {
			var m, err = stream.Recv()

			if err == nil {
				ch <- CaptureResponse{CaptureResponse: m}
				continue
			}

			if err != io.EOF {
				ch <- CaptureResponse{Error: err}
			}
			close(ch)
			return
		}
	}()

	return ch
}

// AdaptServerToClient wraps an in-process DriverServer to provide a DriverClient.
func AdaptServerToClient(srv pc.DriverServer) pc.DriverClient {
	return adapter{srv}
}

// adapter is pc.DriverClient that wraps an in-process pc.DriverServer.
type adapter struct{ pc.DriverServer }

func (a adapter) Spec(ctx context.Context, in *pc.SpecRequest, opts ...grpc.CallOption) (*pc.SpecResponse, error) {
	return a.DriverServer.Spec(ctx, in)
}

func (a adapter) Discover(ctx context.Context, in *pc.DiscoverRequest, opts ...grpc.CallOption) (*pc.DiscoverResponse, error) {
	return a.DriverServer.Discover(ctx, in)
}

func (a adapter) Validate(ctx context.Context, in *pc.ValidateRequest, opts ...grpc.CallOption) (*pc.ValidateResponse, error) {
	return a.DriverServer.Validate(ctx, in)
}

func (a adapter) Capture(ctx context.Context, in *pc.CaptureRequest, opts ...grpc.CallOption) (pc.Driver_CaptureClient, error) {
	var respCh = make(chan CaptureResponse, 4)
	var doneCh = make(chan struct{})

	var clientStream = &adapterStreamClient{
		ctx:          ctx,
		rx:           respCh,
		done:         doneCh,
		ClientStream: nil,
	}
	var serverStream = &adapterStreamServer{
		ctx:          ctx,
		tx:           respCh,
		ServerStream: nil,
	}

	go func() (err error) {
		defer func() {
			if err != nil {
				respCh <- CaptureResponse{Error: err}
			}
			close(respCh)
			close(doneCh)
		}()
		return a.DriverServer.Capture(in, serverStream)
	}()

	return clientStream, nil
}

type adapterStreamClient struct {
	ctx  context.Context
	rx   <-chan CaptureResponse
	done <-chan struct{}

	// Embed a nil ClientStream to provide remaining methods of the pm.Driver_TransactionClient
	// interface. It's left as nil, so these methods will panic if called!
	grpc.ClientStream
}

func (a *adapterStreamClient) Context() context.Context {
	return a.ctx
}

func (a *adapterStreamClient) Recv() (*pc.CaptureResponse, error) {
	if m, ok := <-a.rx; ok {
		return m.CaptureResponse, m.Error
	}
	return nil, io.EOF
}

type adapterStreamServer struct {
	ctx context.Context
	tx  chan<- CaptureResponse

	// Embed a nil ServerStream to provide remaining methods of the pm.Driver_TransactionServer
	// interface. It's left as nil, so these methods will panic if called!
	grpc.ServerStream
}

var _ pc.Driver_CaptureServer = new(adapterStreamServer)

func (a *adapterStreamServer) Context() context.Context {
	return a.ctx
}

func (a *adapterStreamServer) Send(m *pc.CaptureResponse) error {
	// Under the gRPC model, the server controls RPC termination. The client cannot
	// revoke the server's ability to send (in the absence of a broken transport,
	// which we don't model here).
	a.tx <- CaptureResponse{CaptureResponse: m}
	return nil
}
