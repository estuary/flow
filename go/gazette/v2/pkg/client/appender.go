package client

import (
	"context"
	"errors"

	"google.golang.org/grpc"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// Appender adapts an Append RPC to the io.WriteCloser interface. Its usages
// should be limited to cases where the full and complete buffer to append is
// already available and can be immediately dispatched as, by design, an in-
// progress RPC prevents the broker from serving other Append RPCs concurrently.
type Appender struct {
	Request  pb.AppendRequest  // AppendRequest of the Append.
	Response pb.AppendResponse // AppendResponse sent by broker.

	ctx    context.Context
	client AppenderClient         // Client against which Read is dispatched.
	stream pb.Broker_AppendClient // Server stream.
}

// AppenderClient is the journal Append interface which Appender utilizes.
type AppenderClient interface {
	Append(ctx context.Context, opts ...grpc.CallOption) (pb.Broker_AppendClient, error)
}

// NewAppender returns an Appender initialized with the BrokerClient and AppendRequest.
func NewAppender(ctx context.Context, client AppenderClient, req pb.AppendRequest) *Appender {
	var a = &Appender{
		Request: req,
		ctx:     ctx,
		client:  client,
	}
	return a
}

func (a *Appender) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return // The broker interprets empty chunks as "commit".
	}

	// Lazy initialization: begin the Append RPC.
	if err = a.lazyInit(); err != nil {
		// Pass.
	} else if err = a.stream.SendMsg(&pb.AppendRequest{Content: p}); err != nil {
		// Pass.
	} else {
		n = len(p)
	}

	if err != nil {
		if u, ok := a.client.(RouteUpdater); ok {
			u.UpdateRoute(a.Request.Journal, nil) // Purge cached Route.
		}
	}
	return
}

// Close the Append to complete the transaction, committing previously
// written content. If Close returns without an error, Append.Response
// will hold the broker response.
func (a *Appender) Close() (err error) {
	// Send an empty chunk to signal commit of previously written content
	if err = a.lazyInit(); err != nil {
		return
	} else if err = a.stream.SendMsg(new(pb.AppendRequest)); err != nil {
		// Pass.
	} else if err = a.stream.CloseSend(); err != nil {
		// Pass.
	} else if err = a.stream.RecvMsg(&a.Response); err != nil {
		// Pass.
	} else if err = a.Response.Validate(); err != nil {
		// Pass.
	} else if a.Response.Status != pb.Status_OK {
		err = errors.New(a.Response.Status.String())
	}

	if u, ok := a.client.(RouteUpdater); ok {
		if err == nil {
			u.UpdateRoute(a.Request.Journal, &a.Response.Header.Route)
		} else {
			u.UpdateRoute(a.Request.Journal, nil)
		}
	}
	return
}

// Abort the write, causing the broker to discard previously written content.
func (a *Appender) Abort() {
	if a.stream != nil {
		// Abort is implied by sending EOF without a preceding empty chunk.
		_, _ = a.stream.CloseAndRecv()
	}
}

func (a *Appender) lazyInit() (err error) {
	if a.stream == nil {
		if a.Request.Journal == "" {
			return pb.NewValidationError("expected Request.Journal")
		} else if err = a.Request.Validate(); err != nil {
			return pb.ExtendContext(err, "Request")
		}
		a.stream, err = a.client.Append(WithJournalHint(a.ctx, a.Request.Journal))

		if err == nil {
			// Send request preamble metadata prior to append content chunks.
			err = a.stream.SendMsg(&a.Request)
		}
	}
	return
}
