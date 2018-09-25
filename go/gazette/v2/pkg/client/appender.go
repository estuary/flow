package client

import (
	"context"
	"errors"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
)

// Appender adapts an Append RPC to the io.WriteCloser interface. Its usages
// should be limited to cases where the full and complete buffer to append is
// already available and can be immediately dispatched as, by design, an in-
// progress RPC prevents the broker from serving other Append RPCs concurrently.
type Appender struct {
	Request  pb.AppendRequest  // AppendRequest of the Append.
	Response pb.AppendResponse // AppendResponse sent by broker.

	ctx    context.Context
	client pb.RoutedJournalClient  // Client against which Read is dispatched.
	stream pb.Journal_AppendClient // Server stream.
}

// NewAppender returns an Appender initialized with the BrokerClient and AppendRequest.
func NewAppender(ctx context.Context, client pb.RoutedJournalClient, req pb.AppendRequest) *Appender {
	var a = &Appender{
		Request: req,
		ctx:     ctx,
		client:  client,
	}
	return a
}

// Reset the Appender to its post-construction state,
// allowing it to be re-used or re-tried.
func (a *Appender) Reset() { a.Response, a.stream = pb.AppendResponse{}, nil }

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
	} else {
		a.client.UpdateRoute(a.Request.Journal.String(), &a.Response.Header.Route)

		if a.Response.Status != pb.Status_OK {
			err = errors.New(a.Response.Status.String())
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

		a.stream, err = a.client.Append(
			pb.WithDispatchItemRoute(a.ctx, a.client, a.Request.Journal.String(), true))

		if err == nil {
			// Send request preamble metadata prior to append content chunks.
			err = a.stream.SendMsg(&a.Request)
		}
	}
	return
}
