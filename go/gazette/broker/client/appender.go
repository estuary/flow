package client

import (
	"context"
	"io"
	"math"
	"time"

	"github.com/pkg/errors"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	} else if err = a.sendMsg(&pb.AppendRequest{Content: p}); err != nil {
		// Pass.
	} else {
		n = len(p)
	}

	if err != nil {
		err = mapGRPCCtxErr(a.ctx, err)
	}
	return
}

// Close the Append to complete the transaction, committing previously
// written content. If Close returns without an error, Append.Response
// will hold the broker response.
func (a *Appender) Close() (err error) {
	// Send an empty chunk to signal commit of previously written content
	if err = a.lazyInit(); err != nil {
		// Pass.
	} else if err = a.sendMsg(new(pb.AppendRequest)); err != nil {
		// Pass.
	} else if _ = a.stream.CloseSend(); false {
		// Ignore CloseSend's error. Currently, gRPC will never return one. If the
		// stream is broken, it *could* return io.EOF but we'd rather read the actual
		// casual error with RecvMsg.
	} else if err = a.stream.RecvMsg(&a.Response); err != nil {
		// Pass.
	} else if err = a.Response.Validate(); err != nil {
		err = errors.Wrap(err, "validating broker response")
	} else {
		a.client.UpdateRoute(a.Request.Journal.String(), &a.Response.Header.Route)

		switch a.Response.Status {
		case pb.Status_OK:
			// Pass.
		case pb.Status_NOT_JOURNAL_PRIMARY_BROKER:
			err = ErrNotJournalPrimaryBroker
		case pb.Status_WRONG_APPEND_OFFSET:
			err = ErrWrongAppendOffset
		case pb.Status_REGISTER_MISMATCH:
			err = errors.Wrapf(ErrRegisterMismatch, "selector %v doesn't match registers %v",
				a.Request.CheckRegisters, a.Response.Registers)
		default:
			err = errors.New(a.Response.Status.String())
		}
	}

	if err != nil {
		err = mapGRPCCtxErr(a.ctx, err)
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
			err = a.sendMsg(&a.Request)
		}
	}
	return
}

func (a *Appender) sendMsg(r *pb.AppendRequest) (err error) {
	if err = a.stream.SendMsg(r); err == io.EOF {
		// EOF indicates that a server-side error has occurred, but it must
		// still be read via RecvMsg. See SendMsg docs.
		err = a.stream.RecvMsg(&a.Response)
	}
	return
}

// Append zero or more ReaderAts of |content| to a journal as a single Append
// transaction. Append retries on transport or routing errors, but fails
// on all other errors. If no ReaderAts are provided, an Append RPC with no
// content is issued.
func Append(ctx context.Context, rjc pb.RoutedJournalClient, req pb.AppendRequest,
	content ...io.ReaderAt) (pb.AppendResponse, error) {

	for attempt := 0; true; attempt++ {
		var a = NewAppender(ctx, rjc, req)
		var err error

		for r := 0; r != len(content) && err == nil; r++ {
			_, err = io.Copy(a, io.NewSectionReader(content[r], 0, math.MaxInt64))
		}
		if err == nil {
			err = a.Close()
		} else {
			a.Abort()
		}

		if err == nil {
			return a.Response, nil
		} else if s, ok := status.FromError(err); ok && s.Code() == codes.Unavailable {
			// Fallthrough to retry
		} else if err == ErrNotJournalPrimaryBroker {
			// Fallthrough.
		} else {
			return a.Response, err
		}

		select {
		case <-ctx.Done():
			return a.Response, ctx.Err()
		case <-time.After(backoff(attempt)):
		}
	}
	panic("not reached")
}
