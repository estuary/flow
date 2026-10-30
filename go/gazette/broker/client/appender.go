package client

import (
	"context"
	"io"
	"math"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Appender adapts an Append RPC to the io.WriteCloser interface. The first byte
// written to the Appender initiates the RPC. Subsequent bytes are streamed to
// brokers as they are written. Writes to the Appender may stall as the RPC
// window fills, while waiting for brokers to sequence this Append into the
// journal. Once they do, brokers will expect remaining content to append is
// quickly written to this Appender (and may time-out the RPC if it's not).
//
// Content written to this Appender does not commit until Close is called,
// including cases where the application dies without calling Close. If a
// call to Close is started and the application dies before Close returns,
// the append may or may commit.
//
// The application can cleanly roll-back a started Appender by Aborting it.
type Appender struct {
	Request  pb.AppendRequest  // AppendRequest of the Append.
	Response pb.AppendResponse // AppendResponse sent by broker.

	ctx     context.Context
	client  pb.RoutedJournalClient  // Client against which Read is dispatched.
	counter prometheus.Counter      // Counter of appended bytes.
	stream  pb.Journal_AppendClient // Server stream.
}

// NewAppender returns an initialized Appender of the given AppendRequest.
func NewAppender(ctx context.Context, client pb.RoutedJournalClient, req pb.AppendRequest) *Appender {
	var a = &Appender{
		Request: req,
		ctx:     ctx,
		client:  client,
		counter: appendBytes.WithLabelValues(req.Journal.String()),
	}
	return a
}

// Reset the Appender to its post-construction state, allowing it to be re-used
// or re-tried. Reset without a prior Close or Abort will leak resources.
func (a *Appender) Reset() { a.Response, a.stream = pb.AppendResponse{}, nil }

// Write to the Appender, starting an Append RPC if this is the first Write.
func (a *Appender) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil // The broker interprets empty chunks as "commit".
	}

	// Lazy initialization: begin the Append RPC.
	if err := a.lazyInit(); err != nil {
		return 0, err
	}

	if err := a.stream.SendMsg(&pb.AppendRequest{Content: p}); err != nil {
		return 0, a.readResponse()
	}

	a.counter.Add(float64(len(p)))
	return len(p), nil
}

// Close the Append to complete the transaction, committing previously
// written content. If Close returns without an error, Append.Response
// will hold the broker response.
func (a *Appender) Close() error {
	if err := a.lazyInit(); err != nil {
		return err
	}

	// Send an empty chunk to signal commit of previously written content
	// We don't test for failure because we're about to read the response.
	_ = a.stream.SendMsg(new(pb.AppendRequest))

	// Similarly ignore CloseSend's error. Currently, gRPC will never return one.
	// If the stream is broken, it *could* return io.EOF but we'd rather read
	// the actual causal error with RecvMsg.
	_ = a.stream.CloseSend()

	return a.readResponse()
}

// Abort the append, causing the broker to discard previously written content.
func (a *Appender) Abort() {
	if a.stream != nil {
		// Abort is implied by sending EOF without a preceding empty chunk.
		_, _ = a.stream.CloseAndRecv()
	}
}

func (a *Appender) lazyInit() error {
	if a.stream != nil {
		return nil
	}
	if a.Request.Journal == "" {
		return pb.NewValidationError("expected Request.Journal")
	} else if err := a.Request.Validate(); err != nil {
		return pb.ExtendContext(err, "Request")
	}

	var ctx = pb.WithClaims(a.ctx, pb.Claims{
		Capability: pb.Capability_APPEND,
		Selector: pb.LabelSelector{
			Include: pb.MustLabelSet("name", a.Request.Journal.StripMeta().String()),
		},
	})
	var err error

	a.stream, err = a.client.Append(
		pb.WithDispatchItemRoute(ctx, a.client, a.Request.Journal.String(), true))

	if err != nil {
		return mapGRPCCtxErr(a.ctx, err)
	}

	// Send request preamble metadata prior to append content chunks.
	// We don't test for failure, because lazyInit is called immediately
	// prior to sending _more_ data and then testing for a failure or
	// reading the final response.
	_ = a.stream.SendMsg(&a.Request)

	return nil
}

func (a *Appender) readResponse() error {
	var err = a.stream.RecvMsg(&a.Response)

	if err != nil {
		return mapGRPCCtxErr(a.ctx, err)
	}

	// Extra RecvMsg to explicitly read EOF, as a work-around for
	// https://github.com/grpc-ecosystem/go-grpc-prometheus/issues/92
	_ = a.stream.RecvMsg(new(pb.AppendResponse))

	if err = a.Response.Validate(); err != nil {
		return errors.Wrap(err, "validating broker response")
	}
	a.client.UpdateRoute(a.Request.Journal.String(), &a.Response.Header.Route)

	switch a.Response.Status {
	case pb.Status_OK:
		// Pass.
	case pb.Status_INSUFFICIENT_JOURNAL_BROKERS:
		err = ErrInsufficientJournalBrokers
	case pb.Status_JOURNAL_NOT_FOUND:
		err = ErrJournalNotFound
	case pb.Status_NO_JOURNAL_PRIMARY_BROKER:
		err = ErrNoJournalPrimaryBroker
	case pb.Status_NOT_JOURNAL_PRIMARY_BROKER:
		err = ErrNotJournalPrimaryBroker
	case pb.Status_REGISTER_MISMATCH:
		err = errors.Wrapf(ErrRegisterMismatch, "selector %v doesn't match registers %v",
			a.Request.CheckRegisters, a.Response.Registers)
	case pb.Status_SUSPENDED:
		err = ErrSuspended
	case pb.Status_WRONG_APPEND_OFFSET:
		err = ErrWrongAppendOffset
	case pb.Status_INDEX_HAS_GREATER_OFFSET:
		err = ErrIndexHasGreaterOffset
	case pb.Status_NOT_ALLOWED:
		err = ErrNotAllowed
	case pb.Status_FRAGMENT_STORE_UNHEALTHY:
		err = errors.WithMessage(ErrFragmentStoreUnhealthy, a.Response.StoreHealthError)
	default:
		err = errors.New(a.Response.Status.String())
	}
	return err
}

// Append zero or more ReaderAts to a journal as a single Append transaction.
// Append retries on transport or routing errors, but fails on all other errors.
// Each ReaderAt is read from byte zero until EOF, and may be read multiple times.
// If no ReaderAts are provided, an Append RPC with no content is issued.
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

		var squelch bool

		if err == nil {
			return a.Response, nil
		} else if s, ok := status.FromError(err); ok && s.Code() == codes.Unavailable {
			// Fallthrough to retry
		} else if err == ErrNotJournalPrimaryBroker || err == ErrNoJournalPrimaryBroker || err == ErrInsufficientJournalBrokers {
			squelch = attempt == 0 // Fallthrough.
		} else {
			return a.Response, err
		}

		if !squelch {
			log.WithFields(log.Fields{
				"journal": req.Journal,
				"err":     err,
				"attempt": attempt,
			}).Warn("append failure (will retry)")
		}

		select {
		case <-ctx.Done():
			return a.Response, ctx.Err()
		case <-time.After(backoff(attempt)):
		}
	}
	panic("not reached")
}
