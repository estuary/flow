package broker

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// Append dispatches the JournalServer.Append API.
func (svc *Service) Append(stream pb.Journal_AppendServer) (err error) {
	var (
		fsm appendFSM
		req *pb.AppendRequest
	)
	defer instrumentJournalServerOp("Append", &err, &fsm.resolved, time.Now())

	defer func() {
		if err != nil {
			var addr net.Addr
			if p, ok := peer.FromContext(stream.Context()); ok {
				addr = p.Addr
			}
			log.WithFields(log.Fields{"err": err, "req": req, "client": addr}).
				Warn("served Append RPC failed")
		}
	}()

	if req, err = stream.Recv(); err != nil {
		return err
	} else if err = req.Validate(); err != nil {
		return err
	}

	fsm = appendFSM{
		svc: svc,
		ctx: stream.Context(),
		req: *req,
	}
	fsm.run(stream.Recv)

	switch fsm.state {
	case stateProxy:
		req.Header = &fsm.resolved.Header // Attach resolved Header to |req|, which we'll forward.
		return proxyAppend(stream, req, svc.jc)
	case stateFinished:
		metrics.CommitsTotal.WithLabelValues(metrics.Ok).Inc()

		return stream.SendAndClose(&pb.AppendResponse{
			Status:    pb.Status_OK,
			Header:    fsm.resolved.Header,
			Commit:    fsm.clientFragment,
			Registers: &fsm.registers,
		})
	case stateError:
		if fsm.resolved.status != pb.Status_OK {
			metrics.CommitsTotal.WithLabelValues(fsm.resolved.status.String()).Inc()

			var resp = &pb.AppendResponse{
				Status: fsm.resolved.status,
				Header: fsm.resolved.Header,
			}
			if fsm.resolved.status == pb.Status_REGISTER_MISMATCH {
				resp.Registers = &fsm.registers
			}
			return stream.SendAndClose(resp)
		}

		var cause = errors.Cause(fsm.err)
		metrics.CommitsTotal.WithLabelValues(cause.Error()).Inc()

		// Client-initiated RPC cancellations are expected errors.
		if cause == context.Canceled {
			return nil
		}
		return fsm.err
	default:
		panic("not reached")
	}
}

// proxyAppend forwards an AppendRequest to a resolved peer broker.
func proxyAppend(stream grpc.ServerStream, req *pb.AppendRequest, jc pb.JournalClient) error {
	var ctx = pb.WithDispatchRoute(stream.Context(), req.Header.Route, req.Header.ProcessId)

	var client, err = jc.Append(ctx)
	if err != nil {
		return err
	}
	for {
		if err = client.SendMsg(req); err != nil {
			break // Client stream is broken. CloseAndRecv() will return causal error.
		} else if err = stream.RecvMsg(req); err == io.EOF {
			break
		} else if err != nil {
			_, _ = client.CloseAndRecv() // Drain to free resources.
			return err
		}
	}
	if resp, err := client.CloseAndRecv(); err != nil {
		return err
	} else {
		return stream.SendMsg(resp)
	}
}
