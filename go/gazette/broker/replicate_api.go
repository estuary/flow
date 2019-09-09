package broker

import (
	"fmt"
	"io"
	"net"
	"time"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc/peer"
)

// Replicate dispatches the JournalServer.Replicate API.
func (svc *Service) Replicate(stream pb.Journal_ReplicateServer) (err error) {
	var (
		req      *pb.ReplicateRequest
		resolved *resolution
	)
	defer instrumentJournalServerOp("Replicate", &err, &resolved, time.Now())

	defer func() {
		if err != nil {
			var addr net.Addr
			if p, ok := peer.FromContext(stream.Context()); ok {
				addr = p.Addr
			}
			log.WithFields(log.Fields{"err": err, "req": req, "client": addr}).
				Warn("served Replicate RPC failed")
		}
	}()

	if req, err = stream.Recv(); err != nil {
		return err
	} else if err = req.Validate(); err != nil {
		return err
	} else if req.Header == nil {
		return fmt.Errorf("expected first ReplicateRequest to have Header")
	}

	var spool fragment.Spool
	for done := false; !done; {
		resolved, err = svc.resolver.resolve(resolveArgs{
			ctx:            stream.Context(),
			journal:        req.Proposal.Journal,
			mayProxy:       false,
			requirePrimary: false,
			proxyHeader:    req.Header,
		})
		if err != nil {
			return err
		} else if resolved.status != pb.Status_OK {
			return stream.Send(&pb.ReplicateResponse{Status: resolved.status, Header: &resolved.Header})
		} else if !resolved.Header.Route.Equivalent(&req.Header.Route) {
			// Require that the request Route is equivalent to the Route we resolved to.
			return stream.Send(&pb.ReplicateResponse{Status: pb.Status_WRONG_ROUTE, Header: &resolved.Header})
		}

		// Attempt to obtain exclusive ownership of the replica's Spool.
		select {
		case spool = <-resolved.replica.spoolCh:
			addTrace(stream.Context(), "<-replica.spoolCh => %s", spool)
			done = true
		case <-stream.Context().Done(): // Request was cancelled.
			return stream.Context().Err()
		case <-resolved.invalidateCh: // Replica assignments changed.
			addTrace(stream.Context(), " ... resolution was invalidated")
			// Loop to retry.
		}
	}

	// Serve the long-lived replication pipeline. When it completes, roll-back
	// any uncommitted content and release ownership of Spool.
	spool, err = serveReplicate(stream, req, spool, &resolved.Header)

	spool.MustApply(&pb.ReplicateRequest{
		Proposal:  &spool.Fragment.Fragment,
		Registers: &spool.Registers,
	})
	resolved.replica.spoolCh <- spool

	return err
}

// serveReplicate evaluates a client's Replicate RPC against the local Spool.
func serveReplicate(stream pb.Journal_ReplicateServer, req *pb.ReplicateRequest, spool fragment.Spool, hdr *pb.Header) (fragment.Spool, error) {
	var (
		resp = new(pb.ReplicateResponse)
		err  error
	)
	for {
		if *resp, err = spool.Apply(req, false); err != nil {
			return spool, err
		}
		if req.Acknowledge {
			resp.Header, hdr = hdr, nil // Send Header with first ReplicateResponse.

			if err = stream.SendMsg(resp); err != nil {
				return spool, err
			}
		} else if resp.Status != pb.Status_OK {
			return spool, fmt.Errorf("no ack requested but status != OK: %s", resp)
		}

		if req, err = stream.Recv(); err == io.EOF {
			return spool, nil
		} else if err != nil {
			return spool, err
		} else if err = req.Validate(); err != nil {
			return spool, err
		}
	}
}
