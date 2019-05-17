package broker

import (
	"crypto/sha1"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/gazette/gazette/v2/metrics"
	pb "github.com/gazette/gazette/v2/protocol"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// Append dispatches the JournalServer.Append API.
func (srv *Service) Append(stream pb.Journal_AppendServer) error {
	var err error
	defer instrumentJournalServerOp("append", &err, time.Now())

	req, err := stream.Recv()
	if err != nil {
		return err
	} else if err = req.Validate(); err != nil {
		return err
	}

	var rev int64

	for {
		var res resolution
		res, err = srv.resolver.resolve(resolveArgs{
			ctx:                   stream.Context(),
			journal:               req.Journal,
			mayProxy:              !req.DoNotProxy,
			requirePrimary:        true,
			requireFullAssignment: true,
			minEtcdRevision:       rev,
			proxyHeader:           req.Header,
		})

		if err != nil {
			break
		} else if res.status != pb.Status_OK {
			err = stream.SendAndClose(&pb.AppendResponse{Status: res.status, Header: res.Header})
			break
		} else if !res.journalSpec.Flags.MayWrite() {
			err = stream.SendAndClose(&pb.AppendResponse{Status: pb.Status_NOT_ALLOWED, Header: res.Header})
			break
		} else if res.replica == nil {
			req.Header = &res.Header // Attach resolved Header to |req|, which we'll forward.
			err = proxyAppend(stream, req, srv.jc)
			break
		} else if err = res.replica.index.WaitForFirstRemoteRefresh(stream.Context()); err != nil {
			break
		}

		var pln *pipeline
		if pln, rev, err = acquirePipeline(stream.Context(), res.replica, res.Header, srv.jc); err != nil {
			break
		} else if rev != 0 {
			// A peer told us of a future & non-equivalent Route revision.
			// Continue to attempt to start a pipeline again at |rev|.
		} else {
			err = serveAppend(stream, req, res, pln)
			break
		}
	}

	if err != nil {
		log.WithFields(log.Fields{"err": err, "req": req}).Warn("failed to serve Append")
		return err
	}
	return nil
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

// serveAppend evaluates a client's Append RPC against the local coordinated pipeline.
func serveAppend(stream pb.Journal_AppendServer, req *pb.AppendRequest, res resolution, pln *pipeline) error {
	// We start with sole ownership of the _send_ side of the pipeline.

	// The next offset written is always the furthest known journal extent.
	// Usually this is the tracked pipeline offset, but it's possible that
	// a larger offset exists in the fragment index.
	var offset = pln.spool.Fragment.End
	if eo := res.replica.index.EndOffset(); eo > offset {
		offset = eo
	}
	// If the fragment index contains a larger offset than that of the
	// pipeline, it's a likely indication that journal consistency was lost
	// at some point (due to too many broker or Etcd failures). Refuse the
	// append to prevent inadvertently writing an offset more than once,
	// unless the request provides an explicit offset.
	if po := pln.spool.Fragment.End; po != offset && req.Offset == 0 {
		res.replica.pipelineCh <- pln // Release |pln|.

		log.WithFields(log.Fields{
			"journal": req.Journal,
			"offset":  pln.spool.Fragment.End,
			"indexed": offset,
		}).Warn("failing append because fragment index offset > append offset (was consistency lost?)")
		return stream.SendAndClose(&pb.AppendResponse{Status: pb.Status_INDEX_HAS_GREATER_OFFSET, Header: res.Header})
	} else if req.Offset == 0 {
		// Use |offset| (== |po|).
	} else if req.Offset != offset {
		// If a request offset is present, it must match |offset|.
		res.replica.pipelineCh <- pln // Release |pln|.
		return stream.SendAndClose(&pb.AppendResponse{Status: pb.Status_WRONG_APPEND_OFFSET, Header: res.Header})
	} else if po != offset {
		// Send a proposal which rolls the pipeline forward to |offset|.
		var proposal = pln.spool.Fragment.Fragment
		proposal.Begin, proposal.End, proposal.Sum = offset, offset, pb.SHA1Sum{}

		pln.scatter(&pb.ReplicateRequest{
			Proposal:    &proposal,
			Acknowledge: false,
		})
	}

	// Forward the client's content through the pipeline.
	var appender = beginAppending(pln, res.journalSpec.Fragment)
	for appender.onRecv(stream.Recv()) {
	}
	addTrace(stream.Context(), "read client EOF => %s", appender)

	var err = releasePipelineAndGatherResponse(stream.Context(), pln, res.replica.pipelineCh)
	if err != nil {
		metrics.CommitsTotal.WithLabelValues(metrics.Fail).Inc()
		log.WithFields(log.Fields{"err": err, "journal": res.journalSpec.Name}).
			Warn("serveAppend: pipeline failed")
	}
	metrics.CommitsTotal.WithLabelValues(metrics.Ok).Inc()

	if appender.reqErr != nil {
		return appender.reqErr
	} else if err != nil {
		return err
	} else {
		return stream.SendAndClose(&pb.AppendResponse{
			Status: pb.Status_OK,
			Header: pln.Header,
			Commit: appender.reqFragment,
		})
	}
}

// appender streams Append content through the pipeline, tracking the exact
// Journal Fragment appended by the RPC and any client error.
type appender struct {
	pln  *pipeline
	spec pb.JournalSpec_Fragment

	reqCommit   bool
	reqErr      error
	reqFragment *pb.Fragment
	reqSummer   hash.Hash
}

// beginAppending updates the current proposal, if needed, then initializes
// and returns an appender.
func beginAppending(pln *pipeline, spec pb.JournalSpec_Fragment) appender {
	var proposal = nextProposal(pln.spool, spec)
	// Potentially roll the Fragment forward prior to serving the append.
	// We expect this to always succeed and don't ask for an acknowledgement.
	if pln.spool.Fragment.Fragment != proposal {
		pln.scatter(&pb.ReplicateRequest{
			Proposal:    &proposal,
			Acknowledge: false,
		})
	}

	return appender{
		pln:  pln,
		spec: spec,

		reqFragment: &pb.Fragment{
			Journal:          pln.spool.Fragment.Journal,
			Begin:            pln.spool.Fragment.End,
			End:              pln.spool.Fragment.End,
			CompressionCodec: pln.spool.Fragment.CompressionCodec,
		},
		reqSummer: sha1.New(),
	}
}

// onRecv is called with each received content message or error
// from the Append RPC client.
func (a *appender) onRecv(req *pb.AppendRequest, err error) bool {
	// Ensure |req| is a valid content chunk.
	if err == nil {
		if err = req.Validate(); err == nil && req.Journal != "" {
			err = errExpectedContentChunk
		}
	}

	if err == io.EOF && !a.reqCommit {
		// EOF without first receiving an empty chunk is unexpected,
		// and we treat it as a roll-back.
		err = io.ErrUnexpectedEOF
	} else if err == nil && a.reqCommit {
		// *Not* reading an EOF after reading an empty chunk is also unexpected.
		err = errExpectedEOF
	} else if err == nil && len(req.Content) == 0 {
		// Empty chunk indicates an EOF will follow, at which point we commit.
		a.reqCommit = true
		return true
	} else if err == nil {
		// Regular content chunk. Forward it through the pipeline.
		a.pln.scatter(&pb.ReplicateRequest{
			Content:      req.Content,
			ContentDelta: a.reqFragment.ContentLength(),
		})
		_, _ = a.reqSummer.Write(req.Content) // Cannot error.
		a.reqFragment.End += int64(len(req.Content))

		return a.pln.sendErr() == nil
	}

	// We've reached end-of-input for this Append stream.
	a.reqFragment.Sum = pb.SHA1SumFromDigest(a.reqSummer.Sum(nil))

	var proposal = new(pb.Fragment)
	if err == io.EOF {
		if !a.reqCommit {
			panic("invariant violated: reqCommit = true")
		}
		// Commit the Append, by scattering the next Fragment to be committed
		// to each peer. They will inspect & validate the Fragment locally,
		// and commit or return an error.
		*proposal = a.pln.spool.Next()
	} else {
		// A client-side read error occurred. The pipeline is still in a good
		// state, but any partial spooled content must be rolled back.
		*proposal = a.pln.spool.Fragment.Fragment

		a.reqErr = err
		a.reqFragment = nil
	}

	a.pln.scatter(&pb.ReplicateRequest{
		Proposal:    proposal,
		Acknowledge: true,
	})
	return false
}

// String returns a debugging representation of the appender.
func (a appender) String() string {
	return fmt.Sprintf("appender<reqCommit: %t, reqErr: %v, reqFragment: %s>",
		a.reqCommit, a.reqErr, a.reqFragment.String())
}

var (
	errExpectedEOF          = fmt.Errorf("expected EOF after empty Content chunk")
	errExpectedContentChunk = fmt.Errorf("expected Content chunk")
)
