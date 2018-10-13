package broker

import (
	"crypto/sha1"
	"fmt"
	"hash"
	"io"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// Append dispatches the JournalServer.Append API.
func (srv *Service) Append(stream pb.Journal_AppendServer) error {
	var req, err = stream.Recv()
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
			err = stream.SendAndClose(&pb.AppendResponse{Status: res.status, Header: &res.Header})
			break
		} else if !res.journalSpec.Flags.MayWrite() {
			err = stream.SendAndClose(&pb.AppendResponse{Status: pb.Status_NOT_ALLOWED, Header: &res.Header})
			break
		} else if res.replica == nil {
			req.Header = &res.Header // Attach resolved Header to |req|, which we'll forward.
			err = proxyAppend(stream, req, srv.jc)
			break
		}

		var pln *pipeline
		if pln, rev, err = acquirePipeline(stream.Context(), res.replica, res.Header, srv.jc); err != nil {
			break
		} else if rev != 0 {
			// A peer told us of a future & non-equivalent Route revision.
			// Continue to attempt to start a pipeline again at |rev|.
		} else {
			err = serveAppend(stream, pln, res.journalSpec, res.replica.pipelineCh)
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
			return err
		} else if err = stream.RecvMsg(req); err == io.EOF {
			break
		} else if err != nil {
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
func serveAppend(stream pb.Journal_AppendServer, pln *pipeline, spec *pb.JournalSpec, releaseCh chan<- *pipeline) error {
	// We start with sole ownership of the _send_ side of the pipeline.
	// Forward the client's content through the pipeline.
	var appender = beginAppending(pln, spec.Fragment)
	for appender.onRecv(stream.Recv()) {
	}
	addTrace(stream.Context(), "read client EOF => %s", appender)

	var plnSendErr = pln.sendErr()
	var waitFor, closeAfter = pln.barrier()

	if plnSendErr == nil {
		releaseCh <- pln // Release the send-side of |pln|.
	} else {
		pln.closeSend()
		releaseCh <- nil // Allow a new pipeline to be built.

		log.WithFields(log.Fields{"err": plnSendErr, "journal": spec.Name}).
			Warn("pipeline send failed")
	}

	// There may be pipelined commits prior to this one, who have not yet read
	// their responses. Block until they do so, such that our responses are the
	// next to receive. Similarly, defer a close to signal to RPCs pipelined
	// after this one, that they may in turn read their responses. When this
	// completes, we have sole ownership of the _receive_ side of |pln|.
	select {
	case <-waitFor:
	default:
		addTrace(stream.Context(), " ... stalled in <-waitFor read barrier")
		<-waitFor
	}
	defer func() { close(closeAfter) }()

	// We expect an acknowledgement from each peer. If we encountered a send
	// error, we also expect an EOF from remaining non-broken peers.
	if pln.gatherOK(); plnSendErr != nil {
		pln.gatherEOF()
	}

	if pln.recvErr() != nil {
		log.WithFields(log.Fields{"err": pln.recvErr(), "journal": spec.Name}).
			Warn("pipeline receive failed")
	}

	if appender.reqErr != nil {
		return appender.reqErr
	} else if plnSendErr != nil {
		return plnSendErr
	} else if pln.recvErr() != nil {
		return pln.recvErr()
	} else {
		return stream.SendMsg(&pb.AppendResponse{
			Header: &pln.Header,
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
	// Potentially roll the Fragment forward prior to serving the append.
	// We expect this to always succeed and don't ask for an acknowledgement.
	var proposal, update = updateProposal(pln.spool.Fragment.Fragment, spec)

	if update {
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

// updateProposal applies JournalSpec configuration to a replicated pipeline,
// by proposing that the pipeline roll to a new, empty configured Fragment.
func updateProposal(cur pb.Fragment, spec pb.JournalSpec_Fragment) (pb.Fragment, bool) {
	// If the proposed Fragment is non-empty, but not yet at the target length,
	// don't propose changes to it.
	if cur.ContentLength() > 0 && cur.ContentLength() < spec.Length {
		return cur, false
	}

	var next = cur
	next.Begin = next.End
	next.Sum = pb.SHA1Sum{}
	next.CompressionCodec = spec.CompressionCodec

	if len(spec.Stores) != 0 {
		next.BackingStore = spec.Stores[0]
	} else {
		next.BackingStore = ""
	}
	return next, next != cur
}

var (
	errExpectedEOF          = fmt.Errorf("expected EOF after empty Content chunk")
	errExpectedContentChunk = fmt.Errorf("expected Content chunk")
)
