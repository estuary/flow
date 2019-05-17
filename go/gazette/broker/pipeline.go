package broker

import (
	"context"
	"fmt"
	"io"

	"github.com/gazette/gazette/v2/fragment"
	pb "github.com/gazette/gazette/v2/protocol"
	log "github.com/sirupsen/logrus"
)

// pipeline is an in-flight write replication pipeline of a journal.
type pipeline struct {
	pb.Header                                  // Header of the pipeline.
	spool         fragment.Spool               // Local, primary replication Spool.
	returnCh      chan<- fragment.Spool        // |spool| return channel.
	streams       []pb.Journal_ReplicateClient // Established streams to each replication peer.
	sendErrs      []error                      // First error on send from each peer.
	readBarrierCh chan struct{}                // Coordinates hand-off of receive-side of the pipeline.
	recvResp      []pb.ReplicateResponse       // Most recent response gathered from each peer.
	recvErrs      []error                      // First error on receive from each peer.

	// readThroughRev, if set, indicates that a pipeline cannot be established
	// until we have read through (and our Route reflects) this etcd revision.
	readThroughRev int64
}

// newPipeline returns a new pipeline.
func newPipeline(ctx context.Context, hdr pb.Header, spool fragment.Spool, returnCh chan<- fragment.Spool, jc pb.JournalClient) *pipeline {
	if hdr.Route.Primary == -1 {
		panic("dial requires Route with Primary != -1")
	}
	var R = len(hdr.Route.Members)

	var pln = &pipeline{
		Header:        hdr,
		spool:         spool,
		returnCh:      returnCh,
		streams:       make([]pb.Journal_ReplicateClient, R),
		sendErrs:      make([]error, R),
		readBarrierCh: make(chan struct{}),
		recvResp:      make([]pb.ReplicateResponse, R),
		recvErrs:      make([]error, R),
	}
	close(pln.readBarrierCh)

	for i := range pln.Route.Members {
		if i == int(pln.Route.Primary) {
			continue
		}
		pln.streams[i], pln.sendErrs[i] = jc.Replicate(
			pb.WithDispatchRoute(ctx, pln.Route, pln.Route.Members[i]))
	}
	return pln
}

// synchronize all pipeline peers by scattering proposals and gathering peer
// responses. On disagreement, synchronize will iteratively update the proposal
// if it's possible to do so and reach agreement. If peers disagree on Etcd
// revision, synchronize will close the pipeline and set |readThroughRev|.
func (pln *pipeline) synchronize() error {
	var proposal = pln.spool.Fragment.Fragment

	for {
		pln.scatter(&pb.ReplicateRequest{
			Header:      &pln.Header,
			Journal:     pln.spool.Journal,
			Proposal:    &proposal,
			Acknowledge: true,
		})
		var rollToOffset, readThroughRev = pln.gatherSync(proposal)

		var err = pln.recvErr()
		if err == nil {
			err = pln.sendErr()
		}

		if err != nil {
			pln.shutdown(true)
			return err
		}

		if rollToOffset != 0 {
			// Update our |proposal| to roll forward to the new offset. Loop to try
			// again. This time all peers should agree on the new Fragment.
			proposal.Begin = rollToOffset
			proposal.End = rollToOffset
			proposal.Sum = pb.SHA1Sum{}
			continue
		}

		if readThroughRev != 0 {
			// Peer has a non-equivalent Route at a later etcd revision. Close the
			// pipeline, and set its |readThroughRev| as an indication to other RPCs
			// of the revision which must first be read through before attempting
			// another pipeline.
			pln.shutdown(false)
			pln.readThroughRev = readThroughRev
		}
		return nil
	}
}

// scatter asynchronously applies the ReplicateRequest to all replicas.
func (pln *pipeline) scatter(r *pb.ReplicateRequest) {
	for i, s := range pln.streams {
		if s != nil && pln.sendErrs[i] == nil {
			if r.Header != nil {
				// Copy and update to peer ProcessID.
				r.Header = boxHeaderProcessID(*r.Header, pln.Route.Members[i])
			}
			// Send may return an io.EOF if the remote peer breaks the stream.
			// We read the actual error in the gather() phase.
			pln.sendErrs[i] = s.Send(r)
		}
	}
	if i := pln.Route.Primary; pln.sendErrs[i] == nil {
		var resp pb.ReplicateResponse

		// Map an error into a |sendErr|.
		// Status !OK is returned only on proposal mismatch, which cannot happen
		// here as all proposals are derived from the Spool itself.
		if resp, pln.sendErrs[i] = pln.spool.Apply(r, true); resp.Status != pb.Status_OK {
			panic(resp.String())
		}
	}
}

// closeSend closes the send-side of all replica connections.
func (pln *pipeline) closeSend() {
	// Apply a Spool commit which rolls back any partial content.
	pln.spool.MustApply(&pb.ReplicateRequest{
		Proposal: &pln.spool.Fragment.Fragment,
	})
	pln.returnCh <- pln.spool // Release ownership of Spool.

	for i, s := range pln.streams {
		if s != nil && pln.sendErrs[i] == nil {
			pln.sendErrs[i] = s.CloseSend()
		}
	}
}

// sendErr returns the first encountered send-side error.
func (pln *pipeline) sendErr() error {
	for i, err := range pln.sendErrs {
		if err != nil {
			return fmt.Errorf("send to %s: %s", &pln.Route.Members[i], err)
		}
	}
	return nil
}

// barrier installs a new barrier in the pipeline. Clients should:
//   * Invoke barrier after issuing all sent writes, and release the
//     pipeline for other clients.
//   * Block until |waitFor| is selectable.
//   * Read expected responses from the pipeline.
//   * Close |closeAfter|.
// By following this convention a pipeline can safely be passed among multiple
// clients, each performing writes followed by reads, while allowing for those
// writes and reads to happen concurrently.
func (pln *pipeline) barrier() (waitFor <-chan struct{}, closeAfter chan<- struct{}) {
	waitFor, pln.readBarrierCh = pln.readBarrierCh, make(chan struct{})
	closeAfter = pln.readBarrierCh
	return
}

// gather synchronously receives a ReplicateResponse from all replicas.
func (pln *pipeline) gather() {
	for i, s := range pln.streams {
		if s != nil && pln.recvErrs[i] == nil {
			pln.recvErrs[i] = s.RecvMsg(&pln.recvResp[i])
		}
	}
}

// gatherOK calls gather, and treats any non-OK response status as an error.
func (pln *pipeline) gatherOK() {
	pln.gather()

	for i, s := range pln.streams {
		if s == nil || pln.recvErrs[i] != nil {
			// Pass.
		} else if pln.recvResp[i].Status != pb.Status_OK {
			pln.recvErrs[i] = fmt.Errorf("unexpected !OK response: %s", &pln.recvResp[i])
		}
	}
}

// gatherSync calls gather, extracts and returns a peer-advertised future offset
// or etcd revision to read through relative to |proposal|, and treats any other
// non-OK response status as an error.
func (pln *pipeline) gatherSync(proposal pb.Fragment) (rollToOffset, readThroughRev int64) {
	pln.gather()

	for i, s := range pln.streams {
		if s == nil || pln.recvErrs[i] != nil {
			continue
		}

		switch resp := pln.recvResp[i]; resp.Status {
		case pb.Status_OK:
			// Pass.
		case pb.Status_WRONG_ROUTE:
			if !resp.Header.Route.Equivalent(&pln.Route) && resp.Header.Etcd.Revision > pln.Etcd.Revision {
				// Peer has a non-equivalent Route at a later etcd revision.
				if resp.Header.Etcd.Revision > readThroughRev {
					readThroughRev = resp.Header.Etcd.Revision
				}
			} else {
				pln.recvErrs[i] = fmt.Errorf("unexpected WRONG_ROUTE: %s", resp.Header)
			}

		case pb.Status_FRAGMENT_MISMATCH:
			// If peer has an extant Spool at a greater offset, we must roll forward to it.
			if (resp.Fragment.End > proposal.End) ||
				// If the peer rolled its Spool to our offset, but does not have and
				// therefore cannot extend Fragment content from [Begin, End), we
				// must start a new Spool beginning at proposal.End.
				(resp.Fragment.End == proposal.End && resp.Fragment.ContentLength() == 0) {

				if resp.Fragment.End > rollToOffset {
					rollToOffset = resp.Fragment.End
				}
			} else {
				pln.recvErrs[i] = fmt.Errorf("unexpected FRAGMENT_MISMATCH: %s", resp.Fragment)
			}

		default:
			pln.recvErrs[i] = fmt.Errorf("unexpected Status: %s", &resp)
		}
	}
	return
}

// gatherEOF synchronously gathers expected EOFs from all replicas.
// An unexpected received message is treated as an error.
func (pln *pipeline) gatherEOF() {
	for i, s := range pln.streams {
		if s == nil || pln.recvErrs[i] != nil {
			// Local spool placeholder, or the stream has already failed.
		} else if msg, err := s.Recv(); err == io.EOF {
			// Graceful stream closure.
		} else if err != nil {
			pln.recvErrs[i] = err
		} else if pln.recvErrs[i] == nil && err == nil {
			pln.recvErrs[i] = fmt.Errorf("unexpected response: %s", msg.String())
		}
	}
}

// recvErr returns the first encountered receive-side error.
func (pln *pipeline) recvErr() error {
	for i, err := range pln.recvErrs {
		if err != nil {
			return fmt.Errorf("recv from %s: %s", &pln.Route.Members[i], err)
		}
	}
	return nil
}

// shutdown performs a graceful, blocking shutdown of the pipeline.
func (pln *pipeline) shutdown(expectErr bool) {
	var waitFor, closeAfter = pln.barrier()

	if pln.closeSend(); !expectErr && pln.sendErr() != nil {
		log.WithField("err", pln.sendErr()).Warn("tearing down pipeline: failed to closeSend")
	}
	<-waitFor

	if pln.gatherEOF(); !expectErr && pln.recvErr() != nil {
		log.WithField("err", pln.recvErr()).Warn("tearing down pipeline: failed to gatherEOF")
	}
	close(closeAfter)
}

// String is used to provide debugging output of a pipeline in a request trace.
func (pln *pipeline) String() string {
	if pln == nil {
		return "<nil>"
	} else if pln.readThroughRev != 0 {
		return fmt.Sprintf("readThroughRev<%d>", pln.readThroughRev)
	}
	return fmt.Sprintf("pipeline<header: %s, spool: %s>", &pln.Header, pln.spool.String())
}

func boxHeaderProcessID(hdr pb.Header, id pb.ProcessSpec_ID) *pb.Header {
	var out = new(pb.Header)
	*out = hdr
	out.ProcessId = id
	return out
}
