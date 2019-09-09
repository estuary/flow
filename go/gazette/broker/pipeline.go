package broker

import (
	"context"
	"fmt"
	"io"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
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
			var respHeap = resp // Escapes.
			panic(respHeap.String())
		}
	}
}

// closeSend closes the send-side of all replica connections.
func (pln *pipeline) closeSend() {
	// Apply a proposal to our own spool which rolls back any partial content.
	pln.spool.MustApply(&pb.ReplicateRequest{
		Proposal:  &pln.spool.Fragment.Fragment,
		Registers: &pln.spool.Registers,
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
			return errors.WithMessagef(err, "send to %s", &pln.Route.Members[i])
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

			// Map EOF to ErrUnexpectedEOF, as EOFs should only be
			// read by gatherEOF().
			if pln.recvErrs[i] == io.EOF {
				pln.recvErrs[i] = io.ErrUnexpectedEOF
			}
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

// gatherSync gathers and returns either:
// * Zero-valued |rollToOffset|, |rollToRegisters|, & |readThroughRev| if the sync succeeded, or
// * The largest peer |rollToOffset| which is greater than our own, & accompanying |rollToRegisters|, or
// * The |rollToOffset| equal to our own which must be rolled to in order for a peer to participate, or
// * An Etcd revision to read through.
// It treats any other non-OK response status as an error.
func (pln *pipeline) gatherSync() (rollToOffset int64, rollToRegisters *pb.LabelSet, readThroughRev int64) {
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

		case pb.Status_PROPOSAL_MISMATCH:
			switch {
			case resp.Fragment.End > pln.spool.End:
				// If peer has a fragment at a greater offset, we must roll forward to
				// its End and adopt the peer's registers.
				if resp.Fragment.End > rollToOffset {
					rollToOffset, rollToRegisters = resp.Fragment.End, resp.Registers
				}
			case resp.Fragment.End == pln.spool.End && resp.Fragment.ContentLength() == 0:
				// If peer rolled its fragment to our End, but it does not have and
				// therefore cannot extend fragment content from [Begin, End), we must
				// roll to an empty fragment at End to allow the peer to participate.
				if resp.Fragment.End > rollToOffset {
					rollToOffset = resp.Fragment.End
				}
			default:
				pln.recvErrs[i] = fmt.Errorf("unexpected PROPOSAL_MISMATCH: %v, %v",
					resp.Fragment, resp.Registers)
			}

		default:
			var respHeap = resp // Escapes.
			pln.recvErrs[i] = fmt.Errorf("unexpected Status: %s", &respHeap)
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
			return errors.WithMessagef(err, "recv from %s", &pln.Route.Members[i])
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
	}
	return fmt.Sprintf("pipeline<header: %s, spool: %s>", &pln.Header, pln.spool.String())
}

func boxHeaderProcessID(hdr pb.Header, id pb.ProcessSpec_ID) *pb.Header {
	var out = new(pb.Header)
	*out = hdr
	out.ProcessId = id
	return out
}
