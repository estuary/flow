package broker

import (
	"context"
	"crypto/sha1"
	"fmt"
	"hash"
	"io"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
)

// appendFSM is a state machine which models the steps, constraints and
// transitions involved in the execution of an append to a Gazette journal. The
// state machine may restart and back-track at multiple points and as needed,
// typically awaiting a future KeySpace state, as it converges towards the
// distributed consistency required for the execution of appends.
type appendFSM struct {
	svc *Service
	ctx context.Context
	req pb.AppendRequest

	resolved            *resolution      // Current journal resolution.
	pln                 *pipeline        // Current replication pipeline.
	plnReturnCh         chan<- *pipeline // If |pln| is owned, channel to which it must be returned. Else nil.
	readThroughRev      int64            // Etcd revision we must read through to proceed.
	rollToOffset        int64            // Journal write offset we must synchronize on to proceed.
	registers           pb.LabelSet      // Effective journal registers.
	clientCommit        bool             // Did we see a commit chunk from the client?
	clientFragment      *pb.Fragment     // Journal Fragment holding the client's content.
	clientSummer        hash.Hash        // Summer over the client's content.
	clientTotalChunks   int64            // Total number of append chunks.
	clientDelayedChunks int64            // Number of flow-controlled chunks.
	state               appendState      // Current FSM state.
	err                 error            // Error encountered during FSM execution.
}

type appendState int8

const (
	stateResolve               appendState = 0 // Initial state.
	stateAcquirePipeline       appendState = iota
	stateStartPipeline         appendState = iota
	stateSendPipelineSync      appendState = iota
	stateRecvPipelineSync      appendState = iota
	stateUpdateAssignments     appendState = iota
	stateAwaitDesiredReplicas  appendState = iota
	stateValidatePreconditions appendState = iota
	stateStreamContent         appendState = iota // Semi-terminal state (requires more input).
	stateReadAcknowledgements  appendState = iota
	stateError                 appendState = iota // Terminal state.
	stateProxy                 appendState = iota // Terminal state.
	stateFinished              appendState = iota // Terminal state.
)

// run the appendFSM until a terminal state is reached. Upon state
// stateStreamContent, |recv| is repeatedly invoked to read content
// from the client.
func (b *appendFSM) run(recv func() (*pb.AppendRequest, error)) {
	defer b.returnPipeline()

	// Run until we're ready to stream content, or we fail.
	if !b.runTo(stateStreamContent) {
		return
	}

	var fc = &b.resolved.replica.appendFlowControl
	recv = fc.start(b.ctx, b.resolved, recv)

	// Consume chunks from the client.
	for b.state == stateStreamContent {
		b.onStreamContent(recv())
	}
	// Note that we can't access |fc| after calling onReadAcknowledgements.
	b.clientTotalChunks = fc.totalChunks
	b.clientDelayedChunks = fc.delayedChunks

	b.onReadAcknowledgements()
}

// runTo evaluates appendFSM until |state| is reached and returns true.
// If another terminal state is instead reached first, it returns false.
func (b *appendFSM) runTo(state appendState) bool {
	for {
		if b.state == state {
			return true
		}
		switch b.state {
		case stateResolve:
			b.onResolve()
		case stateAcquirePipeline:
			b.onAcquirePipeline()
		case stateStartPipeline:
			b.onStartPipeline()
		case stateSendPipelineSync:
			b.onSendPipelineSync()
		case stateRecvPipelineSync:
			b.onRecvPipelineSync()
		case stateUpdateAssignments:
			b.onUpdateAssignments()
		case stateAwaitDesiredReplicas:
			b.onAwaitDesiredReplicas()
		case stateValidatePreconditions:
			b.onValidatePreconditions()
		case stateError, stateProxy, stateFinished, stateStreamContent:
			return false
		default:
			panic("invalid state")
		}
	}
}

// returnPipeline returns a pipeline owned by the appendFSM, if there is one.
func (b *appendFSM) returnPipeline() {
	if b.plnReturnCh != nil {
		b.plnReturnCh <- b.pln
		b.plnReturnCh = nil
	}
}

// onResolve performs resolution (or re-resolution) of the AppendRequest. If
// the request specifies a future Etcd revision, first block until that
// revision has been applied to the local KeySpace. This state may be
// re-entered multiple times.
func (b *appendFSM) onResolve() {
	b.mustState(stateResolve)

	var args = resolveArgs{
		ctx:             b.ctx,
		journal:         b.req.Journal,
		mayProxy:        !b.req.DoNotProxy,
		requirePrimary:  true,
		minEtcdRevision: b.readThroughRev,
		proxyHeader:     b.req.Header,
	}

	if b.resolved, b.err = b.svc.resolver.resolve(args); b.err != nil {
		b.state = stateError
		b.err = errors.WithMessage(b.err, "resolve")
	} else if b.resolved.status != pb.Status_OK {
		b.state = stateError
	} else if b.resolved.ProcessId != b.resolved.localID {
		// If we hold the pipeline from a previous resolution but are no longer
		// primary, we must release it.
		b.returnPipeline()
		b.state = stateAwaitDesiredReplicas // We must proxy.
	} else if b.plnReturnCh != nil {
		b.state = stateStartPipeline
	} else {
		b.state = stateAcquirePipeline
	}
}

// onAcquirePipeline performs a blocking acquisition of the exclusively-owned
// replica pipeline.
func (b *appendFSM) onAcquirePipeline() {
	b.mustState(stateAcquirePipeline)

	// Attempt to obtain exclusive ownership of the replica's pipeline.
	select {
	case b.pln = <-b.resolved.replica.pipelineCh:
		addTrace(b.ctx, "<-replica.pipelineCh => %s", b.pln)
		b.plnReturnCh = b.resolved.replica.pipelineCh

		// As a post-check, confirm that the replica hasn't been invalidated
		// or the request cancelled. This isn't strictly required for correct
		// behavior, but resolves a subtle race where both |pipelineCh| and
		// an aborting channel became select-able at the same moment
		// (uncovered by TestE2EShutdownWithProxyAppend).
		select {
		case <-b.ctx.Done():
			goto contextCanceled
		case <-b.resolved.invalidateCh:
			goto resolutionInvalidated
		default:
			b.state = stateStartPipeline
			return
		}

	case <-b.ctx.Done():
		goto contextCanceled
	case <-b.resolved.invalidateCh:
		goto resolutionInvalidated
	}

contextCanceled:
	b.err = errors.WithMessage(b.ctx.Err(), "waiting for pipeline")
	b.state = stateError
	return

resolutionInvalidated:
	addTrace(b.ctx, " ... resolution was invalidated")
	b.state = stateResolve
	return

}

// onStartPipeline builds a pipeline by acquiring the exclusively-owned replica Spool,
// and then constructing a new pipeline (which starts Replicate RPCs to each Route peer).
// If the current pipeline is in an initialized state but has an older effective
// Route, it's torn down and a new one started. If the current pipeline Route is correct,
// this state is a no-op.
func (b *appendFSM) onStartPipeline() {
	b.mustState(stateStartPipeline)

	// Do we have an extant pipeline matching our resolved Route? If so, by
	// construction we also know that it's been synchronized. Otherwise tear
	// down an older pipeline and start anew.
	if b.pln != nil && b.pln.Route.Equivalent(&b.resolved.Route) {
		b.registers.Assign(&b.pln.spool.Registers) // Init from spool registers.
		b.state = stateUpdateAssignments
		return
	} else if b.pln != nil {
		go b.pln.shutdown(false)
		b.pln = nil
	}

	addTrace(b.ctx, " ... must start new pipeline")

	// Attempt to obtain exclusive ownership of the replica's Spool.
	var spool fragment.Spool
	select {
	case spool = <-b.resolved.replica.spoolCh: // Success.
		addTrace(b.ctx, "<-replica.spoolCh => %s", spool)
	case <-b.ctx.Done(): // Request was cancelled.
		b.err = errors.WithMessage(b.ctx.Err(), "waiting for spool")
		b.state = stateError
		return
	case <-b.resolved.invalidateCh: // Replica assignments changed.
		addTrace(b.ctx, " ... resolution was invalidated")
		b.state = stateResolve
		return
	}
	b.registers.Assign(&spool.Registers)

	// Build a pipeline around |spool|. Note the pipeline Context is bound
	// to the replica (rather than our |b.args.ctx|).
	b.pln = newPipeline(b.resolved.replica.ctx, b.resolved.Header, spool, b.resolved.replica.spoolCh, b.svc.jc)
	b.state = stateSendPipelineSync
}

// onSendPipelineSync sends a synchronizing ReplicateRequest proposal to all
// replication peers, which includes the current Route, effective Etcd
// revision, and the proposed current Fragment to be extended. Each peer
// verifies the proposal and headers, and may either agree or indicate a conflict.
func (b *appendFSM) onSendPipelineSync() {
	b.mustState(stateSendPipelineSync)

	var proposal = maybeRollFragment(b.pln.spool, b.rollToOffset, b.resolved.journalSpec.Fragment)
	var req = &pb.ReplicateRequest{
		Proposal:    &proposal,
		Registers:   &b.registers,
		Acknowledge: true,
	}
	// Iff |rollToOffset| is zero then this is our first sync of this pipeline,
	// and we must attach a Header.
	// TODO: Remove DeprecatedJournal, which is sent for compatibility with last release.
	if b.rollToOffset == 0 {
		req.Header = &b.pln.Header
		req.DeprecatedJournal = b.pln.spool.Journal
	}

	b.pln.scatter(req)
	b.state = stateRecvPipelineSync
}

// onRecvPipelineSync reads synchronization acknowledgements from all replication peers.
func (b *appendFSM) onRecvPipelineSync() {
	b.mustState(stateRecvPipelineSync)

	var rollToRegisters *pb.LabelSet
	b.rollToOffset, rollToRegisters, b.readThroughRev = b.pln.gatherSync()

	if b.err = b.pln.recvErr(); b.err == nil {
		b.err = b.pln.sendErr()
	}
	addTrace(b.ctx, "gatherSync() => %d, %v, %d, err: %v",
		b.rollToOffset, rollToRegisters, b.readThroughRev, b.err)

	if b.err != nil {
		go b.pln.shutdown(true)
		b.pln = nil
		b.err = errors.WithMessage(b.err, "gatherSync")
		b.state = stateError
		return
	}

	if b.rollToOffset != 0 {
		// Peer has a larger offset, or an equal offset with an incompatible
		// Fragment. Try again, proposing Spools roll forward to |rollToOffset|.
		// This time all peers should agree on the new Fragment.
		b.state = stateSendPipelineSync

		if rollToRegisters != nil {
			b.registers.Assign(rollToRegisters) // Take peer registers.
		}
	} else if b.readThroughRev != 0 {
		// Peer has a non-equivalent Route at a later Etcd revision.
		go b.pln.shutdown(false)
		b.pln = nil
		b.state = stateResolve
	} else {
		b.state = stateUpdateAssignments
	}
	return
}

// onUpdateAssignments verifies and, if required, updates Etcd assignments to
// advertise the consistency of the present Route, which has been now been
// synchronized. Etcd assignment consistency advertises to the allocator that
// all replicas are consistent, and allows it to now remove undesired journal
// assignments.
func (b *appendFSM) onUpdateAssignments() {
	b.mustState(stateUpdateAssignments)

	// Do the Etcd-advertised values of our resolved journal assignments match
	// the current journal Route (indicating the journal is consistent)?
	if JournalRouteMatchesAssignments(b.resolved.Route, b.resolved.assignments) {
		b.state = stateAwaitDesiredReplicas
		return
	}

	addTrace(b.ctx, " ... must update assignments")
	b.readThroughRev, b.err = updateAssignments(b.ctx, b.resolved.assignments, b.svc.etcd)
	addTrace(b.ctx, "updateAssignments() => %d, err: %v", b.readThroughRev, b.err)

	if b.err != nil {
		b.err = errors.WithMessage(b.err, "updateAssignments")
		b.state = stateError
	} else {
		b.state = stateResolve
	}
}

// onAwaitDesiredReplicas ensures the Route has the desired number of journal
// replicas. If there are too many, then the allocator has over-subscribed the
// journal in preparation for removing some of the current members -- possibly
// even the primary. It's expected that the allocator's removal of member(s) is
// imminent, and we should wait for the route to update rather than sending this
// append to N > R members (if primary) or to an old primary (if proxying).
func (b *appendFSM) onAwaitDesiredReplicas() {
	b.mustState(stateAwaitDesiredReplicas)

	if n, d := len(b.resolved.Route.Members), b.resolved.journalSpec.DesiredReplication(); n > d {
		var nHeap, dHeap = n, d
		addTrace(b.ctx, " ... too many assignments @ rev %d (%d > %d);"+
			" waiting for allocator", b.resolved.Etcd.Revision, nHeap, dHeap)

		b.readThroughRev = b.resolved.Etcd.Revision + 1
		b.state = stateResolve
	} else if n < d {
		b.resolved.status = pb.Status_INSUFFICIENT_JOURNAL_BROKERS
		b.state = stateError
	} else if b.resolved.ProcessId != b.resolved.localID {
		b.state = stateProxy
	} else {
		b.state = stateValidatePreconditions
	}
}

// onValidatePreconditions validates preconditions of the request. It ensures
// that current registers match the request's expectation, and if not it will
// fail the RPC with REGISTER_MISMATCH.
//
// It also validates the next offset to be written.
// Appended data must always be written at the furthest known journal extent.
// Usually this will be the offset of the pipeline's Spool. However if journal
// consistency was lost (due to too many broker or Etcd failures), a larger
// offset could exist in the fragment index.
//
// We don't attempt to automatically handle this scenario. There may be other
// brokers that were partitioned from Etcd, but which still have local
// fragments not yet persisted to the store. If we were to attempt automatic
// recovery, we risk double-writing an offset already committed by those brokers.
//
// Instead the operator is required to craft an AppendRequest which explicitly
// captures the new, maximum journal offset to use, as a confirmation that all
// previous brokers have exited or failed (see `gazctl journals reset-head --help`).
//
// We do make an exception if the journal is not writable, in which case
// appendFSM can be used only for issuing zero-byte transaction barriers
// and there's no risk of double-writes to offsets. In particular this
// carve-out allows a journal to be a read-only view of a fragment store
// being written to by a separate & disconnected gazette cluster.
//
// Note that an AppendRequest offset may also be used outside of recovery,
// for example to implement at-most-once writes.
func (b *appendFSM) onValidatePreconditions() {
	b.mustState(stateValidatePreconditions)

	// Ensure an initial refresh of the remote store(s) has completed.
	select {
	case <-b.resolved.replica.index.FirstRefreshCh():
	// Pass.
	default:
		addTrace(b.ctx, " ... stalled on first fragment index refresh")

		select {
		case <-b.resolved.replica.index.FirstRefreshCh():
			// Pass.
		case <-b.ctx.Done(): // Request was cancelled.
			b.err = errors.WithMessage(b.ctx.Err(), "waiting for index refresh")
			b.state = stateError
			return
		case <-b.resolved.invalidateCh: // Replica assignments changed.
			addTrace(b.ctx, " ... resolution was invalidated")
			b.state = stateResolve
			return
		}
	}

	var maxOffset = b.pln.spool.End
	if eo := b.resolved.replica.index.EndOffset(); eo > maxOffset {
		maxOffset = eo
	}

	if b.req.CheckRegisters != nil &&
		len(b.registers.Labels) != 0 &&
		!b.req.CheckRegisters.Matches(b.registers) {

		b.resolved.status = pb.Status_REGISTER_MISMATCH
		b.state = stateError
	} else if b.pln.spool.End != maxOffset && b.req.Offset == 0 && b.resolved.journalSpec.Flags.MayWrite() {
		b.resolved.status = pb.Status_INDEX_HAS_GREATER_OFFSET
		b.state = stateError
	} else if b.req.Offset != 0 && b.req.Offset != maxOffset {
		// If a request offset is present, it must match |maxOffset|.
		b.resolved.status = pb.Status_WRONG_APPEND_OFFSET
		b.state = stateError
	} else if b.req.Offset != 0 && b.pln.spool.End != maxOffset {
		// Re-sync the pipeline at the explicitly requested |maxOffset|.
		b.rollToOffset = maxOffset
		b.state = stateSendPipelineSync
	} else {
		b.state = stateStreamContent
	}
}

// onStreamContent is called with each received content message or error
// from the Append RPC client. On its first call, it may "roll" the present
// Fragment to a new and empty Fragment (for example, if the Fragment is
// at its target length, or if the compression codec changed). Each non-empty
// content chunk is forwarded to all peers of the FSM's pipeline. An error
// of the client causes a roll-back to be sent to all peers. A final empty
// content chunk followed by an io.EOF causes a commit proposal to be sent
// to each peer, which (if adopted) extends the current Fragment with the
// client's appended content.
func (b *appendFSM) onStreamContent(req *pb.AppendRequest, err error) {
	b.mustState(stateStreamContent)

	if b.clientFragment == nil {
		// This is our first call to onStreamContent.

		// Potentially roll the Fragment forward ahead of this append. Our
		// pipeline is synchronized, so we expect this will always succeed
		// and don't ask for an acknowledgement.
		var proposal = maybeRollFragment(b.pln.spool, 0, b.resolved.journalSpec.Fragment)

		if b.pln.spool.Fragment.Fragment != proposal {
			b.pln.scatter(&pb.ReplicateRequest{
				Proposal:    &proposal,
				Registers:   &b.registers,
				Acknowledge: false,
			})
		}

		b.clientFragment = &pb.Fragment{
			Journal:          b.pln.spool.Journal,
			Begin:            b.pln.spool.End,
			End:              b.pln.spool.End,
			CompressionCodec: b.pln.spool.CompressionCodec,
		}
		b.clientSummer = sha1.New()
	}

	// Ensure |req| is a valid content chunk.
	if err == nil {
		if err = req.Validate(); err == nil && req.Journal != "" {
			err = errExpectedContentChunk
		}
	}

	if err == io.EOF && !b.clientCommit {
		// EOF without first receiving an empty chunk is unexpected,
		// and we treat it as a roll-back.
		err = io.ErrUnexpectedEOF
	} else if err == nil && b.clientCommit {
		// *Not* reading an EOF after reading an empty chunk is also unexpected.
		err = errExpectedEOF
	} else if err == nil && len(req.Content) == 0 {
		// Empty chunk indicates an EOF will follow, at which point we commit.
		b.clientCommit = true
		return
	} else if err == nil && !b.resolved.journalSpec.Flags.MayWrite() {
		// Non-empty appends cannot be made to non-writable journals.
		b.resolved.status = pb.Status_NOT_ALLOWED
	} else if err == nil {
		// Regular content chunk. Forward it through the pipeline.
		b.pln.scatter(&pb.ReplicateRequest{
			Content:      req.Content,
			ContentDelta: b.clientFragment.ContentLength(),
		})
		_, _ = b.clientSummer.Write(req.Content) // Cannot error.
		b.clientFragment.End += int64(len(req.Content))

		if b.pln.sendErr() == nil {
			return
		}
	}

	// We've errored, or reached end-of-input for this Append stream.
	b.clientFragment.Sum = pb.SHA1SumFromDigest(b.clientSummer.Sum(nil))

	// Treat a requested register modification without any bytes appended as an error.
	if err != io.EOF || b.clientFragment.ContentLength() != 0 {
		// Pass.
	} else if b.req.UnionRegisters != nil || b.req.SubtractRegisters != nil {
		err = errRegisterUpdateWithEmptyAppend
	}

	var proposal pb.Fragment

	if err == io.EOF && b.pln.sendErr() == nil && b.resolved.status == pb.Status_OK {
		if !b.clientCommit {
			panic("invariant violated: reqCommit = true")
		}
		// Client request is complete. We're ready to ask each replica to commit
		// a next fragment which includes the client content, along with any
		// modifications to journal registers.
		proposal = b.pln.spool.Next()

		if b.req.SubtractRegisters != nil {
			b.registers = pb.SubtractLabelSet(b.registers, *b.req.SubtractRegisters, pb.LabelSet{})
		}
		if b.req.UnionRegisters != nil {
			b.registers = pb.UnionLabelSets(*b.req.UnionRegisters, b.registers, pb.LabelSet{})
		}
	} else {
		// A client or peer error occurred. The pipeline is still in a good
		// state, but any partial spooled content must be rolled back.
		proposal = b.pln.spool.Fragment.Fragment
		b.err = errors.Wrap(err, "append stream") // This may be nil.
	}

	b.pln.scatter(&pb.ReplicateRequest{
		Proposal:    &proposal,
		Registers:   &b.registers,
		Acknowledge: true,
	})
	b.state = stateReadAcknowledgements
}

// onReadAcknowledgements releases ownership of the pipeline's send-side,
// enqueues itself for the pipeline's receive-side, and, upon its turn,
// reads responses from each replication peer.
//
// Recall that pipelines are full-duplex, and there may be other FSMs
// which completed stateStreamContent before we did, and which have not yet read
// their acknowledgements from peers. To account for this, a cooperative pipeline
// "barrier" is installed which is signaled upon our turn to read ordered
// peer acknowledgements, and which we in turn then signal having done so.
func (b *appendFSM) onReadAcknowledgements() {
	b.mustState(stateReadAcknowledgements)

	// Retain sendErr(), as we cannot safely access it upon sending to |releaseCh|.
	var sendErr = b.pln.sendErr()
	var waitFor, closeAfter = b.pln.barrier()

	if sendErr == nil {
		b.plnReturnCh <- b.pln // Release the send-side of |pln| for reuse.
		b.plnReturnCh = nil
	} else {
		b.pln.closeSend()
		b.plnReturnCh <- nil // Allow a new pipeline to be built.
		b.plnReturnCh = nil
	}

	// Block until our response is the next ordered response to be received.
	// When this select completes, we have sole ownership of the _receive_ side of |pln|.
	select {
	case <-waitFor:
	default:
		addTrace(b.ctx, " ... stalled in <-waitFor read barrier")
		<-waitFor
	}
	// Defer a close that will signal operations pipelined after ourselves,
	// that they may in turn read their responses.
	defer func() { close(closeAfter) }()

	// We expect an acknowledgement from each peer. If we encountered a send
	// error, we also expect an EOF from remaining non-broken peers.
	if b.pln.gatherOK(); sendErr != nil {
		b.pln.gatherEOF()
	}

	// recvErr()s are generally more informational that sendErr()s:
	// gRPC SendMsg returns io.EOF on remote stream breaks, while RecvMsg
	// returns the actual causal error.

	if b.err != nil || b.resolved.status != pb.Status_OK {
		b.state = stateError
	} else if b.err = b.pln.recvErr(); b.err != nil {
		b.state = stateError
	} else if b.err = sendErr; b.err != nil {
		b.state = stateError
	} else {
		b.state = stateFinished
	}
}

func (b *appendFSM) mustState(s appendState) {
	if b.state != s {
		var sHeap = s

		log.WithFields(log.Fields{
			"expect": sHeap,
			"actual": b.state,
		}).Panic("unexpected appendFSM state")
	}
}

var (
	errExpectedEOF                   = fmt.Errorf("expected EOF after empty Content chunk")
	errExpectedContentChunk          = fmt.Errorf("expected Content chunk")
	errRegisterUpdateWithEmptyAppend = fmt.Errorf("register modification requires non-empty Append")
)
