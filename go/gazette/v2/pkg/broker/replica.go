package broker

import (
	"context"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/fragment"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
)

// replica is a runtime instance of a journal which is assigned to this broker.
type replica struct {
	journal pb.Journal
	// Context tied to processing lifetime of this replica by this broker.
	// Cancelled when this broker is no longer responsible for the replica.
	ctx    context.Context
	cancel context.CancelFunc
	// Index of all known Fragments of the replica.
	index *fragment.Index
	// spoolCh synchronizes access to the single Spool of the replica.
	spoolCh chan fragment.Spool
	// pipelineCh synchronizes access to the single pipeline of the replica.
	pipelineCh chan *pipeline
	// pulsePipelineCh is signaled by the resolver when the replica is primary
	// for the journal, and current assignments in Etcd are inconsistent.
	// maintenanceLoop() reads the signal and drives a pipeline synchronization,
	// which brings the journal and its Etcd route advertisements to consistency.
	pulsePipelineCh chan struct{}
	// done is called when the replica has completed graceful shutdown.
	// C.f. sync.WaitGroup.Done.
	done func()
}

func newReplica(journal pb.Journal, done func()) *replica {
	var ctx, cancel = context.WithCancel(context.Background())

	var r = &replica{
		journal:         journal,
		ctx:             ctx,
		cancel:          cancel,
		index:           fragment.NewIndex(ctx),
		spoolCh:         make(chan fragment.Spool, 1),
		pipelineCh:      make(chan *pipeline, 1),
		pulsePipelineCh: make(chan struct{}, 1),
		done:            done,
	}

	r.spoolCh <- fragment.NewSpool(journal, struct {
		*fragment.Index
		*fragment.Persister
	}{r.index, sharedPersister})

	r.pipelineCh <- nil

	return r
}

// acquireSpool performs a blocking acquisition of the replica's single Spool.
func acquireSpool(ctx context.Context, r *replica) (spool fragment.Spool, err error) {
	select {
	case <-ctx.Done():
		err = ctx.Err() // |ctx| cancelled.
		return
	case <-r.ctx.Done():
		err = r.ctx.Err() // replica cancelled.
		return
	case spool = <-r.spoolCh:
		// Pass.
	}

	addTrace(ctx, "<-replica.spoolCh => %s", spool)
	return
}

// acquirePipeline performs a blocking acquisition of the replica's single
// pipeline, building a new pipeline if a ready instance doesn't already exist.
func acquirePipeline(ctx context.Context, r *replica, hdr pb.Header, jc pb.JournalClient) (*pipeline, int64, error) {
	var pln *pipeline

	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err() // |ctx| cancelled.
	case <-r.ctx.Done():
		return nil, 0, r.ctx.Err() // replica cancelled.
	case pln = <-r.pipelineCh:
		// Pass.
	}
	addTrace(ctx, "<-replica.pipelineCh => %s", pln)

	// Is |pln| is a placeholder indicating the need to read through a revision, which we've since read through?
	if pln != nil && pln.readThroughRev != 0 && pln.readThroughRev <= hdr.Etcd.Revision {
		pln = nil
	}

	// If |pln| is a valid pipeline but is built on a non-equivalent & older Route,
	// tear it down asynchronously and immediately begin a new one.
	if pln != nil && pln.readThroughRev == 0 &&
		!pln.Route.Equivalent(&hdr.Route) && pln.Etcd.Revision < hdr.Etcd.Revision {

		go pln.shutdown(false)
		pln = nil
	}

	var err error

	if pln == nil {
		addTrace(ctx, " ... must build new pipeline")

		// We must construct a new pipeline.
		var spool fragment.Spool
		spool, err = acquireSpool(ctx, r)

		if err == nil {
			pln = newPipeline(r.ctx, hdr, spool, r.spoolCh, jc)
			err = pln.synchronize()
		}
		addTrace(ctx, "newPipeline() => %s, err: %v", pln, err)
	}

	if err != nil {
		r.pipelineCh <- nil // Release ownership, allow next acquirer to retry.
		return nil, 0, err
	} else if pln.readThroughRev != 0 {
		r.pipelineCh <- pln // Release placeholder for next acquirer to observe.
		return nil, pln.readThroughRev, nil
	}

	return pln, 0, nil
}

// releasePipelineAndGatherResponse from peers. Must be called from a
// pipeline who's send-side is "owned" by the current goroutine (eg, the pipeline
// was returned by acquirePipeline), and after all messages have been sent. This
// routine release the pipeline for other goroutines to acquire, waits for all
// prior readers of the ordered pipeline to complete, and gathers the single
// expected response. Any encountered error is returned.
func releasePipelineAndGatherResponse(ctx context.Context, pln *pipeline, releaseCh chan<- *pipeline) error {
	// Retain sendErr(), as we cannot safely access it upon sending to |releaseCh|.
	var sendErr = pln.sendErr()
	var waitFor, closeAfter = pln.barrier()

	if sendErr == nil {
		releaseCh <- pln // Release the send-side of |pln|.
	} else {
		pln.closeSend()
		releaseCh <- nil // Allow a new pipeline to be built.
	}

	// There may be pipelined operations prior to this one which have not yet
	// read their responses. Block while they do so, until our response is the
	// next ordered response to be received. When this select completes, we have
	// sole ownership of the _receive_ side of |pln|.
	select {
	case <-waitFor:
	default:
		addTrace(ctx, " ... stalled in <-waitFor read barrier")
		<-waitFor
	}
	// Defer a close to signal which signals to operations pipelined
	// after this one, that they may in turn read their responses.
	defer func() { close(closeAfter) }()

	// We expect an acknowledgement from each peer. If we encountered a send
	// error, we also expect an EOF from remaining non-broken peers.
	if pln.gatherOK(); sendErr != nil {
		pln.gatherEOF()
	}

	// recvErr()s are generally more informational that sendErr()s:
	// gRPC SendMsg returns io.EOF on remote stream breaks, while RecvMsg
	// returns the actual causal error.
	if err := pln.recvErr(); err != nil {
		return err
	}
	return sendErr
}

// shutDownReplica drains replica pipeline & spool channels and cancels its context.
func shutDownReplica(r *replica) {
	if pln := <-r.pipelineCh; pln != nil && pln.readThroughRev == 0 {
		pln.shutdown(false)
	}
	var sp = <-r.spoolCh

	// Roll the Spool forward to finalize & persist its current Fragment.
	var proposal = sp.Fragment.Fragment
	proposal.Begin, proposal.Sum = proposal.End, pb.SHA1Sum{}
	sp.MustApply(&pb.ReplicateRequest{Proposal: &proposal})

	// We intentionally don't return the pipeline or spool to their channels.
	// Cancelling the replica Context will immediately fail any current or
	// future attempts to deque either.
	r.cancel()
	r.done()
}

// updateAssignments values to reflect the Route implied by |assignments|,
// as an Etcd transaction.
func updateAssignments(ctx context.Context, assignments keyspace.KeyValues, etcd clientv3.KV) (int64, error) {
	var rt pb.Route
	rt.Init(assignments)

	// Construct an Etcd transaction which asserts |assignments| are unchanged
	// and updates non-equivalent values to the |rt| Route serialization,
	// bringing them to a state of advertised consistency.
	var cmp []clientv3.Cmp
	var ops []clientv3.Op
	var value = rt.MarshalString()

	for _, kv := range assignments {
		var key = string(kv.Raw.Key)

		if !rt.Equivalent(kv.Decoded.(allocator.Assignment).AssignmentValue.(*pb.Route)) {
			ops = append(ops, clientv3.OpPut(key, value, clientv3.WithIgnoreLease()))
		}
		cmp = append(cmp, clientv3.Compare(clientv3.ModRevision(key), "=", kv.Raw.ModRevision))
	}

	if len(ops) == 0 {
		return 0, nil // Trivial success. No |assignment| values need to be updated.
	} else if resp, err := etcd.Txn(ctx).If(cmp...).Then(ops...).Commit(); err != nil {
		return 0, err
	} else {
		// Note that transactions may not succeed under regular operation.
		// For example, a primary may race a journal pulse under an updated
		// route against an allocator's compaction of assignment slots,
		// and lose. We expect to converge quickly via another pulse attempt.
		return resp.Header.Revision, nil
	}
}

// checkHealth of a resolved journal by actively pinging its replication pipeline.
// The commit sent is either a noop commit, or a flush commit prompting a flush of
// the current fragment if the fragment contains data older than the current
// flush interval. If successful, attempt to update advertised Etcd
// Routes of the resolved journal. Returns an Etcd revision to read through
// prior to the next checkHealth attempt, or an encountered error.
func checkHealth(res resolution, jc pb.JournalClient, etcd clientv3.KV) (int64, error) {
	if res.status != pb.Status_OK {
		return 0, errors.Wrap(errors.New(res.status.String()), "resolution")
	}
	var ctx, _ = context.WithTimeout(res.replica.ctx, healthCheckInterval)

	var pln, minRevision, err = acquirePipeline(ctx, res.replica, res.Header, jc)
	if err != nil {
		return 0, errors.Wrap(err, "acquiringPipeline")
	} else if minRevision != 0 {
		// Replica told us of a later revision affecting the journal Route.
		// Silently fail now. We'll retry on reading |minRevision|.
		return minRevision, nil
	}

	var proposal = nextProposal(pln.spool, res.journalSpec.Fragment)
	// Send a proposal which is either:
	//  1) A no-op, acknowledged Proposal, and read its acknowledgement from peers.
	//
	//  2) An empty propsal where the Begin is at the pervious End signifying that replicas should
	//     synchronusly roll their Spools to a new empty Fragment.
	//
	pln.scatter(&pb.ReplicateRequest{
		Proposal:    &proposal,
		Acknowledge: true,
	})
	if err = releasePipelineAndGatherResponse(ctx, pln, res.replica.pipelineCh); err != nil {
		return 0, errors.Wrap(err, "releasePipelineAndGatherResponse")
	} else if minRevision, err = updateAssignments(ctx, res.assignments, etcd); err != nil {
		return 0, errors.Wrap(err, "updateAssignments")
	}
	return minRevision, nil
}

// nextProposal evaluates the |cur| spool for scenarios warrenting a flush message to be sent to the other
// replica nodes. In the event that a flush is warrented a flush proposal, otherwise return the original Fragment.
func nextProposal(cur fragment.Spool, spec pb.JournalSpec_Fragment) pb.Fragment {
	var flushFragment bool
	// If the proposed Fragment is non-empty, but not yet at the target length,
	// don't propose changes to it.
	if cur.ContentLength() == 0 || cur.ContentLength() > spec.Length {
		flushFragment = true
	}

	// If the flush interval of the fragment is less then the number of intervals since the epoch
	// the fragment needs to be flushed as it contains data that belongs to an old flush interval.
	var flushIntervalSecs = int64(spec.FlushInterval.Seconds())
	if flushIntervalSecs > 0 {
		var secsSinceEpoch = timeNow().Unix()
		var intervalsSinceEpoch = secsSinceEpoch / flushIntervalSecs
		var fragmentInterval = cur.FirstAppendTime.Unix() / flushIntervalSecs
		if fragmentInterval < intervalsSinceEpoch {
			flushFragment = true
		}
	}

	// Return a new proposal which will prompt a flush of the current fragment to the backing store.
	if flushFragment {
		var next = cur.Fragment.Fragment
		next.Begin = next.End
		next.Sum = pb.SHA1Sum{}
		next.CompressionCodec = spec.CompressionCodec

		return next
	}

	return cur.Fragment.Fragment
}

var sharedPersister *fragment.Persister

// SetSharedPersister sets the Persister instance used by the `broker` package.
func SetSharedPersister(p *fragment.Persister) { sharedPersister = p }

var timeNow = time.Now
