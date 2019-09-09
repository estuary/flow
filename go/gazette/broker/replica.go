package broker

import (
	"context"
	"io"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/keyspace"
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
}

func newReplica(journal pb.Journal) *replica {
	var ctx, cancel = context.WithCancel(context.Background())

	var r = &replica{
		journal:    journal,
		ctx:        ctx,
		cancel:     cancel,
		index:      fragment.NewIndex(ctx),
		spoolCh:    make(chan fragment.Spool, 1),
		pipelineCh: make(chan *pipeline, 1),
	}

	r.spoolCh <- fragment.NewSpool(journal, struct {
		*fragment.Index
		*fragment.Persister
	}{r.index, sharedPersister})

	r.pipelineCh <- nil

	return r
}

// fragmentRefreshDaemon periodically refreshes the local index of replica
// fragments from configured remote stores, at configured intervals.
func fragmentRefreshDaemon(ks *keyspace.KeySpace, r *replica) {
	var timer = time.NewTimer(0) // Fires immediately.
	defer timer.Stop()

	for {
		select {
		case _ = <-r.ctx.Done():
			return
		case _ = <-timer.C:
		}

		var spec *pb.JournalSpec

		ks.Mu.RLock()
		if item, ok := allocator.LookupItem(ks, r.journal.String()); ok {
			spec = item.ItemValue.(*pb.JournalSpec)
		}
		ks.Mu.RUnlock()

		if spec == nil {
			log.WithField("name", r.journal).
				Error("failed to refresh remote fragments (JournalSpec not found)")
			return
		}

		if set, err := fragment.WalkAllStores(r.ctx, spec.Name, spec.Fragment.Stores); err == nil {
			r.index.ReplaceRemote(set)
		} else {
			log.WithFields(log.Fields{
				"name":     spec.Name,
				"err":      err,
				"interval": spec.Fragment.RefreshInterval,
			}).Warn("failed to refresh remote fragments (will retry)")
		}
		timer.Reset(spec.Fragment.RefreshInterval)
	}
}

// pulseDaemon performs periodic and on-demand invocations of a zero-byte
// append FSM sequence. This action drives re-establishment of advertised
// consistency in Etcd, and these pulses ensure that process happens even in
// the absence of client-initiated Append RPCs. On-demand pulses are performed
// on changes to the replica Route. Additional periodic pulses ensure problems
// with the peer set (eg, half-broken connections) are detected proactively.
func pulseDaemon(svc *Service, r *replica) {
	var timer = time.NewTimer(0) // Fires immediately.
	defer timer.Stop()

	var invalidateCh <-chan struct{}
	for {
		select {
		case _ = <-r.ctx.Done():
			return
		case _ = <-timer.C:
			timer.Reset(healthCheckInterval)
		case _ = <-invalidateCh:
			invalidateCh = nil
		}

		var ctx, _ = context.WithTimeout(r.ctx, healthCheckInterval)
		var fsm = appendFSM{
			svc: svc,
			ctx: ctx,
			req: pb.AppendRequest{
				Journal:    r.journal,
				DoNotProxy: true,
			},
		}
		if fsm.runTo(stateStreamContent) {
			fsm.onStreamContent(&pb.AppendRequest{}, nil) // Intend to commit.
			fsm.onStreamContent(nil, io.EOF)              // Commit.
			fsm.onReadAcknowledgements()
		}
		fsm.returnPipeline()

		if fsm.state == stateFinished {
			// We're done.
		} else if fsm.resolved.status == pb.Status_NOT_JOURNAL_PRIMARY_BROKER {
			// Only the primary pulses the journal. No-op.
		} else if fsm.resolved.status == pb.Status_JOURNAL_NOT_FOUND {
			// Journal was deleted while we waited.
		} else if errors.Cause(fsm.err) == context.Canceled {
			// Replica is shutting down.
		} else if errors.Cause(fsm.err) == errResolverStopped {
			// We've been asked to stop serving local replicas.
		} else {
			log.WithFields(log.Fields{
				"err":     fsm.err,
				"status":  fsm.resolved.status,
				"journal": r.journal,
			}).Warn("journal pulse failed (will retry)")
		}

		if fsm.resolved != nil {
			invalidateCh = fsm.resolved.invalidateCh
		}
	}
}

// shutDownReplica drains replica pipeline & spool channels and cancels its context.
// done is called when shutdown is complete.
func shutDownReplica(r *replica, done func()) {
	if pln := <-r.pipelineCh; pln != nil {
		pln.shutdown(false)
	}
	var sp = <-r.spoolCh

	// Roll the Spool forward to finalize & persist its current Fragment.
	var fragment = sp.Fragment.Fragment
	fragment.Begin, fragment.Sum = fragment.End, pb.SHA1Sum{}
	sp.MustApply(&pb.ReplicateRequest{Proposal: &fragment, Registers: &sp.Registers})

	// We intentionally don't return the pipeline or spool to their channels.
	// Cancelling the replica Context will immediately fail any current or
	// future attempts to deque either.
	r.cancel()
	done()
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
	} else if ctx.Err() != nil {
		return 0, ctx.Err() // Fail-fast: gRPC writes to wire even if |ctx| is Done.
	} else if resp, err := etcd.Txn(ctx).If(cmp...).Then(ops...).Commit(); err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return 0, err
	} else {
		// Note that transactions may not succeed under regular operation.
		// For example, a call to updateAssignments may race against the
		// allocator's compaction of those assignment slots, and lose.
		// We expect to converge quickly via another attempt.
		return resp.Header.Revision, nil
	}
}

// maybeRollFragment returns either the current Fragment, or an empty Fragment
// which has been "rolled" to an offset at or after the current Spool End,
// and which reflects the latest |spec|.
func maybeRollFragment(cur fragment.Spool, rollToOffset int64, spec pb.JournalSpec_Fragment) pb.Fragment {
	var flushFragment bool

	if cl := cur.ContentLength(); cl == 0 {
		flushFragment = true // Empty fragment is trivially rolled.
	} else if cl > spec.Length {
		flushFragment = true // Roll if over the target Fragment length.
	} else if cur.Begin == 0 {
		// We should roll after the journal's very first write. This has the
		// effect of "dirtying" the remote fragment index, and protects against
		// data loss if N > R consistency is lost (eg, Etcd fails). When the
		// remote index is dirty, recovering brokers are clued in that writes
		// against this journal have already occurred (and `gazctl reset-head`
		// must be run to recover). If the index were instead pristine,
		// recovering brokers cannot distinguish this case from a newly-created
		// journal, which risks double-writes to journal offsets.
		flushFragment = true
	} else if rollToOffset != 0 {
		flushFragment = true
	}

	// If the flush interval of the fragment differs from current number of
	// intervals since the epoch, the fragment needs to be flushed as it
	// contains data that belongs to an old flush interval.
	if interval := int64(spec.FlushInterval.Seconds()); interval > 0 {
		var first = cur.FirstAppendTime.Unix() / interval
		if now := timeNow().Unix() / interval; first != now {
			flushFragment = true
		}
	}

	// Return a new proposal which will prompt a flush of the current fragment to the backing store.
	if flushFragment {
		var next = cur.Fragment.Fragment

		if rollToOffset != 0 {
			if next.End > rollToOffset {
				panic("invalid rollToOffset")
			}
			next.End = rollToOffset
		}
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

var (
	timeNow             = time.Now
	healthCheckInterval = time.Minute
)
