package broker

import (
	"context"
	"math"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/fragment"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"
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
	// signalMaintenanceCh allows the resolver to notify the replica's
	// maintenanceLoop of lifecycle events. Notably:
	//  * resolver signals on KeySpace updates when the replica is primary for
	//    the journal, and current assignments in Etcd are inconsistent.
	//  * resolver closes when the replica is no longer routed to this broker,
	//    and should be gracefully terminated.
	signalMaintenanceCh chan struct{}
}

func newReplica(journal pb.Journal) *replica {
	var ctx, cancel = context.WithCancel(context.Background())

	var r = &replica{
		journal:             journal,
		ctx:                 ctx,
		cancel:              cancel,
		index:               fragment.NewIndex(ctx),
		spoolCh:             make(chan fragment.Spool, 1),
		pipelineCh:          make(chan *pipeline, 1),
		signalMaintenanceCh: make(chan struct{}, 1),
	}

	r.spoolCh <- fragment.NewSpool(journal, struct {
		*fragment.Index
		*fragment.Persister
	}{r.index, sharedPersister})

	r.pipelineCh <- nil

	return r
}

// acquireSpool performs a blocking acquisition of the replica's single Spool.
func acquireSpool(ctx context.Context, r *replica, waitForRemoteLoad bool) (spool fragment.Spool, err error) {
	if waitForRemoteLoad {
		if err = r.index.WaitForFirstRemoteRefresh(ctx); err != nil {
			return
		}
	}

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

	if eo := r.index.EndOffset(); eo > spool.Fragment.End {
		// If the Fragment index knows of an offset great than the current Spool,
		// roll the Spool forward to the new offset.
		var proposal = spool.Fragment.Fragment
		proposal.Begin, proposal.End, proposal.Sum = eo, eo, pb.SHA1Sum{}

		spool.MustApply(&pb.ReplicateRequest{Proposal: &proposal})
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
		spool, err = acquireSpool(ctx, r, true)

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

// maintenanceLoop performs periodic tasks of the replica, notably:
//  * Refreshing its remote fragment listings from configured stores.
//  * "Pulsing" the journal to ensure its liveness, and the
//    consistency of allocator assignment values stored in Etcd.
func maintenanceLoop(r *replica, ks *keyspace.KeySpace, rjc pb.RoutedJournalClient, etcd clientv3.KV) {
	// Start a timer and reset channel which trigger refreshes of remote journal
	// fragments. The duration between each refresh can change based on current
	// configurations, so each refresh iteration passes back a duration to wait
	// until the next iteration, via |refreshTimerResetCh|.
	var refreshTimer = time.NewTimer(0)
	var refreshTimerResetCh = make(chan time.Duration, 1)
	defer refreshTimer.Stop()

	// Under normal operation, we want to pulse the journal periodically,
	// and also on-demand when signalled. However, we want to wait until a
	// first remote refresh completes, as all writes block until it does.
	// Then, we may begin pulse iterations.
	var pulseTicker = time.NewTicker(pulseInterval)
	defer pulseTicker.Stop()

	// |pulseCh| is signaled from within this loop when |pulseTicker| fires,
	// or we're signaled. However, |maybePulseCh| is set to |pulseCh| only
	// after |firstRemoteRefreshCh| signals. By driving pulses from |maybePulseCh|,
	// we effectively queue an initial pulse until the first fragment refresh
	// completes, after which the pulse may proceed immediately.
	var pulseCh = make(chan struct{}, 1)
	var maybePulseCh chan struct{}

	var firstRemoteRefreshCh = r.index.FirstRemoteRefresh()

	for {
		select {
		case _ = <-refreshTimer.C:
			// Begin a background refresh of remote replica fragments. When done,
			// signal to restart |refreshTimer| with the current refresh interval.
			go func() {
				refreshTimerResetCh <- refreshFragments(r, ks)
			}()
			continue

		case d := <-refreshTimerResetCh:
			refreshTimer.Reset(d)

		case _ = <-firstRemoteRefreshCh:
			maybePulseCh = pulseCh     // Allow pulses to proceed.
			firstRemoteRefreshCh = nil // No longer selects.

		case _, ok := <-r.signalMaintenanceCh:
			if !ok {
				shutDownReplica(r)
				return
			}
			select {
			case pulseCh <- struct{}{}:
			default: // Already a queued pulse.
			}

		case _ = <-pulseTicker.C:
			select {
			case pulseCh <- struct{}{}:
			default: // Already a queued pulse.
			}

		case _ = <-maybePulseCh:
			var ctx, _ = context.WithTimeout(r.ctx, pulseInterval)
			pulseJournal(ctx, r.journal, ks, rjc, etcd)
		}
	}
}

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
}

func pulseJournal(ctx context.Context, journal pb.Journal, ks *keyspace.KeySpace, rjc pb.RoutedJournalClient, etcd clientv3.KV) {
	ks.Mu.RLock()
	var item, ok = allocator.LookupItem(ks, journal.String())
	ks.Mu.RUnlock()

	// Bail out if the JournalSpec has been deleted, or is marked as disallowing writes.
	if !ok || !item.ItemValue.(*pb.JournalSpec).Flags.MayWrite() {
		return
	}

	var app = client.NewAppender(ctx, rjc, pb.AppendRequest{Journal: journal})

	if err := app.Close(); err != nil {
		log.WithFields(log.Fields{
			"journal": journal,
			"err":     err,
			"resp":    app.Response,
		}).Warn("pulse append failed")
		return
	}

	ks.Mu.RLock()
	var assignments = ks.Prefixed(allocator.ItemAssignmentsPrefix(ks, journal.String())).Copy()
	ks.Mu.RUnlock()

	// Check that the response Route is equivalent to current |assignments|. It
	// may not be, if this pulse happened to race a concurrent allocator update.
	var rt pb.Route
	rt.Init(assignments)

	if !rt.Equivalent(&app.Response.Header.Route) {
		log.WithFields(log.Fields{
			"journal": journal,
			"rt":      rt.String(),
			"resp":    app.Response,
		}).Warn("pulse Route differs")
		return
	}

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
		// Trivial success. No |assignment| values need to be updated.
	} else if resp, err := etcd.Txn(ctx).If(cmp...).Then(ops...).Commit(); err != nil {
		log.WithField("err", err).Warn("etcd txn failed")
	} else {

		// Note that transactions may not succeed under regular operation.
		// For example, a primary may race a journal pulse under an updated
		// route against an allocator's compaction of assignment slots,
		// and lose. We expect to converge quickly via another pulse attempt.

		// Wait for KeySpace to reflect our txn revision.
		ks.Mu.RLock()
		ks.WaitForRevision(ctx, resp.Header.Revision)
		ks.Mu.RUnlock()
	}
}

func refreshFragments(r *replica, ks *keyspace.KeySpace) time.Duration {
	ks.Mu.RLock()
	var item, ok = allocator.LookupItem(ks, r.journal.String())
	ks.Mu.RUnlock()

	if !ok {
		return math.MaxInt64
	}

	var spec = item.ItemValue.(*pb.JournalSpec)
	var set, err = fragment.WalkAllStores(r.ctx, spec.Name, spec.Fragment.Stores)

	if err == nil {
		r.index.ReplaceRemote(set)
	} else {
		log.WithFields(log.Fields{
			"name":     spec.Name,
			"err":      err,
			"interval": spec.Fragment.RefreshInterval,
		}).Warn("failed to refresh remote fragments (will retry)")
	}
	return spec.Fragment.RefreshInterval
}

var (
	pulseInterval   = time.Minute
	sharedPersister *fragment.Persister
)

// SetSharedPersister sets the Persister instance used by the `broker` package.
func SetSharedPersister(p *fragment.Persister) { sharedPersister = p }
