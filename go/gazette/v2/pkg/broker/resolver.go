package broker

import (
	"context"
	"fmt"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
)

// resolver maps journals to responsible broker instances and, potentially, a local replica.
type resolver struct {
	state      *allocator.State
	replicas   map[pb.Journal]*replica
	newReplica func(pb.Journal) *replica
}

func newResolver(state *allocator.State, newReplica func(pb.Journal) *replica) *resolver {
	var r = &resolver{
		state:      state,
		replicas:   make(map[pb.Journal]*replica),
		newReplica: newReplica,
	}

	state.KS.Mu.Lock()
	state.KS.Observers = append(state.KS.Observers, r.updateResolutions)
	state.KS.Mu.Unlock()

	return r
}

type resolveArgs struct {
	ctx context.Context
	// Journal to be dispatched.
	journal pb.Journal
	// Whether we may proxy to another broker.
	mayProxy bool
	// Whether we require the primary broker of the journal.
	requirePrimary bool
	// Whether we require that the journal be fully assigned, or will otherwise
	// tolerate fewer broker assignments than the desired journal replication.
	requireFullAssignment bool
	// Minimum Etcd Revision to have read through, before generating a resolution.
	minEtcdRevision int64
	// Optional Header attached to the request from a proxying peer.
	proxyHeader *pb.Header
}

type resolution struct {
	status pb.Status
	// Header defines the selected broker ID, effective Etcd Revision,
	// and Journal Route of the resolution.
	pb.Header
	// JournalSpec of the Journal at the current Etcd Revision.
	journalSpec *pb.JournalSpec
	// Assignments of the Journal at the current Etcd Revision.
	assignments keyspace.KeyValues
	// replica of the local assigned journal, iff the journal was resolved to this broker
	// (as opposed to a remote peer).
	replica *replica
}

func (r *resolver) resolve(args resolveArgs) (res resolution, err error) {
	var ks = r.state.KS
	defer ks.Mu.RUnlock()
	ks.Mu.RLock()

	var localID pb.ProcessSpec_ID

	if r.state.LocalMemberInd != -1 {
		localID = r.state.Members[r.state.LocalMemberInd].
			Decoded.(allocator.Member).MemberValue.(*pb.BrokerSpec).Id
	} else {
		// During graceful shutdown, we may still serve requests even after our
		// local member key has been removed from Etcd. We don't want to outright
		// fail these requests as we can usefully proxy them. Use a placeholder
		// to ensure |localID| doesn't match ProcessSpec_ID{}, and for logging.
		localID = pb.ProcessSpec_ID{Zone: "local-BrokerSpec", Suffix: "missing-from-Etcd"}
	}

	if hdr := args.proxyHeader; hdr != nil {
		// Sanity check the proxy broker is using our same Etcd cluster.
		if hdr.Etcd.ClusterId != ks.Header.ClusterId {
			err = fmt.Errorf("proxied request Etcd ClusterId doesn't match our own (%d vs %d)",
				hdr.Etcd.ClusterId, ks.Header.ClusterId)
			return
		}
		// Sanity-check that the proxy broker reached the intended recipient.
		if hdr.ProcessId != (pb.ProcessSpec_ID{}) && hdr.ProcessId != localID {
			err = fmt.Errorf("proxied request ProcessId doesn't match our own (%s vs %s)",
				&hdr.ProcessId, &localID)
			return
		}
		// We want to wait for the greater of a |proxyHeader| or |minEtcdRevision|.
		if args.proxyHeader.Etcd.Revision > args.minEtcdRevision {
			args.minEtcdRevision = args.proxyHeader.Etcd.Revision
		}
	}

	if args.minEtcdRevision > ks.Header.Revision {
		addTrace(args.ctx, " ... at revision %d, but want at least %d",
			ks.Header.Revision, args.minEtcdRevision)

		if err = ks.WaitForRevision(args.ctx, args.minEtcdRevision); err != nil {
			return
		}
		addTrace(args.ctx, "WaitForRevision(%d) => %d",
			args.minEtcdRevision, ks.Header.Revision)
	}
	res.Etcd = pb.FromEtcdResponseHeader(ks.Header)

	// Extract JournalSpec.
	if item, ok := allocator.LookupItem(ks, args.journal.String()); ok {
		res.journalSpec = item.ItemValue.(*pb.JournalSpec)
	}
	// Extract Assignments and build Route.
	res.assignments = ks.KeyValues.Prefixed(
		allocator.ItemAssignmentsPrefix(ks, args.journal.String())).Copy()

	res.Route.Init(res.assignments)
	res.Route.AttachEndpoints(ks)

	// Select a definite ProcessID if we require the primary and there is one,
	// or if we're a member of the Route (and authoritative).
	if args.requirePrimary && res.Route.Primary != -1 {
		res.ProcessId = res.Route.Members[res.Route.Primary]
	} else if !args.requirePrimary {
		for i := range res.Route.Members {
			if res.Route.Members[i] == localID {
				res.ProcessId = localID
			}
		}
	}

	// If we're authoritative, attach our replica to the resolution.
	if res.ProcessId == localID {
		res.replica = r.replicas[args.journal]
	}

	// Select a response Status code.
	if res.journalSpec == nil {
		res.status = pb.Status_JOURNAL_NOT_FOUND
	} else if args.requirePrimary && res.Route.Primary == -1 {
		res.status = pb.Status_NO_JOURNAL_PRIMARY_BROKER
	} else if len(res.Route.Members) == 0 {
		res.status = pb.Status_INSUFFICIENT_JOURNAL_BROKERS
	} else if args.requireFullAssignment && len(res.Route.Members) < int(res.journalSpec.Replication) {
		res.status = pb.Status_INSUFFICIENT_JOURNAL_BROKERS
	} else if !args.mayProxy && res.ProcessId != localID {
		if args.requirePrimary {
			res.status = pb.Status_NOT_JOURNAL_PRIMARY_BROKER
		} else {
			res.status = pb.Status_NOT_JOURNAL_BROKER
		}
	} else {
		res.status = pb.Status_OK
	}

	// If we're returning an error, the effective ProcessId is ourselves
	// (since we authored the error response).
	if res.status != pb.Status_OK {
		res.ProcessId = localID
	}

	addTrace(args.ctx, "resolve(%s) => %s, local: %t, header: %s",
		args.journal, res.status, res.replica != nil, &res.Header)

	return
}

// updateResolutions, by virtue of being a KeySpace.Observer, expects that the
// KeySpace.Mu Lock is held.
func (r *resolver) updateResolutions() {
	var next = make(map[pb.Journal]*replica, len(r.state.LocalItems))

	for _, li := range r.state.LocalItems {
		var item = li.Item.Decoded.(allocator.Item)
		var assignment = li.Assignments[li.Index].Decoded.(allocator.Assignment)
		var name = pb.Journal(item.ID)

		var rep *replica
		var ok bool

		if rep, ok = r.replicas[name]; ok {
			next[name] = rep
			delete(r.replicas, name)
		} else {
			rep = r.newReplica(name)
			next[name] = rep
		}

		if assignment.Slot == 0 && !item.IsConsistent(keyspace.KeyValue{}, li.Assignments) {
			// Attempt to signal maintenanceLoop that the journal should be pulsed.
			select {
			case rep.maintenanceCh <- struct{}{}:
			default: // Pass (non-blocking).
			}
		}
	}

	var prev = r.replicas
	r.replicas = next

	for _, rep := range prev {
		// Signal maintenanceLoop to stop the replica.
		close(rep.maintenanceCh)
	}
	return
}
