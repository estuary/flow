package consumer

import (
	"context"
	"fmt"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
)

// Resolver maps shards to responsible consumer processes, and manages the set
// of local shard Replicas which are assigned to the local ConsumerSpec.
type Resolver struct {
	state      *allocator.State
	newReplica func() *Replica
	replicas   map[ShardID]*Replica
}

// NewResolver returns a Resolver derived from the allocator.State, which
// will use the |newReplica| closure to instantiate Replicas for Assignments
// of the local ConsumerSpec.
func NewResolver(state *allocator.State, newReplica func() *Replica) *Resolver {
	var r = &Resolver{
		state:      state,
		newReplica: newReplica,
		replicas:   make(map[ShardID]*Replica),
	}
	state.KS.Mu.Lock()
	state.KS.Observers = append(state.KS.Observers, r.updateResolutions)
	state.KS.Mu.Unlock()
	return r
}

// ResolveArgs which parametrize resolution of ShardIDs.
type ResolveArgs struct {
	Context context.Context
	// ShardID to be resolved.
	ShardID ShardID
	// Whether we may resolve to another consumer peer.
	MayProxy bool
	// Optional Header attached to the request from a proxy-ing peer.
	ProxyHeader *pb.Header
}

// Resolution result of a ShardID.
type Resolution struct {
	Status Status
	// Header captures the resolved consumer ProcessId, effective Etcd Revision,
	// and Route of the shard resolution.
	Header pb.Header
	// Spec of the Shard at the current Etcd revision.
	Spec *ShardSpec
	// Shard processing context, or nil if this process is not primary for the ShardID.
	Shard Shard
	// Store of the Shard, or nil if this process is not primary for the ShardID.
	Store Store
	// Done releases Shard & Store, and must be called when no longer needed.
	// Iff Shard & Store are nil, so is Done.
	Done func()
}

// Resolve a ShardID to its Resolution.
func (r *Resolver) Resolve(args ResolveArgs) (res Resolution, err error) {
	var ks = r.state.KS

	defer func() {
		if ks != nil {
			ks.Mu.RUnlock()
		}
	}()
	ks.Mu.RLock()

	var localID pb.ProcessSpec_ID

	if r.state.LocalMemberInd != -1 {
		localID = r.state.Members[r.state.LocalMemberInd].
			Decoded.(allocator.Member).MemberValue.(*ConsumerSpec).Id
	} else {
		// During graceful shutdown, we may still serve requests even after our
		// local member key has been removed from Etcd. We don't want to outright
		// fail these requests as we can usefully proxy them. Use a placeholder
		// to ensure |localID| doesn't match ProcessSpec_ID{}, and for logging.
		localID = pb.ProcessSpec_ID{Zone: "local ConsumerSpec", Suffix: "missing from Etcd"}
	}

	if hdr := args.ProxyHeader; hdr != nil {
		// Sanity check the proxy broker is using our same Etcd cluster.
		if hdr.Etcd.ClusterId != ks.Header.ClusterId {
			err = fmt.Errorf("proxied request Etcd ClusterId doesn't match our own (%d vs %d)",
				hdr.Etcd.ClusterId, ks.Header.ClusterId)
			return
		}
		// Sanity-check that the proxy broker reached the intended recipient.
		if hdr.ProcessId != localID {
			err = fmt.Errorf("proxied request ProcessId doesn't match our own (%s vs %s)",
				&hdr.ProcessId, &localID)
			return
		}

		if hdr.Etcd.Revision > ks.Header.Revision {
			addTrace(args.Context, " ... at revision %d, but want at least %d", ks.Header.Revision, hdr.Etcd.Revision)

			if err = ks.WaitForRevision(args.Context, hdr.Etcd.Revision); err != nil {
				return
			}
			addTrace(args.Context, "WaitForRevision(%d) => %d", hdr.Etcd.Revision, ks.Header.Revision)
		}
	}
	res.Header.Etcd = pb.FromEtcdResponseHeader(ks.Header)

	// Extract ShardSpec.
	if item, ok := allocator.LookupItem(ks, args.ShardID.String()); ok {
		res.Spec = item.ItemValue.(*ShardSpec)
	}
	// Extract Route.
	var assignments = ks.KeyValues.Prefixed(
		allocator.ItemAssignmentsPrefix(ks, args.ShardID.String()))

	res.Header.Route.Init(assignments)
	res.Header.Route.AttachEndpoints(ks)

	// Select a responsible ConsumerSpec.
	if res.Header.Route.Primary != -1 {
		res.Header.ProcessId = res.Header.Route.Members[res.Header.Route.Primary]
	}

	// Select a response Status code.
	if res.Spec == nil {
		res.Status = Status_SHARD_NOT_FOUND
	} else if res.Header.ProcessId == (pb.ProcessSpec_ID{}) {
		res.Status = Status_NO_SHARD_PRIMARY
	} else if !args.MayProxy && res.Header.ProcessId != localID {
		res.Status = Status_NOT_SHARD_PRIMARY
	} else {
		res.Status = Status_OK
	}

	if res.Status != Status_OK {
		// If we're returning an error, the effective ProcessId is ourselves
		// (since we authored the error response).
		res.Header.ProcessId = localID
	} else if res.Header.ProcessId == localID {
		// Attach the local replica, blocking on its Store being ready and
		// incrementing its WaitGroup prior to return.
		var replica, ok = r.replicas[args.ShardID]
		if !ok {
			panic("expected replica for shard " + args.ShardID.String())
		}

		ks.Mu.RUnlock() // We no longer require |ks|; don't hold the lock while we select.
		ks = nil

		select {
		case <-replica.storeReadyCh:
			// Pass.
		default:
			addTrace(args.Context, " ... still recovering; must wait for storeReadyCh to resolve")
			select {
			case <-replica.storeReadyCh:
				// Pass.
			case <-args.Context.Done():
				err = args.Context.Err()
				return
			case <-replica.ctx.Done():
				err = replica.ctx.Err()
				return
			}
			addTrace(args.Context, "<-replica.storeReadyCh")
		}

		replica.wg.Add(1)
		res.Shard = replica
		res.Store = replica.store
		res.Done = replica.wg.Done
	}

	addTrace(args.Context, "resolve(%s) => %s, header: %s, shard: %t",
		args.ShardID, res.Status, &res.Header, res.Shard != nil)

	return
}

// updateResolutions updates |replicas| to match LocalItems, creating,
// transitioning, and cancelling Replicas as needed. The KeySpace
// lock must be held.
func (r *Resolver) updateResolutions() {
	var next = make(map[ShardID]*Replica, len(r.state.LocalItems))

	for _, li := range r.state.LocalItems {
		var item = li.Item.Decoded.(allocator.Item)
		var assignment = li.Assignments[li.Index]
		var id = ShardID(item.ID)

		var replica, ok = r.replicas[id]
		if !ok {
			replica = r.newReplica() // Newly assigned shard.
		} else {
			delete(r.replicas, id) // Move from |r.replicas| to |next|.
		}
		next[id] = replica
		transition(replica, item.ItemValue.(*ShardSpec), assignment)
	}

	var prev = r.replicas
	r.replicas = next

	// Any remaining Replicas in |prev| were not in LocalItems.
	for _, replica := range prev {
		replica.cancel()
		go replica.WaitAndTearDown()
	}
	return
}
