package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/keyspace"
)

// Resolver maps ShardIDs to responsible consumer processes, and manages the set
// of local shards which are assigned to this process.
type Resolver struct {
	state *allocator.State
	// Set of local shards known to this Resolver. Nil iff
	// stopServingLocalReplicas() has been called.
	shards map[pc.ShardID]*shard
	// newShard builds a new local shard instance.
	newShard func(keyspace.KeyValue) *shard
	// wg synchronizes over all running local shards.
	wg sync.WaitGroup
}

// NewResolver returns a Resolver derived from the allocator.State, which
// will use the |newShard| closure to instantiate a *shard for each local
// Assignment of the local ConsumerSpec.
func NewResolver(state *allocator.State, newShard func(keyspace.KeyValue) *shard) *Resolver {
	var r = &Resolver{
		state:    state,
		newShard: newShard,
		shards:   make(map[pc.ShardID]*shard),
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
	ShardID pc.ShardID
	// Whether we may resolve to another consumer peer.
	MayProxy bool
	// Optional Header attached to the request from a proxy-ing peer.
	ProxyHeader *pb.Header
	// ReadThrough is journals and offsets which must be reflected in a
	// completed transaction before Resolve returns, blocking if required.
	// Offsets of journals not read by this shard are ignored.
	ReadThrough pb.Offsets
}

// Resolution result of a ShardID.
type Resolution struct {
	Status pc.Status
	// Header captures the resolved consumer ProcessId, effective Etcd Revision,
	// and Route of the shard resolution.
	Header pb.Header
	// Spec of the Shard at the current Etcd revision.
	Spec *pc.ShardSpec
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
			Decoded.(allocator.Member).MemberValue.(*pc.ConsumerSpec).Id
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
		res.Spec = item.ItemValue.(*pc.ShardSpec)
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
		res.Status = pc.Status_SHARD_NOT_FOUND
	} else if res.Header.ProcessId == (pb.ProcessSpec_ID{}) {
		res.Status = pc.Status_NO_SHARD_PRIMARY
	} else if !args.MayProxy && res.Header.ProcessId != localID {
		res.Status = pc.Status_NOT_SHARD_PRIMARY
	} else {
		res.Status = pc.Status_OK
	}

	if res.Status != pc.Status_OK {
		// If we're returning an error, the effective ProcessId is ourselves
		// (since we authored the error response).
		res.Header.ProcessId = localID
	} else if res.Header.ProcessId == localID {
		if r.shards == nil {
			err = ErrResolverStopped
			return
		}

		var shard, ok = r.shards[args.ShardID]
		if !ok {
			panic("expected local shard for ID " + args.ShardID.String())
		}

		ks.Mu.RUnlock() // We no longer require |ks|; don't hold the lock.
		ks = nil

		// |shard| is the assigned primary, but it may still be recovering.
		// Block until |storeReadyCh| is select-able.
		select {
		case <-shard.storeReadyCh:
			// Pass.
		default:
			addTrace(args.Context, " ... still recovering; must wait for storeReadyCh to resolve")
			select {
			case <-shard.storeReadyCh:
				// Pass.
			case <-args.Context.Done():
				err = args.Context.Err()
				return
			case <-shard.ctx.Done():
				err = shard.ctx.Err()
				return
			}
			addTrace(args.Context, "<-shard.storeReadyCh")
		}

		// Ensure args.ReadThrough are satisfied, blocking if required.
		for {
			var ch chan struct{}

			shard.progress.Lock()
			for j, o := range args.ReadThrough {
				if a, ok := shard.progress.readThrough[j]; ok && a < o {
					addTrace(args.Context, " ... journal %s at read offset %d, but want at least %d", j, a, o)
					ch = shard.progress.signalCh
				}
			}
			shard.progress.Unlock()

			if ch == nil {
				break // All args.ReadThrough are satisfied.
			}

			select {
			case <-ch:
				// Pass.
			case <-args.Context.Done():
				err = args.Context.Err()
				return
			case <-shard.ctx.Done():
				err = shard.ctx.Err()
				return
			}
		}

		shard.wg.Add(1)
		res.Shard = shard
		res.Store = shard.store
		res.Done = shard.wg.Done
	}

	addTrace(args.Context, "resolve(%s) => %s, header: %s, shard: %t",
		args.ShardID, res.Status, &res.Header, res.Shard != nil)

	return
}

// updateResolutions updates |shards| to match LocalItems, creating,
// transitioning, and cancelling Replicas as needed. The KeySpace
// lock must be held.
func (r *Resolver) updateResolutions() {
	if r.shards == nil {
		return // We've stopped serving local shards.
	}
	var next = make(map[pc.ShardID]*shard, len(r.state.LocalItems))

	for _, li := range r.state.LocalItems {
		var assignment = li.Assignments[li.Index]
		var id = pc.ShardID(li.Item.Decoded.(allocator.Item).ID)

		var shard, ok = r.shards[id]
		if !ok {
			r.wg.Add(1)
			shard = r.newShard(li.Item) // Newly assigned shard.
		} else {
			delete(r.shards, id) // Move from |r.shards| to |next|.
		}
		next[id] = shard
		transition(shard, li.Item, assignment)
	}

	var prev = r.shards
	r.shards = next

	// Any remaining Replicas in |prev| were not in LocalItems.
	r.cancelShards(prev)
}

// stopServingLocalReplicas begins immediate shutdown of any & all local
// shards, and causes future attempts to resolve to local shards to
// return an error.
func (r *Resolver) stopServingLocalShards() {
	r.state.KS.Mu.Lock()
	defer r.state.KS.Mu.Unlock()

	r.cancelShards(r.shards)
	r.shards = nil
}

func (r *Resolver) cancelShards(m map[pc.ShardID]*shard) {
	for id, shard := range m {
		log.WithField("id", id).Info("stopping local shard")
		shard.cancel()
		go waitAndTearDown(shard, r.wg.Done)
	}
}

func (r *Resolver) watch(ctx context.Context, etcd *clientv3.Client) error {
	var err = r.state.KS.Watch(ctx, etcd)
	if errors.Cause(err) == context.Canceled {
		err = nil
	}

	r.stopServingLocalShards()
	r.wg.Wait() // Wait for all shards shutdowns to complete.
	return err
}

// ErrResolverStopped is returned by Resolver if a ShardID resolves to a local
// shard, but the Resolver has already been asked to stop serving local
// shards. This happens when the consumer is shutting down abnormally (eg, due
// to Etcd lease keep-alive failure) but its local KeySpace doesn't reflect a
// re-assignment of the Shard, and we therefore cannot hope to proxy. Resolve
// fails in this case to encourage the immediate completion of related RPCs,
// as we're probably also trying to drain the gRPC server.
var ErrResolverStopped = errors.New("resolver has stopped serving local shards")
