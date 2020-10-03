package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	pbx "go.gazette.dev/core/broker/protocol/ext"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/keyspace"
)

// Resolver applies the current allocator.State to map journals to shards,
// and shards to responsible consumer processes. It also monitors the State
// to manage the lifetime of local shards which are assigned to this process.
type Resolver struct {
	state *allocator.State
	// Set of local shards known to this Resolver. Nil iff
	// stopServingLocalReplicas() has been called.
	shards map[pc.ShardID]*shard
	// newShard builds a new local shard instance.
	newShard func(keyspace.KeyValue) *shard
	// wg synchronizes over all running local shards.
	wg sync.WaitGroup
	// journals indexes ShardSpecs by the journals they read.
	journals map[pb.Journal][]*pc.ShardSpec
}

// NewResolver returns a Resolver derived from the allocator.State, which
// will use the newShard closure to instantiate a *shard for each local
// Assignment of the local ConsumerSpec.
func NewResolver(state *allocator.State, newShard func(keyspace.KeyValue) *shard) *Resolver {
	var r = &Resolver{
		state:    state,
		newShard: newShard,
		shards:   make(map[pc.ShardID]*shard),
	}
	state.KS.Mu.Lock()
	state.KS.Observers = append(state.KS.Observers, r.updateLocalShards, r.updateJournalsIndex)
	state.KS.Mu.Unlock()
	return r
}

// ResolveArgs parametrize a request to resolve a ShardID to a current,
// responsible consumer process.
type ResolveArgs struct {
	Context context.Context
	// ShardID to be resolved.
	ShardID pc.ShardID
	// Whether we may resolve to another consumer peer. If false and this
	// process is not assigned as shard primary, Resolve returns status
	// NOT_SHARD_PRIMARY.
	MayProxy bool
	// If this request is a proxy from a consumer process peer, Header is
	// the forwarded Header of that peer's Resolution. ProxyHeader is used
	// to ensure this resolver first reads through the Etcd revision which
	// was effective at the peer at time of resolution, and to sanity check
	// the consistency of those resolutions.
	ProxyHeader *pb.Header
	// ReadThrough is journals and offsets which must have been read through in a
	// completed consumer transaction before Resolve may return (blocking if
	// required). Offsets of journals not read by this shard are ignored.
	//
	// ReadThrough aides in the implementation of services which "read their
	// own writes", where those writes propagate through one or more consumer
	// applications: callers pass offsets of their appends to Resolve, which
	// will block until those appends have been processed.
	// See also: Shard.Progress.
	ReadThrough pb.Offsets
}

// Resolution is the result of resolving a ShardID to a responsible consumer process.
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

// Resolve resolves a ShardID to a responsible consumer process. If this
// process is the assigned primary for a ShardID but its Store is not yet ready
// (eg, because it's still playing back from its recovery log), then Resolve
// will block until the Store is ready.
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
	res.Header.Etcd = pbx.FromEtcdResponseHeader(ks.Header)

	// Extract ShardSpec.
	if item, ok := allocator.LookupItem(ks, args.ShardID.String()); ok {
		res.Spec = item.ItemValue.(*pc.ShardSpec)
	}
	// Extract Route.
	var assignments = ks.KeyValues.Prefixed(
		allocator.ItemAssignmentsPrefix(ks, args.ShardID.String()))

	pbx.Init(&res.Header.Route, assignments)
	pbx.AttachEndpoints(&res.Header.Route, ks)

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

		var readThrough pb.Offsets

		if mp, ok := shard.svc.App.(MessageProducer); ok {
			readThrough, err = mp.ReadThrough(shard, shard.store, args)
			if err != nil {
				err = fmt.Errorf("MessageProducer.ReadThrough: %w", err)
				return
			}
		} else if l := len(args.ReadThrough); l != 0 {
			readThrough = make(pb.Offsets, l)

			// Filter ReadThrough to journals which are shard sources.
			for _, source := range res.Spec.Sources {
				if offset := args.ReadThrough[source.Journal]; offset != 0 {
					readThrough[source.Journal] = offset
				}
			}
		}

		// Ensure |readThrough| are satisfied, blocking if required.
		for {
			var ch chan struct{}

			shard.progress.Lock()
			for j, desired := range readThrough {
				if actual := shard.progress.readThrough[j]; actual < desired {
					addTrace(args.Context, " ... journal %s at read offset %d, but want at least %d", j, actual, desired)
					ch = shard.progress.signalCh
				}
			}
			shard.progress.Unlock()

			if ch == nil {
				break // All |readThrough| are satisfied.
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

// updateLocalShards updates |shards| to match LocalItems, creating,
// transitioning, and cancelling Replicas as needed. The KeySpace
// lock must be held.
func (r *Resolver) updateLocalShards() {
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

			var rt pb.Route
			pbx.Init(&rt, li.Assignments)

			log.WithFields(log.Fields{
				"id":    id,
				"route": rt,
			}).Info("starting local shard")

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

// ShardsWithSource returns specs having the journal as a source, in ShardID order.
// APIs presented by consumer applications often want to map an app-defined key
// to an associated journal through a message.MappingFunc, and from there to a
// responsible shard against which the API request is ultimately dispatched.
// "Responsible" is up to the application: while it's common to have 1:1 assignment
// between shards and source journals, other patterns are possible and the
// application must make an appropriate selection from among the returned
// ShardSpecs for its use case.
//
//      var mapping message.MappingFunc = ...
//      var mappedID pc.ShardID
//
//      if journal, _, err := mapping(key); err != nil {
//          // Handle error.
//      } else if specs := resolver.ShardsWithSource(journal); len(specs) == 0 {
//          err = fmt.Errorf("no ShardSpec is consuming mapped journal %s", journal)
//          // Handle error.
//      } else {
//          mappedID = specs[0].Id
//      }
//
//      var resolution, err = svc.Resolver.Resolve(consumer.ResolveArgs{
//          ShardID:     specs[0].Id,
//          ...
//      })
//
func (r *Resolver) ShardsWithSource(journal pb.Journal) []*pc.ShardSpec {
	r.state.KS.Mu.RLock()
	var specs = r.journals[journal]
	r.state.KS.Mu.RUnlock()
	return specs
}

// updateJournalsIndex updates |journals| to match ShardSpecs and current
// source assignments. The KeySpace lock must be held.
func (r *Resolver) updateJournalsIndex() {
	var next = make(map[pb.Journal][]*pc.ShardSpec, len(r.state.Items))

	for _, kv := range r.state.Items {
		var spec = kv.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)

		for _, src := range spec.Sources {
			next[src.Journal] = append(next[src.Journal], spec)
		}
	}
	r.journals = next
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
	return err
}

// Describe implements prometheus.Collector
func (r *Resolver) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(r, ch)
}

// Collect implements prometheus.Collector
func (r *Resolver) Collect(ch chan<- prometheus.Metric) {
	r.state.KS.Mu.RLock()
	defer r.state.KS.Mu.RUnlock()
	for shardID, shard := range r.shards {
		status := shard.resolved.assignment.Decoded.(allocator.Assignment).AssignmentValue.(*pc.ReplicaStatus)
		ch <- prometheus.MustNewConstMetric(
			shardUpDesc,
			prometheus.GaugeValue,
			1,
			shardID.String(),
			status.Code.String())
	}
}

// ErrResolverStopped is returned by Resolver if a ShardID resolves to a local
// shard, but the Resolver has already been asked to stop serving local
// shards. This happens when the consumer is shutting down abnormally (eg, due
// to Etcd lease keep-alive failure) but its local KeySpace doesn't reflect a
// re-assignment of the Shard, and we therefore cannot hope to proxy the request
// to another server. Resolve fails in this case to encourage the immediate
// completion of related RPCs, as we're probably also trying to drain the gRPC
// server.
var ErrResolverStopped = errors.New("resolver has stopped serving local shards")
