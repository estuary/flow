package consumer

import (
	"sort"

	log "github.com/Sirupsen/logrus"
	etcd "github.com/coreos/etcd/client"

	"github.com/pippio/consensus"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
)

type Runner struct {
	Consumer Consumer
	// An identifier for this particular runner. Eg, the hostname.
	LocalRouteKey string
	// Base local directory into which shards should be staged.
	LocalDir string
	// Base path in Etcd via which the consumer should coordinate.
	ConsumerRoot string
	// Base journal path for recovery logs of this consumer.
	RecoveryLogRoot string
	// Required number of replicas of the consumer.
	ReplicaCount int

	Etcd    etcd.Client
	Gazette journal.Client

	// Optional hooks for notification of Shard lifecycle. These are largely
	// intended to facilicate testing cases.
	ShardPreInitHook     func(Shard)
	ShardPostConsumeHook func(message.Message, Shard)
	ShardPostCommitHook  func(Shard)
	ShardPostStopHook    func(Shard)

	shardIDs     []ShardID           // Ordered shard IDs.
	liveShards   map[string]*shard   // Live shards, by name.
	zombieShards map[*shard]struct{} // Cancelled shards which are shutting down.
}

func (r *Runner) Run() error {
	r.liveShards = make(map[string]*shard)
	r.zombieShards = make(map[*shard]struct{})

	groups := r.Consumer.Groups()
	if err := groups.Validate(); err != nil {
		return err
	}

	for _, group := range groups {
		numShards, err := group.NumShards()
		if err != nil {
			return err
		}
		for id := 0; id != numShards; id++ {
			r.shardIDs = append(r.shardIDs, ShardID{group.Name, id})
		}
	}

	err := consensus.CreateAndAllocateWithSignalHandling(r)

	// Allocate should exit only after all shards have been cancelled.
	if err == nil && len(r.liveShards) != 0 {
		log.WithField("shards", r.liveShards).Panic("live shards on Allocate exit")
	}

	// Wait for all shards to complete teardown before returning. This gives
	// shards a chance to finish background IO and close cleanly.
	for name, shard := range r.liveShards {
		shard.transitionCancel()
		shard.blockUntilHalted()
		delete(r.liveShards, name)
	}

	for s := range r.zombieShards {
		s.blockUntilHalted()
		delete(r.zombieShards, s)
	}
	return err
}

func (r *Runner) shardNames() []string {
	ret := make([]string, len(r.shardIDs))
	for i := range r.shardIDs {
		ret[i] = r.shardIDs[i].String()
	}
	return ret
}

// consumer.Allocator implementation.
func (r *Runner) FixedItems() []string  { return r.shardNames() }
func (r *Runner) InstanceKey() string   { return r.LocalRouteKey }
func (r *Runner) KeysAPI() etcd.KeysAPI { return etcd.NewKeysAPI(r.Etcd) }
func (r *Runner) PathRoot() string      { return r.ConsumerRoot }
func (r *Runner) Replicas() int         { return r.ReplicaCount }

// TODO(johnny): Issue 1197. Wire this to recoverylog.Player.
func (r *Runner) ItemState(shardName string) string               { return "ready" }
func (r *Runner) ItemIsReadyForPromotion(item, state string) bool { return state == "ready" }

func (r *Runner) ItemRoute(name string, rt consensus.Route, index int, tree *etcd.Node) {
	shard, exists := r.liveShards[name]

	// |index| captures the allocator's role in processing |shard|.
	isMaster, isReplica := (index == 0), (index > 0 && index < r.ReplicaCount)

	if !exists && (isMaster || isReplica) {
		ind := sort.Search(len(r.shardIDs), func(i int) bool {
			return r.shardIDs[i].String() >= name
		})
		if ind >= len(r.shardIDs) || r.shardIDs[ind].String() != name {
			log.WithField("shard", name).Warn("unexpected consumer shard name")
			return
		}
		shard = newShard(r.shardIDs[ind], r)
		r.liveShards[name] = shard
	}

	if isMaster {
		shard.transitionMaster(r, tree)
	} else if isReplica {
		shard.transitionReplica(r, tree)
	} else if exists {
		shard.transitionCancel()

		delete(r.liveShards, name)
		r.zombieShards[shard] = struct{}{}
	}

	// Non-blocking reap of previously-cancelled shards.
	for s := range r.zombieShards {
		if s.hasHalted() {
			delete(r.zombieShards, s)
		}
	}
}
