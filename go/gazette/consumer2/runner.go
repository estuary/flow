package consumer

import (
	"sort"

	log "github.com/Sirupsen/logrus"
	etcd "github.com/coreos/etcd/client"

	"github.com/pippio/consensus"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/topic"
)

const (
	// Peer is ready to immediately transition to shard master.
	Ready = "ready"
	// Peer is still rebuilding from the recovery log.
	Recovering = "recovering"
	// Peer is responsible for a consumer Shard it doesn't know about.
	// This typically happens when topics are removed from a consumer,
	// but remain in (and should be removed from) the consumer's Etcd directory.
	UnknownShard = "unknown-shard"
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
	// intended to facilitate testing cases.
	ShardPreInitHook     func(Shard)
	ShardPostConsumeHook func(topic.Envelope, Shard)
	ShardPostCommitHook  func(Shard)
	ShardPostStopHook    func(Shard)

	partitions map[journal.Name]*topic.Description // Previously enumerated topic partitions.
	shardNames []string                            // Allocator FixedItems support.

	allShards    map[ShardID]topic.Partition // All shards and their Partition, by name.
	liveShards   map[ShardID]*shard  // Live shards, by name.
	zombieShards map[*shard]struct{} // Cancelled shards which are shutting down.
}

func (r *Runner) updateShards() {
	var added bool
	for _, t := range r.Consumer.Topics() {
		for _, j := range t.Partitions() {
			if _, ok := r.partitions[j]; !ok {
				r.partitions[j] = t
				added = true
			}
		}
	}
	if !added {
		return
	}

	r.allShards = EnumerateShards(r.Consumer)

	var names []string
	for id := range r.allShards {
		names = append(names, id.String())
	}
	sort.Strings(names)
	r.shardNames = names
}

func (r *Runner) Run() error {
	r.partitions = make(map[journal.Name]*topic.Description)
	r.allShards = make(map[ShardID]topic.Partition)
	r.liveShards = make(map[ShardID]*shard)
	r.zombieShards = make(map[*shard]struct{})

	var err = consensus.CreateAndAllocateWithSignalHandling(r)

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

// consumer.Allocator implementation.
func (r *Runner) FixedItems() []string  {
	r.updateShards()
	return r.shardNames
}
func (r *Runner) InstanceKey() string   { return r.LocalRouteKey }
func (r *Runner) KeysAPI() etcd.KeysAPI { return etcd.NewKeysAPI(r.Etcd) }
func (r *Runner) PathRoot() string      { return r.ConsumerRoot }
func (r *Runner) Replicas() int         { return r.ReplicaCount }

func (r *Runner) ItemState(name string) string {
	if shard, ok := r.liveShards[ShardID(name)]; !ok {
		return UnknownShard
	} else if shard.replica.player.IsAtLogHead() {
		return Ready
	} else {
		return Recovering
	}
}

func (r *Runner) ItemIsReadyForPromotion(item, state string) bool {
	return state == Ready
}

func (r *Runner) ItemRoute(name string, rt consensus.Route, index int, tree *etcd.Node) {
	var id = ShardID(name)
	var current, exists = r.liveShards[id]

	// |index| captures the allocator's role in processing |current|.
	var isMaster, isReplica = (index == 0), (index > 0 && index <= r.ReplicaCount)

	if !exists && (isMaster || isReplica) {
		var partition, ok = r.allShards[id]
		if !ok {
			log.WithField("shard", id).Warn("unexpected consumer shard name")
			return
		}

		// Look for a matching zombie shard (ie, still in tear-down). This happens
		// if, for example, a prior shard master gives up control and then
		// immediately becomes a shard replica. Other races are possible.
		var zombie *shard

		for s := range r.zombieShards {
			if s.id == id {
				delete(r.zombieShards, s)
				zombie = s
			}
		}

		current = newShard(id, partition, r, zombie)
		r.liveShards[id] = current
	}

	if isMaster {
		current.transitionMaster(r, tree)
	} else if isReplica {
		current.transitionReplica(r, tree)
	} else if exists {
		current.transitionCancel()

		delete(r.liveShards, id)
		r.zombieShards[current] = struct{}{}
	}

	// Non-blocking reap of previously-cancelled shards.
	for s := range r.zombieShards {
		if s.hasHalted() {
			delete(r.zombieShards, s)
		}
	}
}
