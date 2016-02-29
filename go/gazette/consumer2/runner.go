package consumer

import (
	"os"
	"os/signal"
	"sort"
	"syscall"

	log "github.com/Sirupsen/logrus"
	etcd "github.com/coreos/etcd/client"

	"github.com/pippio/consensus"
	"github.com/pippio/gazette/journal"
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

	Etcd   etcd.Client
	Getter journal.Getter
	Writer journal.Writer

	shardNames   []string            // Ordered names of all shards.
	liveShards   map[string]*shard   // Live shards, by name.
	zombieShards map[*shard]struct{} // Cancelled shards which are shutting down.
}

func (r *Runner) Run() error {
	numShards, err := numShards(r.Consumer.Topics())
	if err != nil {
		return err
	}

	r.shardNames = make([]string, numShards)
	r.liveShards = make(map[string]*shard)
	r.zombieShards = make(map[*shard]struct{})

	for id := 0; id != numShards; id++ {
		r.shardNames[id] = shardName(ShardID(id))
	}

	// Install a signal handler to exit on external signal.
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig, ok := <-interrupt
		if ok {
			log.WithField("signal", sig).Info("caught signal")

			// Cancel Allocate, allowing it to gracefully tear down.
			if err := consensus.Cancel(r); err != nil {
				log.WithField("err", err).Error("allocator cancel failed")
			}
		}
	}()

	err = consensus.Allocate(r)

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
func (r *Runner) FixedItems() []string  { return r.shardNames }
func (r *Runner) InstanceKey() string   { return r.LocalRouteKey }
func (r *Runner) KeysAPI() etcd.KeysAPI { return etcd.NewKeysAPI(r.Etcd) }
func (r *Runner) PathRoot() string      { return r.ConsumerRoot }
func (r *Runner) Replicas() int         { return r.ReplicaCount }

// TODO(johnny): Issue 1197. Wire this to recoverylog.Player.
func (r *Runner) ItemState(shardName string) string         { return "ready" }
func (r *Runner) ItemIsReadyForPromotion(state string) bool { return state == "ready" }

func (r *Runner) ItemRoute(name string, rt consensus.Route, index int, tree *etcd.Node) {
	shard, exists := r.liveShards[name]

	// |index| captures the allocator's role in processing |shard|.
	isMaster, isReplica := (index == 0), (index > 0 && index < r.ReplicaCount)

	if !exists && (isMaster || isReplica) {
		ind := sort.SearchStrings(r.shardNames, name)
		if ind >= len(r.shardNames) || r.shardNames[ind] != name {
			log.WithField("shard", name).Warn("unexpected consumer shard name")
			return
		}
		shard = newShard(ShardID(ind), r)
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
