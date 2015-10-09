package journal

import (
	log "github.com/Sirupsen/logrus"

	"github.com/pippio/api-server/cloudstore"
)

// Replica manages journal components required to serve brokered writes,
// replications, and reads. A Replica instance is capable of switching roles
// at any time (and multiple times), from a pure replica which may serve
// replication requests only, to a broker of the journal.
type Replica struct {
	journal Name
	// Token representing this Replica's current understanding of the overall
	// topology of the journal. Replication requests are verified against this
	// token, to ensure that all replicas involved agree on a consistent
	// topology for a transaction.
	currentRouteToken string
	// Whether this Replica is currently acting as the journal broker.
	isCurrentBroker bool
	// Fragment updates are written into |updates| by |index| and |head|,
	// and are consumed by |tail|.
	updates chan Fragment
	// Watches the long-term storage location for newly available fragments.
	index *IndexWatcher
	// Serves fragment reads.
	tail *Tail
	// Serves replicated writes.
	head *Head
	// Brokers transactions which result in replicated writes to the journal.
	broker *Broker
}

func NewReplica(journal Name, localDir string, persister FragmentPersister,
	cfs cloudstore.FileSystem) *Replica {

	updates := make(chan Fragment, 1)
	r := &Replica{
		journal: journal,
		updates: updates,
		index:   NewIndexWatcher(journal, cfs, updates).StartWatchingIndex(),
		tail:    NewTail(journal, updates).StartServingOps(),
		head:    NewHead(journal, localDir, persister, updates),
		broker:  NewBroker(journal),
	}

	// Defer writes until local fragments & the remote index are fully loaded.
	go func() {
		for _, f := range LocalFragments(localDir, journal) {
			updates <- f
		}
		r.index.WaitForInitialLoad()

		log.WithField("journal", journal).Info("starting head and broker")

		r.head.StartServingOps(r.tail.EndOffset())
		r.broker.StartServingOps(r.tail.EndOffset())
	}()

	return r
}

func (r *Replica) Append(op AppendOp) {
	if !r.isCurrentBroker {
		op.Result <- ErrNotBroker
	} else {
		r.broker.Append(op)
	}
}

func (r *Replica) Replicate(op ReplicateOp) {
	if op.RouteToken != r.currentRouteToken {
		op.Result <- ReplicateResult{Error: ErrWrongRouteToken}
	} else {
		r.head.Replicate(op)
	}
}

func (r *Replica) Read(op ReadOp) {
	r.index.WaitForInitialLoad()
	r.tail.Read(op)
}

func (r *Replica) IsBroker() bool {
	return r.isCurrentBroker
}

// Switch the Replica into pure-replica mode. New replication requests will be
// verified against expected |routeToken|.
func (r *Replica) StartReplicating(routeToken string) {
	r.currentRouteToken = routeToken
	r.isCurrentBroker = false

	log.WithFields(log.Fields{"journal": r.journal, "route": routeToken}).
		Info("now replicating")
}

// Switch the Replica into broker mode. Appends will be brokered to |peers| with
// the topology captured by |routeToken|.
func (r *Replica) StartBrokeringWithPeers(routeToken string, peers []Replicator) {
	r.currentRouteToken = routeToken
	r.isCurrentBroker = true

	log.WithFields(log.Fields{"journal": r.journal, "route": routeToken}).
		Info("now brokering")

	var config BrokerConfig
	config.RouteToken = routeToken
	config.WriteHead = r.tail.EndOffset()

	config.Replicas = append(peers, r.head)
	r.broker.UpdateConfig(config)
}

func (r *Replica) Shutdown() {
	log.WithField("journal", r.journal).Info("beginning journal shutdown")
	go func() {
		r.broker.Stop()
		r.head.Stop()
		r.index.Stop()
		close(r.updates)
		r.tail.Stop()
		log.WithField("journal", r.journal).Info("completed journal shutdown")
	}()
}
