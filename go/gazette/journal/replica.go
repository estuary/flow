package gazette

import (
	log "github.com/Sirupsen/logrus"
	"github.com/pippio/api-server/cloudstore"
	"github.com/pippio/api-server/discovery"
)

const FragmentUpdateBufferSize = 10

type TrackedJournal struct {
	name string

	currentRouteToken string
	isCurrentBroker   bool

	// Written to by |index| and |head|, and consumed by |tail|.
	updates chan Fragment

	index  *IndexWatcher
	tail   *Tail
	head   *Head
	broker *Broker
}

func NewTrackedJournal(name, directory string, persister fragmentPersister,
	cfs cloudstore.FileSystem) *TrackedJournal {

	updates := make(chan Fragment, FragmentUpdateBufferSize)
	j := &TrackedJournal{
		name:    name,
		updates: updates,
		index:   NewIndexWatcher(name, cfs, updates).StartWatchingIndex(),
		tail:    NewTail(name, updates).StartServingOps(),
		head:    NewHead(name, directory, persister, updates),
		broker:  NewBroker(name),
	}

	// Defer writes until local fragments & the remote index are fully loaded.
	go func() {
		for _, f := range LocalFragments(directory, name) {
			updates <- f
		}
		j.index.WaitForInitialLoad()

		log.WithField("journal", name).Info("starting head and broker")

		j.head.StartServingOps(j.WriteHead())
		j.broker.StartServingOps(j.WriteHead())
	}()

	return j
}

func (j *TrackedJournal) Append(op AppendOp) {
	if !j.isCurrentBroker {
		op.Result <- ErrNotBroker
	} else {
		j.broker.Append(op)
	}
}

func (j *TrackedJournal) Replicate(op ReplicateOp) {
	if op.RouteToken != j.currentRouteToken {
		op.Result <- ReplicateResult{Error: ErrWrongRouteToken}
	} else {
		j.head.Replicate(op)
	}
}

func (j *TrackedJournal) Read(op ReadOp) {
	j.tail.Read(op)
}

func (j *TrackedJournal) WriteHead() int64 {
	return j.tail.EndOffset()
}

func (j *TrackedJournal) StartReplicating(routeToken string) {
	j.currentRouteToken = routeToken
	j.isCurrentBroker = false

	log.WithFields(log.Fields{"journal": j.name, "route": routeToken}).
		Info("now replicating")
}

func (j *TrackedJournal) StartBrokeringWithPeers(routeToken string,
	peers []*discovery.Endpoint) {

	j.currentRouteToken = routeToken
	j.isCurrentBroker = true

	log.WithFields(log.Fields{"journal": j.name, "route": routeToken}).
		Info("now brokering")

	var config BrokerConfig
	config.RouteToken = routeToken
	config.WriteHead = j.WriteHead()

	config.Replicas = append(config.Replicas, j.head)
	for _, ep := range peers {
		config.Replicas = append(config.Replicas, NewReplicateClient(ep))
	}
	j.broker.UpdateConfig(config)
}

func (j *TrackedJournal) Shutdown() {
	log.WithField("journal", j.name).Info("beginning journal shutdown")
	go func() {
		j.broker.Stop()
		j.head.Stop()
		j.index.Stop()
		close(j.updates)
		j.tail.Stop()
		log.WithField("journal", j.name).Info("completed journal shutdown")
	}()
}
