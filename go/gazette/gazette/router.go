package gazette

import (
	log "github.com/Sirupsen/logrus"
	"github.com/pippio/api-server/discovery"
	"sync"
)

const (
	MembersPrefix = "members/"
)

type DispatchedJournal interface {
	Read(ReadOp)
	Replicate(ReplicateOp)
	Append(AppendOp)
	WriteHead() int64

	// Journal lifecycle transitions.
	StartBrokeringWithPeers(routeToken string, peers []*discovery.Endpoint)
	StartReplicating(routeToken string)
	Shutdown()
}

type dispatcherContext interface {
	CreateReplica(journal, routeToken string) DispatchedJournal

	CreateBroker(journal, routeToken string,
		peers []*discovery.Endpoint) DispatchedJournal
}

type dispatcher struct {
	context       dispatcherContext
	journals      map[string]DispatchedJournal
	localRouteKey string
	router        discovery.HRWRouter

	// Guards |journals| and |router|.
	mu sync.Mutex
}

func NewDispatcher(kvs *discovery.KeyValueService,
	context dispatcherContext,
	localRouteKey string,
	replicaCount int) *dispatcher {
	d := &dispatcher{
		context:       context,
		journals:      make(map[string]DispatchedJournal),
		localRouteKey: localRouteKey,
	}
	d.router = discovery.NewHRWRouter(replicaCount, d.onRouteUpdate)
	kvs.AddObserver(MembersPrefix, d.onMembershipChange)
	return d
}

func (d *dispatcher) DispatchRead(op ReadOp) {
	d.mu.Lock()
	journal := d.obtainJournal(op.Journal)
	d.mu.Unlock()

	if journal == nil {
		op.Result <- ReadResult{Error: ErrNotReplica}
	} else {
		journal.Read(op)
	}
}

func (d *dispatcher) DispatchReplicate(op ReplicateOp) {
	d.mu.Lock()
	journal := d.obtainJournal(op.Journal)
	d.mu.Unlock()

	if journal == nil {
		op.Result <- ReplicateResult{Error: ErrNotReplica}
	} else {
		journal.Replicate(op)
	}
}

func (d *dispatcher) DispatchAppend(op AppendOp) {
	d.mu.Lock()
	journal := d.obtainJournal(op.Journal)
	d.mu.Unlock()

	if journal == nil {
		op.Result <- ErrNotBroker
	} else {
		journal.Append(op)
	}
}

func (d *dispatcher) WriteHead(journalName string) int64 {
	d.mu.Lock()
	journal, ok := d.journals[journalName]
	d.mu.Unlock()

	if !ok {
		return -1
	} else {
		return journal.WriteHead()
	}
}

func (d *dispatcher) obtainJournal(name string) DispatchedJournal {
	journal, ok := d.journals[name]
	if ok {
		return journal
	}
	routes := d.router.Route(name)
	token := d.routeToken(routes)

	if d.isBroker(routes) {
		journal = d.context.CreateBroker(name, token, d.peers(routes))
	} else if d.isReplica(routes) {
		journal = d.context.CreateReplica(name, token)
	} else {
		return nil
	}
	d.journals[name] = journal
	d.router.Track(name, routes)
	return journal
}

func (d *dispatcher) isBroker(routes []discovery.HRWRoute) bool {
	return len(routes) != 0 && routes[0].Key == d.localRouteKey
}

func (d *dispatcher) isReplica(routes []discovery.HRWRoute) bool {
	for _, r := range routes {
		if r.Key == d.localRouteKey {
			return true
		}
	}
	return false
}

func (d *dispatcher) routeToken(routes []discovery.HRWRoute) string {
	var token string
	for i, r := range routes {
		if i == 0 {
			token = r.Key[len(MembersPrefix):]
		} else {
			token += "|" + r.Key[len(MembersPrefix):]
		}
	}
	return token
}

func (d *dispatcher) peers(routes []discovery.HRWRoute) []*discovery.Endpoint {
	peers := make([]*discovery.Endpoint, 0, len(routes)-1)
	for _, r := range routes {
		if r.Key == d.localRouteKey {
			continue
		}
		peers = append(peers, r.Value.(*discovery.Endpoint))
	}
	return peers
}

func (d *dispatcher) onMembershipChange(members, old, new discovery.KeyValues) {
	// Lock because we're called from EtcdService's goroutine.
	d.mu.Lock()
	d.router.RebuildRoutes(members, old, new)
	d.mu.Unlock()
}

func (d *dispatcher) onRouteUpdate(journal string,
	oldRoutes, newRoutes []discovery.HRWRoute) {
	// Called from within onMembershipChange(), so we're already locked.

	if !d.isReplica(newRoutes) {
		// We're no longer responsible for this journal.
		if d.isBroker(oldRoutes) {
			log.WithFields(log.Fields{
				"oldRoutes": oldRoutes,
				"newRoutes": newRoutes,
				"journal":   journal,
			}).Error("was broker, now not even replica!")
		}
		d.journals[journal].Shutdown()

		d.router.Drop(journal)
		delete(d.journals, journal)
		return
	}

	if d.isBroker(newRoutes) {
		d.journals[journal].StartBrokeringWithPeers(d.routeToken(newRoutes),
			d.peers(newRoutes))
	} else {
		d.journals[journal].StartReplicating(d.routeToken(newRoutes))
	}
	return
}
