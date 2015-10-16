package gazette

import (
	"net/url"
	"sync"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/api-server/discovery"
	"github.com/pippio/gazette/journal"
)

const MembersPrefix = "members/"

type Router struct {
	// Builder of new Replica instances.
	factory ReplicaFactory
	// Static routing key of this server process.
	localRouteKey string
	// Index of initialized Replica instances.
	replicas map[journal.Name]JournalReplica
	// Router of journal names to responsible server processes.
	router discovery.HRWRouter
	// Guards access to |replicas| and |router|.
	mu sync.Mutex
}

func NewRouter(kvs *discovery.KeyValueService, factory ReplicaFactory,
	localRouteKey string, replicaCount int) *Router {

	r := &Router{
		factory:       factory,
		localRouteKey: localRouteKey,
		replicas:      make(map[journal.Name]JournalReplica),
	}
	r.router = discovery.NewHRWRouter(replicaCount, r.onRouteUpdate)

	// Receive continous notifications of topology changes which affect routing.
	kvs.AddObserver(MembersPrefix, r.onMembershipChange)
	return r
}

func (r *Router) Read(op journal.ReadOp) {
	replica, err := r.obtainReplica(op.Journal, false)

	if err != nil {
		op.Result <- journal.ReadResult{Error: err}
	} else {
		replica.Read(op)
	}
}

func (r *Router) Replicate(op journal.ReplicateOp) {
	replica, err := r.obtainReplica(op.Journal, false)

	if err != nil {
		op.Result <- journal.ReplicateResult{Error: err}
	} else {
		replica.Replicate(op)
	}
}

func (r *Router) Append(op journal.AppendOp) {
	replica, err := r.obtainReplica(op.Journal, true)

	if err != nil {
		op.Result <- err
	} else {
		replica.Append(op)
	}
}

// Retrieves or creates a new Replica for |name|, iff this router is responsible
// for journal |name| in role |wantBroker|.
func (r *Router) obtainReplica(name journal.Name, wantBroker bool) (JournalReplica, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Fast-path: return a live replica in the desired role.
	replica, hasReplica := r.replicas[name]
	if hasReplica && (!wantBroker || replica.IsBroker()) {
		return replica, nil
	}

	routes := r.router.Route(name.String())
	token := routeToken(routes)

	if wantBroker {
		if hasReplica {
			// |replica| is already known to not be a broker.
			return nil, AsRouteError(journal.ErrNotBroker, routes)
		} else if isBroker(r.localRouteKey, routes) {
			// Correctly routed request for a new replica in broker role.
			replica = r.factory.NewReplica(name)
			replica.StartBrokeringWithPeers(token, peers(r.localRouteKey, routes))
			// Fallthrough below to begin tracking replica.
		} else {
			// We are not the correct broker for |name|.
			return nil, AsRouteError(journal.ErrNotBroker, routes)
		}
	} else { // We want any replica.
		if isBroker(r.localRouteKey, routes) {
			// We *could* use a non-broker to fulfill the request, but because
			// we're the broker, we should broker.
			replica = r.factory.NewReplica(name)
			replica.StartBrokeringWithPeers(token, peers(r.localRouteKey, routes))
		} else if isReplica(r.localRouteKey, routes) {
			// Correctly routed request for a new replica in non-broker role.
			replica = r.factory.NewReplica(name)
			replica.StartReplicating(token)
			// Fallthrough below to begin tracking replica.
		} else {
			// We are not a correct replica for |name|.
			return nil, AsRouteError(journal.ErrNotReplica, routes)
		}
	}
	// Arrange to observe updates of |name|'s routing within the server topology.
	r.router.Track(name.String(), routes)

	r.replicas[name] = replica
	return replica, nil
}

// |key| is a broker if it is index 0 in |routes|.
func isBroker(key string, routes []discovery.HRWRoute) bool {
	return len(routes) != 0 && routes[0].Key == key
}

// |key| is a replica if it appears in |routes|.
func isReplica(key string, routes []discovery.HRWRoute) bool {
	for _, r := range routes {
		if r.Key == key {
			return true
		}
	}
	return false
}

// Composes an opaque token which captures the topology described in |routes|.
func routeToken(routes []discovery.HRWRoute) string {
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

// Builds a ReplicateClient for each remote route (not matching |localKey|).
func peers(localKey string, routes []discovery.HRWRoute) []journal.Replicator {
	peers := make([]journal.Replicator, 0, len(routes)-1)
	for _, r := range routes {
		if r.Key == localKey {
			continue
		}
		peers = append(peers, NewReplicateClient(r.Value.(*discovery.Endpoint)))
	}
	return peers
}

func (r *Router) onMembershipChange(members, old, new discovery.KeyValues) {
	// Note that we're called from EtcdService's goroutine.
	r.mu.Lock()
	defer r.mu.Unlock()

	r.router.RebuildRoutes(members, old, new)
}

func (r *Router) onRouteUpdate(journalName string,
	oldRoutes, newRoutes []discovery.HRWRoute) {
	// Called from within onMembershipChange(), so we're already locked.
	name := journal.Name(journalName)

	if !isReplica(r.localRouteKey, newRoutes) {
		// We're no longer responsible for this journal.
		if isBroker(r.localRouteKey, oldRoutes) {
			log.WithFields(log.Fields{
				"oldRoutes": oldRoutes,
				"newRoutes": newRoutes,
				"journal":   name,
			}).Error("was broker, now not even replica!")
		}
		r.replicas[name].Shutdown()

		r.router.Drop(journalName)
		delete(r.replicas, name)
		return
	}

	newToken := routeToken(newRoutes)

	if isBroker(r.localRouteKey, newRoutes) {
		r.replicas[name].StartBrokeringWithPeers(newToken,
			peers(r.localRouteKey, newRoutes))
	} else {
		r.replicas[name].StartReplicating(newToken)
	}
	return
}

// RouteError represents an Error that can be retried against |Location|.
type RouteError struct {
	Err error
	// Location of responsible server for the operation.
	Location *url.URL
}

// Builds a RouteError around |wrapped|, if possible. Otherwise, returns |wrapped|.
func AsRouteError(wrapped error, routes []discovery.HRWRoute) error {
	if len(routes) == 0 {
		return wrapped
	}
	url, err := routes[0].Value.(*discovery.Endpoint).ResolveURL()

	if err != nil {
		log.WithFields(log.Fields{"err": err, "ep": routes[0].Value}).Warn("resolve failed")
		return wrapped
	}
	return RouteError{Err: wrapped, Location: url}
}

func (re RouteError) Error() string {
	return re.Err.Error()
}

func (err RouteError) RerouteURL(in *url.URL) *url.URL {
	rewrite := &url.URL{}
	*rewrite = *in
	rewrite.Host = err.Location.Host
	rewrite.Scheme = err.Location.Scheme
	return rewrite
}
