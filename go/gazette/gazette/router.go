package gazette

import (
	"encoding/json"
	"expvar"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/api-server/discovery"
	"github.com/pippio/gazette/journal"
)

// Builds a JournalReplica instance with the given journal.Name.
type ReplicaFactory func(journal.Name) JournalReplica

// Routes and dispatches Read, Append, and Replicate operations to a
// collection of responsible JournalReplicas.
type Router struct {
	replicaFactory ReplicaFactory

	routes   map[journal.Name]*journalRoute
	routesMu sync.Mutex
}

func NewRouter(factory ReplicaFactory) *Router {
	r := &Router{
		replicaFactory: factory,
		routes:         make(map[journal.Name]*journalRoute),
	}

	gazetteMap.Set("brokers", journalStringer(r.BrokeredJournals))
	gazetteMap.Set("replicas", journalStringer(r.ReplicatedJournals))
	return r
}

func (r *Router) Read(op journal.ReadOp) {
	r.routesMu.Lock()
	defer r.routesMu.Unlock()

	if route, ok := r.routes[op.Journal]; !ok || route.token == "" {
		op.Result <- journal.ReadResult{Error: journal.ErrNotFound}
	} else if route.replica == nil {
		op.Result <- journal.ReadResult{
			Error:      journal.ErrNotReplica,
			RouteToken: route.token,
		}
	} else {

		// Proxy result to extend with RouteToken.
		forward := op.Result
		op.Result = make(chan journal.ReadResult, 1)

		go func(token journal.RouteToken) {
			result := <-op.Result
			result.RouteToken = token
			forward <- result
		}(route.token)

		route.replica.Read(op)
	}
}

func (r *Router) Append(op journal.AppendOp) {
	r.routesMu.Lock()
	defer r.routesMu.Unlock()

	if route, ok := r.routes[op.Journal]; !ok || route.token == "" {
		op.Result <- journal.AppendResult{Error: journal.ErrNotFound}
	} else if !route.broker {
		op.Result <- journal.AppendResult{
			Error:      journal.ErrNotBroker,
			RouteToken: route.token,
		}
	} else if !route.brokerReady {
		op.Result <- journal.AppendResult{
			Error:      journal.ErrReplicationFailed,
			RouteToken: route.token,
		}
	} else {

		// Proxy result to extend with RouteToken, and to potentially update
		// |lastAppendToken| on a successful Append.
		forward := op.Result
		op.Result = make(chan journal.AppendResult, 1)

		go func(token, lastAppendToken journal.RouteToken) {
			result := <-op.Result
			result.RouteToken = token

			if result.Error != nil || token == lastAppendToken {
				forward <- result
				return
			}

			r.routesMu.Lock()
			defer r.routesMu.Unlock()

			// Note that we must use the retained |token|, and not |route.token|,
			// as the latter may have changed out from under us during the call.
			// Similarly |route.lastAppendToken| may have been updated: in this
			// case we don't care, as it will still converge to the correct value.
			route.lastAppendToken = token
			forward <- result

		}(route.token, route.lastAppendToken)

		route.replica.Append(op)
	}
}

func (r *Router) Replicate(op journal.ReplicateOp) {
	r.routesMu.Lock()
	defer r.routesMu.Unlock()

	if route, ok := r.routes[op.Journal]; !ok {
		op.Result <- journal.ReplicateResult{Error: journal.ErrNotFound}
	} else if route.replica == nil {
		op.Result <- journal.ReplicateResult{Error: journal.ErrNotReplica}
	} else if op.RouteToken != route.token {
		op.Result <- journal.ReplicateResult{Error: journal.ErrWrongRouteToken}
	} else {
		route.replica.Replicate(op)
	}
}

// Returns whether |name| is both locally brokered and has successfully served
// an Append operation under the current route topology. This is an important
// indicator for consistency, as a successful Append ensures that all replicas
// reached agreement on the route token & write head during the transaction.
func (r *Router) HasServedAppend(name journal.Name) bool {
	r.routesMu.Lock()
	defer r.routesMu.Unlock()

	route, ok := r.routes[name]
	return ok && route.brokerReady && route.token == route.lastAppendToken
}

// Returns the set of Journals which are brokered by this Router.
func (r *Router) BrokeredJournals() []journal.Name {
	r.routesMu.Lock()
	defer r.routesMu.Unlock()

	var result []journal.Name

	for name, route := range r.routes {
		if route.broker {
			result = append(result, name)
		}
	}
	return result
}

// Returns the set of Journals which are replicated by this Router.
func (r *Router) ReplicatedJournals() []journal.Name {
	r.routesMu.Lock()
	defer r.routesMu.Unlock()

	var result []journal.Name

	for name, route := range r.routes {
		if !route.broker && route.replica != nil {
			result = append(result, name)
		}
	}
	return result
}

type journalRoute struct {
	// Nil iff journal is not replicated locally.
	replica JournalReplica
	// True iff journal is locally brokered.
	broker bool
	// True iff journal is locally brokered, and the required number of
	// replicas exist in the route topology.
	brokerReady bool
	// Current topology |token| of journal, and the token of the most-recent
	// Append operation which we successfully brokered.
	token, lastAppendToken journal.RouteToken
}

func (r *Router) transition(name journal.Name, rt journal.RouteToken,
	index, requiredReplicas int) {
	r.routesMu.Lock()
	defer r.routesMu.Unlock()

	// We are a replica if our index is within the range of required replicas.
	// Note the broker is a replica, and |requiredReplicas| is zero-indexed
	// (eg |requiredReplicas| of 2 implies one master and two replicas).
	var replica = index != -1 && index <= requiredReplicas

	route, ok := r.routes[name]
	if !ok {
		// Journal |name| is being tracked for the first time.
		route = new(journalRoute)
		r.routes[name] = route
	}

	if route.replica == nil && replica {
		// The replica doesn't exist, but should.
		route.replica = r.replicaFactory(name)
	} else if route.replica != nil && !replica {
		// The replica exists, but should not.
		route.replica.Shutdown()
		route.replica = nil
	}

	if route.token == rt {
		// This Journal's route is unchanged. No further work.
		return
	}
	route.token = rt

	// We serve as the broker iff we hold the master item lock, and a sufficent
	// number of replication peers are present in the route topology.
	var broker, brokerReady bool
	var peers []journal.Replicator

	if index == 0 {
		broker = true

		if peers = routePeers(rt); len(peers) >= requiredReplicas {
			brokerReady = true
		}
	}
	route.broker = broker
	route.brokerReady = brokerReady

	if route.broker {
		route.replica.StartBrokeringWithPeers(rt, peers)
	} else if route.replica != nil {
		route.replica.StartReplicating(rt)
	}
}

// Builds a Replicator for each non-master replica of |route|.
func routePeers(rt journal.RouteToken) []journal.Replicator {
	var peers []journal.Replicator

	for i, url := range strings.Split(string(rt), "|") {
		if i == 0 {
			// Skip local token.
			continue
		}
		ep := &discovery.Endpoint{BaseURL: url}
		peers = append(peers, NewReplicateClient(ep))
	}
	return peers
}

// Issues an HTTP redirect the current request applied to the journal broker.
func brokerRedirect(w http.ResponseWriter, r *http.Request, rt journal.RouteToken, code int) {
	broker := string(rt)
	if ind := strings.IndexByte(broker, '|'); ind != -1 {
		broker = broker[:ind]
	}

	redirect, err := url.Parse(broker)
	if err != nil {
		log.WithFields(log.Fields{"err": err, "broker": broker}).Error("failed to parse URL")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	redirect.Path = path.Join(redirect.Path, r.URL.Path)
	redirect.RawQuery = r.URL.RawQuery
	http.Redirect(w, r, redirect.String(), code)
}

// Helper functions for expvar purposes.
type journalStringer func() []journal.Name

func (js journalStringer) String() string {
	var ret []string

	for _, name := range js() {
		ret = append(ret, string(name))
	}
	sort.Strings(ret)

	if encoded, err := json.Marshal(ret); err != nil {
		return fmt.Sprintf("%q", err.Error())
	} else {
		return string(encoded)
	}
}

// Only publish the 'gazette' expvar once.
var gazetteMap *expvar.Map

func init() {
	gazetteMap = expvar.NewMap("gazette")
}
