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

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/trace"

	"github.com/LiveRamp/gazette/journal"
)

// Builds a JournalReplica instance with the given journal.Name.
type ReplicaFactory func(journal.Name) JournalReplica

// Routes and dispatches Read, Append, and Replicate operations to a
// collection of responsible JournalReplicas.
type Router struct {
	replicaFactory ReplicaFactory

	routes map[journal.Name]*journalRoute

	// This mutex guards any read or write operation on |routes| *and* its
	// underlying |*journalRoute| values.
	routesMu sync.Mutex
}

func NewRouter(factory ReplicaFactory) *Router {
	var r = &Router{
		replicaFactory: factory,
		routes:         make(map[journal.Name]*journalRoute),
	}

	gazetteMap.Set("brokers", journalStringer(r.BrokeredJournals))
	gazetteMap.Set("replicas", journalStringer(r.ReplicatedJournals))
	return r
}

func (r *Router) Read(op journal.ReadOp) {
	if tr, ok := trace.FromContext(op.Context); ok {
		tr.LazyPrintf("Read request: %s", op.ReadArgs)
	}

	var route, ok = r.readRoute(op.Journal)
	var result journal.ReadResult

	if !ok || route.token == "" {
		// This journal is unknown to us.
		result = journal.ReadResult{Error: journal.ErrNotFound}
	} else if route.replica == nil {
		// We're not a replica for this journal.
		result = journal.ReadResult{Error: journal.ErrNotReplica, RouteToken: route.token}
	}

	if result.Error != nil {
		if tr, ok := trace.FromContext(op.Context); ok {
			tr.LazyPrintf("Read result: %s", result)
			tr.SetError()
		}
		op.Result <- result
		return
	}

	// Proxy result to extend with RouteToken and trace.
	var forward = op.Result
	op.Result = make(chan journal.ReadResult, 1)

	go func(token journal.RouteToken) {
		var result = <-op.Result
		result.RouteToken = token

		if tr, ok := trace.FromContext(op.Context); ok {
			tr.LazyPrintf("Read result: %s", result)

			switch result.Error {
			case nil, journal.ErrNotYetAvailable:
				// Pass.
			default:
				tr.SetError()
			}
		}
		forward <- result
	}(route.token)

	route.replica.Read(op)
}

func (r *Router) Append(op journal.AppendOp) {
	if tr, ok := trace.FromContext(op.Context); ok {
		tr.LazyPrintf("Append request: %s", op.AppendArgs)
	}

	var route, ok = r.readRoute(op.Journal)
	var result journal.AppendResult

	if !ok || route.token == "" {
		// This journal is unknown to us.
		result = journal.AppendResult{Error: journal.ErrNotFound}
	} else if !route.broker {
		// We are not the broker for this journal.
		result = journal.AppendResult{Error: journal.ErrNotBroker, RouteToken: route.token}
	} else if !route.brokerReady {
		// We are the broker, but do not have the required number of replicas.
		result = journal.AppendResult{
			Error:      journal.ErrReplicationFailed,
			RouteToken: route.token,
		}
	}

	if result.Error != nil {
		if tr, ok := trace.FromContext(op.Context); ok {
			tr.LazyPrintf("Append result: %s", result)
			tr.SetError()
		}
		op.Result <- result
		return
	}

	// Proxy result to extend with RouteToken, and to potentially update
	// |lastAppendToken| on a successful Append.
	var forward = op.Result
	op.Result = make(chan journal.AppendResult, 1)

	go func(token, lastAppendToken journal.RouteToken) {
		var result = <-op.Result
		result.RouteToken = token

		if tr, ok := trace.FromContext(op.Context); ok {
			tr.LazyPrintf("Append result: %s", result)
			if result.Error != nil {
				tr.SetError()
			}
		}

		if result.Error == nil && token != lastAppendToken {
			// Note that the journal route may have changed on us during the
			// append operation, and we therefore apply our retained |token|
			// rather than its present value under |r.routes|.
			// Similarly |route.lastAppendToken| may have been updated by a raced
			// append: in this case we don't care, as it will still converge to
			// the correct value.
			r.routesMu.Lock()
			r.routes[op.Journal].lastAppendToken = token
			r.routesMu.Unlock()
		}

		forward <- result
	}(route.token, route.lastAppendToken)

	route.replica.Append(op)
}

func (r *Router) Replicate(op journal.ReplicateOp) {
	if tr, ok := trace.FromContext(op.Context); ok {
		tr.LazyPrintf("Replicate request: %s", op.ReplicateArgs)
	}

	var route, ok = r.readRoute(op.Journal)
	var result journal.ReplicateResult

	if !ok {
		result = journal.ReplicateResult{Error: journal.ErrNotFound}
	} else if route.replica == nil {
		result = journal.ReplicateResult{Error: journal.ErrNotReplica}
	} else if route.token != op.RouteToken {
		result = journal.ReplicateResult{Error: journal.ErrWrongRouteToken}
	}

	if result.Error != nil {
		if tr, ok := trace.FromContext(op.Context); ok {
			tr.LazyPrintf("Replicate result: %s", result)
			tr.SetError()
		}
		op.Result <- result
		return
	}

	// Proxy result to enable tracing.
	var forward = op.Result
	op.Result = make(chan journal.ReplicateResult, 1)

	go func() {
		var result = <-op.Result

		if tr, ok := trace.FromContext(op.Context); ok {
			tr.LazyPrintf("Replicate result: %s", result)
			if result.Error != nil {
				tr.SetError()
			}
		}
		forward <- result
	}()

	route.replica.Replicate(op)
}

// Returns whether |name| is both locally brokered and has successfully served
// an Append operation under the current route topology. This is an important
// indicator for consistency, as a successful Append ensures that all replicas
// reached agreement on the route token & write head during the transaction.
func (r *Router) HasServedAppend(name journal.Name) bool {
	var route, ok = r.readRoute(name)
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

// Updates |routes| with new information about the journal. Creates a route if
// it does not already exist, or updates the existing one otherwise. Holds the
// lock to update both |routes| and underlying |journalRoute| objects.
// Once created, a route cannot be removed from |routes| without restarting the
// process.
func (r *Router) transition(name journal.Name, rt journal.RouteToken,
	index, requiredReplicas int) {

	// We are a replica if our index is within the range of required replicas.
	// Note the broker is a replica, and |requiredReplicas| is zero-indexed
	// (eg |requiredReplicas| of 2 implies one master and two replicas).
	var replica = index != -1 && index <= requiredReplicas

	r.routesMu.Lock()
	defer r.routesMu.Unlock()

	var route, ok = r.routes[name]
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

func (r *Router) readRoute(name journal.Name) (journalRoute, bool) {
	r.routesMu.Lock()
	defer r.routesMu.Unlock()

	if route, ok := r.routes[name]; ok {
		return *route, ok
	}
	return journalRoute{}, false
}

// Builds a Replicator for each non-master replica of |route|.
func routePeers(rt journal.RouteToken) []journal.Replicator {
	var peers []journal.Replicator

	for i, url := range strings.Split(string(rt), "|") {
		if i == 0 {
			// Skip local token.
			continue
		}
		var ep = &CachedURL{Base: url}
		peers = append(peers, NewReplicateClient(ep))
	}
	return peers
}

// Issues an HTTP redirect the current request applied to the journal broker.
func brokerRedirect(w http.ResponseWriter, r *http.Request, rt journal.RouteToken, code int) {
	var broker = string(rt)
	if ind := strings.IndexByte(broker, '|'); ind != -1 {
		broker = broker[:ind]
	}

	var redirect, err = url.Parse(broker)
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
