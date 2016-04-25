package gazette

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	gc "github.com/go-check/check"

	"github.com/pippio/gazette/journal"
)

type RouterSuite struct{}

func (s *RouterSuite) TestReadConditions(c *gc.C) {
	var recorder routerRecorder
	var router = NewRouter(recorder.NewReplica)

	var resultCh = make(chan journal.ReadResult, 1)
	var op = journal.ReadOp{
		ReadArgs: journal.ReadArgs{Journal: "foo/bar"},
		Result:   resultCh,
	}

	// Journal is not known.
	router.Read(op)
	c.Check(<-resultCh, gc.DeepEquals, journal.ReadResult{Error: journal.ErrNotFound})

	// Journal is now known, but non-local.
	router.transition("foo/bar", "http://server-one|http://server-two", -1, 1)
	recorder.verify(c) // Expect no replica was created.

	router.Read(op)
	c.Check(<-resultCh, gc.DeepEquals, journal.ReadResult{
		Error:      journal.ErrNotReplica,
		RouteToken: "http://server-one|http://server-two",
	})

	// Journal is now a local replica.
	router.transition("foo/bar", "http://server|http://local", 1, 1)
	recorder.verify(c, "created replica foo/bar",
		"foo/bar => replica http://server|http://local")

	router.Read(op)
	c.Check(<-resultCh, gc.DeepEquals, journal.ReadResult{
		WriteHead:  2345,
		RouteToken: "http://server|http://local", // Added by Router.
	})

	// Journal is no longer local.
	router.transition("foo/bar", "http://server-fin", -1, 1)
	recorder.verify(c, "foo/bar => shutdown")

	router.Read(op)
	c.Check(<-resultCh, gc.DeepEquals, journal.ReadResult{
		Error:      journal.ErrNotReplica,
		RouteToken: "http://server-fin",
	})
}

func (s *RouterSuite) TestAppendConditions(c *gc.C) {
	var recorder routerRecorder
	var router = NewRouter(recorder.NewReplica)

	var resultCh = make(chan journal.AppendResult, 1)
	var op = journal.AppendOp{
		AppendArgs: journal.AppendArgs{Journal: "foo/bar"},
		Result:     resultCh,
	}

	// Journal is not known.
	router.Append(op)
	c.Check(<-resultCh, gc.DeepEquals, journal.AppendResult{Error: journal.ErrNotFound})

	// Journal is now known, but non-local.
	router.transition("foo/bar", "http://server-one|http://server-two", 2, 1)
	recorder.verify(c)

	router.Append(op)
	c.Check(<-resultCh, gc.DeepEquals, journal.AppendResult{
		Error:      journal.ErrNotBroker,
		RouteToken: "http://server-one|http://server-two",
	})

	// Journal is now a local replica.
	router.transition("foo/bar", "http://server-one|http://server-two", 1, 1)
	recorder.verify(c, "created replica foo/bar")

	router.Append(op)
	c.Check(<-resultCh, gc.DeepEquals, journal.AppendResult{
		Error:      journal.ErrNotBroker,
		RouteToken: "http://server-one|http://server-two", // Added by Router.
	})

	// Journal is now a local broker.
	router.transition("foo/bar", "http://local|http://remote", 0, 1)
	recorder.verify(c, "foo/bar => broker http://local|http://remote ([remote])")
	c.Check(router.HasServedAppend("foo/bar"), gc.Equals, false)

	router.Append(op)
	c.Check(<-resultCh, gc.DeepEquals, journal.AppendResult{
		WriteHead:  1234,
		RouteToken: "http://local|http://remote",
	})
	c.Check(router.HasServedAppend("foo/bar"), gc.Equals, true)

	// Change topology. Expect HasServedAppend changes accordingly.
	router.transition("foo/bar", "http://local|http://remote-two", 0, 1)
	recorder.verify(c, "foo/bar => broker http://local|http://remote-two ([remote-two])")
	c.Check(router.HasServedAppend("foo/bar"), gc.Equals, false)

	router.Append(op)
	c.Check(<-resultCh, gc.DeepEquals, journal.AppendResult{
		WriteHead:  1234,
		RouteToken: "http://local|http://remote-two",
	})
	c.Check(router.HasServedAppend("foo/bar"), gc.Equals, true)

	// A replica is removed, and we are no longer able to broker.
	router.transition("foo/bar", "http://local", 0, 1)
	recorder.verify(c, "foo/bar => broker http://local ([])")

	router.Append(op)
	c.Check(<-resultCh, gc.DeepEquals, journal.AppendResult{
		Error:      journal.ErrReplicationFailed,
		RouteToken: "http://local",
	})
	c.Check(router.HasServedAppend("foo/bar"), gc.Equals, false)
}

func (s *RouterSuite) TestReplicateConditions(c *gc.C) {
	var recorder routerRecorder
	var router = NewRouter(recorder.NewReplica)

	var resultCh = make(chan journal.ReplicateResult, 1)
	var op = journal.ReplicateOp{
		ReplicateArgs: journal.ReplicateArgs{Journal: "foo/bar"},
		Result:        resultCh,
	}

	router.Replicate(op)
	c.Check(<-resultCh, gc.DeepEquals, journal.ReplicateResult{
		Error: journal.ErrNotFound,
	})

	// Journal is now known, but non-local.
	router.transition("foo/bar", "http://server-one|http://server-two", -1, 1)
	recorder.verify(c) // Expect no replica was created.

	router.Replicate(op)
	c.Check(<-resultCh, gc.DeepEquals, journal.ReplicateResult{
		Error: journal.ErrNotReplica,
	})

	// Journal is local.
	router.transition("foo/bar", "http://server|http://local", 1, 1)
	recorder.verify(c, "created replica foo/bar",
		"foo/bar => replica http://server|http://local")

	// Expect RouteToken is verified.
	op.ReplicateArgs.RouteToken = "http://wrong-token"
	router.Replicate(op)
	c.Check(<-resultCh, gc.DeepEquals, journal.ReplicateResult{
		Error: journal.ErrWrongRouteToken,
	})

	op.ReplicateArgs.RouteToken = "http://server|http://local"
	router.Replicate(op)
	c.Check(<-resultCh, gc.DeepEquals, journal.ReplicateResult{
		ErrorWriteHead: 3456,
	})
}

func (s *RouterSuite) TestBrokerRedirect(c *gc.C) {
	req, _ := http.NewRequest("GET", "/foo/bar?baz", nil)

	// Empty base path.
	w := httptest.NewRecorder()
	brokerRedirect(w, req, "https://server-one|http://server-two", http.StatusGone)
	c.Check(w.Code, gc.Equals, http.StatusGone)
	c.Check(w.Header().Get("Location"), gc.Equals, "https://server-one/foo/bar?baz")

	// Prefixed path.
	w = httptest.NewRecorder()
	brokerRedirect(w, req, "https://server-one/base/path|http://server-two", http.StatusGone)
	c.Check(w.Code, gc.Equals, http.StatusGone)
	c.Check(w.Header().Get("Location"), gc.Equals, "https://server-one/base/path/foo/bar?baz")

	// Single server.
	w = httptest.NewRecorder()
	brokerRedirect(w, req, "https://server/", http.StatusGone)
	c.Check(w.Code, gc.Equals, http.StatusGone)
	c.Check(w.Header().Get("Location"), gc.Equals, "https://server/foo/bar?baz")
}

// Implementation of ReplicaFactory. Returns a JournalReplica implementation
// which records calls.
type routerRecorder []string

type replicaRecorder struct {
	journal.Name
	recorder *routerRecorder
}

func (r *routerRecorder) NewReplica(name journal.Name) JournalReplica {
	*r = append(*r, fmt.Sprintf("created replica %s", name))
	return replicaRecorder{name, r}
}

func (r *routerRecorder) verify(c *gc.C, events ...string) {
	c.Check([]string(*r), gc.DeepEquals, events)
	*r = (*r)[:0]
}

func (r replicaRecorder) StartBrokeringWithPeers(token journal.RouteToken,
	peers []journal.Replicator) {

	*r.recorder = append(*r.recorder, fmt.Sprintf(
		"%s => broker %s (%s)", r.Name, token, flatPaths(peers)))
}

func (r replicaRecorder) StartReplicating(token journal.RouteToken) {
	*r.recorder = append(*r.recorder,
		fmt.Sprintf("%s => replica %s", r.Name, token))
}

func (r replicaRecorder) Shutdown() {
	*r.recorder = append(*r.recorder, fmt.Sprintf("%s => shutdown", r.Name))
}

// Trivial implementations of each operation handler,
// which pass back a distinguishing WriteHead.
func (r replicaRecorder) Append(op journal.AppendOp) {
	op.Result <- journal.AppendResult{WriteHead: 1234}
}
func (r replicaRecorder) Read(op journal.ReadOp) {
	op.Result <- journal.ReadResult{WriteHead: 2345}
}
func (r replicaRecorder) Replicate(op journal.ReplicateOp) {
	op.Result <- journal.ReplicateResult{ErrorWriteHead: 3456}
}

func flatPaths(peers []journal.Replicator) string {
	var tmp []string
	for _, peer := range peers {
		url, _ := peer.(ReplicateClient).endpoint.URL()
		tmp = append(tmp, url.Host)
	}
	return "[" + strings.Join(tmp, ",") + "]"
}

var _ = gc.Suite(&RouterSuite{})
