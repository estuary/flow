package gazette

import (
	"encoding/json"
	"fmt"
	"github.com/coreos/go-etcd/etcd"
	gc "github.com/go-check/check"
	"github.com/pippio/api-server/discovery"
	"sort"
	"strings"
)

type DispatcherSuite struct {
	etcd       *discovery.EtcdMemoryService
	dispatcher *dispatcher

	recorded []string
}

func (s *DispatcherSuite) SetUpSuite(c *gc.C) {
	s.etcd = discovery.NewEtcdMemoryService()
	s.etcd.MakeDirectory("/gazette/members")

	kvs, err := discovery.NewKeyValueService("/gazette", s.etcd,
		func(key, value string) (interface{}, error) {
			ep := &discovery.Endpoint{}
			return ep, json.Unmarshal([]byte(value), ep)
		})
	c.Assert(err, gc.IsNil)

	s.dispatcher = NewDispatcher(kvs, s, "members/localRoute", 3)
}

func (s *DispatcherSuite) TestLifecycleWithRegressionFixture(c *gc.C) {
	// Names in this test have been tweaked to produce the desired
	// routing relationships.
	s.etcd.Announce("/gazette/members/peerOne",
		&discovery.Endpoint{BaseURL: "one"}, 0)

	journal := s.dispatcher.obtainJournal("journal/abcde")
	c.Check(journal, gc.IsNil)

	// Announce the local route.
	s.etcd.Announce("/gazette/members/localRoute",
		&discovery.Endpoint{BaseURL: "local"}, 0)

	// Obtain a new journal we're a replica of.
	journal = s.dispatcher.obtainJournal("journal/abcde")
	c.Check(journal, gc.NotNil)

	s.checkRecorded(c, []string{
		"created replica journal/abcde (peerOne|localRoute)"})

	// Another peer, ranking after peerOne and localRoute.
	s.etcd.Announce("/gazette/members/peerOther",
		&discovery.Endpoint{BaseURL: "other"}, 0)

	s.checkRecorded(c, []string{
		"journal/abcde => replica peerOne|localRoute|peerOther"})

	// Drop peerOne. We become the journal broker
	s.etcd.Apply(&etcd.Response{Action: discovery.EtcdExpireOp,
		Node: &etcd.Node{Key: "/gazette/members/peerOne"}})

	s.checkRecorded(c, []string{
		"journal/abcde => broker localRoute|peerOther ([other])"})

	// Obtain a new journal we're the broker of.
	journal = s.dispatcher.obtainJournal("journal/foobar")
	c.Check(journal, gc.NotNil)

	s.checkRecorded(c, []string{
		"created broker journal/foobar (localRoute|peerOther, [other])"})

	// Two new peers. abcde is a replica again, and foobar remains a
	// broker but is notified of route updates.
	s.etcd.Announce("/gazette/members/peerTwo",
		&discovery.Endpoint{BaseURL: "two"}, 0)

	s.checkRecorded(c, []string{
		"journal/abcde => replica peerTwo|localRoute|peerOther",
		"journal/foobar => broker localRoute|peerTwo|peerOther ([two,other])"})

	s.etcd.Announce("/gazette/members/peerThree",
		&discovery.Endpoint{BaseURL: "three"}, 0)

	s.checkRecorded(c, []string{
		"journal/abcde => replica peerThree|peerTwo|localRoute",
		"journal/foobar => broker localRoute|peerThree|peerTwo ([three,two])"})

	// Another peer. We're a replica of foobar, and no longer a replica of abcde.
	s.etcd.Announce("/gazette/members/peerFour",
		&discovery.Endpoint{BaseURL: "four"}, 0)

	s.checkRecorded(c, []string{
		"journal/abcde => shutdown",
		"journal/foobar => replica peerFour|localRoute|peerThree"})

	journal = s.dispatcher.obtainJournal("journal/abcde")
	c.Check(journal, gc.IsNil)
	c.Check(s.dispatcher.journals, gc.HasLen, 1)

	// Drop a peer, such that abcde is a replica again. Because it's no
	// longer tracked, it's replica status must be lazily discovered again.
	s.etcd.Apply(&etcd.Response{Action: discovery.EtcdExpireOp,
		Node: &etcd.Node{Key: "/gazette/members/peerThree"}})

	s.checkRecorded(c, []string{
		"journal/foobar => replica peerFour|localRoute|peerTwo"})

	// Re-discover that we're a replica of abcde.
	journal = s.dispatcher.obtainJournal("journal/abcde")
	c.Check(journal, gc.NotNil)

	s.checkRecorded(c, []string{
		"created replica journal/abcde (peerFour|peerTwo|localRoute)"})
}

func (s *DispatcherSuite) checkRecorded(c *gc.C, expect []string) {
	sort.Strings(s.recorded)
	c.Check(s.recorded, gc.DeepEquals, expect)
	s.recorded = nil
}

func (s *DispatcherSuite) LocalRouteKey() string {
	return "members/localRoute"
}

func (s *DispatcherSuite) CreateReplica(journal,
	routeToken string) DispatchedJournal {
	s.recorded = append(s.recorded, fmt.Sprintf(
		"created replica %s (%s)", journal, routeToken))
	return &dispatchRecorder{suite: s, journal: journal}
}

func (s *DispatcherSuite) CreateBroker(journal, routeToken string,
	peers []*discovery.Endpoint) DispatchedJournal {
	s.recorded = append(s.recorded, fmt.Sprintf(
		"created broker %s (%s, %s)", journal, routeToken, flatUrls(peers)))
	return &dispatchRecorder{suite: s, journal: journal}
}

func flatUrls(eps []*discovery.Endpoint) string {
	var tmp []string
	for _, ep := range eps {
		tmp = append(tmp, ep.BaseURL)
	}
	return "[" + strings.Join(tmp, ",") + "]"
}

type dispatchRecorder struct {
	suite   *DispatcherSuite
	journal string
}

func (r *dispatchRecorder) StartBrokeringWithPeers(routeToken string,
	peers []*discovery.Endpoint) {
	r.suite.recorded = append(r.suite.recorded, fmt.Sprintf(
		"%s => broker %s (%s)", r.journal, routeToken, flatUrls(peers)))
}

func (r *dispatchRecorder) StartReplicating(routeToken string) {
	r.suite.recorded = append(r.suite.recorded,
		fmt.Sprintf("%s => replica %s", r.journal, routeToken))
}

func (r *dispatchRecorder) Shutdown() {
	r.suite.recorded = append(r.suite.recorded,
		fmt.Sprintf("%s => shutdown", r.journal))
}

func (r *dispatchRecorder) Append(AppendOp)       {}
func (r *dispatchRecorder) Read(ReadOp)           {}
func (r *dispatchRecorder) Replicate(ReplicateOp) {}
func (r *dispatchRecorder) WriteHead() int64      { return -1 }

var _ = gc.Suite(&DispatcherSuite{})
