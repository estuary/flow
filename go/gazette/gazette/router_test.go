package gazette

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/coreos/go-etcd/etcd"
	gc "github.com/go-check/check"

	"github.com/pippio/api-server/discovery"
	. "github.com/pippio/gazette/journal"
)

type RouterSuite struct {
	etcd   *discovery.EtcdMemoryService
	router *Router

	recorded []string
}

func (s *RouterSuite) SetUpSuite(c *gc.C) {
	s.etcd = discovery.NewEtcdMemoryService()
	s.etcd.MakeDirectory("/gazette/members")

	kvs, err := discovery.NewKeyValueService("/gazette", s.etcd,
		func(key, value string) (interface{}, error) {
			ep := &discovery.Endpoint{}
			return ep, json.Unmarshal([]byte(value), ep)
		})
	c.Assert(err, gc.IsNil)

	s.router = NewRouter(kvs, s, "members/localRoute", 3)
}

func (s *RouterSuite) TestLifecycleWithRegressionFixture(c *gc.C) {
	// Note that the expected outcomes of this test are tightly coupled to the
	// particular way which service member names are hashed & mapped into
	// replica sets. Eg, any change to hashing our routing will break this test.
	// That's a good thing! We don't want to change this unintentionally, as it
	// introduces inconsistency between clients & servers at different versions.
	s.etcd.Announce("/gazette/members/peerOne",
		&discovery.Endpoint{BaseURL: "one"}, 0)

	replica := s.router.obtainReplica("journal/abcde")
	c.Check(replica, gc.IsNil)

	// Announce the local route.
	s.etcd.Announce("/gazette/members/localRoute",
		&discovery.Endpoint{BaseURL: "local"}, 0)

	// Obtain a new journal we're a replica of.
	replica = s.router.obtainReplica("journal/abcde")
	c.Check(replica, gc.NotNil)

	s.checkRecorded(c, []string{
		"created replica journal/abcde",
		"journal/abcde => replica peerOne|localRoute"})

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
	replica = s.router.obtainReplica("journal/foobar")
	c.Check(replica, gc.NotNil)

	s.checkRecorded(c, []string{
		"created replica journal/foobar",
		"journal/foobar => broker localRoute|peerOther ([other])"})

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

	replica = s.router.obtainReplica("journal/abcde")
	c.Check(replica, gc.IsNil)
	c.Check(s.router.replicas, gc.HasLen, 1)

	// Drop a peer, such that abcde is a replica again. Because it's no
	// longer tracked, it's replica status must be lazily discovered again.
	s.etcd.Apply(&etcd.Response{Action: discovery.EtcdExpireOp,
		Node: &etcd.Node{Key: "/gazette/members/peerThree"}})

	s.checkRecorded(c, []string{
		"journal/foobar => replica peerFour|localRoute|peerTwo"})

	// Re-discover that we're a replica of abcde.
	replica = s.router.obtainReplica("journal/abcde")
	c.Check(replica, gc.NotNil)

	s.checkRecorded(c, []string{
		"created replica journal/abcde",
		"journal/abcde => replica peerFour|peerTwo|localRoute"})
}

func (s *RouterSuite) checkRecorded(c *gc.C, expect []string) {
	sort.Strings(s.recorded)
	c.Check(s.recorded, gc.DeepEquals, expect)
	s.recorded = nil
}

func (s *RouterSuite) LocalRouteKey() string {
	return "members/localRoute"
}

// Implementation of ReplicaFactory. Returns a JournalReplica implementation
// which records calls into |RouterSuite.recorded|.
func (s *RouterSuite) NewReplica(name Name) JournalReplica {
	s.recorded = append(s.recorded, fmt.Sprintf("created replica %s", name))
	return &replicaRecorder{suite: s, journal: name}
}

func flatUrls(peers []Replicator) string {
	var tmp []string
	for _, peer := range peers {
		tmp = append(tmp, peer.(ReplicateClient).endpoint.BaseURL)
	}
	return "[" + strings.Join(tmp, ",") + "]"
}

type replicaRecorder struct {
	suite   *RouterSuite
	journal Name
}

func (r *replicaRecorder) StartBrokeringWithPeers(token string, peers []Replicator) {
	r.suite.recorded = append(r.suite.recorded, fmt.Sprintf(
		"%s => broker %s (%s)", r.journal, token, flatUrls(peers)))
}

func (r *replicaRecorder) StartReplicating(token string) {
	r.suite.recorded = append(r.suite.recorded,
		fmt.Sprintf("%s => replica %s", r.journal, token))
}

func (r *replicaRecorder) Shutdown() {
	r.suite.recorded = append(r.suite.recorded,
		fmt.Sprintf("%s => shutdown", r.journal))
}

func (r *replicaRecorder) Append(AppendOp)       {}
func (r *replicaRecorder) Read(ReadOp)           {}
func (r *replicaRecorder) Replicate(ReplicateOp) {}

var _ = gc.Suite(&RouterSuite{})
