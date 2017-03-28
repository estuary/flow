package consumer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"

	etcd "github.com/coreos/etcd/client"
	etcd3 "github.com/coreos/etcd/clientv3"
	gc "github.com/go-check/check"
	uuid "github.com/satori/go.uuid"

	"github.com/pippio/consensus"
	"github.com/pippio/endpoints"
	"github.com/pippio/gazette/gazette"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/message"
	"github.com/pippio/gazette/topic"
)

// Test topics for events which add or subtract an amount to a key. Note that
// subtractTopic has a different Partitions count than addTopic. The import is
// that subtractions will be seen by just one shard, but additions will be seen
// by two. We thus produce up to two merged values for each key: the
// combination of additions, and the combination of additions + subtractions.
//
// Additionally, to test independent handling of multiple topic groups, we also
// consume a topic of UUIDs that get reversed and place into an output journal,
// which should occur _out_ of lockstep with the handling of adds/subtracts.
var (
	addTopic = &topic.Description{
		Name:       "pippio-journals/integration-tests/add-updates",
		Partitions: 2,
		RoutingKey: func(m interface{}) string {
			return strconv.FormatInt(m.(*addSubMessage).Key, 16)
		},
		GetMessage: func() topic.Unmarshallable {
			return new(addSubMessage)
		},
		PutMessage: func(m topic.Unmarshallable) {},
	}

	subtractTopic = &topic.Description{
		Name:       "pippio-journals/integration-tests/subtract-updates",
		Partitions: 4,
		RoutingKey: addTopic.RoutingKey,
		GetMessage: addTopic.GetMessage,
		PutMessage: addTopic.PutMessage,
	}

	reverseInTopic = &topic.Description{
		Name:       "pippio-journals/integration-tests/reverse-in",
		Partitions: 3,
		RoutingKey: func(m interface{}) string { return string(*m.(*stringMessage)) },
		GetMessage: func() topic.Unmarshallable { return new(stringMessage) },
		PutMessage: func(m topic.Unmarshallable) {},
	}
)

// Output journal into which merged CSV key/value rows are produced.
const (
	kAddSubtractLog journal.Name = "pippio-journals/integration-tests/add-subtract-merged"
	kReverseLog     journal.Name = "pippio-journals/integration-tests/reverse-out"
	kConsumerRoot                = "/gazette/consumers/tests/consumer2-test"
)

type ConsumerSuite struct {
	etcdClient  etcd.Client
	etcdClient3 *etcd3.Client
	gazette     struct {
		*gazette.Client
		*gazette.WriteService
	}
	runFinishedCh chan struct{}
}

func (s *ConsumerSuite) SetUpSuite(c *gc.C) {
	if testing.Short() {
		c.Skip("skipping consumer2 integration tests in short mode")
	}

	endpoints.ParseFromEnvironment()

	var err error
	s.etcdClient, err = etcd.New(etcd.Config{
		Endpoints: []string{"http://" + *endpoints.EtcdEndpoint}})
	c.Assert(err, gc.IsNil)

	s.etcdClient3, err = etcd3.New(etcd3.Config{
		Endpoints: []string{"http://" + *endpoints.EtcdEndpoint}})
	c.Assert(err, gc.IsNil)

	s.gazette.Client, err = gazette.NewClient(*endpoints.GazetteEndpoint)
	c.Assert(err, gc.IsNil)

	// Skip suite if Etcd is not available.
	if _, err = etcd.NewKeysAPI(s.etcdClient).Get(context.Background(), "/",
		&etcd.GetOptions{Recursive: false}); err != nil {
		c.Skip("Etcd not available: " + err.Error())
	}
	// Skip suite if Etcd3 is not available.
	if _, err = s.etcdClient3.Get(context.Background(), "/"); err != nil {
		c.Skip("Etcd3 not available: " + err.Error())
	}
	// Skip if a Gazette endpoint is not reachable.
	result, _ := s.gazette.Head(journal.ReadArgs{Journal: kAddSubtractLog, Offset: -1})
	if _, ok := result.Error.(net.Error); ok {
		c.Skip("Gazette not available: " + result.Error.Error())
		return
	}

	s.gazette.WriteService = gazette.NewWriteService(s.gazette.Client)
	s.gazette.WriteService.Start()

	s.runFinishedCh = make(chan struct{})
}

func (s *ConsumerSuite) SetUpTest(c *gc.C) {
	// Truncate existing recovery logs to read from current write heads.
	// Note: Setting up may fail due to stale etcd keys. Wait 1 minute or
	// delete the stale etcd directory structure yourself.
	c.Assert(ResetShardsToJournalHeads(s.buildRunner(0, 0)), gc.IsNil)
}

func (s *ConsumerSuite) TearDownSuite(c *gc.C) {
	if s.gazette.WriteService != nil {
		s.gazette.WriteService.Stop()
	}
}

func (s *ConsumerSuite) TestBasic(c *gc.C) {
	var csvReader = s.openResultReader(kAddSubtractLog, c)
	var reverseReader = s.openResultReader(kReverseLog, c)
	var generator = newTestGenerator(s.gazette)

	var runner = s.buildRunner(0, 0)
	go s.serveRunner(c, runner)

	generator.publishSomeValues(c)
	generator.verifyExpectedOutputs(c, csvReader, reverseReader)

	s.stopRunner(c, runner)
}

func (s *ConsumerSuite) TestHandoffBlocksUntilReady(c *gc.C) {
	var generator = newTestGenerator(s.gazette)

	var runner = s.buildRunner(0, 2) // Require 2 replicas.
	go s.serveRunner(c, runner)

	// Verifies |runner| doesn't exit over the next second.
	var expectDoesntExit = func() {
		select {
		case <-s.runFinishedCh:
			c.Error("consumer runner exited")
		case <-time.After(time.Second):
			// Pass.
		}
	}

	// Updates a peer consumer's item fixture statuses in Etcd.
	var setPeerStatus = func(peer, status string) {

		for _, group := range runner.Consumer.Groups() {
			var n, _ = group.NumShards()

			for i := 0; i != n; i++ {
				var shard = ShardID{group.Name, i}.String()
				var key = runner.PathRoot() + "/" + consensus.ItemsPrefix + "/" + shard + "/" + peer

				var _, err = runner.KeysAPI().Set(context.Background(),
					key, status, &etcd.SetOptions{TTL: 2 * time.Second})
				c.Check(err, gc.IsNil)
			}
		}
	}

	generator.publishSomeValues(c)

	// Cancel the runner. It should still run until two replicas declare themselves ready.
	c.Check(consensus.Cancel(runner), gc.IsNil)
	expectDoesntExit()

	setPeerStatus("peer-1", Ready)
	setPeerStatus("peer-2", Recovering)
	expectDoesntExit()

	// Allow the consumer to exit.
	setPeerStatus("peer-2", Ready)

	select {
	case <-s.runFinishedCh:
		// Pass.
	case <-time.After(time.Second):
		c.Error("consumer didn't exit")
	}
}

func (s *ConsumerSuite) TestHandoffWithMultipleRunners(c *gc.C) {
	var csvReader = s.openResultReader(kAddSubtractLog, c)
	var reverseReader = s.openResultReader(kReverseLog, c)

	// Intermix starting & stopping runners with message processing, handing off
	// responsibility for processing around the "cluster". publishSomeValues
	// regulates publishing over the course of > 1s, so we expect publishes to
	// race startRunner, and we expect to get the right answer anyway.
	var generator = newTestGenerator(s.gazette)

	var r0 = s.buildRunner(0, 1)
	go s.serveRunner(c, r0)
	generator.publishSomeValues(c)

	var r1 = s.buildRunner(1, 1)
	go s.serveRunner(c, r1)
	generator.publishSomeValues(c)

	s.stopRunner(c, r0) // r0 hands off to r1.
	generator.publishSomeValues(c)

	var r2 = s.buildRunner(2, 1)
	go s.serveRunner(c, r2)
	generator.publishSomeValues(c)

	s.stopRunner(c, r1) // r1 hands off to r2.
	generator.publishSomeValues(c)

	// Verify expected values appear in the merged output log.
	generator.verifyExpectedOutputs(c, csvReader, reverseReader)

	r2.ReplicaCount = 0 // Drop replica count so that runner will exit.
	s.stopRunner(c, r2)
}

func (s *ConsumerSuite) buildRunner(i, replicas int) *Runner {
	return &Runner{
		Consumer:        new(testConsumer),
		LocalRouteKey:   fmt.Sprintf("test-consumer-%d", i),
		LocalDir:        fmt.Sprintf("/var/tmp/integration-tests/consumer2/runner-%d", i),
		ConsumerRoot:    kConsumerRoot,
		RecoveryLogRoot: "pippio-journals/integration-tests/consumer2",
		ReplicaCount:    replicas,

		Etcd:    s.etcdClient,
		Etcd3:   s.etcdClient3,
		Gazette: s.gazette,
	}
}

func (s *ConsumerSuite) serveRunner(c *gc.C, r *Runner) {
	c.Check(r.Run(), gc.IsNil)
	s.runFinishedCh <- struct{}{}
}

func (s *ConsumerSuite) stopRunner(c *gc.C, r *Runner) {
	c.Check(consensus.Cancel(r), gc.IsNil)
	<-s.runFinishedCh
}

func (s *ConsumerSuite) openResultReader(name journal.Name, c *gc.C) io.Reader {
	if err := s.gazette.Create(name); err != nil && err != journal.ErrExists {
		c.Fatal(err)
	}
	// Issue a long-lived Gazette read to the result journal.
	readResp, reader := s.gazette.GetDirect(journal.ReadArgs{
		Journal:  name,
		Offset:   -1,
		Deadline: time.Now().Add(time.Minute),
	})
	c.Assert(readResp.Error, gc.IsNil)

	return reader
}

// A topic.Marshallable / Unmarshallable which encodes a Key and Update.
type addSubMessage struct {
	Key, Update int64
}

func (t *addSubMessage) Unmarshal(b []byte) error {
	return binary.Read(bytes.NewReader(b), binary.LittleEndian, t)
}

func (t *addSubMessage) Size() int {
	return binary.Size(t)
}

func (t *addSubMessage) MarshalTo(b []byte) (n int, err error) {
	w := bytes.NewBuffer(b[:0])
	err = binary.Write(w, binary.LittleEndian, t)
	n = w.Len()
	return
}

type stringMessage string

func (s *stringMessage) Unmarshal(b []byte) error {
	*s = stringMessage(b)
	return nil
}

func (s *stringMessage) Size() int { return len(*s) }
func (s *stringMessage) MarshalTo(b []byte) (n int, err error) {
	return copy(b, []byte(*s)), nil
}

// Generator which publishes random add and subtraction events to their
// respective topics. testGenerator maintains an expected final result for each
// published key.
type testGenerator struct {
	publisher                    topic.Publisher
	addValues, addSubtractValues map[int64]int64
	stringValues                 map[stringMessage]struct{}

	cancelCh chan struct{}
	doneCh   chan struct{}
}

func newTestGenerator(writer journal.Writer) *testGenerator {
	return &testGenerator{
		publisher:         message.SimplePublisher{writer},
		addValues:         make(map[int64]int64),
		addSubtractValues: make(map[int64]int64),
		stringValues:      make(map[stringMessage]struct{}),
	}
}

func (g *testGenerator) publishSomeValues(c *gc.C) {
	src := rand.New(rand.NewSource(time.Now().UnixNano()))
	zipf := rand.NewZipf(src, 1.1, 10.0, 1000)

	// Publish 10K messages over at least 1 second (sleeping 10ms every 100 messages).
	for i := 0; i != 10000; i++ {
		// Writing to add and subtract topics.
		var msg = addSubMessage{
			Key:    int64(zipf.Uint64()),
			Update: int64(src.Int31()),
		}

		if src.Int()%2 == 0 { // Add.
			c.Check(g.publisher.Publish(&msg, addTopic), gc.IsNil)
			g.addValues[msg.Key] = g.addValues[msg.Key] + msg.Update
			g.addSubtractValues[msg.Key] = g.addSubtractValues[msg.Key] + msg.Update
		} else { // Subtract.
			c.Check(g.publisher.Publish(&msg, subtractTopic), gc.IsNil)
			g.addSubtractValues[msg.Key] = g.addSubtractValues[msg.Key] - msg.Update
		}

		// Writing to string topic.
		var smsg = stringMessage(uuid.NewV4().String())
		g.stringValues[smsg] = struct{}{}
		c.Check(g.publisher.Publish(&smsg, reverseInTopic), gc.IsNil)

		if i%100 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (g *testGenerator) verifyExpectedOutputs(c *gc.C, addReader, reverseReader io.Reader) {
	csvReader := csv.NewReader(addReader)
	reverseScanner := bufio.NewScanner(reverseReader)

	// Consume merged output, looking for expected values.
	for len(g.addValues) != 0 && len(g.addSubtractValues) != 0 {
		record, err := csvReader.Read()
		c.Assert(err, gc.IsNil)

		key, err := strconv.ParseInt(record[0], 10, 64)
		c.Assert(err, gc.IsNil)
		value, err := strconv.ParseInt(record[1], 10, 64)
		c.Assert(err, gc.IsNil)

		if g.addValues[key] == value {
			delete(g.addValues, key)
		}
		if g.addSubtractValues[key] == value {
			delete(g.addSubtractValues, key)
		}
	}

	uniqueItems := make(map[stringMessage]struct{})
	for reverseScanner.Scan() {
		key := reverse(reverseScanner.Text())
		_, ok := g.stringValues[stringMessage(key)]
		c.Check(ok, gc.Equals, true)
		uniqueItems[stringMessage(key)] = struct{}{}
		if len(uniqueItems) == len(g.stringValues) {
			break
		}
	}
	c.Check(uniqueItems, gc.DeepEquals, g.stringValues)
}

// A test Consumer which combines add & subtract updates into |kAddSubtractLog|.
type testConsumer struct{}

func (c *testConsumer) Groups() TopicGroups {
	return TopicGroups{
		{
			Name:   "add-subtract",
			Topics: []*topic.Description{addTopic, subtractTopic},
		},
		{
			Name:   "reverse",
			Topics: []*topic.Description{reverseInTopic},
		},
	}
}

func (c *testConsumer) InitShard(s Shard) error {
	s.SetCache(make(map[int64]int64))
	return nil
}

func (c *testConsumer) Consume(m message.Message, s Shard, pub topic.Publisher) error {
	switch m.Value.(type) {
	case *addSubMessage:
		event := m.Value.(*addSubMessage)
		cache := s.Cache().(map[int64]int64)

		value, ok := cache[event.Key]
		if !ok {
			// Fill from RocksDB.
			key := strconv.FormatInt(event.Key, 10)
			result, err := s.Database().GetBytes(s.ReadOptions(), []byte(key))
			if err == nil && len(result) != 0 {
				value, err = strconv.ParseInt(string(result), 10, 64)
			}
			if err != nil {
				return err
			}
		}

		if m.Topic == addTopic {
			cache[event.Key] = value + event.Update
		} else {
			cache[event.Key] = value - event.Update
		}
	case *stringMessage:
		// RocksDB coverage already exists for the add-subtract test. Just
		// reverse the string and write to the Publisher immediately.
		// Note: Breaks 'exactly-once' processing just for this topic group.
		s := string(*m.Value.(*stringMessage))
		pub.Write(kReverseLog, []byte(reverse(s)+"\n"))
	}
	return nil
}

func (c *testConsumer) Flush(s Shard, pub topic.Publisher) error {
	cache := s.Cache().(map[int64]int64)
	writeBatch := s.Transaction()

	for key, value := range cache {
		keyStr, valStr := strconv.FormatInt(key, 10), strconv.FormatInt(value, 10)
		writeBatch.Put([]byte(keyStr), []byte(valStr))

		// Publish the current merged value to the result log.
		out := fmt.Sprintf("%d,%d\n", key, value)
		if _, err := pub.Write(kAddSubtractLog, []byte(out)); err != nil {
			return err
		}
		delete(cache, key) // Reset for next transaction.
	}
	return nil
}

func reverse(in string) string {
	l := len(in)
	out := make([]rune, l)
	for i, c := range in {
		out[l-i-1] = c
	}
	return string(out)
}

var _ = gc.Suite(&ConsumerSuite{})
