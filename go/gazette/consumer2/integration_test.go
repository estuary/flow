package consumer

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	etcd "github.com/coreos/etcd/client"
	gc "github.com/go-check/check"
	"golang.org/x/net/context"

	"github.com/pippio/api-server/endpoints"
	"github.com/pippio/consensus"
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
var addTopic = &topic.Description{
	Name:       "pippio-journals/integration-tests/add-updates",
	Partitions: 2,
	RoutingKey: func(m interface{}) string {
		return strconv.FormatInt(m.(*testMessage).Key, 16)
	},
	GetMessage: func() topic.Unmarshallable {
		return new(testMessage)
	},
	PutMessage: func(m topic.Unmarshallable) {},
}

var subtractTopic = &topic.Description{
	Name:       "pippio-journals/integration-tests/subtract-updates",
	Partitions: 4,
	RoutingKey: addTopic.RoutingKey,
	GetMessage: addTopic.GetMessage,
	PutMessage: addTopic.PutMessage,
}

// Output journal into which merged CSV key/value rows are produced.
const kMergedResultLog journal.Name = "pippio-journals/integration-tests/add-subtract-merged"

type ConsumerSuite struct {
	etcdClient    etcd.Client
	gazetteClient *gazette.Client
	writer        *gazette.WriteService
	runFinishedCh chan struct{}
}

func (s *ConsumerSuite) SetUpSuite(c *gc.C) {
	if testing.Short() {
		c.Skip("skipping consumer2 integration tests in short mode")
	}

	var err error
	s.etcdClient, err = etcd.New(etcd.Config{
		Endpoints: []string{"http://" + *endpoints.EtcdEndpoint}})
	c.Assert(err, gc.IsNil)

	s.gazetteClient, err = gazette.NewClient(*endpoints.GazetteEndpoint)
	c.Assert(err, gc.IsNil)

	// Skip suite if Etcd is not available.
	if _, err = etcd.NewKeysAPI(s.etcdClient).Get(context.Background(), "/",
		&etcd.GetOptions{Recursive: false}); err != nil {
		c.Skip("Etcd not available: " + err.Error())
	}
	// Skip if a Gazette endpoint is not reachable.
	result, _ := s.gazetteClient.Head(journal.ReadArgs{Journal: kMergedResultLog, Offset: -1})
	if result.Error != journal.ErrNotYetAvailable {
		c.Skip("Gazette not available: " + result.Error.Error())
		return
	}

	s.writer = gazette.NewWriteService(s.gazetteClient)
	s.writer.Start()

	s.runFinishedCh = make(chan struct{})
}

func (s *ConsumerSuite) SetUpTest(c *gc.C) {
	// Truncate existing recovery logs to read from current write heads.
	c.Assert(ResetShardsToJournalHeads(s.buildRunner(0)), gc.IsNil)
}

func (s *ConsumerSuite) TearDownSuite(c *gc.C) {
	if s.writer != nil {
		s.writer.Stop()
	}
}

func (s *ConsumerSuite) TestBasic(c *gc.C) {
	var csvReader = s.openResultReader(c)
	var generator = newTestGenerator(s.writer)

	go s.startRunner(c, 0)
	generator.publishSomeValues(c)

	generator.verifyExpectedOutputs(c, csvReader)
	s.stopRunner(c, 0)
}

func (s *ConsumerSuite) TestHandoffWithMultipleRunners(c *gc.C) {
	var csvReader = s.openResultReader(c)

	// Intermix starting & stopping runners with message processing, handing off
	// responsibility for processing around the "cluster". publishSomeValues
	// regulates publishing over the course of > 1s, so we expect publishes to
	// race startRunner, and we expect to get the right answer anyway.
	var generator = newTestGenerator(s.writer)

	go s.startRunner(c, 0)
	generator.publishSomeValues(c)

	go s.startRunner(c, 1)
	generator.publishSomeValues(c)

	s.stopRunner(c, 0)
	generator.publishSomeValues(c)

	go s.startRunner(c, 2)
	generator.publishSomeValues(c)

	s.stopRunner(c, 1)
	generator.publishSomeValues(c)

	// Verify expected values appear in the merged output log.
	generator.verifyExpectedOutputs(c, csvReader)

	s.stopRunner(c, 2)
}

func (s *ConsumerSuite) buildRunner(i int) *Runner {
	return &Runner{
		Consumer:        new(testConsumer),
		LocalRouteKey:   fmt.Sprintf("test-consumer-%d", i),
		LocalDir:        fmt.Sprintf("/var/tmp/integration-tests/add-subtract/runner-%d", i),
		ConsumerRoot:    "/gazette/consumers/tests/add-subtract-test",
		RecoveryLogRoot: "pippio-journals/integration-tests/add-subtract",
		ReplicaCount:    0,

		Etcd:   s.etcdClient,
		Getter: s.gazetteClient,
		Writer: s.writer,
	}
}

func (s *ConsumerSuite) startRunner(c *gc.C, i int) {
	c.Check(s.buildRunner(i).Run(), gc.IsNil)
	s.runFinishedCh <- struct{}{}
}

func (s *ConsumerSuite) stopRunner(c *gc.C, i int) {
	c.Check(consensus.Cancel(s.buildRunner(i)), gc.IsNil)
	<-s.runFinishedCh
}

func (s *ConsumerSuite) openResultReader(c *gc.C) *csv.Reader {
	// Issue a long-lived Gazette read to the result journal.
	readResp, reader := s.gazetteClient.GetDirect(journal.ReadArgs{
		Journal:  kMergedResultLog,
		Offset:   -1,
		Deadline: time.Now().Add(time.Minute),
	})
	c.Assert(readResp.Error, gc.IsNil)

	return csv.NewReader(reader)
}

// A topic.Marshallable / Unmarshallable which encodes a Key and Update.
type testMessage struct {
	Key, Update int64
}

func (t *testMessage) Unmarshal(b []byte) error {
	return binary.Read(bytes.NewReader(b), binary.LittleEndian, t)
}

func (t *testMessage) Size() int {
	return binary.Size(t)
}

func (t *testMessage) MarshalTo(b []byte) (n int, err error) {
	w := bytes.NewBuffer(b[:0])
	err = binary.Write(w, binary.LittleEndian, t)
	n = w.Len()
	return
}

// Generator which publishes random add and subtraction events to their
// respective topics. testGenerator maintains an expected final result for each
// published key.
type testGenerator struct {
	publisher                    topic.Publisher
	addValues, addSubtractValues map[int64]int64

	cancelCh chan struct{}
	doneCh   chan struct{}
}

func newTestGenerator(writer journal.Writer) *testGenerator {
	return &testGenerator{
		publisher:         publisher{writer},
		addValues:         make(map[int64]int64),
		addSubtractValues: make(map[int64]int64),
	}
}

func (g *testGenerator) publishSomeValues(c *gc.C) {
	src := rand.New(rand.NewSource(time.Now().UnixNano()))
	zipf := rand.NewZipf(src, 1.1, 10.0, 1000)

	// Publish 10K messages over at least 1 second (sleeping 10ms every 100 messages).
	for i := 0; i != 10000; i++ {
		var msg = testMessage{
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

		if i%100 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (g *testGenerator) verifyExpectedOutputs(c *gc.C, csvReader *csv.Reader) {
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
}

// A test Consumer which combines add & subtract updates into |kMergedResultLog|.
type testConsumer struct{}

func (c *testConsumer) Topics() []*topic.Description {
	return []*topic.Description{addTopic, subtractTopic}
}

func (c *testConsumer) InitShard(s Shard) error {
	s.SetCache(make(map[int64]int64))
	return nil
}

func (c *testConsumer) Consume(m message.Message, s Shard, pub topic.Publisher) error {
	event := m.Value.(*testMessage)
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
		if _, err := pub.Write(kMergedResultLog, []byte(out)); err != nil {
			return err
		}
		delete(cache, key) // Reset for next transaction.
	}
	return nil
}

var _ = gc.Suite(&ConsumerSuite{})
