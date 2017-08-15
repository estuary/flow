package consumer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"

	etcd "github.com/coreos/etcd/client"
	gc "github.com/go-check/check"
	uuid "github.com/satori/go.uuid"

	"github.com/pippio/gazette/consensus"
	"github.com/pippio/gazette/envflag"
	"github.com/pippio/gazette/gazette"
	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/recoverylog"
	"github.com/pippio/gazette/topic"
)

var (
	// Test topics for events which add or subtract an amount to a key.
	// We compute and emit the running value of the key.
	addSubTopic = &topic.Description{
		Name:       "pippio-journals/integration-tests/add-subtract-updates",
		Framing:    topic.FixedFraming,
		GetMessage: func() topic.Message { return new(addSubMessage) },
		PutMessage: func(m topic.Message) {},
	}
	// Test topic of UUIDs that get reversed and place into an output journal.
	reverseInTopic = &topic.Description{
		Name:       "pippio-journals/integration-tests/reverse-in",
		Framing:    topic.FixedFraming,
		GetMessage: func() topic.Message { return new(stringMessage) },
		PutMessage: func(m topic.Message) {},
	}
)

// Output journal into which merged CSV key/value rows are produced.
const (
	addSubOutput  journal.Name = "pippio-journals/integration-tests/add-subtract-merged"
	reverseOutput journal.Name = "pippio-journals/integration-tests/reverse-out"
	consumerRoot               = "/tests/ConsumerSuite"
)

func init() {
	addSubTopic.Partitions = topic.EnumeratePartitions(addSubTopic.Name, 4)
	addSubTopic.MappedPartition = topic.ModuloPartitionMapping(addSubTopic.Partitions,
		func(m topic.Message, b []byte) []byte {
			return strconv.AppendInt(b, m.(*addSubMessage).Key, 16)
		})

	reverseInTopic.Partitions = topic.EnumeratePartitions(reverseInTopic.Name, 3)
	reverseInTopic.MappedPartition = topic.ModuloPartitionMapping(reverseInTopic.Partitions,
		func(m topic.Message, b []byte) []byte {
			return append(b, *m.(*stringMessage)...)
		})
}

type ConsumerSuite struct {
	etcdClient etcd.Client
	gazette    struct {
		*gazette.Client
		*gazette.WriteService
	}
	runFinishedCh chan struct{}
}

func (s *ConsumerSuite) SetUpSuite(c *gc.C) {
	if testing.Short() {
		c.Skip("skipping consumer2 integration tests in short mode")
	}

	var etcdEndpoint = envflag.NewEtcdServiceEndpoint()
	var gazetteEndpoint = envflag.NewGazetteServiceEndpoint()

	envflag.Parse()

	var err error
	s.etcdClient, err = etcd.New(etcd.Config{
		Endpoints: []string{"http://" + *etcdEndpoint}})
	c.Assert(err, gc.IsNil)

	s.gazette.Client, err = gazette.NewClient(*gazetteEndpoint)
	c.Assert(err, gc.IsNil)

	// Skip suite if Etcd is not available.
	if _, err = etcd.NewKeysAPI(s.etcdClient).Get(context.Background(), "/",
		&etcd.GetOptions{Recursive: false}); err != nil {
		c.Skip("Etcd not available: " + err.Error())
	}
	// Skip if a Gazette endpoint is not reachable.
	var result, _ = s.gazette.Head(journal.ReadArgs{Journal: addSubOutput, Offset: -1})
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
	var csvReader = s.openResultReader(addSubOutput, c)
	var reverseReader = s.openResultReader(reverseOutput, c)
	var generator = newTestGenerator(s.gazette)

	var runner = s.buildRunner(0, 0)
	go s.serveRunner(c, runner)

	generator.publishSomeValues(c)
	generator.verifyExpectedOutputs(c, csvReader, reverseReader)

	s.stopRunner(c, runner)
}

func (s *ConsumerSuite) TestConsumerState(c *gc.C) {
	var runner = s.buildRunner(0, 0)
	go s.serveRunner(c, runner)

	var generator = newTestGenerator(s.gazette)
	generator.publishSomeValues(c)

	var state, err = runner.CurrentConsumerState(context.Background(), nil)
	c.Check(err, gc.IsNil)

	c.Check(state.Root, gc.Equals, runner.ConsumerRoot)
	c.Check(state.LocalRouteKey, gc.Equals, runner.LocalRouteKey)
	c.Check(state.ReplicaCount, gc.Equals, int32(runner.ReplicaCount))

	c.Check(state.Endpoints, gc.DeepEquals, []string{runner.LocalRouteKey})

	c.Check(state.Shards, gc.HasLen, len(addSubTopic.Partitions())+len(reverseInTopic.Partitions()))
	c.Check(state.Shards[0].Topic, gc.Equals, addSubTopic.Name)
	c.Check(state.Shards[0].Partition, gc.Equals, addSubTopic.Partitions()[0])
	c.Check(state.Shards[0].Id, gc.Equals, ShardID("shard-add-subtract-updates-000"))

	c.Check(state.Shards[0].Replicas, gc.HasLen, 1)
	c.Check(state.Shards[0].Replicas[0].Endpoint, gc.Equals, runner.LocalRouteKey)

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
		for shard := range EnumerateShards(runner.Consumer) {
			var key = runner.PathRoot() + "/" + consensus.ItemsPrefix + "/" + shard.String() + "/" + peer

			var _, err = runner.KeysAPI().Set(context.Background(),
				key, status, &etcd.SetOptions{TTL: 2 * time.Second})
			c.Check(err, gc.IsNil)
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
	var csvReader = s.openResultReader(addSubOutput, c)
	var reverseReader = s.openResultReader(reverseOutput, c)

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

func (s *ConsumerSuite) TestMasterWritesLastReadHints(c *gc.C) {
	var runner = s.buildRunner(0, 1)
	var initCh = make(chan struct{})

	runner.ShardPreInitHook = func(shard Shard) {
		close(initCh)
	}
	var keysAPI = runner.KeysAPI()
	var sid = ShardID("shard-add-subtract-updates-000")
	var shard = newShard(sid, topic.Partition{
		Topic:   addSubTopic,
		Journal: addSubTopic.Partitions()[0],
	}, runner, nil)
	var cons, _ = keysAPI.Get(context.Background(), consumerRoot,
		&etcd.GetOptions{Recursive: true, Sort: true})

	// Start up a consumer shard and play back.
	shard.transitionMaster(runner, cons.Node)

	// Block until pre init hook is called by shard.
	<-initCh

	// Get hints and last recovered hints from etcd and compare them.
	var err error
	var resp, lrResp *etcd.Response
	resp, err = keysAPI.Get(context.Background(), hintsPath(consumerRoot, sid), nil)
	c.Assert(err, gc.IsNil)
	lrResp, err = keysAPI.Get(context.Background(), hintsPath(consumerRoot, sid)+".lastRecovered", nil)
	c.Assert(err, gc.IsNil)

	var lpSeg recoverylog.Segment
	var hints, lrHints recoverylog.FSMHints
	c.Assert(json.Unmarshal([]byte(resp.Node.Value), &hints), gc.IsNil)
	c.Assert(json.Unmarshal([]byte(lrResp.Node.Value), &lrHints), gc.IsNil)
	for n, liveNode := range hints.LiveNodes {
		for s, seg := range liveNode.Segments {
			lpSeg = lrHints.LiveNodes[n].Segments[s]
			c.Assert(seg.GetAuthor(), gc.Equals, lpSeg.GetAuthor())
			c.Assert(seg.GetFirstChecksum(), gc.Equals, lpSeg.GetFirstChecksum())
			c.Assert(seg.GetFirstSeqNo(), gc.Equals, lpSeg.GetFirstSeqNo())
			c.Assert(seg.GetLastSeqNo(), gc.Equals, lpSeg.GetLastSeqNo())
			c.Assert(seg.GetFirstOffset() <= lpSeg.GetFirstOffset(), gc.Equals, true)
		}
	}

	// Clean up shard and recovered hints.
	shard.transitionCancel()
	keysAPI.Delete(context.Background(), hintsPath(consumerRoot, sid)+".lastRecovered", nil)
}

func (s *ConsumerSuite) buildRunner(i, replicas int) *Runner {
	return &Runner{
		Consumer:        new(testConsumer),
		LocalRouteKey:   fmt.Sprintf("test-consumer-%d", i),
		LocalDir:        fmt.Sprintf("/var/tmp/integration-tests/consumer2/runner-%d", i),
		ConsumerRoot:    consumerRoot,
		RecoveryLogRoot: "pippio-journals/integration-tests/consumer2",
		ReplicaCount:    replicas,

		Etcd:    s.etcdClient,
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
	publisher    *topic.Publisher
	addSubValues map[int64]int64
	stringValues map[stringMessage]struct{}

	cancelCh chan struct{}
	doneCh   chan struct{}
}

func newTestGenerator(writer journal.Writer) *testGenerator {
	return &testGenerator{
		publisher:    topic.NewPublisher(writer),
		addSubValues: make(map[int64]int64),
		stringValues: make(map[stringMessage]struct{}),
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

		if src.Int()%2 == 1 {
			msg.Update = -msg.Update
		}

		c.Check(g.publisher.Publish(&msg, addSubTopic), gc.IsNil)
		g.addSubValues[msg.Key] = g.addSubValues[msg.Key] + msg.Update

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
	var csvReader = csv.NewReader(addReader)
	var reverseScanner = bufio.NewScanner(reverseReader)

	// Consume merged output, looking for expected values.
	for len(g.addSubValues) != 0 {
		var record, err = csvReader.Read()
		c.Assert(err, gc.IsNil)

		key, err := strconv.ParseInt(record[0], 10, 64)
		c.Assert(err, gc.IsNil)
		value, err := strconv.ParseInt(record[1], 10, 64)
		c.Assert(err, gc.IsNil)

		if g.addSubValues[key] == value {
			delete(g.addSubValues, key)
		}
	}

	var uniqueItems = make(map[stringMessage]struct{})
	for reverseScanner.Scan() {
		var key = reverse(reverseScanner.Text())
		var _, ok = g.stringValues[stringMessage(key)]
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

func (c *testConsumer) Topics() []*topic.Description {
	return []*topic.Description{addSubTopic, reverseInTopic}
}

func (c *testConsumer) InitShard(s Shard) error {
	s.SetCache(make(map[int64]int64))
	return nil
}

func (c *testConsumer) Consume(m topic.Envelope, s Shard, pub *topic.Publisher) error {
	switch m.Message.(type) {
	case *addSubMessage:
		var event = m.Message.(*addSubMessage)
		var cache = s.Cache().(map[int64]int64)

		var value, ok = cache[event.Key]
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

		cache[event.Key] = value + event.Update

	case *stringMessage:
		// RocksDB coverage already exists for the add-subtract test. Just
		// reverse the string and write to the Publisher immediately.
		// Note: Breaks 'exactly-once' processing just for this topic group.
		s := string(*m.Message.(*stringMessage))
		pub.Write(reverseOutput, []byte(reverse(s)+"\n"))
	}
	return nil
}

func (c *testConsumer) Flush(s Shard, pub *topic.Publisher) error {
	var cache = s.Cache().(map[int64]int64)
	var writeBatch = s.Transaction()

	for key, value := range cache {
		var keyStr, valStr = strconv.FormatInt(key, 10), strconv.FormatInt(value, 10)
		writeBatch.Put([]byte(keyStr), []byte(valStr))

		// Publish the current merged value to the result log.
		var out = fmt.Sprintf("%d,%d\n", key, value)
		if _, err := pub.Write(addSubOutput, []byte(out)); err != nil {
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
