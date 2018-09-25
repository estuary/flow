// +build !norocksdb

package recoverylog

import (
	"context"
	"io/ioutil"
	"os"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
	rocks "github.com/tecbot/gorocksdb"
)

type RecordedRocksDBSuite struct{}

func (s *RecordedRocksDBSuite) TestSimpleStopAndStart(c *gc.C) {
	var bk, cleanup = newBrokerAndLog(c)
	defer cleanup()

	var replica1 = NewTestReplica(c, bk)
	defer replica1.teardown(c)

	replica1.startWriting(c, aRecoveryLog)
	replica1.put(c, "key3", "value three!")
	replica1.put(c, "key1", "value one")
	replica1.put(c, "key2", "value2")

	var hints = replica1.recorder.BuildHints()

	// |replica1| was initialized from empty hints and began writing at the
	// recoverylog head (offset -1). However, expect that the produced hints
	// reference absolute offsets of the log.
	c.Assert(hints.LiveNodes, gc.NotNil)
	for _, node := range hints.LiveNodes {
		for _, s := range node.Segments {
			c.Check(s.FirstOffset >= 0, gc.Equals, true)
		}
	}

	var replica2 = NewTestReplica(c, bk)
	defer replica2.teardown(c)

	replica2.startReading(c, hints)
	replica2.makeLive(c)

	replica2.expectValues(c, map[string]string{
		"key1": "value one",
		"key2": "value2",
		"key3": "value three!",
	})

	// Expect |replica1| & |replica2| share identical non-empty properties.
	c.Check(replica1.recorder.fsm.Properties, gc.Not(gc.HasLen), 0)
	c.Check(replica1.recorder.fsm.Properties, gc.DeepEquals,
		replica2.recorder.fsm.Properties)
}

func (s *RecordedRocksDBSuite) TestWarmStandbyHandoff(c *gc.C) {
	var bk, cleanup = newBrokerAndLog(c)
	defer cleanup()

	var fo = rocks.NewDefaultFlushOptions()
	defer fo.Destroy()

	var replica1 = NewTestReplica(c, bk)
	defer replica1.teardown(c)
	var replica2 = NewTestReplica(c, bk)
	defer replica2.teardown(c)
	var replica3 = NewTestReplica(c, bk)
	defer replica3.teardown(c)

	replica1.startWriting(c, aRecoveryLog)
	var hints = replica1.recorder.BuildHints()

	// Both replicas begin reading at the same time.
	replica2.startReading(c, hints)
	replica3.startReading(c, hints)

	// |replica1| writes content, while |replica2| & |replica3| are reading.
	replica1.put(c, "key foo", "baz")
	replica1.put(c, "key bar", "bing")
	replica1.db.Flush(fo) // Synchronize to log.

	// Make |replica2| live. Expect |replica1|'s content to be present.
	replica2.makeLive(c)
	replica2.expectValues(c, map[string]string{
		"key foo": "baz",
		"key bar": "bing",
	})

	// Begin raced writes. We expect that the hand-off mechanism allows |replica3|
	// to consistently follow |replica2|'s fork of history.
	replica1.put(c, "raced", "and loses")
	replica1.db.Flush(fo)
	replica2.put(c, "raced", "and wins")
	replica2.db.Flush(fo)

	replica3.makeLive(c)
	replica2.expectValues(c, map[string]string{
		"key foo": "baz",
		"key bar": "bing",
		"raced":   "and wins",
	})

	// Expect |replica2| & |replica3| share identical, non-empty properties.
	c.Check(replica2.recorder.fsm.Properties, gc.Not(gc.HasLen), 0)
	c.Check(replica3.recorder.fsm.Properties, gc.DeepEquals,
		replica3.recorder.fsm.Properties)
}

func (s *RecordedRocksDBSuite) TestResolutionOfConflictingWriters(c *gc.C) {
	var bk, cleanup = newBrokerAndLog(c)
	defer cleanup()

	// Begin with two replicas.
	var replica1 = NewTestReplica(c, bk)
	defer replica1.teardown(c)
	var replica2 = NewTestReplica(c, bk)
	defer replica2.teardown(c)

	// |replica1| begins as primary.
	replica1.startWriting(c, aRecoveryLog)
	replica2.startReading(c, replica1.recorder.BuildHints())
	replica1.put(c, "key one", "value one")

	// |replica2| now becomes live. |replica1| and |replica2| intersperse writes.
	replica2.makeLive(c)
	replica1.put(c, "rep1 foo", "value foo")
	replica2.put(c, "rep2 bar", "value bar")
	replica1.put(c, "rep1 baz", "value baz")
	replica2.put(c, "rep2 bing", "value bing")

	// Ensure all writes are sync'd to the recoverylog.
	var flushOpts = rocks.NewDefaultFlushOptions()
	flushOpts.SetWait(true)
	replica1.db.Flush(flushOpts)
	replica2.db.Flush(flushOpts)
	flushOpts.Destroy()

	// New |replica3| is hinted from |replica1|, and |replica4| from |replica2|.
	var replica3 = NewTestReplica(c, bk)
	defer replica3.teardown(c)
	var replica4 = NewTestReplica(c, bk)
	defer replica4.teardown(c)

	replica3.startReading(c, replica1.recorder.BuildHints())
	replica3.makeLive(c)
	replica4.startReading(c, replica2.recorder.BuildHints())
	replica4.makeLive(c)

	// Expect |replica3| recovered |replica1| history.
	replica3.expectValues(c, map[string]string{
		"key one":  "value one",
		"rep1 foo": "value foo",
		"rep1 baz": "value baz",
	})
	// Expect |replica4| recovered |replica2| history.
	replica4.expectValues(c, map[string]string{
		"key one":   "value one",
		"rep2 bar":  "value bar",
		"rep2 bing": "value bing",
	})
}

func (s *RecordedRocksDBSuite) TestPlayThenCancel(c *gc.C) {
	var bk, cleanup = newBrokerAndLog(c)
	defer cleanup()

	var r = NewTestReplica(c, bk)
	defer r.teardown(c)

	// Create a Context which will cancel itself after a delay.
	var deadlineCtx, _ = context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond*10))

	// Blocks until |ctx| is cancelled.
	c.Check(r.player.Play(deadlineCtx, FSMHints{Log: aRecoveryLog}, r.tmpdir, bk),
		gc.Equals, context.DeadlineExceeded)

	r.player.FinishAtWriteHead() // A raced call to FinishAtWriteHead doesn't block.
	<-r.player.Done()

	// Expect the local directory was deleted.
	var _, err = os.Stat(r.tmpdir)
	c.Check(os.IsNotExist(err), gc.Equals, true)
}

func (s *RecordedRocksDBSuite) TestCancelThenPlay(c *gc.C) {
	var bk, cleanup = newBrokerAndLog(c)
	defer cleanup()

	var r = NewTestReplica(c, bk)
	defer r.teardown(c)

	// Create a Context which is cancelled immediately.
	var cancelCtx, cancelFn = context.WithCancel(context.Background())
	cancelFn()

	c.Check(r.player.Play(cancelCtx, FSMHints{Log: aRecoveryLog}, r.tmpdir, bk),
		gc.ErrorMatches, `rpc error: code = Canceled desc = context canceled`)

	<-r.player.Done()
}

// Models the typical lifetime of an observed rocks database:
//  * Begin by reading from the most-recent available hints.
//  * When ready, make the database "Live".
//  * Perform new writes against the replica, which are recorded in the log.
type testReplica struct {
	client client.AsyncJournalClient

	tmpdir string
	dbO    *rocks.Options
	dbWO   *rocks.WriteOptions
	dbRO   *rocks.ReadOptions
	db     *rocks.DB

	author   Author
	recorder *Recorder
	player   *Player
}

func NewTestReplica(c *gc.C, client client.AsyncJournalClient) *testReplica {
	var r = &testReplica{
		client: client,
		player: NewPlayer(),
	}

	var err error
	r.tmpdir, err = ioutil.TempDir("", "recoverylog-suite")
	c.Assert(err, gc.IsNil)

	r.author, err = NewRandomAuthorID()
	c.Assert(err, gc.IsNil)

	return r
}

func (r *testReplica) startReading(c *gc.C, hints FSMHints) {
	go func() {
		c.Assert(r.player.Play(context.Background(), hints, r.tmpdir, r.client), gc.IsNil)
	}()
}

func (r *testReplica) startWriting(c *gc.C, log protocol.Journal) {
	var fsm, err = NewFSM(FSMHints{Log: log})
	c.Assert(err, gc.IsNil)
	r.initDB(c, fsm)
}

// Finish playback, build a new recorder, and open an observed database.
func (r *testReplica) makeLive(c *gc.C) {
	r.player.InjectHandoff(r.author)
	<-r.player.Done()

	c.Assert(r.player.FSM, gc.NotNil)
	r.initDB(c, r.player.FSM)
}

func (r *testReplica) initDB(c *gc.C, fsm *FSM) {
	r.recorder = NewRecorder(fsm, r.author, r.tmpdir, r.client)

	r.dbO = rocks.NewDefaultOptions()
	r.dbO.SetCreateIfMissing(true)
	r.dbO.SetEnv(rocks.NewObservedEnv(RecordedRocksDB{r.recorder}))

	r.dbRO = rocks.NewDefaultReadOptions()

	r.dbWO = rocks.NewDefaultWriteOptions()
	r.dbWO.SetSync(true)

	var err error
	r.db, err = rocks.OpenDb(r.dbO, r.tmpdir)
	c.Assert(err, gc.IsNil)
}

func (r *testReplica) put(c *gc.C, key, value string) {
	c.Check(r.db.Put(r.dbWO, []byte(key), []byte(value)), gc.IsNil)
}

func (r *testReplica) expectValues(c *gc.C, expect map[string]string) {
	it := r.db.NewIterator(r.dbRO)
	defer it.Close()

	it.SeekToFirst()
	for ; it.Valid(); it.Next() {
		key, value := string(it.Key().Data()), string(it.Value().Data())

		c.Check(expect[key], gc.Equals, value)
		delete(expect, key)
	}
	c.Check(it.Err(), gc.IsNil)
	c.Check(expect, gc.HasLen, 0)
}

func (r *testReplica) teardown(c *gc.C) {
	if r.db != nil {
		r.db.Close()
		r.dbRO.Destroy()
		r.dbWO.Destroy()
		r.dbO.Destroy()
	}
	c.Assert(os.RemoveAll(r.tmpdir), gc.IsNil)
}

var _ = gc.Suite(&RecordedRocksDBSuite{})
