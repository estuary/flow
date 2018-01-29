package recoverylog

import (
	"context"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	gc "github.com/go-check/check"
	rocks "github.com/tecbot/gorocksdb"

	"github.com/LiveRamp/gazette/pkg/envflag"
	"github.com/LiveRamp/gazette/pkg/envflagfactory"
	"github.com/LiveRamp/gazette/pkg/gazette"
	"github.com/LiveRamp/gazette/pkg/journal"
)

type RecoveryLogSuite struct {
	gazette struct {
		*gazette.Client
		*gazette.WriteService
	}
}

func (s *RecoveryLogSuite) SetUpSuite(c *gc.C) {
	var gazetteEndpoint = envflagfactory.NewGazetteServiceEndpoint()
	envflag.CommandLine.Parse()

	var err error
	s.gazette.Client, err = gazette.NewClient(*gazetteEndpoint)
	c.Assert(err, gc.IsNil)

	// Skip if in Short mode, or if a Gazette endpoint is not reach-able.
	if testing.Short() {
		c.Skip("skipping recoverylog integration tests in short mode")
	}
	result, _ := s.gazette.Head(journal.ReadArgs{Journal: aRecoveryLog, Offset: -1})
	if _, ok := result.Error.(net.Error); ok {
		c.Skip("Gazette not available: " + result.Error.Error())
		return
	}

	s.gazette.WriteService = gazette.NewWriteService(s.gazette.Client)
	s.gazette.WriteService.Start()
}

func (s *RecoveryLogSuite) TearDownSuite(c *gc.C) {
	if s.gazette.WriteService != nil {
		s.gazette.WriteService.Stop()
	}
}

func (s *RecoveryLogSuite) TestSimpleStopAndStart(c *gc.C) {
	var env = testEnv{c, s.gazette}

	var replica1 = NewTestReplica(&env)
	defer replica1.teardown()

	replica1.startWriting(aRecoveryLog)
	replica1.put("key3", "value three!")
	replica1.put("key1", "value one")
	replica1.put("key2", "value2")

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

	var replica2 = NewTestReplica(&env)
	defer replica2.teardown()

	replica2.startReading(hints)
	replica2.makeLive()

	replica2.expectValues(map[string]string{
		"key1": "value one",
		"key2": "value2",
		"key3": "value three!",
	})

	// Expect |replica1| & |replica2| share identical non-empty properties.
	c.Check(replica1.recorder.fsm.Properties, gc.Not(gc.HasLen), 0)
	c.Check(replica1.recorder.fsm.Properties, gc.DeepEquals,
		replica2.recorder.fsm.Properties)
}

func (s *RecoveryLogSuite) TestWarmStandbyHandoff(c *gc.C) {
	var env = testEnv{c, s.gazette}

	var fo = rocks.NewDefaultFlushOptions()
	defer fo.Destroy()

	var replica1 = NewTestReplica(&env)
	defer replica1.teardown()
	var replica2 = NewTestReplica(&env)
	defer replica2.teardown()
	var replica3 = NewTestReplica(&env)
	defer replica3.teardown()

	replica1.startWriting(aRecoveryLog)
	var hints = replica1.recorder.BuildHints()

	// Both replicas begin reading at the same time.
	replica2.startReading(hints)
	replica3.startReading(hints)

	// |replica1| writes content, while |replica2| & |replica3| are reading.
	replica1.put("key foo", "baz")
	replica1.put("key bar", "bing")
	replica1.db.Flush(fo) // Synchronize to log.

	// Make |replica2| live. Expect |replica1|'s content to be present.
	replica2.makeLive()
	replica2.expectValues(map[string]string{
		"key foo": "baz",
		"key bar": "bing",
	})

	// Begin raced writes. We expect that the hand-off mechanism allows |replica3|
	// to consistently follow |replica2|'s fork of history.
	replica1.put("raced", "and loses")
	replica1.db.Flush(fo)
	replica2.put("raced", "and wins")
	replica2.db.Flush(fo)

	replica3.makeLive()
	replica2.expectValues(map[string]string{
		"key foo": "baz",
		"key bar": "bing",
		"raced":   "and wins",
	})

	// Expect |replica2| & |replica3| share identical, non-empty properties.
	c.Check(replica2.recorder.fsm.Properties, gc.Not(gc.HasLen), 0)
	c.Check(replica3.recorder.fsm.Properties, gc.DeepEquals,
		replica3.recorder.fsm.Properties)
}

func (s *RecoveryLogSuite) TestResolutionOfConflictingWriters(c *gc.C) {
	var env = testEnv{c, s.gazette}

	// Begin with two replicas.
	var replica1 = NewTestReplica(&env)
	defer replica1.teardown()
	var replica2 = NewTestReplica(&env)
	defer replica2.teardown()

	// |replica1| begins as primary.
	replica1.startWriting(aRecoveryLog)
	replica2.startReading(replica1.recorder.BuildHints())
	replica1.put("key one", "value one")

	// |replica2| now becomes live. |replica1| and |replica2| intersperse writes.
	replica2.makeLive()
	replica1.put("rep1 foo", "value foo")
	replica2.put("rep2 bar", "value bar")
	replica1.put("rep1 baz", "value baz")
	replica2.put("rep2 bing", "value bing")

	// Ensure all writes are sync'd to the recoverylog.
	var flushOpts = rocks.NewDefaultFlushOptions()
	flushOpts.SetWait(true)
	replica1.db.Flush(flushOpts)
	replica2.db.Flush(flushOpts)
	flushOpts.Destroy()

	// New |replica3| is hinted from |replica1|, and |replica4| from |replica2|.
	var replica3 = NewTestReplica(&env)
	defer replica3.teardown()
	var replica4 = NewTestReplica(&env)
	defer replica4.teardown()

	replica3.startReading(replica1.recorder.BuildHints())
	replica3.makeLive()
	replica4.startReading(replica2.recorder.BuildHints())
	replica4.makeLive()

	// Expect |replica3| recovered |replica1| history.
	replica3.expectValues(map[string]string{
		"key one":  "value one",
		"rep1 foo": "value foo",
		"rep1 baz": "value baz",
	})
	// Expect |replica4| recovered |replica2| history.
	replica4.expectValues(map[string]string{
		"key one":   "value one",
		"rep2 bar":  "value bar",
		"rep2 bing": "value bing",
	})
}

func (s *RecoveryLogSuite) TestPlayThenCancel(c *gc.C) {
	var r = NewTestReplica(&testEnv{c, s.gazette})
	defer r.teardown()

	var err error
	r.player, err = NewPlayer(FSMHints{Log: aRecoveryLog}, r.tmpdir)
	c.Assert(err, gc.IsNil)

	// Create a Context which will cancel itself after a delay.
	var ctx, _ = context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond*10))

	// Blocks until |ctx| is cancelled.
	c.Check(r.player.PlayContext(ctx, r.gazette), gc.Equals, context.DeadlineExceeded)
	c.Check(r.player.FinishAtWriteHead(), gc.IsNil)

	// Expect the local directory was deleted.
	_, err = os.Stat(r.player.dir)
	c.Check(os.IsNotExist(err), gc.Equals, true)
}

func (s *RecoveryLogSuite) TestCancelThenPlay(c *gc.C) {
	var r = NewTestReplica(&testEnv{c, s.gazette})
	defer r.teardown()

	var err error
	r.player, err = NewPlayer(FSMHints{Log: aRecoveryLog}, r.tmpdir)
	c.Assert(err, gc.IsNil)

	var ctx, cancelFn = context.WithCancel(context.Background())
	cancelFn()

	c.Check(r.player.PlayContext(ctx, r.gazette), gc.Equals, context.Canceled)
	c.Check(r.player.FinishAtWriteHead(), gc.IsNil)
}

// Test state shared by multiple testReplica instances.
type testEnv struct {
	*gc.C
	gazette journal.Client
}

// Models the typical lifetime of an observed rocks database:
//  * Begin by reading from the most-recent available hints.
//  * When ready, make the database "Live".
//  * Perform new writes against the replica, which are recorded in the log.
type testReplica struct {
	*testEnv

	tmpdir string
	dbO    *rocks.Options
	dbWO   *rocks.WriteOptions
	dbRO   *rocks.ReadOptions
	db     *rocks.DB

	author   Author
	recorder *Recorder
	player   *Player
}

func NewTestReplica(env *testEnv) *testReplica {
	var r = &testReplica{
		testEnv: env,
	}

	var err error
	r.tmpdir, err = ioutil.TempDir("", "recoverylog-suite")
	r.Assert(err, gc.IsNil)

	r.author, err = NewRandomAuthorID()
	r.Assert(err, gc.IsNil)

	return r
}

func (r *testReplica) startReading(hints FSMHints) {
	var err error
	r.player, err = NewPlayer(hints, r.tmpdir)
	r.Assert(err, gc.IsNil)

	go func() {
		r.Assert(r.player.Play(r.gazette), gc.IsNil)
	}()
}

func (r *testReplica) startWriting(log journal.Name) {
	var fsm, err = NewFSM(FSMHints{Log: log})
	r.Assert(err, gc.IsNil)
	r.initDB(fsm)
}

// Finish playback, build a new recorder, and open an observed database.
func (r *testReplica) makeLive() {
	var fsm = r.player.InjectHandoff(r.author)
	r.Assert(fsm, gc.NotNil)
	r.initDB(fsm)
}

func (r *testReplica) initDB(fsm *FSM) {
	r.recorder = NewRecorder(fsm, r.author, len(r.tmpdir), r.gazette)

	r.dbO = rocks.NewDefaultOptions()
	r.dbO.SetCreateIfMissing(true)
	r.dbO.SetEnv(rocks.NewObservedEnv(r.recorder))

	r.dbRO = rocks.NewDefaultReadOptions()

	r.dbWO = rocks.NewDefaultWriteOptions()
	r.dbWO.SetSync(true)

	var err error
	r.db, err = rocks.OpenDb(r.dbO, r.tmpdir)
	r.Assert(err, gc.IsNil)
}

func (r *testReplica) put(key, value string) {
	r.Check(r.db.Put(r.dbWO, []byte(key), []byte(value)), gc.IsNil)
}

func (r *testReplica) expectValues(expect map[string]string) {
	it := r.db.NewIterator(r.dbRO)
	defer it.Close()

	it.SeekToFirst()
	for ; it.Valid(); it.Next() {
		key, value := string(it.Key().Data()), string(it.Value().Data())

		r.Check(expect[key], gc.Equals, value)
		delete(expect, key)
	}
	r.Check(it.Err(), gc.IsNil)
	r.Check(expect, gc.HasLen, 0)
}

func (r *testReplica) teardown() {
	if r.db != nil {
		r.db.Close()
		r.dbRO.Destroy()
		r.dbWO.Destroy()
		r.dbO.Destroy()
	}
	r.Assert(os.RemoveAll(r.tmpdir), gc.IsNil)
}

var _ = gc.Suite(&RecoveryLogSuite{})
