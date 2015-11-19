// +build integration

package recoverylog

import (
	"io/ioutil"
	"os"

	gc "github.com/go-check/check"
	rocks "github.com/tecbot/gorocksdb"

	"github.com/pippio/api-server/endpoints"
	"github.com/pippio/gazette/gazette"
	"github.com/pippio/gazette/journal"
)

const (
	kTestLogName journal.Name = "pippio-journals/integration-tests/recovery-log"
)

type RecoveryLogSuite struct {
	client *gazette.Client
	writer *gazette.WriteService
}

func (s *RecoveryLogSuite) SetUpSuite(c *gc.C) {
	endpoints.ParseFromEnvironment()

	var err error
	s.client, err = gazette.NewClient(*endpoints.GazetteEndpoint)
	c.Assert(err, gc.IsNil)
	s.writer = gazette.NewWriteService(s.client)
	s.writer.Start()
}

func (s *RecoveryLogSuite) TearDownSuite(c *gc.C) {
	s.writer.Stop()
}

func (s *RecoveryLogSuite) TestSimpleStopAndStart(c *gc.C) {
	env := testEnv{c, s.client, s.writer}

	replica1 := NewTestReplica(&env)
	defer replica1.teardown()

	replica1.startReading(s.initialHints(c))
	replica1.makeLive()

	replica1.put("key3", "value three!")
	replica1.put("key1", "value one")
	replica1.put("key2", "value2")

	replica2 := NewTestReplica(&env)
	defer replica2.teardown()

	replica2.startReading(replica1.recorder.BuildHints())
	replica2.makeLive()

	replica2.expectValues(map[string]string{
		"key1": "value one",
		"key2": "value2",
		"key3": "value three!",
	})
}

func (s *RecoveryLogSuite) TestSimpleWarmStandby(c *gc.C) {
	env := testEnv{c, s.client, s.writer}

	replica1 := NewTestReplica(&env)
	defer replica1.teardown()
	replica2 := NewTestReplica(&env)
	defer replica2.teardown()

	// Both replicas begin reading at the same time.
	hints := s.initialHints(c)
	replica1.startReading(hints)
	replica2.startReading(hints)

	// |replica1| is made live and writes content, while |replica2| is reading.
	replica1.makeLive()
	replica1.put("key foo", "baz")
	replica1.put("key bar", "bing")

	// Make |replica2| live. Expect |replica1|'s content to be present.
	replica2.makeLive()
	replica2.expectValues(map[string]string{
		"key foo": "baz",
		"key bar": "bing",
	})
}

func (s *RecoveryLogSuite) TestResolutionOfConflictingWriters(c *gc.C) {
	env := testEnv{c, s.client, s.writer}

	// Begin with two replicas, both reading from the initial state.
	replica1 := NewTestReplica(&env)
	defer replica1.teardown()
	replica2 := NewTestReplica(&env)
	defer replica2.teardown()

	replica1.startReading(s.initialHints(c))
	replica2.startReading(s.initialHints(c))

	// |replica1| begins as master.
	replica1.makeLive()
	replica1.put("key one", "value one")

	// |replica2| now becomes live. |replica1| and |replica2| intersperse writes.
	replica2.makeLive()
	replica1.put("rep1 foo", "value foo")
	replica2.put("rep2 bar", "value bar")
	replica1.put("rep1 baz", "value baz")
	replica2.put("rep2 bing", "value bing")

	// New |replica3| is hinted from |replica1|, and |replica4| from |replica2|.
	replica3 := NewTestReplica(&env)
	defer replica3.teardown()
	replica4 := NewTestReplica(&env)
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

// Returns hints at the current log head (eg, resulting in an empty database).
func (s *RecoveryLogSuite) initialHints(c *gc.C) FSMHints {
	// Determine current recovery-log head.
	result, _ := s.client.Head(journal.ReadArgs{Journal: kTestLogName, Offset: -1})
	c.Assert(result.Error, gc.Equals, journal.ErrNotYetAvailable)

	hints := EmptyHints(kTestLogName)

	// Explicitly note we expect to start at SeqNo 1. This resolves race
	// conditions between an issued HEAD and tear-down of a previous test
	// (that test may sneak in an extra write after HEAD).
	hints.FirstSeqNo = 1
	hints.LogMark.Offset = result.WriteHead
	return hints
}

// Test state shared by multiple testReplica instances.
type testEnv struct {
	*gc.C
	client *gazette.Client
	writer *gazette.WriteService
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

	recorder *Recorder
	player   *Player
}

func NewTestReplica(env *testEnv) *testReplica {
	r := &testReplica{
		testEnv: env,
	}
	var err error
	r.tmpdir, err = ioutil.TempDir("", "recoverylog-suite")
	r.Assert(err, gc.IsNil)
	return r
}

func (r *testReplica) startReading(hints FSMHints) {
	var err error
	r.player, err = PreparePlayback(hints, r.tmpdir)
	r.Assert(err, gc.IsNil)

	go func() {
		r.Assert(r.player.Play(r.client), gc.IsNil)
	}()
}

// Finish playback, build a new recorder, and open an observed database.
func (r *testReplica) makeLive() {
	fsm, err := r.player.MakeLive()
	r.Assert(err, gc.IsNil)

	r.recorder, err = NewRecorder(fsm, len(r.tmpdir), r.writer)
	r.Assert(err, gc.IsNil)

	r.dbO = rocks.NewDefaultOptions()
	r.dbO.SetCreateIfMissing(true)
	r.dbO.SetEnv(rocks.NewObservedEnv(r.recorder))

	r.dbRO = rocks.NewDefaultReadOptions()

	r.dbWO = rocks.NewDefaultWriteOptions()
	r.dbWO.SetSync(true)

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
	r.db.Close()
	r.dbRO.Destroy()
	r.dbWO.Destroy()
	r.dbO.Destroy()

	r.Assert(os.RemoveAll(r.tmpdir), gc.IsNil)
}

var _ = gc.Suite(&RecoveryLogSuite{})
