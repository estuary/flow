package store_rocksdb

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	rocks "github.com/jgraettinger/gorocksdb"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/etcdtest"
)

func TestSimpleStopAndStart(t *testing.T) {
	var bk, cleanup = newBrokerAndLog(t)
	defer cleanup()

	var replica1 = newTestReplica(t, bk)
	defer replica1.teardown()

	replica1.startWriting(aRecoveryLog)
	replica1.put("key3", "value three!")
	replica1.put("key1", "value one")
	replica1.put("key2", "value2")

	var hints, _ = replica1.recorder.BuildHints()

	// |replica1| was initialized from empty hints and began writing at the
	// recovery log head. However, expect that the produced hints reference
	// resolved absolute offsets of the log.
	require.NotNil(t, hints.LiveNodes)
	for _, node := range hints.LiveNodes {
		for _, s := range node.Segments {
			require.True(t, s.FirstOffset >= 0)
		}
	}

	var replica2 = newTestReplica(t, bk)
	defer replica2.teardown()

	replica2.startReading(hints)
	replica2.makeLive("")

	replica2.expectValues(map[string]string{
		"key1": "value one",
		"key2": "value2",
		"key3": "value three!",
	})

	// Expect |replica1| & |replica2| share identical non-empty properties
	// (specifically, properties hold the /IDENTITY GUID that RocksDB creates at initialization).
	var h1, _ = replica1.recorder.BuildHints()
	var h2, _ = replica2.recorder.BuildHints()

	require.NotEmpty(t, h1.Properties)
	require.Equal(t, h1.Properties[0].Path, "/IDENTITY")
	require.Equal(t, h1.Properties, h2.Properties)
}

func TestWarmStandbyHandoff(t *testing.T) {
	var bk, cleanup = newBrokerAndLog(t)
	defer cleanup()

	var fo = rocks.NewDefaultFlushOptions()
	fo.SetWait(true) // Sync to log before returning.
	defer fo.Destroy()

	var replica1 = newTestReplica(t, bk)
	defer replica1.teardown()
	var replica2 = newTestReplica(t, bk)
	defer replica2.teardown()
	var replica3 = newTestReplica(t, bk)
	defer replica3.teardown()

	replica1.startWriting(aRecoveryLog)
	var hints, _ = replica1.recorder.BuildHints()

	// Both replicas begin reading at the same time.
	replica2.startReading(hints)
	replica3.startReading(hints)

	// |replica1| writes content, while |replica2| & |replica3| are reading.
	replica1.put("key foo", "baz")
	replica1.put("key bar", "bing")
	require.NoError(t, replica1.db.Flush(fo))

	// Make |replica2| live. Expect |replica1|'s content to be present.
	replica2.makeLive("")
	replica2.expectValues(map[string]string{
		"key foo": "baz",
		"key bar": "bing",
	})

	// Begin raced writes. We expect that the hand-off mechanism allows
	// |replica3| to consistently follow |replica2|'s version of history.
	replica1.put("raced", "and loses")
	require.NoError(t, replica1.db.Flush(fo))
	replica2.put("raced", "and wins")
	require.NoError(t, replica2.db.Flush(fo))

	replica3.makeLive("")
	replica3.expectValues(map[string]string{
		"key foo": "baz",
		"key bar": "bing",
		"raced":   "and wins",
	})

	// Expect replicas share identical, non-empty properties.
	var h1, _ = replica1.recorder.BuildHints()
	var h3, _ = replica2.recorder.BuildHints()

	require.NotEmpty(t, h1.Properties)
	require.Equal(t, h1.Properties[0].Path, "/IDENTITY")
	require.Equal(t, h1.Properties, h3.Properties)
}

func TestResolutionOfConflictingWriters(t *testing.T) {
	var bk, cleanup = newBrokerAndLog(t)
	defer cleanup()

	// Begin with two replicas.
	var replica1 = newTestReplica(t, bk)
	defer replica1.teardown()
	var replica2 = newTestReplica(t, bk)
	defer replica2.teardown()

	// |replica1| begins as primary.
	replica1.startWriting(aRecoveryLog)
	var hints, _ = replica1.recorder.BuildHints()
	replica2.startReading(hints)
	replica1.put("key one", "value one")

	// |replica2| now becomes live. |replica1| and |replica2| intersperse writes.
	replica2.makeLive("")
	replica1.put("rep1 foo", "value foo")
	replica2.put("rep2 bar", "value bar")
	replica1.put("rep1 baz", "value baz")
	replica2.put("rep2 bing", "value bing")

	var fo = rocks.NewDefaultFlushOptions()
	fo.SetWait(true) // Sync to log before returning.
	defer fo.Destroy()

	require.NoError(t, replica1.db.Flush(fo))
	require.NoError(t, replica2.db.Flush(fo))

	// New |replica3| is hinted from |replica1|, and |replica4| from |replica2|.
	var replica3 = newTestReplica(t, bk)
	defer replica3.teardown()
	var replica4 = newTestReplica(t, bk)
	defer replica4.teardown()

	hints, _ = replica1.recorder.BuildHints()
	replica3.startReading(hints)
	replica3.makeLive("")

	hints, _ = replica2.recorder.BuildHints()
	replica4.startReading(hints)
	replica4.makeLive("")

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

func TestStopAndStartWithLogChange(t *testing.T) {
	var bk, cleanup = newBrokerAndLog(t)
	defer cleanup()

	var replica1 = newTestReplica(t, bk)
	defer replica1.teardown()

	replica1.startWriting(aRecoveryLog)
	replica1.put("key1", "one")
	var hints, _ = replica1.recorder.BuildHints()
	replica1.put("key2", "two")

	var replica2 = newTestReplica(t, bk)
	defer replica2.teardown()

	replica2.startReading(hints)
	replica2.makeLive(otherRecoveryLog)
	replica2.put("key3", "three")

	hints, _ = replica2.recorder.BuildHints()

	var replica3 = newTestReplica(t, bk)
	defer replica3.teardown()

	replica3.startReading(hints)
	replica3.makeLive("")

	replica3.expectValues(map[string]string{
		"key1": "one",
		"key2": "two",
		"key3": "three",
	})
}

func TestPlayThenCancel(t *testing.T) {
	var bk, cleanup = newBrokerAndLog(t)
	defer cleanup()

	var r = newTestReplica(t, bk)
	defer r.teardown()

	// Create a Context which will cancel itself after a delay.
	var deadlineCtx, _ = context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond*10))
	// Blocks until |ctx| is cancelled.
	var err = r.player.Play(deadlineCtx, recoverylog.FSMHints{Log: aRecoveryLog}, r.tmpdir, bk)
	require.Equal(t, context.DeadlineExceeded, errors.Cause(err))

	r.player.FinishAtWriteHead() // A raced call to FinishAtWriteHead doesn't block.
	<-r.player.Done()

	// Expect the local directory was deleted.
	_, err = os.Stat(r.tmpdir)
	require.True(t, os.IsNotExist(err))
}

func TestCancelThenPlay(t *testing.T) {
	var bk, cleanup = newBrokerAndLog(t)
	defer cleanup()

	var r = newTestReplica(t, bk)
	defer r.teardown()

	// Create a Context which is cancelled immediately.
	var cancelCtx, cancelFn = context.WithCancel(context.Background())
	cancelFn()

	require.EqualError(t, r.player.Play(cancelCtx, recoverylog.FSMHints{Log: aRecoveryLog}, r.tmpdir, bk),
		`playerReader.peek: context canceled`)

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

	author   recoverylog.Author
	recorder *recoverylog.Recorder
	player   *recoverylog.Player
	t        require.TestingT
}

func newTestReplica(t require.TestingT, client client.AsyncJournalClient) *testReplica {
	var r = &testReplica{
		client: client,
		player: recoverylog.NewPlayer(),
		author: recoverylog.NewRandomAuthor(),
		t:      t,
	}

	var err error
	r.tmpdir, err = ioutil.TempDir("", "store-rocksdb-test")
	require.NoError(r.t, err)

	return r
}

func (r *testReplica) startReading(hints recoverylog.FSMHints) {
	go func() {
		require.NoError(r.t, r.player.Play(context.Background(), hints, r.tmpdir, r.client))
	}()
}

func (r *testReplica) startWriting(log pb.Journal) {
	var fsm, err = recoverylog.NewFSM(recoverylog.FSMHints{Log: log})
	require.NoError(r.t, err)
	r.initDB(log, fsm)
}

// Finish playback, build a new recorder, and open an observed database.
func (r *testReplica) makeLive(log pb.Journal) {
	r.player.InjectHandoff(r.author)
	<-r.player.Done()

	require.NotNil(r.t, r.player.Resolved.FSM)

	if log == "" {
		log = r.player.Resolved.Log
	}
	r.initDB(log, r.player.Resolved.FSM)
}

func (r *testReplica) initDB(log pb.Journal, fsm *recoverylog.FSM) {
	r.recorder = recoverylog.NewRecorder(log, fsm, r.author, r.tmpdir, r.client)

	// Tests in this package predate register checks, and deliberately
	// exercise recovery log sequencing and conflict handling.
	r.recorder.DisableRegisterChecks()

	r.dbO = rocks.NewDefaultOptions()
	r.dbO.SetCreateIfMissing(true)
	r.dbO.SetEnv(NewHookedEnv(NewRecorder(r.recorder)))

	r.dbRO = rocks.NewDefaultReadOptions()

	r.dbWO = rocks.NewDefaultWriteOptions()
	r.dbWO.SetSync(true)

	var err error
	r.db, err = rocks.OpenDb(r.dbO, r.tmpdir)
	require.NoError(r.t, err)
}

func (r *testReplica) put(key, value string) {
	require.NoError(r.t, r.db.Put(r.dbWO, []byte(key), []byte(value)))
}

func (r *testReplica) expectValues(expect map[string]string) {
	it := r.db.NewIterator(r.dbRO)
	defer it.Close()

	it.SeekToFirst()
	for ; it.Valid(); it.Next() {
		key, value := string(it.Key().Data()), string(it.Value().Data())

		require.Equal(r.t, expect[key], value)
		delete(expect, key)
	}
	require.NoError(r.t, it.Err())
	require.Empty(r.t, expect)
}

func (r *testReplica) teardown() {
	if r.db != nil {
		r.db.Close()
		r.dbRO.Destroy()
		r.dbWO.Destroy()
		r.dbO.Destroy()
	}
	require.NoError(r.t, os.RemoveAll(r.tmpdir))
}

func newBrokerAndLog(t require.TestingT) (client.AsyncJournalClient, func()) {
	var etcd = etcdtest.TestClient()
	var broker = brokertest.NewBroker(t, etcd, "local", "broker")

	brokertest.CreateJournals(t, broker,
		brokertest.Journal(pb.JournalSpec{Name: aRecoveryLog}),
		brokertest.Journal(pb.JournalSpec{Name: otherRecoveryLog}))

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var as = client.NewAppendService(context.Background(), rjc)

	return as, func() {
		broker.Tasks.Cancel()
		require.NoError(t, broker.Tasks.Wait())
		etcdtest.Cleanup()
	}
}

const aRecoveryLog pb.Journal = "test/store-rocksdb/recovery-log"
const otherRecoveryLog pb.Journal = "test/store-rocksdb/other-recovery-log"

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
