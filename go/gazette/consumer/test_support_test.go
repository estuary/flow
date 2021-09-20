package consumer

import (
	"context"
	"database/sql"
	"errors"
	"io/ioutil"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/message"
	"go.gazette.dev/core/task"
)

var (
	localID  = pb.ProcessSpec_ID{Zone: "local", Suffix: "consumer"}
	remoteID = pb.ProcessSpec_ID{Zone: "remote", Suffix: "consumer"}

	sourceA = brokertest.Journal(pb.JournalSpec{
		Name:     "source/A",
		LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines),
	})
	sourceB = brokertest.Journal(pb.JournalSpec{
		Name:     "source/B",
		LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines),
	})
	echoOut = brokertest.Journal(pb.JournalSpec{
		Name:     "echo/out",
		LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines),
	})
)

const (
	aRecoveryLogPrefix string = "recovery/logs"

	shardA = "shard-A"
	shardB = "shard-B"
	shardC = "shard-C"

	sourceAWriteFixture = "bad leading content"
)

type testMessage struct {
	UUID       message.UUID
	Key, Value string
}

func (m *testMessage) GetUUID() message.UUID                         { return m.UUID }
func (m *testMessage) SetUUID(uuid message.UUID)                     { m.UUID = uuid }
func (m *testMessage) NewAcknowledgement(pb.Journal) message.Message { return new(testMessage) }

// errStore is a Store that errors.
type errStore struct{ app *testApplication }

func (s *errStore) StartCommit(Shard, pc.Checkpoint, OpFutures) OpFuture {
	return client.FinishedOperation(s.app.startCommitErr)
}

func (s *errStore) RestoreCheckpoint(Shard) (pc.Checkpoint, error) {
	return pc.Checkpoint{}, s.app.restoreCheckpointErr
}

func (s *errStore) Destroy() {}

type testApplication struct {
	newStoreErr          error         // Error returned by Application.NewStore().
	beginErr             error         // Error returned by Application.BeginTxn().
	newMsgErr            error         // Error returned by Application.NewMessage().
	consumeErr           error         // Error returned by Application.ConsumeMessage().
	finalizeErr          error         // Error returned by Application.FinalizeTxn().
	startCommitErr       error         // Error returned by Store.StartCommit().
	restoreCheckpointErr error         // Error returned by Store.RestoreCheckpoint().
	finishedCh           chan OpFuture // Signaled on FinishedTxn().
	db                   *sql.DB       // "Remote" sqlite database.
}

func newTestApplication(t require.TestingT, dbPath string) *testApplication {
	var db, err = sql.Open("sqlite3", dbPath)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE gazette_checkpoints (
			shard_fqn  TEXT PRIMARY KEY NOT NULL,
			fence      INTEGER NOT NULL,
			checkpoint BLOB NOT NULL
		);
		CREATE TABLE kvstates (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);
		`)
	require.NoError(t, err)

	return &testApplication{finishedCh: make(chan OpFuture, 1), db: db}
}

func (a *testApplication) NewStore(shard Shard, rec *recoverylog.Recorder) (Store, error) {
	if a.newStoreErr != nil {
		return nil, a.newStoreErr
	} else if a.startCommitErr != nil || a.restoreCheckpointErr != nil {
		return &errStore{app: a}, nil
	} else if rec == nil {
		return NewSQLStore(a.db), nil
	} else {
		var state = make(map[string]string)
		return NewJSONFileStore(rec, &state)
	}
}

func (a *testApplication) NewMessage(*pb.JournalSpec) (message.Message, error) {
	return new(testMessage), a.newMsgErr
}

func (a *testApplication) BeginTxn(Shard, Store) error { return a.beginErr }

func (a *testApplication) ConsumeMessage(shard Shard, store Store, env message.Envelope, pub *message.Publisher) error {
	if a.consumeErr != nil {
		return a.consumeErr
	}

	var msg = env.Message.(*testMessage)
	if message.GetFlags(msg.UUID) == message.Flag_ACK_TXN {
		return nil
	}

	switch s := store.(type) {
	case *JSONFileStore:
		(*s.State.(*map[string]string))[msg.Key] = msg.Value
	case *SQLStore:
		if txn, err := s.Transaction(shard.Context(), nil); err != nil {
			return err
		} else if _, err = txn.Exec(`INSERT INTO kvstates (key, value) VALUES (?, ?)
			ON CONFLICT(key) DO UPDATE SET value = ?;`, msg.Key, msg.Value, msg.Value); err != nil {
			return err
		}
	}

	if _, err := pub.PublishUncommitted(toEchoOut, msg); err != nil {
		return err
	}
	return nil
}

func (a *testApplication) FinalizeTxn(Shard, Store, *message.Publisher) error { return a.finalizeErr }

func (a *testApplication) FinishedTxn(_ Shard, _ Store, op OpFuture) {
	select {
	case a.finishedCh <- op:
	// Pass.
	default:
	}
}

type testFixture struct {
	t        require.TestingT
	tasks    *task.Group
	app      *testApplication
	broker   *brokertest.Broker
	etcd     *clientv3.Client
	ks       *keyspace.KeySpace
	resolver *Resolver
	service  *Service
	state    *allocator.State
	ajc      *client.AppendService
	pub      *message.Publisher
}

func newTestFixture(t require.TestingT) (*testFixture, func()) {
	var etcd = etcdtest.TestClient()
	var bk = brokertest.NewBroker(t, etcd, "local", "broker")

	brokertest.CreateJournals(t, bk,
		brokertest.Journal(pb.JournalSpec{
			Name:     pb.Journal(aRecoveryLogPrefix + "/" + shardA),
			LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_RecoveryLog),
		}),
		brokertest.Journal(pb.JournalSpec{
			Name:     pb.Journal(aRecoveryLogPrefix + "/" + shardB),
			LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_RecoveryLog),
		}),
		brokertest.Journal(pb.JournalSpec{
			Name:     pb.Journal(aRecoveryLogPrefix + "/" + shardC),
			LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_RecoveryLog),
		}),
		sourceA,
		sourceB,
		echoOut,
	)

	// Write a fixture of invalid content (we'll use MinOffset to skip over it).
	var a = client.NewAppender(context.Background(), bk.Client(), pb.AppendRequest{Journal: sourceA.Name})
	_, _ = a.Write([]byte(sourceAWriteFixture))
	require.NoError(t, a.Close())

	var ks = NewKeySpace("/consumer.test")
	ks.WatchApplyDelay = 0 // Speed test execution.

	var state = allocator.NewObservedState(ks,
		allocator.MemberKey(ks, localID.Zone, localID.Suffix), ShardIsConsistent)

	var tmpSqlite, err = ioutil.TempFile("", "consumer-test")
	require.NoError(t, err)
	var app = newTestApplication(t, tmpSqlite.Name())

	var svc = NewService(app, state, bk.Client(), nil, etcd)

	var tasks = task.NewGroup(context.Background())
	require.NoError(t, ks.Load(tasks.Context(), etcd, 0))
	tasks.Queue("service.Watch", func() error {
		return svc.Resolver.watch(tasks.Context(), svc.Etcd)
	})
	tasks.GoRun()

	var ajc = client.NewAppendService(tasks.Context(), bk.Client())

	return &testFixture{
			t:        t,
			tasks:    tasks,
			app:      app,
			broker:   bk,
			etcd:     etcd,
			ks:       ks,
			resolver: svc.Resolver,
			service:  svc,
			state:    state,
			ajc:      ajc,
			pub:      message.NewPublisher(ajc, nil),
		}, func() {
			// Ensure that the test cleaned up after itself by removing all assignments.
			var resp, err = etcd.Get(context.Background(), ks.Root+allocator.AssignmentsPrefix,
				clientv3.WithPrefix(), clientv3.WithLimit(1))

			require.NoError(t, err)
			require.Len(t, resp.Kvs, 0)

			tasks.Cancel()
			require.NoError(t, tasks.Wait())

			bk.Tasks.Cancel()
			require.NoError(t, bk.Tasks.Wait())

			etcdtest.Cleanup()

			var name = tmpSqlite.Name()
			require.NoError(t, tmpSqlite.Close())
			require.NoError(t, os.Remove(name))
		}
}

func newTestFixtureWithIdleShard(t require.TestingT) (*testFixture, *shard, func()) {
	var tf, cleanup = newTestFixture(t)

	var realTransition = transition
	transition = func(s *shard, item, assignment keyspace.KeyValue) {
		s.resolved.spec = item.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)
		s.resolved.assignment = assignment
	}
	tf.allocateShard(makeShard(shardA), localID)

	return tf, tf.resolver.shards[shardA], func() {
		tf.allocateShard(makeShard(shardA)) // Remove assignment.

		transition = realTransition
		cleanup()
	}
}

func (f *testFixture) allocateShard(spec *pc.ShardSpec, assignments ...pb.ProcessSpec_ID) {
	var ops []clientv3.Op

	// Upsert ConsumerSpec fixtures.
	for _, id := range []pb.ProcessSpec_ID{localID, remoteID} {
		var key = allocator.MemberKey(f.ks, id.Zone, id.Suffix)
		ops = append(ops, clientv3.OpPut(key, makeConsumer(id).MarshalString()))
	}
	// Upsert the ShardSpec.
	ops = append(ops, clientv3.OpPut(allocator.ItemKey(f.ks, spec.Id.String()), spec.MarshalString()))

	// Fetch current assignments which we may need to drop (if they're not updated).
	var toRemove = make(map[string]struct{})
	{
		var resp, err = f.etcd.Get(context.Background(),
			allocator.ItemAssignmentsPrefix(f.ks, spec.Id.String()), clientv3.WithPrefix())
		require.NoError(f.t, err)

		for _, kv := range resp.Kvs {
			toRemove[string(kv.Key)] = struct{}{}
		}
	}

	// Set updated |assignments|.
	for slot, id := range assignments {
		if id == (pb.ProcessSpec_ID{}) {
			continue
		}
		var asn = allocator.Assignment{
			ItemID:       spec.Id.String(),
			MemberZone:   id.Zone,
			MemberSuffix: id.Suffix,
			Slot:         slot,
		}
		var key = allocator.AssignmentKey(f.ks, asn)
		ops = append(ops, clientv3.OpPut(key, ""))

		delete(toRemove, key)
	}

	// Remove any left-over assignments we don't want.
	for key := range toRemove {
		ops = append(ops, clientv3.OpDelete(key))
	}

	var resp, err = f.etcd.Txn(context.Background()).If().Then(ops...).Commit()
	require.NoError(f.t, err)
	require.Equal(f.t, true, resp.Succeeded)

	f.ks.Mu.RLock()
	require.NoError(f.t, f.ks.WaitForRevision(f.tasks.Context(), resp.Header.Revision))
	f.ks.Mu.RUnlock()
}

func (f *testFixture) writeTxnPubACKs() []*client.AsyncAppend {
	var intents, err = f.pub.BuildAckIntents()
	require.NoError(f.t, err)

	var appends []*client.AsyncAppend
	for _, i := range intents {
		var aa = f.ajc.StartAppend(pb.AppendRequest{Journal: i.Journal}, nil)
		_, _ = aa.Writer().Write(i.Intent)
		require.NoError(f.t, aa.Release())
		appends = append(appends, aa)
	}
	return appends
}

func makeShard(id pc.ShardID) *pc.ShardSpec {
	return &pc.ShardSpec{
		Id: id,
		Sources: []pc.ShardSpec_Source{
			{Journal: sourceA.Name, MinOffset: int64(len(sourceAWriteFixture))},
			{Journal: sourceB.Name},
		},
		RecoveryLogPrefix: aRecoveryLogPrefix,
		HintPrefix:        "/hints",
		HintBackups:       2,
		MinTxnDuration:    1 * time.Millisecond,
		MaxTxnDuration:    10 * time.Millisecond,
	}
}

func makeRemoteShard(id pc.ShardID) *pc.ShardSpec {
	return &pc.ShardSpec{
		Id: id,
		Sources: []pc.ShardSpec_Source{
			{Journal: sourceA.Name, MinOffset: int64(len(sourceAWriteFixture))},
			{Journal: sourceB.Name},
		},
		MinTxnDuration: 10 * time.Millisecond,
		MaxTxnDuration: 100 * time.Millisecond,
	}
}

func makeConsumer(id pb.ProcessSpec_ID) *pc.ConsumerSpec {
	return &pc.ConsumerSpec{
		ProcessSpec: pb.ProcessSpec{
			Id:       id,
			Endpoint: pb.Endpoint("http://" + id.Zone + "/endpoint"),
		},
		ShardLimit: 100,
	}
}

func etcdGet(t require.TestingT, etcd clientv3.KV, key string) *clientv3.GetResponse {
	var resp, err = etcd.Get(context.Background(), key)
	require.NoError(t, err)
	return resp
}

func pluckTheAssignment(t require.TestingT, state *allocator.State) (*pc.ShardSpec, keyspace.KeyValue) {
	require.Len(t, state.LocalItems, 1)
	return state.LocalItems[0].Item.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec),
		state.LocalItems[0].Assignments[state.LocalItems[0].Index]
}

func expectStatusCode(t require.TestingT, state *allocator.State, code pc.ReplicaStatus_Code) *pc.ReplicaStatus {
	defer state.KS.Mu.RUnlock()
	state.KS.Mu.RLock()

	for {
		var _, kv = pluckTheAssignment(t, state)
		var status = kv.Decoded.(allocator.Assignment).AssignmentValue.(*pc.ReplicaStatus)

		require.True(t, status.Code <= code)

		if status.Code >= code {
			return status
		}
		require.NoError(t, state.KS.WaitForRevision(context.Background(), state.KS.Header.Revision+1))
	}
}

func playAndComplete(t require.TestingT, shard *shard) pc.Checkpoint {
	go func() { require.NoError(t, beginRecovery(shard)) }()

	// This steps are ordinarily done by servePrimary(),
	// which we're not running in order to instead build
	// a shard that we can manually drive within our test.
	var cp, err = completeRecovery(shard)
	require.NoError(t, err)

	shard.publisher = message.NewPublisher(shard.ajc, &shard.clock)
	shard.sequencer = message.NewSequencer(
		pc.FlattenReadThrough(cp),
		pc.FlattenProducerStates(cp),
		defaultMessageRingSize,
	)
	return cp
}

func verifyStoreAndEchoOut(t require.TestingT, s *shard, expect map[string]string) {
	if js, ok := s.store.(*JSONFileStore); ok {
		require.Equal(t, &expect, js.State)
	} else {
		var rows, err = s.store.(*SQLStore).DB.Query(`SELECT key, value FROM kvstates;`)
		require.NoError(t, err)

		var m = make(map[string]string)
		for rows.Next() {
			var key, value string
			require.NoError(t, rows.Scan(&key, &value))
			m[key] = value
		}
		require.NoError(t, rows.Err())
		require.Equal(t, expect, m)
	}

	// Wait for all writes to complete.
	for op := range s.ajc.PendingExcept("") {
		<-op.Done()
	}

	var it = message.NewReadCommittedIter(
		client.NewRetryReader(context.Background(), s.ajc,
			pb.ReadRequest{Journal: echoOut.Name}),
		new(testApplication).NewMessage,
		message.NewSequencer(nil, nil, 16))

	var m = make(map[string]string)
	for {
		var env, err = it.Next()
		if errors.Is(err, client.ErrOffsetNotYetAvailable) {
			require.Equal(t, expect, m)
			return
		}
		require.NoError(t, err)

		if msg := env.Message.(*testMessage); message.GetFlags(msg.UUID) != message.Flag_ACK_TXN {
			m[msg.Key] = msg.Value
		}
	}
}

func runTransaction(tf *testFixture, s Shard, in map[string]string) {
	for k, v := range in {
		var _, err = tf.pub.PublishUncommitted(toSourceA, &testMessage{Key: k, Value: v})
		require.NoError(tf.t, err)
	}
	var appends = tf.writeTxnPubACKs()

	var offsets = make(pb.Offsets)
	for _, aa := range appends {
		require.NoError(tf.t, aa.Err())
		offsets[aa.Request().Journal] = aa.Response().Commit.End
	}

	// Block until ACKs have been read through, or an error occurred.
	var _, err = ShardStat(tf.tasks.Context(), tf.service, &pc.StatRequest{
		Shard:       s.Spec().Id,
		ReadThrough: offsets,
	})
	require.NoError(tf.t, err)
}

type testTimer struct {
	txnTimer
	ch        chan time.Time
	stopped   bool
	reset     time.Duration
	timepoint time.Time
}

func newTestTimer() *testTimer {
	var t = &testTimer{
		ch:        make(chan time.Time, 1),
		timepoint: faketime(0),
	}

	t.txnTimer = txnTimer{
		C:     t.ch,
		Reset: func(duration time.Duration) bool { t.reset = duration; return true },
		Stop:  func() bool { t.stopped = true; return true },
		Now:   func() time.Time { return t.timepoint },
	}
	return t
}

func (t testTimer) signal() { t.ch <- t.timepoint }

func faketime(delta time.Duration) time.Time { return time.Unix(1500000000, 0).Add(delta) }

func toSourceA(message.Mappable) (pb.Journal, string, error) {
	return sourceA.Name, labels.ContentType_JSONLines, nil
}
func toSourceB(message.Mappable) (pb.Journal, string, error) {
	return sourceB.Name, labels.ContentType_JSONLines, nil
}
func toEchoOut(message.Mappable) (pb.Journal, string, error) {
	return echoOut.Name, labels.ContentType_JSONLines, nil
}

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
