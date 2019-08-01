package consumer

import (
	"context"
	"time"

	gc "github.com/go-check/check"
	"go.etcd.io/etcd/clientv3"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/message"
)

var (
	localID  = pb.ProcessSpec_ID{Zone: "local", Suffix: "consumer"}
	remoteID = pb.ProcessSpec_ID{Zone: "remote", Suffix: "consumer"}
)

const (
	sourceA            pb.Journal = "source/A"
	sourceB            pb.Journal = "source/B"
	aRecoveryLogPrefix string     = "recovery/logs"

	shardA = "shard-A"
	shardB = "shard-B"
	shardC = "shard-C"

	sourceAWriteFixture = "bad leading content"
)

type testMessage struct {
	Key, Value string
}

type testApplication struct {
	// Fixture errors that testApplication can be configured to return.
	newStoreErr error
	beginErr    error
	newMsgErr   error
	consumeErr  error
	finalizeErr error
	finishErr   error
	// Signals when FinishTxn is called.
	finishCh chan struct{}
}

func newTestApplication() *testApplication {
	return &testApplication{finishCh: make(chan struct{})}
}

func (a *testApplication) NewStore(shard Shard, dir string, rec *recoverylog.Recorder) (Store, error) {
	if a.newStoreErr != nil {
		return nil, a.newStoreErr
	}
	var state = make(map[string]string)
	return NewJSONFileStore(rec, dir, &state)
}

func (a *testApplication) NewMessage(*pb.JournalSpec) (message.Message, error) {
	return new(testMessage), a.newMsgErr
}

func (a *testApplication) BeginTxn(shard Shard, store Store) error { return a.beginErr }

func (a *testApplication) ConsumeMessage(shard Shard, store Store, env message.Envelope) error {
	var js = store.(*JSONFileStore)
	var msg = env.Message.(*testMessage)
	(*js.State.(*map[string]string))[msg.Key] = msg.Value
	return a.consumeErr
}

func (a *testApplication) FinalizeTxn(shard Shard, store Store) error { return a.finalizeErr }

func (a *testApplication) FinishTxn(shard Shard, store Store, _ error) error {
	var ch = a.finishCh
	a.finishCh = make(chan struct{})
	defer close(ch)
	return a.finishErr
}

type testFixture struct {
	ctx      context.Context
	app      *testApplication
	broker   *brokertest.Broker
	etcd     *clientv3.Client
	ks       *keyspace.KeySpace
	resolver *Resolver
	service  *Service
	state    *allocator.State
}

func newTestFixture(c *gc.C) (*testFixture, func()) {
	var etcd = etcdtest.TestClient()
	var broker = brokertest.NewBroker(c, etcd, "local", "broker")

	brokertest.CreateJournals(c, broker,
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
		brokertest.Journal(pb.JournalSpec{
			Name:     sourceA,
			LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines),
		}),
		brokertest.Journal(pb.JournalSpec{
			Name:     sourceB,
			LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines),
		}),
	)

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})

	// Write a fixture of invalid content (we'll use MinOffset to skip over it).
	var a = client.NewAppender(context.Background(), rjc, pb.AppendRequest{Journal: sourceA})
	_, _ = a.Write([]byte(sourceAWriteFixture))
	c.Assert(a.Close(), gc.IsNil)

	var ks = NewKeySpace("/consumertest")
	ks.WatchApplyDelay = 0

	var state = allocator.NewObservedState(ks,
		allocator.MemberKey(ks, localID.Zone, localID.Suffix))

	var app = newTestApplication()
	var svc = NewService(app, state, rjc, nil, etcd)
	var ctx, cancel = context.WithCancel(context.Background())

	c.Assert(ks.Load(ctx, etcd, 0), gc.IsNil)
	go func() { c.Assert(ks.Watch(ctx, etcd), gc.Equals, context.Canceled) }()

	return &testFixture{
			ctx:      ctx,
			app:      app,
			broker:   broker,
			etcd:     etcd,
			ks:       ks,
			resolver: svc.Resolver,
			service:  svc,
			state:    state,
		}, func() {
			// Ensure that the test cleaned up after itself by removing all assignments.
			var resp, err = etcd.Get(ctx, ks.Root+allocator.AssignmentsPrefix,
				clientv3.WithPrefix(), clientv3.WithLimit(1))

			c.Check(err, gc.IsNil)
			c.Check(resp.Kvs, gc.HasLen, 0)

			broker.Tasks.Cancel()
			c.Check(broker.Tasks.Wait(), gc.IsNil)

			cancel()
			etcdtest.Cleanup()
		}
}

func (f *testFixture) allocateShard(c *gc.C, spec *ShardSpec, assignments ...pb.ProcessSpec_ID) {
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
		var resp, err = f.etcd.Get(f.ctx,
			allocator.ItemAssignmentsPrefix(f.ks, spec.Id.String()), clientv3.WithPrefix())
		c.Assert(err, gc.IsNil)

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

	var resp, err = f.etcd.Txn(f.ctx).If().Then(ops...).Commit()
	c.Assert(err, gc.IsNil)
	c.Assert(resp.Succeeded, gc.Equals, true)

	f.ks.Mu.RLock()
	f.ks.WaitForRevision(f.ctx, resp.Header.Revision)
	f.ks.Mu.RUnlock()
}

func makeShard(id ShardID) *ShardSpec {
	return &ShardSpec{
		Id: id,
		Sources: []ShardSpec_Source{
			{Journal: sourceA, MinOffset: int64(len(sourceAWriteFixture))},
			{Journal: sourceB},
		},
		RecoveryLogPrefix: aRecoveryLogPrefix,
		HintPrefix:        "/hints",
		HintBackups:       2,
		MinTxnDuration:    10 * time.Millisecond,
		MaxTxnDuration:    100 * time.Millisecond,
	}
}

func makeConsumer(id pb.ProcessSpec_ID) *ConsumerSpec {
	return &ConsumerSpec{
		ProcessSpec: pb.ProcessSpec{
			Id:       id,
			Endpoint: pb.Endpoint("http://" + id.Zone + "/endpoint"),
		},
		ShardLimit: 100,
	}
}

func pluckTheAssignment(c *gc.C, state *allocator.State) (*ShardSpec, keyspace.KeyValue) {
	c.Assert(state.LocalItems, gc.HasLen, 1)
	return state.LocalItems[0].Item.Decoded.(allocator.Item).ItemValue.(*ShardSpec),
		state.LocalItems[0].Assignments[state.LocalItems[0].Index]
}

func expectStatusCode(c *gc.C, state *allocator.State, code ReplicaStatus_Code) *ReplicaStatus {
	defer state.KS.Mu.RUnlock()
	state.KS.Mu.RLock()

	for {
		var _, kv = pluckTheAssignment(c, state)
		var status = kv.Decoded.(allocator.Assignment).AssignmentValue.(*ReplicaStatus)

		c.Check(status.Code <= code, gc.Equals, true)

		if status.Code >= code {
			return status
		}
		c.Check(state.KS.WaitForRevision(context.Background(), state.KS.Header.Revision+1), gc.IsNil)
	}
}

func runSomeTransactions(c *gc.C, shard Shard) {
	var (
		r     *Replica = shard.(*Replica)
		app            = r.app.(*testApplication)
		store          = r.store.(*JSONFileStore)
	)
	for _, write := range []string{
		`{"key":"foo","value":"bar"}`,
		`{"key":"baz","value":"bing"}`,
		`{"key":"ring","value":"ting"}`,
		`{"key":"foo","value":"fin"}`,
	} {
		var finishCh = app.finishCh

		var aa = r.JournalClient().StartAppend(sourceA)
		aa.Writer().WriteString(write + "\n")
		c.Check(aa.Release(), gc.IsNil)

		<-finishCh // Block until txn finishes.
	}
	// Verify we reduced the expected final state.
	c.Check(store.State, gc.DeepEquals,
		&map[string]string{"foo": "fin", "baz": "bing", "ring": "ting"})

	<-store.Recorder().WeakBarrier().Done() // Reduce noisy logging by allowing log writes to complete.
}
