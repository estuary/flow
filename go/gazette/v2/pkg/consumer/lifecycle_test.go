package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/brokertest"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	"github.com/LiveRamp/gazette/v2/pkg/labels"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/recoverylog"
	"github.com/coreos/etcd/clientv3"
	gc "github.com/go-check/check"
)

type LifecycleSuite struct{}

func (s *LifecycleSuite) TestRecoveryFromEmptyLog(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	go func() { c.Assert(playLog(r, r.player, r.etcd), gc.IsNil) }()

	// Precondition: no existing hints in etcd.
	c.Check(mustGet(c, r.etcd, r.spec.HintPrimaryKey()).Kvs, gc.HasLen, 0)
	c.Check(mustGet(c, r.etcd, r.spec.HintBackupKeys()[0]).Kvs, gc.HasLen, 0)

	var store, offsets, err = completePlayback(r, r.app, r.player, r.etcd)
	c.Check(err, gc.IsNil)
	r.store = store

	// Expect |offsets| reflects MinOffset from the ShardSpec fixture.
	c.Check(offsets, gc.DeepEquals,
		map[pb.Journal]int64{sourceA: r.spec.Sources[0].MinOffset})

	// Post-condition: backup (but not primary) hints were updated.
	c.Check(mustGet(c, r.etcd, r.spec.HintPrimaryKey()).Kvs, gc.HasLen, 0)
	c.Check(mustGet(c, r.etcd, r.spec.HintBackupKeys()[0]).Kvs, gc.HasLen, 1)
}

func (s *LifecycleSuite) TestRecoveryOfNonEmptyLog(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	playAndComplete(c, r)

	for _, tm := range []*testMessage{
		{Key: "foo", Value: "1"},
		{Key: "bar", Value: "2"},
		{Key: "foo", Value: "3"},
		{Key: "baz", Value: "4"},
	} {
		c.Check(r.app.ConsumeMessage(r, r.store, message.Envelope{Message: tm}), gc.IsNil)
	}
	c.Check(r.app.FinalizeTxn(r, r.store), gc.IsNil)
	c.Check(r.store.Flush(map[pb.Journal]int64{sourceA: 123, sourceB: 456}), gc.IsNil)
	var hints, _ = r.store.Recorder().BuildHints()
	c.Check(storeRecordedHints(r, hints, r.etcd), gc.IsNil)

	// Reset for the next player.
	r.store.Destroy()
	r.player = recoverylog.NewPlayer()

	go func() { c.Assert(playLog(r, r.player, r.etcd), gc.IsNil) }()

	store, offsets, err := completePlayback(r, r.app, r.player, r.etcd)
	c.Check(err, gc.IsNil)
	c.Check(offsets, gc.DeepEquals, map[pb.Journal]int64{"source/A": 123, "source/B": 456})
	r.store = store

	c.Check(r.store.(*JSONFileStore).State, gc.DeepEquals, &map[string]string{
		"foo": "3",
		"bar": "2",
		"baz": "4",
	})
}

func (s *LifecycleSuite) TestRecoveryFailsFromInvalidHints(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	var _, err = r.etcd.Put(r.ctx, r.spec.HintPrimaryKey(), "invalid hints")
	c.Check(err, gc.IsNil)

	// Expect playLog returns an immediate error.
	c.Check(playLog(r, r.player, r.etcd), gc.ErrorMatches,
		`fetching FSM hints: unmarshal FSMHints: invalid character .*`)

	// Expect completePlayback blocks waiting for Play completion, but
	// aborts on context cancellation.
	time.AfterFunc(10*time.Millisecond, r.cancel)

	_, _, err = completePlayback(r, r.app, r.player, r.etcd)
	c.Check(err, gc.Equals, context.Canceled)
}

func (s *LifecycleSuite) TestRecoveryFailsFromMissingLog(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	r.spec.RecoveryLogPrefix = "does/not/exist"

	// Case: playLog fails while attempting to fetch spec.
	c.Check(playLog(r, r.player, r.etcd), gc.ErrorMatches,
		`fetching JournalSpec: named journal does not exist \(does/not/exist/`+shardA+`\)`)
}

func (s *LifecycleSuite) TestRecoveryFailsFromWrongContentType(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	var ctx = pb.WithDispatchDefault(r.ctx)

	// Fetch current log spec, set an incorrect ContentType, and re-apply.
	var lr, err = r.JournalClient().List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("name", r.spec.RecoveryLog().String())},
	})
	c.Assert(err, gc.IsNil)

	lr.Journals[0].Spec.LabelSet.SetValue(labels.ContentType, "wrong/type")
	_, err = r.JournalClient().Apply(ctx, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{{Upsert: &lr.Journals[0].Spec, ExpectModRevision: lr.Journals[0].ModRevision}},
	})
	c.Assert(err, gc.IsNil)

	// Case: playLog fails while attempting to fetch spec.
	c.Check(playLog(r, r.player, r.etcd), gc.ErrorMatches,
		`expected label `+labels.ContentType+` value `+labels.ContentType_RecoveryLog+` \(got wrong/type\)`)
}

func (s *LifecycleSuite) TestRecoveryFailsFromPlayError(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	// Write a valid FSMHints that references a log offset that doesn't exist.
	var fixture = recoverylog.FSMHints{
		Log: r.spec.RecoveryLog(),
		LiveNodes: []recoverylog.FnodeSegments{
			{Fnode: 1, Segments: []recoverylog.Segment{{Author: 123, FirstSeqNo: 1, FirstOffset: 100, LastSeqNo: 1}}},
		},
	}
	var fixtureBytes, _ = json.Marshal(&fixture)

	var _, err = r.etcd.Put(r.ctx, r.spec.HintPrimaryKey(), string(fixtureBytes))
	c.Check(err, gc.IsNil)

	// Expect playLog returns an immediate error.
	c.Check(playLog(r, r.player, r.etcd), gc.ErrorMatches, `playing log .*: max write-head of .* is 0, vs .*`)

	// Since the error occurred within Player.Play, it also causes completePlayback to immediately fail.
	_, _, err = completePlayback(r, r.app, r.player, r.etcd)
	c.Check(err, gc.ErrorMatches, `completePlayback aborting due to Play failure`)
}

func (s *LifecycleSuite) TestMessagePump(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	var msgCh = make(chan message.Envelope, 128)

	go func() {
		var src = r.spec.Sources[0]
		c.Check(pumpMessages(r, r.app, src.Journal, src.MinOffset, msgCh), gc.Equals, context.Canceled)
	}()

	var aa = r.JournalClient().StartAppend(sourceA)
	_, _ = aa.Writer().WriteString(
		`{"key":"foo","value":"bar"}
				bad line
				{"key":"baz","value":"bing"}
				another bad line
				{"key":"fin"}
			`)
	c.Check(aa.Release(), gc.IsNil)

	var expect = message.Envelope{
		JournalSpec: brokertest.Journal(pb.JournalSpec{
			Name:     "source/A",
			LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines),
		}),
		Fragment: nil,
	}

	var off = r.spec.Sources[0].MinOffset

	// Expect pumpMessages continues to extract messages despite unmarshal errors.
	expect.NextOffset, expect.Message = off+28, &testMessage{Key: "foo", Value: "bar"}
	c.Check(<-msgCh, gc.DeepEquals, expect)
	expect.NextOffset, expect.Message = off+74, &testMessage{Key: "baz", Value: "bing"}
	c.Check(<-msgCh, gc.DeepEquals, expect)
	expect.NextOffset, expect.Message = off+113, &testMessage{Key: "fin", Value: ""}
	c.Check(<-msgCh, gc.DeepEquals, expect)
}

func (s *LifecycleSuite) TestMessagePumpFailsOnUnknownJournal(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	c.Check(pumpMessages(r, r.app, "unknown/journal", 0, nil),
		gc.ErrorMatches, `fetching JournalSpec: named journal does not exist \(unknown/journal\)`)
}

func (s *LifecycleSuite) TestMessagePumpFailsOnBadFraming(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	c.Check(pumpMessages(r, r.app, r.spec.RecoveryLog(), 0, nil),
		gc.ErrorMatches, `determining framing (.*): unrecognized `+labels.ContentType+` \(`+labels.ContentType_RecoveryLog+`\)`)
}

func (s *LifecycleSuite) TestMessagePumpFailsOnNewMessageError(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	r.app.(*testApplication).newMsgErr = errors.New("new message error")

	// Write a newline so that pumpMessages completes a message frame read.
	var aa = r.JournalClient().StartAppend(sourceA)
	_, _ = aa.Writer().WriteString("\n")
	c.Check(aa.Release(), gc.IsNil)

	c.Check(pumpMessages(r, r.app, sourceA, 0, nil),
		gc.ErrorMatches, `NewMessage \(source/A\): new message error`)
}

func (s *LifecycleSuite) TestTxnPriorSyncsThenMinDurElapses(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	playAndComplete(c, r)
	var msgCh = make(chan message.Envelope, 128)

	var timer, restore = newTestTimer()
	defer restore()

	var priorDoneCh = make(chan struct{})
	var prior, txn = transaction{}, transaction{
		minDur:  3 * time.Second,
		maxDur:  5 * time.Second,
		msgCh:   msgCh,
		offsets: make(map[pb.Journal]int64),
		doneCh:  priorDoneCh,
	}

	// Resolve prior commit before txn begins.
	timer.timepoint = faketime(1)
	close(priorDoneCh)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)

	// Initial message opens the txn.
	timer.timepoint = faketime(2)
	sendMsgFixture(msgCh, true, 100)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)
	c.Check(timer.reset, gc.Equals, 3*time.Second) // Reset to |minDur|.

	// Expect it continues to block.
	sendMsgFixture(msgCh, true, 200)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)

	// Signal that |minDur| has elapsed.
	timer.timepoint = faketime(5)
	timer.signal()
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)
	c.Check(timer.reset, gc.Equals, 2*time.Second) // Reset to remainder of |maxDur|.

	// Consume additional ready message.
	sendMsgFixture(msgCh, false, 300)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)

	// |msgCh| stalls, and the transaction completes.
	timer.timepoint = faketime(6)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, true)

	c.Check(timer.stopped, gc.Equals, true)
	c.Check(txn.barrier, gc.NotNil)
	c.Check(txn.minDur, gc.Equals, time.Duration(-1))
	c.Check(txn.maxDur, gc.Equals, 5*time.Second) // Did not elapse.
	c.Check(txn.msgCh, gc.NotNil)                 // Did not stall.
	c.Check(txn.msgCount, gc.Equals, 3)
	c.Check(txn.doneCh, gc.IsNil)

	c.Check(r.store.(*JSONFileStore).State, gc.DeepEquals, &map[string]string{"key": "300"})
	c.Check(r.store.(*JSONFileStore).offsets, gc.DeepEquals, map[pb.Journal]int64{"source/A": 300})

	c.Check(txn.beganAt, gc.Equals, faketime(2))
	c.Check(prior.syncedAt, gc.Equals, faketime(1))
	c.Check(txn.stalledAt, gc.Equals, faketime(6))
	c.Check(txn.flushedAt, gc.Equals, faketime(6))
	c.Check(txn.committedAt, gc.Equals, faketime(6))
}

func (s *LifecycleSuite) TestTxnMinDurElapsesThenPriorSyncs(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	playAndComplete(c, r)
	var msgCh = make(chan message.Envelope, 128)

	var timer, restore = newTestTimer()
	defer restore()

	var priorDoneCh = make(chan struct{})
	var prior, txn = transaction{}, transaction{
		minDur:  3 * time.Second,
		maxDur:  5 * time.Second,
		msgCh:   msgCh,
		offsets: make(map[pb.Journal]int64),
		doneCh:  priorDoneCh,
	}

	// Initial message opens the txn.
	sendMsgFixture(msgCh, true, 100)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)
	c.Check(timer.reset, gc.Equals, 3*time.Second) // Reset to |minDur|.

	// Signal that |minDur| has elapsed.
	timer.timepoint = faketime(3)
	timer.signal()
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)
	c.Check(timer.reset, gc.Equals, 2*time.Second) // Reset to remainder of |maxDur|.

	// Expect it continues to block.
	sendMsgFixture(msgCh, true, 200)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)

	// Resolve prior commit.
	timer.timepoint = faketime(4)
	close(priorDoneCh)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)

	// Consume additional ready message.
	sendMsgFixture(msgCh, false, 300)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)

	// |msgCh| stalls, and the transaction completes.
	timer.timepoint = faketime(5)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, true)

	c.Check(timer.stopped, gc.Equals, true)
	c.Check(txn.barrier, gc.NotNil)
	c.Check(txn.minDur, gc.Equals, time.Duration(-1))
	c.Check(txn.maxDur, gc.Equals, 5*time.Second) // Did not elapse.
	c.Check(txn.msgCh, gc.NotNil)                 // Did not stall.
	c.Check(txn.msgCount, gc.Equals, 3)
	c.Check(txn.doneCh, gc.IsNil)

	c.Check(r.store.(*JSONFileStore).State, gc.DeepEquals, &map[string]string{"key": "300"})
	c.Check(r.store.(*JSONFileStore).offsets, gc.DeepEquals, map[pb.Journal]int64{"source/A": 300})

	c.Check(txn.beganAt, gc.Equals, faketime(0))
	c.Check(prior.syncedAt, gc.Equals, faketime(4))
	c.Check(txn.stalledAt, gc.Equals, faketime(5))
	c.Check(txn.flushedAt, gc.Equals, faketime(5))
	c.Check(txn.committedAt, gc.Equals, faketime(5))
}

func (s *LifecycleSuite) TestTxnMaxDurElapsesThenPriorSyncs(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	playAndComplete(c, r)
	var msgCh = make(chan message.Envelope, 128)

	var timer, restore = newTestTimer()
	defer restore()

	var priorDoneCh = make(chan struct{})
	var prior, txn = transaction{}, transaction{
		minDur:  3 * time.Second,
		maxDur:  5 * time.Second,
		msgCh:   msgCh,
		offsets: make(map[pb.Journal]int64),
		doneCh:  priorDoneCh,
	}

	// Initial message opens the txn.
	sendMsgFixture(msgCh, true, 100)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)
	c.Check(timer.reset, gc.Equals, 3*time.Second) // Reset to |minDur|.

	// Signal that |minDur| has elapsed.
	timer.timepoint = faketime(3)
	timer.signal()
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)
	c.Check(timer.reset, gc.Equals, 2*time.Second) // Reset to remainder of |maxDur|.

	// Expect it continues to block.
	sendMsgFixture(msgCh, true, 200)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)

	// Signal that |maxDur| has elapsed.
	timer.timepoint = faketime(5)
	timer.signal()
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)

	// Additional messages are not consumed. Blocks for |priorDoneCh| to close.
	sendMsgFixture(msgCh, false, 300)
	timer.timepoint = faketime(7)

	time.AfterFunc(time.Millisecond, func() { close(priorDoneCh) })
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)

	// |msgCh| stalls, and the transaction completes.
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, true)

	c.Check(timer.stopped, gc.Equals, false)
	c.Check(txn.barrier, gc.NotNil)
	c.Check(txn.minDur, gc.Equals, time.Duration(-1))
	c.Check(txn.maxDur, gc.Equals, time.Duration(-1))
	c.Check(txn.msgCh, gc.IsNil)
	c.Check(txn.msgCount, gc.Equals, 2)
	c.Check(txn.doneCh, gc.IsNil)

	c.Check(r.store.(*JSONFileStore).State, gc.DeepEquals, &map[string]string{"key": "200"})
	c.Check(r.store.(*JSONFileStore).offsets, gc.DeepEquals, map[pb.Journal]int64{"source/A": 200})

	c.Check(txn.beganAt, gc.Equals, faketime(0))
	c.Check(prior.syncedAt, gc.Equals, faketime(7))
	c.Check(txn.stalledAt, gc.Equals, faketime(5))
	c.Check(txn.flushedAt, gc.Equals, faketime(7))
	c.Check(txn.committedAt, gc.Equals, faketime(7))
}

func (s *LifecycleSuite) TestTxnMaxDurElapsesBusyPipeline(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	playAndComplete(c, r)
	var msgCh = make(chan message.Envelope, 128)

	var timer, restore = newTestTimer()
	defer restore()

	var priorDoneCh = make(chan struct{})
	var prior, txn = transaction{}, transaction{
		minDur:  3 * time.Second,
		maxDur:  5 * time.Second,
		msgCh:   msgCh,
		offsets: make(map[pb.Journal]int64),
		doneCh:  priorDoneCh,
	}

	// Initial message opens the txn.
	sendMsgFixture(msgCh, true, 100)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)
	c.Check(timer.reset, gc.Equals, 3*time.Second) // Reset to |minDur|.

	// Resolve prior commit.
	timer.timepoint = faketime(1)
	close(priorDoneCh)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)

	// Signal that |minDur| has elapsed.
	timer.timepoint = faketime(3)
	timer.signal()
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)
	c.Check(timer.reset, gc.Equals, 2*time.Second) // Reset to remainder of |maxDur|.

	// Consume additional ready message.
	sendMsgFixture(msgCh, false, 200)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)

	// Signal that |maxDur| has elapsed.
	timer.timepoint = faketime(5)
	timer.signal()
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)

	// Additional message is not consumed.
	sendMsgFixture(msgCh, false, 300)
	// |msgCh| stalls, and the transaction completes.
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, true)

	c.Check(timer.stopped, gc.Equals, false)
	c.Check(txn.barrier, gc.NotNil)
	c.Check(txn.minDur, gc.Equals, time.Duration(-1))
	c.Check(txn.maxDur, gc.Equals, time.Duration(-1))
	c.Check(txn.msgCh, gc.IsNil)
	c.Check(txn.msgCount, gc.Equals, 2)
	c.Check(txn.doneCh, gc.IsNil)

	c.Check(r.store.(*JSONFileStore).State, gc.DeepEquals, &map[string]string{"key": "200"})
	c.Check(r.store.(*JSONFileStore).offsets, gc.DeepEquals, map[pb.Journal]int64{"source/A": 200})

	c.Check(txn.beganAt, gc.Equals, faketime(0))
	c.Check(prior.syncedAt, gc.Equals, faketime(1))
	c.Check(txn.stalledAt, gc.Equals, faketime(5))
	c.Check(txn.flushedAt, gc.Equals, faketime(5))
	c.Check(txn.committedAt, gc.Equals, faketime(5))
}

func (s *LifecycleSuite) TestTxnCancelledBeforeStart(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	playAndComplete(c, r)

	var prior, txn = transaction{}, transaction{
		minDur: 3 * time.Second,
		maxDur: 5 * time.Second,
		msgCh:  nil,
		doneCh: make(chan struct{}),
	}

	r.cancel()
	var _, err = txnStep(&txn, &prior, r, r.store, r.app, txnTimer{})
	c.Check(err, gc.Equals, context.Canceled)

	c.Check(txn.minDur, gc.Equals, 3*time.Second)
	c.Check(txn.msgCount, gc.Equals, 0)
	c.Check(txn.doneCh, gc.NotNil)
}

func (s *LifecycleSuite) TestTxnCancelledAfterStart(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	playAndComplete(c, r)
	var msgCh = make(chan message.Envelope, 128)

	var timer, restore = newTestTimer()
	defer restore()

	var priorDoneCh = make(chan struct{})
	var prior, txn = transaction{}, transaction{
		minDur:  3 * time.Second,
		maxDur:  5 * time.Second,
		msgCh:   msgCh,
		offsets: make(map[pb.Journal]int64),
		doneCh:  priorDoneCh,
	}

	// Initial message opens the txn.
	sendMsgFixture(msgCh, true, 100)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)
	c.Check(timer.reset, gc.Equals, 3*time.Second) // Reset to |minDur|.

	// Resolve prior commit.
	timer.timepoint = faketime(1)
	close(priorDoneCh)
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)

	// Signal that |minDur| has elapsed.
	timer.timepoint = faketime(3)
	timer.signal()
	c.Check(mustTxnStep(c, r, &txn, &prior, timer.txnTimer), gc.Equals, false)
	c.Check(timer.reset, gc.Equals, 2*time.Second) // Reset to remainder of |maxDur|.

	r.cancel()
	var _, err = txnStep(&txn, &prior, r, r.store, r.app, txnTimer{})
	c.Check(err, gc.Equals, context.Canceled)

	c.Check(txn.minDur, gc.Equals, time.Duration(-1))
	c.Check(txn.msgCount, gc.Equals, 1)
	c.Check(txn.doneCh, gc.IsNil)
}

func (s *LifecycleSuite) TestConsumeUpdatesPrimaryHints(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	playAndComplete(c, r)
	var msgCh = make(chan message.Envelope)
	var hintsCh = make(chan time.Time, 1)

	go func() {
		c.Check(consumeMessages(r, r.store, r.app, r.etcd, msgCh, hintsCh), gc.Equals, context.Canceled)
	}()
	// Precondition: recorded hints are not set.
	c.Check(mustGet(c, r.etcd, r.spec.HintPrimaryKey()).Kvs, gc.HasLen, 0)

	hintsCh <- time.Time{}

	// consumeMessages does a non-blocking select of |hintsCh|, so run two txns
	// to ensure that |hintsCh| is selected and the operation has completed.
	sendMsgAndWait(r.app.(*testApplication), msgCh)
	sendMsgAndWait(r.app.(*testApplication), msgCh)

	c.Check(mustGet(c, r.etcd, r.spec.HintPrimaryKey()).Kvs, gc.HasLen, 1)
}

func (s *LifecycleSuite) TestConsumeErrorCases(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	playAndComplete(c, r)
	var msgCh = make(chan message.Envelope, 128)
	var app = r.app.(*testApplication)

	// Case: FinalizeTxn fails.
	var finishCh = app.finishCh
	app.finalizeErr = errors.New("finalize error")

	sendMsgFixture(msgCh, false, 100)
	c.Check(consumeMessages(r, r.store, r.app, r.etcd, msgCh, nil),
		gc.ErrorMatches, `txnStep: app.FinalizeTxn: finalize error`)

	<-finishCh // Expect FinishTxn was still called and |finishCh| closed.

	// Case: ConsumeMessage fails.
	app.consumeErr = errors.New("consume error")

	sendMsgFixture(msgCh, false, 100)
	c.Check(consumeMessages(r, r.store, r.app, r.etcd, msgCh, nil),
		gc.ErrorMatches, `txnStep: app.ConsumeMessage: consume error`)

	// Case: BeginTxn fails.
	app.beginErr = errors.New("begin error")

	sendMsgFixture(msgCh, false, 100)
	c.Check(consumeMessages(r, r.store, r.app, r.etcd, msgCh, nil),
		gc.ErrorMatches, `txnStep: app.BeginTxn: begin error`)
}

func (s *LifecycleSuite) TestPumpAndConsume(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	playAndComplete(c, r)
	var msgCh = make(chan message.Envelope, 128)

	go func() {
		var src = r.spec.Sources[0]
		c.Check(pumpMessages(r, r.app, src.Journal, src.MinOffset, msgCh), gc.Equals, context.Canceled)
	}()

	go func() {
		c.Check(consumeMessages(r, r.store, r.app, r.etcd, msgCh, nil), gc.Equals, context.Canceled)
	}()

	runSomeTransactions(c, r)
}

func (s *LifecycleSuite) TestFetchJournalSpec(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var spec, err = fetchJournalSpec(tf.ctx, sourceA, tf.broker.Client())
	c.Check(err, gc.IsNil)
	c.Check(spec, gc.DeepEquals, brokertest.Journal(
		pb.JournalSpec{Name: sourceA, LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines)}))

	_, err = fetchJournalSpec(tf.ctx, "not/here", tf.broker.Client())
	c.Check(err, gc.ErrorMatches, `named journal does not exist \(not/here\)`)
}

func (s *LifecycleSuite) TestStoreAndFetchHints(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	// Note that |r|'s shard fixture has three hint keys.

	// mkHints builds a valid FSMHints fixture which is unique on |id|.
	var mkHints = func(id int64) recoverylog.FSMHints {
		defer r.ks.Mu.RUnlock()
		r.ks.Mu.RLock() // Hold to access |r.spec|.

		return recoverylog.FSMHints{
			Log: r.spec.RecoveryLog(),
			LiveNodes: []recoverylog.FnodeSegments{{
				Fnode: recoverylog.Fnode(id),
				Segments: []recoverylog.Segment{
					{Author: 0x1234, FirstSeqNo: id, LastSeqNo: id},
				},
			}},
		}
	}
	// verifyHints confirms that fetchHints returns |id|, and the state of hint
	// keys in etcd matches |idA|, |idB|, |idC|.
	var verifyHints = func(id, idA, idB, idC int64) {
		var h, err = fetchHints(r.ctx, r.Spec(), r.etcd)
		c.Check(err, gc.IsNil)
		c.Check(h.spec, gc.DeepEquals, r.Spec())
		c.Check(h.txnResp.Responses, gc.HasLen, 3)
		c.Check(h.hints, gc.HasLen, 3)

		var hints = pickFirstHints(h)
		c.Check(pickFirstHints(h), gc.DeepEquals, mkHints(id))

		var recovered [3]int64
		for i, op := range h.txnResp.Responses {
			switch len(op.GetResponseRange().Kvs) {
			case 0: // Pass.
			case 1:
				c.Check(json.Unmarshal(op.GetResponseRange().Kvs[0].Value, &hints), gc.IsNil)
				recovered[i] = hints.LiveNodes[0].Segments[0].FirstSeqNo
			default:
				c.Fatal("unexpected length ", op)
			}
		}
		c.Check(recovered, gc.Equals, [3]int64{idA, idB, idC})
	}

	// Perform another assignment Put without updating the KeyValue fixture.
	// Ie, the fixture will on match CreateRevision but not ModRevision.
	_, err := r.etcd.Put(r.ctx, string(r.assignment.Raw.Key), "")
	c.Assert(err, gc.IsNil)

	c.Check(storeRecoveredHints(r, mkHints(111), r.etcd), gc.IsNil)
	verifyHints(111, 0, 111, 0)
	c.Check(storeRecordedHints(r, mkHints(222), r.etcd), gc.IsNil)
	verifyHints(222, 222, 111, 0)
	c.Check(storeRecoveredHints(r, mkHints(333), r.etcd), gc.IsNil)
	verifyHints(222, 222, 333, 111)
	c.Check(storeRecordedHints(r, mkHints(444), r.etcd), gc.IsNil)
	verifyHints(444, 444, 333, 111)
	c.Check(storeRecoveredHints(r, mkHints(555), r.etcd), gc.IsNil)
	verifyHints(444, 444, 555, 333)

	// Delete hints in key priority order. Expect older hints are used instead.
	_, _ = r.etcd.Delete(r.ctx, r.spec.HintPrimaryKey())
	verifyHints(555, 0, 555, 333)
	_, _ = r.etcd.Delete(r.ctx, r.spec.HintBackupKeys()[0])
	verifyHints(333, 0, 0, 333)
	_, _ = r.etcd.Delete(r.ctx, r.spec.HintBackupKeys()[1])

	// When no hints exist, default hints are returned.
	h, err := fetchHints(r.ctx, r.spec, r.etcd)
	c.Check(err, gc.IsNil)
	c.Check(pickFirstHints(h), gc.DeepEquals, recoverylog.FSMHints{Log: r.spec.RecoveryLog()})
}

// newLifecycleTestFixture extends newTestFixture by stubbing out |transition|
// and allocating an assigned local shard.
func newLifecycleTestFixture(c *gc.C) (*Replica, func()) {
	var tf, cleanup = newTestFixture(c)

	var realTransition = transition
	transition = func(r *Replica, spec *ShardSpec, assignment keyspace.KeyValue) {
		r.spec, r.assignment = spec, assignment
	}
	tf.allocateShard(c, makeShard(shardA), localID)

	return tf.resolver.replicas[shardA], func() {
		tf.allocateShard(c, makeShard(shardA)) // Remove assignment.

		transition = realTransition
		cleanup()
	}
}

type testTimer struct {
	txnTimer
	ch        chan time.Time
	stopped   bool
	reset     time.Duration
	timepoint time.Time
}

func newTestTimer() (*testTimer, func()) {
	var t = &testTimer{
		ch:        make(chan time.Time, 1),
		timepoint: faketime(0),
	}

	t.txnTimer = txnTimer{
		C:     t.ch,
		Reset: func(duration time.Duration) bool { t.reset = duration; return true },
		Stop:  func() bool { t.stopped = true; return true },
	}

	var restore = timeNow
	timeNow = func() time.Time { return t.timepoint }
	return t, func() { timeNow = restore }
}

func (t testTimer) signal() { t.ch <- t.timepoint }

func playAndComplete(c *gc.C, r *Replica) {
	go func() { c.Assert(playLog(r, r.player, r.etcd), gc.IsNil) }()

	var store, _, err = completePlayback(r, r.app, r.player, r.etcd)
	c.Check(err, gc.IsNil)
	r.store = store
}

func mustGet(c *gc.C, etcd clientv3.KV, key string) *clientv3.GetResponse {
	var resp, err = etcd.Get(context.Background(), key)
	c.Assert(err, gc.IsNil)
	return resp
}

func mustTxnStep(c *gc.C, r *Replica, txn, prior *transaction, timer txnTimer) bool {
	done, err := txnStep(txn, prior, r, r.store, r.app, timer)
	c.Check(err, gc.IsNil)
	return done
}

func sendMsgFixture(msgCh chan<- message.Envelope, async bool, offset int64) {
	var env = message.Envelope{
		Message:     &testMessage{Key: "key", Value: strconv.FormatInt(offset, 10)},
		JournalSpec: &pb.JournalSpec{Name: "source/A"},
		NextOffset:  offset,
	}
	if async {
		time.AfterFunc(time.Millisecond, func() { msgCh <- env })
	} else {
		msgCh <- env
	}
}

func sendMsgAndWait(ta *testApplication, msgCh chan<- message.Envelope) {
	var finishCh = ta.finishCh
	sendMsgFixture(msgCh, false, 100)
	<-finishCh
}

func faketime(delta int64) time.Time { return time.Unix(1500000000+delta, 0) }

var _ = gc.Suite(&LifecycleSuite{})
