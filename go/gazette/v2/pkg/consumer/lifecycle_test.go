package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/brokertest"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
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
	c.Check(mustGet(c, r.etcd, r.spec.HintKeys[0]).Kvs, gc.HasLen, 0)
	c.Check(mustGet(c, r.etcd, r.spec.HintKeys[1]).Kvs, gc.HasLen, 0)

	var store, offsets, err = completePlayback(r, r.app, r.player, r.etcd)
	c.Check(err, gc.IsNil)
	r.store = store

	// Expect |offsets| reflects MinOffset from the ShardSpec fixture.
	c.Check(offsets, gc.DeepEquals,
		map[pb.Journal]int64{sourceA: r.spec.Sources[0].MinOffset})

	// Post-condition: recovered (but not recorded) hints were updated.
	c.Check(mustGet(c, r.etcd, r.spec.HintKeys[0]).Kvs, gc.HasLen, 0)
	c.Check(mustGet(c, r.etcd, r.spec.HintKeys[1]).Kvs, gc.HasLen, 1)
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
	c.Check(storeRecordedHints(r, r.store.Recorder().BuildHints(), r.etcd), gc.IsNil)

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

func (s *LifecycleSuite) TestRecoveryFailsFromBadHints(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	var _, err = r.etcd.Put(r.ctx, r.spec.HintKeys[0], "invalid hints")
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

func (s *LifecycleSuite) TestRecoveryFailsFromPlayError(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	r.spec.RecoveryLog = "does/not/exist"

	// Expect playLog returns an immediate error.
	c.Check(playLog(r, r.player, r.etcd), gc.ErrorMatches,
		`playing log does/not/exist: determining log head: JOURNAL_NOT_FOUND`)

	// As does completePlayback.
	var _, _, err = completePlayback(r, r.app, r.player, r.etcd)
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
	aa.Writer().WriteString(
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
			LabelSet: pb.MustLabelSet("framing", "json"),
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

	c.Check(pumpMessages(r, r.app, aRecoveryLog, 0, nil),
		gc.ErrorMatches, `determining framing (.*): expected exactly one framing label \(got \[\]\)`)
}

func (s *LifecycleSuite) TestMessagePumpFailsOnNewMessageError(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	r.app.(*testApplication).newMsgErr = errors.New("new message error")

	// Write a newline so that pumpMessages completes a message frame read.
	var aa = r.JournalClient().StartAppend(sourceA)
	aa.Writer().WriteString("\n")
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

func (s *LifecycleSuite) TestConsumeUpdatesRecordedHints(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	playAndComplete(c, r)
	var msgCh = make(chan message.Envelope)
	var hintsCh = make(chan time.Time, 1)

	go func() {
		c.Check(consumeMessages(r, r.store, r.app, r.etcd, msgCh, hintsCh), gc.Equals, context.Canceled)
	}()
	// Precondition: recorded hints are not set.
	c.Check(mustGet(c, r.etcd, r.spec.HintKeys[0]).Kvs, gc.HasLen, 0)

	hintsCh <- time.Time{}

	// consumeMessages does a non-blocking select of |hintsCh|, so run two txns
	// to ensure that |hintsCh| is selected and the operation has completed.
	sendMsgAndWait(r.app.(*testApplication), msgCh)
	sendMsgAndWait(r.app.(*testApplication), msgCh)

	c.Check(mustGet(c, r.etcd, r.spec.HintKeys[0]).Kvs, gc.HasLen, 1)
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

	runSomeTransactions(c, r, r.app.(*testApplication), r.store.(*JSONFileStore))
}

func (s *LifecycleSuite) TestFetchJournalSpec(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var spec, err = fetchJournalSpec(tf.ctx, aRecoveryLog, tf.broker.Client())
	c.Check(err, gc.IsNil)
	c.Check(spec, gc.DeepEquals, brokertest.Journal(pb.JournalSpec{Name: aRecoveryLog}))

	_, err = fetchJournalSpec(tf.ctx, "not/here", tf.broker.Client())
	c.Check(err, gc.ErrorMatches, `named journal does not exist \(not/here\)`)
}

func (s *LifecycleSuite) TestStoreAndFetchHints(c *gc.C) {
	var r, cleanup = newLifecycleTestFixture(c)
	defer cleanup()

	// Note that |r|'s shard fixture has three HintKeys (/hints-A, /hints-B, /hints-C).

	// mkHints builds a valid FSMHints fixture which is unique on |id|.
	var mkHints = func(id int64) recoverylog.FSMHints {
		return recoverylog.FSMHints{
			Log: aRecoveryLog,
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
		var hints, resp, err = fetchHints(r.ctx, r.Spec(), r.etcd)
		c.Check(err, gc.IsNil)
		c.Check(hints, gc.DeepEquals, mkHints(id))
		c.Check(resp.Responses, gc.HasLen, 3)

		var recovered [3]int64
		for i, r := range resp.Responses {
			switch len(r.GetResponseRange().Kvs) {
			case 0: // Pass.
			case 1:
				json.Unmarshal(r.GetResponseRange().Kvs[0].Value, &hints)
				recovered[i] = hints.LiveNodes[0].Segments[0].FirstSeqNo
			default:
				c.Fatal("unexpected length ", r)
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
	r.etcd.Delete(r.ctx, r.spec.HintKeys[0])
	verifyHints(555, 0, 555, 333)
	r.etcd.Delete(r.ctx, r.spec.HintKeys[1])
	verifyHints(333, 0, 0, 333)
	r.etcd.Delete(r.ctx, r.spec.HintKeys[2])

	// When no hints exist, default hints are returned.
	hints, _, err := fetchHints(r.ctx, r.spec, r.etcd)
	c.Check(err, gc.IsNil)
	c.Check(hints, gc.DeepEquals, recoverylog.FSMHints{Log: aRecoveryLog})
}

// newLifecycleTestFixture extends newTestFixture by stubbing out |transition|
// and allocating an assigned local shard.
func newLifecycleTestFixture(c *gc.C) (*Replica, func()) {
	var tf, cleanup = newTestFixture(c)

	var realTransition = transition
	transition = func(r *Replica, spec *ShardSpec, assignment keyspace.KeyValue) {
		r.spec, r.assignment = spec, assignment
	}
	tf.allocateShard(c, makeShard("a-shard"), localID)

	return tf.resolver.replicas["a-shard"], func() {
		tf.allocateShard(c, makeShard("a-shard")) // Remove assignment.

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
