package consumer

import (
	"io"
	"time"

	"github.com/pkg/errors"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

// runTransactions runs consumer transactions. It consumes from the provided
// |readCh| and, when notified by |hintsCh|, occasionally stores recorded FSMHints.
func runTransactions(s *shard, cp pc.Checkpoint, readCh <-chan EnvelopeOrError, hintsCh <-chan time.Time) error {
	var (
		realTimer = time.NewTimer(0)
		txnTimer  = txnTimer{
			C:     realTimer.C,
			Reset: realTimer.Reset,
			Stop:  realTimer.Stop,
			Now:   time.Now,
		}
		prev = transaction{
			readThrough:   pc.FlattenReadThrough(cp),
			commitBarrier: client.FinishedOperation(nil),
			acks:          make(OpFutures),
			prepareDoneAt: txnTimer.Now(),
		}
		txn = transaction{
			readThrough: make(pb.Offsets),
		}
	)
	<-realTimer.C

	// Begin by acknowledging (or re-acknowledging) messages published as part
	// of the most-recent recovered transaction checkpoint.
	if err := txnAcknowledge(s, &prev, cp); err != nil {
		return errors.WithMessage(err, "txnAcknowledge(recovered Checkpoint)")
	}

	for {
		select {
		case <-hintsCh:
			var hints, err = s.recovery.recorder.BuildHints()
			if err == nil {
				err = storeRecordedHints(s, hints)
			}
			if err != nil {
				return errors.WithMessage(err, "storeRecordedHints")
			}
			continue
		default:
			// Pass.
		}

		txnInit(s, &txn, &prev, readCh, txnTimer)
		if err := txnRun(s, &txn, &prev); err != nil {
			return err
		}
		prev, txn = txn, prev
	}
}

// transaction models a single consumer shard transaction.
type transaction struct {
	minDur, maxDur time.Duration      // Min/max processing durations. Set to -1 when elapsed.
	waitForAck     bool               // Wait for ACKs of pending messages read this txn?
	barrierCh      <-chan struct{}    // Next barrier of previous transaction to resolve.
	readCh         <-chan EnvelopeOrError // Message source. Nil'd upon reaching |maxDur|.
	readThrough    pb.Offsets         // Offsets read through this transaction.
	consumedCount  int                // Number of acknowledged Messages consumed.
	consumedBytes  int64              // Number of acknowledged Message bytes consumed.
	commitBarrier  OpFuture           // Barrier at which this transaction commits.
	acks           OpFutures          // ACKs of published messages, queued on |commitBarrier|.

	timer             txnTimer
	prevPrepareDoneAt time.Time // Time at which previous transaction finished preparing.
	beganAt           time.Time // Time at which first transaction message was processed.
	stalledAt         time.Time // Time at which |maxDur| elapsed without |priorCommittedCh| resolving.
	prepareBeganAt    time.Time // Time at which we began preparing to commit.
	prepareDoneAt     time.Time // Time at which we finished preparing to commit.
	committedAt       time.Time // Time at which |commitBarrier| resolved.
	ackedAt           time.Time // Time at which published |acks| resolved.
}

// txnInit initializes transaction |txn| in preparation to run.
func txnInit(s *shard, txn, prev *transaction, readCh <-chan EnvelopeOrError, timer txnTimer) {
	var spec = s.Spec()

	*txn = transaction{
		readCh:            readCh,
		readThrough:       txn.readThrough,
		acks:              make(OpFutures, len(prev.acks)),
		timer:             timer,
		minDur:            spec.MinTxnDuration,
		maxDur:            spec.MaxTxnDuration,
		waitForAck:        !spec.DisableWaitForAck,
		barrierCh:         prev.commitBarrier.Done(),
		prevPrepareDoneAt: prev.prepareDoneAt,
	}
	for j, o := range prev.readThrough {
		txn.readThrough[j] = o
	}
}

// txnRun runs a single consumer transaction |txn| until it starts to commit.
func txnRun(s *shard, txn, prev *transaction) error {
	var done, err = txnStep(s, txn, prev)
	for !done && err == nil {
		done, err = txnStep(s, txn, prev)
	}

	if bf, ok := s.svc.App.(BeginFinisher); ok && txn.consumedCount != 0 {
		if err != nil {
			bf.FinishedTxn(s, s.store, client.FinishedOperation(err))
		} else {
			bf.FinishedTxn(s, s.store, txn.commitBarrier)
		}
	}
	return err
}

func txnBlocks(s *shard, txn, prev *transaction) bool {
	// Block if we haven't consumed messages yet.
	return txn.consumedCount == 0 ||
		// Or if the minimum batching duration hasn't elapsed.
		txn.minDur != -1 ||
		// Or if the prior transaction hasn't completed.
		txn.barrierCh != nil ||
		// Or if the maximum batching duration hasn't elapsed, and a sequence
		// started this transaction awaits an ACK which we want to wait for.
		(txn.waitForAck && txn.maxDur != -1 && s.sequencer.HasPending(prev.readThrough))
}

// txnStep steps the transaction one time, and returns true iff it has started to commit.
func txnStep(s *shard, txn, prev *transaction) (bool, error) {
	if txnBlocks(s, txn, prev) {
		select {
		case env := <-txn.readCh:
			return false, txnRead(s, txn, env)
		case tick := <-txn.timer.C:
			return false, txnTick(s, txn, tick)
		case _ = <-txn.barrierCh:
			return false, txnBarrierResolved(s, txn, prev)
		}
	} else {
		select {
		case env := <-txn.readCh:
			return false, txnRead(s, txn, env)
		case tick := <-txn.timer.C:
			return false, txnTick(s, txn, tick)
		default:
			// Start to commit.
		}
	}

	if cp, err := txnStartCommit(s, txn); err != nil {
		return false, errors.WithMessage(err, "txnStartCommit")
	} else if err = txnAcknowledge(s, txn, cp); err != nil {
		return false, errors.WithMessage(err, "txnAcknowledge")
	}

	// If the timer is still running, stop and drain it.
	if txn.maxDur != -1 && !txn.timer.Stop() {
		<-txn.timer.C
	}
	return true, nil
}

func txnRead(s *shard, txn *transaction, env EnvelopeOrError) error {
	if env.Error != nil {
		return errors.WithMessage(env.Error, "readMessage")
	}
	txn.readThrough[env.Journal.Name] = env.End
	s.sequencer.QueueUncommitted(env.Envelope)
	// DEPRECATED metrics to be removed:
	bytesConsumedTotal.Add(float64(env.End - env.Begin))
	readHeadGauge.WithLabelValues(env.Journal.Name.String()).Set(float64(env.End))
	// End DEPRECATED metrics.

	for {
		switch env, err := s.sequencer.DequeCommitted(); err {
		case nil:
			if err = txnConsume(s, txn, env); err != nil {
				return err
			}

		case io.EOF:
			return nil

		case message.ErrMustStartReplay:
			var from, to = s.sequencer.ReplayRange()
			var rr = client.NewRetryReader(s.ctx, s.ajc, pb.ReadRequest{
				Journal:    env.Journal.Name,
				Offset:     from,
				EndOffset:  to,
				Block:      true,
				DoNotProxy: !s.ajc.IsNoopRouter(),
			})
			s.sequencer.StartReplay(message.NewReadUncommittedIter(rr, s.svc.App.NewMessage))

		default:
			return errors.WithMessage(err, "dequeCommitted")
		}
	}
}

func txnConsume(s *shard, txn *transaction, env message.Envelope) error {
	// Does this message begin the transaction?
	if txn.consumedCount == 0 {
		if ba, ok := s.svc.App.(BeginFinisher); ok {
			// BeginTxn may block arbitrarily, for example by obtaining a
			// semaphore to constrain maximum concurrency.
			if err := ba.BeginTxn(s, s.store); err != nil {
				return errors.WithMessage(err, "app.BeginTxn")
			}
		}
		txn.beganAt = txn.timer.Now()
		txn.timer.Reset(txn.minDur)
		s.clock.Update(txn.beganAt)
	}
	txn.consumedCount++
	txn.consumedBytes += (env.End - env.Begin)

	if err := s.svc.App.ConsumeMessage(s, s.store, env, s.publisher); err != nil {
		return errors.WithMessage(err, "app.ConsumeMessage")
	}
	return nil
}

func txnTick(_ *shard, txn *transaction, tick time.Time) error {
	if tick.Before(txn.beganAt.Add(txn.minDur)) {
		panic("unexpected tick")
	}
	if txn.minDur == -1 && tick.Before(txn.beganAt.Add(txn.maxDur)) {
		panic("unexpected tick")
	}

	txn.minDur = -1 // Mark as completed.

	if tick.Before(txn.beganAt.Add(txn.maxDur)) {
		txn.timer.Reset(txn.beganAt.Add(txn.maxDur).Sub(tick))
	} else {
		txn.maxDur = -1  // Mark as completed.
		txn.readCh = nil // Stop reading messages.

		if txn.barrierCh != nil {
			// If the prior transaction hasn't completed, we must wait until it
			// does but are now "stalled" as we cannot also read messages.
			txn.stalledAt = tick
		}
	}
	return nil
}

func txnBarrierResolved(s *shard, txn, prev *transaction) error {
	if !prev.ackedAt.IsZero() {
		panic("unexpected txnBarrierResolved")
	}
	var now = txn.timer.Now()

	if prev.committedAt.IsZero() {
		if prev.commitBarrier.Err() != nil {
			return errors.WithMessage(prev.commitBarrier.Err(), "store.StartCommit")
		}
		prev.committedAt = now
	}

	// Find the next ACK append that hasn't finished.
	// It must resolve before |prev| is considered complete.
	for ack := range prev.acks {
		select {
		case <-ack.Done(): // Already resolved?
			if ack.Err() != nil {
				return errors.WithMessage(ack.Err(), "prev.ack")
			}
		default:
			txn.barrierCh = ack.Done()
			return nil
		}
	}

	// All barriers have finished. |prev| transaction is complete.
	txn.barrierCh = nil
	prev.ackedAt = now
	recordMetrics(s, prev)

	// Update shard |progress| with results of |prev| transaction.
	s.progress.Lock()
	defer s.progress.Unlock()

	for j, o := range prev.readThrough {
		s.progress.readThrough[j] = o
	}
	for op := range prev.acks {
		if ack, ok := op.(*client.AsyncAppend); ok {
			s.progress.publishAt[ack.Request().Journal] = ack.Response().Commit.End
		}
	}

	close(s.progress.signalCh)
	s.progress.signalCh = make(chan struct{})

	return nil
}

func txnStartCommit(s *shard, txn *transaction) (pc.Checkpoint, error) {
	if txn.prepareBeganAt = txn.timer.Now(); txn.stalledAt.IsZero() {
		txn.stalledAt = txn.prepareBeganAt // We spent no time stalled.
	}

	var err = s.svc.App.FinalizeTxn(s, s.store, s.publisher)
	if err != nil {
		return pc.Checkpoint{}, errors.WithMessage(err, "app.FinalizeTxn")
	}

	var bca = pc.BuildCheckpointArgs{
		ReadThrough:    txn.readThrough,
		ProducerStates: s.sequencer.ProducerStates(messageSequencerPruneHorizon),
	}
	if bca.AckIntents, err = s.publisher.BuildAckIntents(); err != nil {
		return pc.Checkpoint{}, errors.WithMessage(err, "publisher.BuildAckIntents")
	}
	var cp = pc.BuildCheckpoint(bca)

	// Collect pending journal writes before we start to commit. We'll require
	// that the Store wait on all |waitFor| operations before it commits, to
	// ensure that writes driven by messages of the transaction have completed
	// before we could possibly persist their acknowledgments, or commit offsets
	// which step past those messages.
	var waitFor = s.ajc.PendingExcept(s.recovery.log)

	txn.commitBarrier = s.store.StartCommit(s, cp, waitFor)
	txn.prepareDoneAt = txn.timer.Now()

	return cp, nil
}

func txnAcknowledge(s *shard, txn *transaction, cp pc.Checkpoint) error {
	// The transaction is committed when |commitBarrier| resolves. At that
	// point any reader of the log must see our checkpoint.
	var waitFor = OpFutures{txn.commitBarrier: {}}

	// After |commitBarrier| resolves, publish our ACKs to unblock downstream
	// readers to process messages we published during this transaction. If we
	// fault after |commitBarrier| resolves and before we send all ACKs, the
	// next process to recover from our log will re-send those ACKs, so they're
	// guaranteed to eventually be received.
	for journal, ack := range cp.AckIntents {
		var aa = s.ajc.StartAppend(pb.AppendRequest{Journal: journal}, waitFor)
		_, _ = aa.Writer().Write(ack)

		if err := aa.Release(); err != nil {
			return errors.WithMessage(err, "writing ACK")
		}
		txn.acks[aa] = struct{}{}
	}
	return nil
}

// recordMetrics of a fully completed transaction.
func recordMetrics(s *shard, txn *transaction) {

	// DEPRECATED metrics, to be removed:
	txCountTotal.Inc()
	txMessagesTotal.Add(float64(txn.consumedCount))

	txSecondsTotal.Add(txn.committedAt.Sub(txn.beganAt).Seconds())
	txConsumeSecondsTotal.Add(txn.stalledAt.Sub(txn.beganAt).Seconds())
	txStalledSecondsTotal.Add(txn.prepareBeganAt.Sub(txn.stalledAt).Seconds())
	txFlushSecondsTotal.Add(txn.prepareDoneAt.Sub(txn.prepareBeganAt).Seconds())
	txSyncSecondsTotal.Add(txn.committedAt.Sub(txn.prepareDoneAt).Seconds())
	// End DEPRECATED metrics.

	shardTxnTotal.WithLabelValues(s.FQN()).Inc()
	shardReadMsgsTotal.WithLabelValues(s.FQN()).Add(float64(txn.consumedCount))
	shardReadBytesTotal.WithLabelValues(s.FQN()).Add(float64(txn.consumedBytes))
	for journal, offset := range txn.readThrough {
		shardReadHeadGauge.
			WithLabelValues(s.FQN(), journal.String()).
			Set(float64(offset))
	}

	var (
		durNotRunning    = txn.beganAt.Sub(txn.prevPrepareDoneAt)
		durConsuming     = txn.stalledAt.Sub(txn.beganAt)
		durStalled       = txn.prepareBeganAt.Sub(txn.stalledAt)
		durPreparing     = txn.prepareDoneAt.Sub(txn.prepareBeganAt)
		durCommitting    = txn.committedAt.Sub(txn.prepareDoneAt)
		durAcknowledging = txn.ackedAt.Sub(txn.committedAt)
	)
	// Phases which run synchronously within the transaction loop.
	shardTxnPhaseSecondsTotal.WithLabelValues(s.FQN(), "10-not-running", "sync").Add(durNotRunning.Seconds())
	shardTxnPhaseSecondsTotal.WithLabelValues(s.FQN(), "20-consuming", "sync").Add(durConsuming.Seconds())
	shardTxnPhaseSecondsTotal.WithLabelValues(s.FQN(), "30-stalled", "sync").Add(durStalled.Seconds())
	shardTxnPhaseSecondsTotal.WithLabelValues(s.FQN(), "40-preparing", "sync").Add(durPreparing.Seconds())
	// Phases which run asynchronously, in parallel with later transaction.
	shardTxnPhaseSecondsTotal.WithLabelValues(s.FQN(), "50-committing", "async").Add(durCommitting.Seconds())
	shardTxnPhaseSecondsTotal.WithLabelValues(s.FQN(), "60-acknowledging", "async").Add(durAcknowledging.Seconds())
}

// txnTimer is a time.Timer which can be mocked within unit tests.
type txnTimer struct {
	C     <-chan time.Time
	Reset func(time.Duration) bool
	Stop  func() bool
	Now   func() time.Time
}
