package consumer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/labels"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	"github.com/LiveRamp/gazette/v2/pkg/metrics"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/recoverylog"
	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// playLog fetches current shard hints and plays them back into a temporary directory using the Player.
func playLog(shard Shard, pl *recoverylog.Player, etcd *clientv3.Client) error {
	if dir, err := ioutil.TempDir("", shard.Spec().Id.String()+"-"); err != nil {
		return extendErr(err, "creating shard working directory")
	} else if h, err := fetchHints(shard.Context(), shard.Spec(), etcd); err != nil {
		return extendErr(err, "fetching FSM hints")
	} else if logSpec, err := fetchJournalSpec(shard.Context(), pickFirstHints(h).Log, shard.JournalClient()); err != nil {
		return extendErr(err, "fetching JournalSpec")
	} else if ct := logSpec.LabelSet.ValueOf(labels.ContentType); ct != labels.ContentType_RecoveryLog {
		return errors.Errorf("expected label %s value %s (got %v)", labels.ContentType, labels.ContentType_RecoveryLog, ct)
	} else if err = pl.Play(shard.Context(), pickFirstHints(h), dir, shard.JournalClient()); err != nil {
		return extendErr(err, "playing log %s", pickFirstHints(h).Log)
	}
	return nil
}

// completePlayback injects a new AuthorID into the log to complete playback,
// stores recovered hints, initializes an Application Store, and returns
// offsets at which journal consumption should continue.
func completePlayback(shard Shard, app Application, pl *recoverylog.Player,
	etcd *clientv3.Client) (Store, map[pb.Journal]int64, error) {

	var author, err = recoverylog.NewRandomAuthorID()
	if err != nil {
		return nil, nil, extendErr(err, "generating Author")
	}
	// Ask |pl| to inject a hand-off to our generated |author|, so that other
	// tailing readers will apply our write operations over those of a previous
	// recorder which may still be shutting down.
	pl.InjectHandoff(author)

	select {
	case <-pl.Done():
		// Pass.
	case <-shard.Context().Done():
		return nil, nil, shard.Context().Err()
	}

	if pl.FSM == nil {
		return nil, nil, errors.Errorf("completePlayback aborting due to Play failure")
	}

	// We've completed log playback, and we're likely the most recent shard
	// primary to do so. Store our recovered hints.
	if err = storeRecoveredHints(shard, pl.FSM.BuildHints(), etcd); err != nil {
		return nil, nil, extendErr(err, "storingRecoveredHints")
	}
	// Initialize the store.
	var recorder = recoverylog.NewRecorder(pl.FSM, author, pl.Dir, shard.JournalClient())
	var store Store
	var offsets map[pb.Journal]int64

	if store, err = app.NewStore(shard, pl.Dir, recorder); err != nil {
		return nil, nil, extendErr(err, "initializing store")
	} else if offsets, err = store.FetchJournalOffsets(); err != nil {
		return nil, nil, extendErr(err, "fetching journal offsets from store")
	}

	// Lower-bound each source to its ShardSpec.Source.MinOffset.
	for _, src := range shard.Spec().Sources {
		if offsets[src.Journal] < src.MinOffset {
			offsets[src.Journal] = src.MinOffset
		}
	}
	return store, offsets, nil
}

// pumpMessages reads and decodes messages from a Journal & offset into the provided channel.
func pumpMessages(shard Shard, app Application, journal pb.Journal, offset int64, msgCh chan<- message.Envelope) error {
	var spec, err = fetchJournalSpec(shard.Context(), journal, shard.JournalClient())
	if err != nil {
		return extendErr(err, "fetching JournalSpec")
	}
	framing, err := message.FramingByContentType(spec.LabelSet.ValueOf(labels.ContentType))
	if err != nil {
		return extendErr(err, "determining framing (%s)", journal)
	}

	var rr = client.NewRetryReader(shard.Context(), shard.JournalClient(), pb.ReadRequest{
		Journal:    journal,
		Offset:     offset,
		Block:      true,
		DoNotProxy: !shard.JournalClient().IsNoopRouter(),
	})
	var br = bufio.NewReader(rr)

	for next := offset; ; offset = next {
		var frame []byte
		var msg message.Message

		if frame, err = framing.Unpack(br); err != nil {
			return extendErr(err, "unpacking frame (%s:%d)", spec.Name, offset)
		}
		next = rr.AdjustedOffset(br)

		if msg, err = app.NewMessage(spec); err != nil {
			return extendErr(err, "NewMessage (%s)", journal)
		} else if err = framing.Unmarshal(frame, msg); err != nil {
			log.WithFields(log.Fields{"journal": journal, "offset": offset, "err": err}).
				Error("failed to unmarshal message")
			continue
		}

		select {
		case msgCh <- message.Envelope{
			JournalSpec: spec,
			Fragment:    rr.Reader.Response.Fragment,
			NextOffset:  next,
			Message:     msg,
		}: // Pass.
		case <-shard.Context().Done():
			return extendErr(shard.Context().Err(), "sending msg (%s:%d)", spec.Name, offset)
		}
		metrics.GazetteConsumerBytesConsumedTotal.Add(float64(next - offset))
	}
}

// consumeMessages runs consumer transactions, consuming from the provided
// |msgCh| and, when notified by |hintsCh|, occasionally stores recorded FSMHints.
func consumeMessages(shard Shard, store Store, app Application, etcd *clientv3.Client,
	msgCh <-chan message.Envelope, hintsCh <-chan time.Time) (err error) {

	// Supply an idle timer for txnStep's use in timing transaction durations.
	var realTimer = time.NewTimer(0)
	if !realTimer.Stop() {
		<-realTimer.C
	}
	var timer = txnTimer{
		C:     realTimer.C,
		Reset: realTimer.Reset,
		Stop:  realTimer.Stop,
	}
	var txn, prior transaction

	for {
		select {
		case <-hintsCh:
			var hints recoverylog.FSMHints
			if hints, err = store.Recorder().BuildHints(); err == nil {
				err = storeRecordedHints(shard, hints, etcd)
			}
			if err != nil {
				err = extendErr(err, "storeRecordedHints")
				return
			}
		default:
			// Pass.
		}

		var spec = shard.Spec()
		txn.minDur, txn.maxDur = spec.MinTxnDuration, spec.MaxTxnDuration
		txn.msgCh = msgCh
		txn.offsets = make(map[pb.Journal]int64)

		// Run the transaction until completion or error.
		for done := false; !done && err == nil; done, err = txnStep(&txn, &prior, shard, store, app, timer) {
		}
		if err != nil {
			err = extendErr(err, "txnStep")
		}
		if ba, ok := app.(BeginFinisher); ok && txn.msgCount != 0 {
			if finishErr := ba.FinishTxn(shard, store, err); err == nil && finishErr != nil {
				err = extendErr(finishErr, "FinishTxn")
			}
		}
		if err != nil {
			return
		}

		recordMetrics(&prior)
		prior, txn = txn, transaction{doneCh: txn.barrier.Done()}
	}
}

// fetchJournalSpec retrieves the current JournalSpec.
func fetchJournalSpec(ctx context.Context, name pb.Journal, journals pb.JournalClient) (spec *pb.JournalSpec, err error) {
	var lr *pb.ListResponse
	lr, err = client.ListAllJournals(ctx, journals, pb.ListRequest{
		Selector: pb.LabelSelector{
			Include: pb.LabelSet{Labels: []pb.Label{{Name: "name", Value: name.String()}}},
		},
	})
	if err == nil && len(lr.Journals) == 0 {
		err = errors.Errorf("named journal does not exist (%s)", name)
	}
	if err == nil {
		spec = &lr.Journals[0].Spec
	}
	return
}

type fetchedHints struct {
	spec    *ShardSpec
	txnResp *clientv3.TxnResponse
	hints   []*recoverylog.FSMHints
}

// pickFirstHints retrieves the first hints from |f|. If there are no primary
// hints available the most recent backup hints will be returned. If there are
// no hints available an empty set of hints is returned.
func pickFirstHints(f fetchedHints) recoverylog.FSMHints {
	for _, currHints := range f.hints {
		if currHints == nil {
			continue
		}
		return *currHints
	}

	return recoverylog.FSMHints{Log: f.spec.RecoveryLog()}
}

// fetchHints retrieves and decodes all FSMHints for the ShardSpec.
// Nil values will be returned where hint values have not been written. It also
// returns a TxnResponse holding each of the hints values, which can be used for
// transactional updates of hints.
func fetchHints(ctx context.Context, spec *ShardSpec, etcd *clientv3.Client) (out fetchedHints, err error) {
	var ops = []clientv3.Op{clientv3.OpGet(spec.HintPrimaryKey())}
	for _, hk := range spec.HintBackupKeys() {
		ops = append(ops, clientv3.OpGet(hk))
	}

	out.spec = spec
	if out.txnResp, err = etcd.Txn(ctx).If().Then(ops...).Commit(); err != nil {
		err = extendErr(err, "fetching ShardSpec.HintKeys")
		return
	}

	for i := range out.txnResp.Responses {
		var currHints recoverylog.FSMHints
		if kvs := out.txnResp.Responses[i].GetResponseRange().Kvs; len(kvs) == 0 {
			out.hints = append(out.hints, nil)
			continue
		} else if err = json.Unmarshal(kvs[0].Value, &currHints); err != nil {
			err = extendErr(err, "unmarshal FSMHints")
		} else if _, err = recoverylog.NewFSM(currHints); err != nil { // Validate hints.
			err = extendErr(err, "validating FSMHints")
		} else if currHints.Log != spec.RecoveryLog() {
			err = errors.Errorf("recovered hints recovery log doesn't match ShardSpec.RecoveryLog (%s vs %s)",
				currHints.Log, spec.RecoveryLog())
		}
		if err != nil {
			return
		}
		out.hints = append(out.hints, &currHints)
	}
	return
}

// storeRecordedHints writes FSMHints into the primary hint key of the spec.
func storeRecordedHints(shard Shard, hints recoverylog.FSMHints, etcd *clientv3.Client) (err error) {
	var val []byte
	if val, err = json.Marshal(hints); err != nil {
		err = extendErr(err, "marshal FSMHints")
		return
	}
	var asn = shard.Assignment()

	if _, err = etcd.Txn(shard.Context()).
		// Verify our Assignment is still in effect (eg, we're still primary), then write |hints| to HintPrimaryKey.
		// Compare CreateRevision to allow for a raced ReplicaState update.
		If(clientv3.Compare(clientv3.CreateRevision(string(asn.Raw.Key)), "=", asn.Raw.CreateRevision)).
		Then(clientv3.OpPut(shard.Spec().HintPrimaryKey(), string(val))).
		Commit(); err != nil {
		err = extendErr(err, "storing recorded FSMHints")
	}
	return
}

// storeRecoveredHints writes the FSMHints into the first backup hint key of the spec,
// rotating hints previously stored under that key to the second backup hint key,
// and so on as a single transaction.
func storeRecoveredHints(shard Shard, hints recoverylog.FSMHints, etcd *clientv3.Client) (err error) {
	var (
		spec    = shard.Spec()
		asn     = shard.Assignment()
		backups = shard.Spec().HintBackupKeys()
		h       fetchedHints
	)
	if h, err = fetchHints(shard.Context(), spec, etcd); err != nil {
		return
	}

	// |hints| is serialized and written to backups[1]. In the same txn,
	// rotate the current value at backups[1] => backups[2], and so on.
	var val []byte
	if val, err = json.Marshal(hints); err != nil {
		return
	}

	var cmp []clientv3.Cmp
	var ops []clientv3.Op

	// The txn responses returned from fetchHints are structured such that the first response will
	// be the primary response and the subsequent responses are the backup responses, this slice
	// represents just the backup responses.
	var backupResponses = h.txnResp.Responses[1:]
	// Verify our Assignment is still in effect (eg, we're still primary).
	cmp = append(cmp, clientv3.Compare(clientv3.CreateRevision(string(asn.Raw.Key)),
		"=", asn.Raw.CreateRevision))

	for i := 0; i != len(backups) && val != nil; i++ {
		ops = append(ops, clientv3.OpPut(backups[i], string(val)))

		if kvs := backupResponses[i].GetResponseRange().Kvs; len(kvs) == 0 {
			// Verify there is still no current key/value at this hints key slot.
			cmp = append(cmp, clientv3.Compare(clientv3.ModRevision(backups[i]), "=", 0))
			val = nil
		} else {
			// Verify the key/value at this hints key slot is unchanged.
			// Retain its value to rotate into the next slot (if one exists).
			cmp = append(cmp, clientv3.Compare(clientv3.ModRevision(backups[i]), "=", kvs[0].ModRevision))
			val = kvs[0].Value
		}
	}
	if _, err = etcd.Txn(shard.Context()).If(cmp...).Then(ops...).Commit(); err != nil {
		err = extendErr(err, "storing recovered FSMHints")
	}
	return
}

// transaction models state and metrics used in the execution of a consumer transaction.
type transaction struct {
	barrier        *client.AsyncAppend     // Write barrier of the txn at commit.
	minDur, maxDur time.Duration           // Minimum and maximum durations. Marked as -1 when elapsed.
	msgCh          <-chan message.Envelope // Message source. Nil'd upon reaching |maxDur|.
	msgCount       int                     // Number of messages batched into this transaction.
	offsets        map[pb.Journal]int64    // End (exclusive) journal offsets of the transaction.
	doneCh         <-chan struct{}         // DoneCh of prior transaction barrier.

	beganAt     time.Time // Time at which transaction began.
	stalledAt   time.Time // Time at which processing stalled while waiting on IO.
	flushedAt   time.Time // Time at which flush began.
	committedAt time.Time // Time at which commit began.
	syncedAt    time.Time // Time at which txn |barrier| resolved.
}

// txnTimer is a time.Timer which can be mocked within unit tests.
type txnTimer struct {
	C     <-chan time.Time
	Reset func(time.Duration) bool
	Stop  func() bool
}

// txnStep progresses a consumer transaction by a single step. If the transaction
// is complete, it returns done=true. Otherwise, txnStep should be called again
// to continue making progress on the transaction.
func txnStep(txn, prior *transaction, shard Shard, store Store, app Application, timer txnTimer) (done bool, err error) {

	// If the minimum batching duration hasn't elapsed *or* the prior transaction
	// barrier hasn't completed, continue performing blocking reads of messages.
	if txn.msgCount == 0 || txn.minDur != -1 || txn.doneCh != nil {

		select {
		case msg := <-txn.msgCh:
			if txn.msgCount == 0 {
				if ba, ok := app.(BeginFinisher); ok {
					// BeginTxn may block arbitrarily.
					if err = ba.BeginTxn(shard, store); err != nil {
						err = extendErr(err, "app.BeginTxn")
						return
					}
				}
				txn.beganAt = timeNow()
				timer.Reset(txn.minDur)
			}
			txn.msgCount++
			txn.offsets[msg.JournalSpec.Name] = msg.NextOffset

			if err = app.ConsumeMessage(shard, store, msg); err != nil {
				err = extendErr(err, "app.ConsumeMessage")
			}
			return

		case tick := <-timer.C:
			if tick.Before(txn.beganAt.Add(txn.minDur)) {
				panic("unexpected tick")
			}
			txn.minDur = -1 // Mark as completed.

			if tick.Before(txn.beganAt.Add(txn.maxDur)) {
				timer.Reset(txn.beganAt.Add(txn.maxDur).Sub(tick))
			} else {
				txn.maxDur = -1           // Mark as completed.
				txn.msgCh = nil           // Stop reading messages.
				txn.stalledAt = timeNow() // We're stalled waiting for prior txn IO.
			}
			return

		case _ = <-txn.doneCh:
			prior.syncedAt = timeNow()
			txn.doneCh = nil
			return

		case _ = <-shard.Context().Done():
			err = shard.Context().Err()
			return
		}

		panic("not reached")
	}

	// Continue reading messages so long as we do not block or reach |maxDur|.
	select {
	case msg := <-txn.msgCh:
		txn.msgCount++
		txn.offsets[msg.JournalSpec.Name] = msg.NextOffset

		if err = app.ConsumeMessage(shard, store, msg); err != nil {
			err = extendErr(err, "app.ConsumeMessage")
		}
		return

	case tick := <-timer.C:
		if tick.Before(txn.beganAt.Add(txn.maxDur)) {
			panic("unexpected tick")
		}

		txn.maxDur = -1 // Mark as completed.
		txn.msgCh = nil // Stop reading messages.
		return

	case _ = <-shard.Context().Done():
		err = shard.Context().Err()
		return

	default:
		// |msgCh| stalled. Fallthrough to complete the transaction.
	}

	if txn.flushedAt = timeNow(); txn.stalledAt.IsZero() {
		txn.stalledAt = txn.flushedAt // We spent no time stalled.
	}
	if err = app.FinalizeTxn(shard, store); err != nil {
		err = extendErr(err, "app.FinalizeTxn")
		return
	}

	// Inject a strong write barrier which resolves only after pending writes
	// to all journals have completed. We do this before store.Flush to ensure
	// that writes driven by transaction messages have completed before we
	// persist updated offsets which step past those messages.
	store.Recorder().StrongBarrier()

	if err = store.Flush(txn.offsets); err != nil {
		err = extendErr(err, "store.Flush")
		return
	}
	txn.barrier = store.Recorder().WeakBarrier()
	txn.committedAt = timeNow()

	// If the timer is still running, stop and drain it.
	if txn.maxDur != -1 && !timer.Stop() {
		<-timer.C
	}

	done = true
	return
}

// recordMetrics of a fully completed transaction.
func recordMetrics(txn *transaction) {
	metrics.GazetteConsumerTxCountTotal.Inc()
	metrics.GazetteConsumerTxMessagesTotal.Add(float64(txn.msgCount))

	metrics.GazetteConsumerTxSecondsTotal.Add(txn.syncedAt.Sub(txn.beganAt).Seconds())
	metrics.GazetteConsumerTxConsumeSecondsTotal.Add(txn.stalledAt.Sub(txn.beganAt).Seconds())
	metrics.GazetteConsumerTxStalledSecondsTotal.Add(txn.flushedAt.Sub(txn.stalledAt).Seconds())
	metrics.GazetteConsumerTxFlushSecondsTotal.Add(txn.committedAt.Sub(txn.flushedAt).Seconds())
	metrics.GazetteConsumerTxSyncSecondsTotal.Add(txn.syncedAt.Sub(txn.committedAt).Seconds())
}

func extendErr(err error, mFmt string, args ...interface{}) error {
	if err == nil {
		panic("expected error")
	} else if err == context.Canceled || err == context.DeadlineExceeded {
		return err
	} else if _, ok := err.(interface{ StackTrace() errors.StackTrace }); ok {
		// Avoid attaching another errors.StackTrace if one is already present.
		return errors.WithMessage(err, fmt.Sprintf(mFmt, args...))
	} else {
		// Use Wrapf to simultaneously attach |mFmt| and the current stack trace.
		return errors.Wrapf(err, mFmt, args...)
	}
}

var timeNow = time.Now
