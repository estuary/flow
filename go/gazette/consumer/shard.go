package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/message"
)

const (
	// Frequency with which current FSM hints are written to Etcd.
	storeHintsInterval = 5 * time.Minute
	// Size of the channel used between message decode & consumption. Needs to
	// be rather large, to minimize processing stalls. The current value will
	// tolerate a data delay of up to 82ms @ 100K messages / sec without stalling.
	messageBufferSize = 1 << 13 // 8192.
	// Size of the ring buffer used by message.Sequencer.
	messageRingSize = messageBufferSize
	// Maximum interval between the newest and an older producer within a journal,
	// before the message sequencer will prune the older producer state.
	messageSequencerPruneHorizon = time.Hour * 24
)

type shard struct {
	svc          *Service                  // Service which owns the shard.
	ctx          context.Context           // Context tied to shard Assignment lifetime.
	cancel       context.CancelFunc        // Invoked when Assignment is lost.
	ajc          client.AsyncJournalClient // Async client using shard context.
	store        Store                     // Store of the shard.
	storeReadyCh chan struct{}             // Closed when |store| is ready.
	sequencer    *message.Sequencer        // Sequencer of uncommitted messages.
	publisher    *message.Publisher        // Publisher of messages from this shard.
	clock        message.Clock             // Clock which sequences messages from this shard.
	wg           sync.WaitGroup            // Synchronizes over references to the shard.

	// recovery of the shard from its log (if applicable).
	recovery struct {
		log      pb.Journal            // Point-in-time snapshot of ShardSpec.RecoveryLog().
		player   *recoverylog.Player   // Player of shard's recovery log, if used.
		recorder *recoverylog.Recorder // Recorder of shard's recovery log.
	}
	// resolved state as-of the most recent shard transition().
	resolved struct {
		fqn           string            // Fully qualified Etcd key of this ShardSpec.
		spec          *pc.ShardSpec     // Last transitioned ShardSpec.
		assignment    keyspace.KeyValue // Last transitioned shard Assignment.
		*sync.RWMutex                   // Guards |spec| and |assignment| (is actually KS.Mu).
	}
	// progress as-of the most recent completed transaction.
	progress struct {
		readThrough pb.Offsets    // Offsets read through.
		publishAt   pb.Offsets    // ACKs started to each journal.
		signalCh    chan struct{} // Signalled on update to progress.
		sync.Mutex                // Guards |progress|.
	}
}

func newShard(svc *Service, item keyspace.KeyValue) *shard {
	var ctx, cancel = context.WithCancel(context.Background())

	var s = &shard{
		svc:          svc,
		ctx:          ctx,
		cancel:       cancel,
		ajc:          client.NewAppendService(ctx, svc.Journals),
		storeReadyCh: make(chan struct{}),
	}
	s.resolved.fqn = string(item.Raw.Key)
	s.resolved.spec = item.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)
	s.resolved.RWMutex = &svc.State.KS.Mu

	// We grab this value only once (since RecoveryLog()'s value may change in
	// the future). Elsewhere we use |s.recovery.log|.
	if rl := s.resolved.spec.RecoveryLog(); rl != "" {
		s.recovery.log = rl
		s.recovery.player = recoverylog.NewPlayer()
	}
	// Initialize |progress|. After completeRecovery(), Resolve() may begin
	// returning this shard and/or test against |progress.readThrough|.
	// From here on out, |progress| is only updated by txnBarrier().
	s.progress.signalCh = make(chan struct{})
	s.progress.readThrough = make(pb.Offsets)
	s.progress.publishAt = make(pb.Offsets)
	for _, src := range s.resolved.spec.Sources {
		s.progress.readThrough[src.Journal] = 0
	}
	return s
}

func (s *shard) Context() context.Context                 { return s.ctx }
func (s *shard) FQN() string                              { return s.resolved.fqn }
func (s *shard) JournalClient() client.AsyncJournalClient { return s.ajc }

func (s *shard) Spec() *pc.ShardSpec {
	s.resolved.RLock()
	defer s.resolved.RUnlock()
	return s.resolved.spec
}

// Assignment of the ShardSpec to the local ConsumerSpec, which motivates this Replica.
func (s *shard) Assignment() keyspace.KeyValue {
	s.resolved.RLock()
	defer s.resolved.RUnlock()
	return s.resolved.assignment
}

// Progress of this Shard in processing consumer transactions.
func (s *shard) Progress() (readThrough, publishAt pb.Offsets) {
	s.progress.Lock()
	defer s.progress.Unlock()
	return s.progress.readThrough.Copy(), s.progress.publishAt.Copy()
}

// transition is called by Resolver with the current ShardSpec and allocator
// Assignment of the replica, and transitions the Replica from its initial
// state to a standby or primary state. |spec| and |assignment| must always be
// non-zero-valued, and r.Mu.Lock must be held.
var transition = func(s *shard, item, assignment keyspace.KeyValue) {
	var (
		isInit   = s.resolved.assignment.Raw.CreateRevision == 0
		isSlot0  = assignment.Decoded.(allocator.Assignment).Slot == 0
		wasSlot0 = !isInit && s.resolved.assignment.Decoded.(allocator.Assignment).Slot == 0
	)

	if isInit && !isSlot0 {
		s.wg.Add(1) // Transition initial => standby.
		go serveStandby(s)
	} else if isInit && isSlot0 {
		s.wg.Add(2) // Transition initial => primary.
		go serveStandby(s)
		go servePrimary(s)
	} else if !isInit && isSlot0 && !wasSlot0 {
		s.wg.Add(1) // Transition standby => primary.
		go servePrimary(s)
	}
	s.resolved.spec = item.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)
	s.resolved.assignment = assignment
}

// serveStandby recovers and tails the shard recovery log, until the Replica is
// cancelled or promoted to primary.
func serveStandby(s *shard) (err error) {
	// Defer a trap which logs and updates Etcd status based on exit error.
	defer func() {
		if err != nil && s.ctx.Err() == nil {
			log.WithFields(log.Fields{"err": err, "shard": s.FQN()}).Error("serveStandby failed")

			updateStatusWithRetry(s, pc.ReplicaStatus{
				Code:   pc.ReplicaStatus_FAILED,
				Errors: []string{err.Error()},
			})
		}
		s.wg.Done()
	}()

	// If there is no recovery log, serving as standby is a no-op. Advertise
	// immediate ability to transition to PRIMARY.
	if s.recovery.log == "" {
		updateStatusWithRetry(s, pc.ReplicaStatus{Code: pc.ReplicaStatus_STANDBY})
		return nil
	}

	go func() {
		updateStatusWithRetry(s, pc.ReplicaStatus{Code: pc.ReplicaStatus_BACKFILL})

		// When the player completes back-fill, advertise that we're tailing the log
		// and ready to transition to PRIMARY.
		select {
		case <-s.Context().Done():
			return
		case <-s.recovery.player.Tailing():
			log.WithFields(log.Fields{
				"log": s.recovery.log,
				"id":  s.Spec().Id,
			}).Info("now tailing live log")

			updateStatusWithRetry(s, pc.ReplicaStatus{Code: pc.ReplicaStatus_STANDBY})
		}
	}()

	return errors.WithMessage(beginRecovery(s), "beginRecovery")
}

// servePrimary completes playback of the recovery log,
// performs initialization, and runs consumer transactions.
func servePrimary(s *shard) (err error) {
	// Defer a trap which logs and updates Etcd status based on exit error.
	defer func() {
		if err != nil && s.ctx.Err() == nil {
			log.WithFields(log.Fields{"err": err, "shard": s.FQN()}).Error("servePrimary failed")

			updateStatusWithRetry(s, pc.ReplicaStatus{
				Code:   pc.ReplicaStatus_FAILED,
				Errors: []string{err.Error()},
			})
		}
		s.wg.Done()
	}()

	log.WithFields(log.Fields{
		"id":  s.Spec().Id,
		"log": s.recovery.log,
	}).Info("promoted to primary")

	// Complete recovery log playback (if applicable) and restore the last
	// transaction checkpoint.
	var cp pc.Checkpoint
	if cp, err = completeRecovery(s); err != nil {
		return errors.WithMessage(err, "completeRecovery")
	}
	updateStatusWithRetry(s, pc.ReplicaStatus{Code: pc.ReplicaStatus_PRIMARY})

	// If the shard store records to a log, arrange to periodically write FSMHints.
	var hintsCh <-chan time.Time
	if s.recovery.log != "" {
		var t = time.NewTicker(storeHintsInterval)
		defer t.Stop()
		hintsCh = t.C
	}
	// Run consumer transactions until an error occurs (such as context.Cancelled).
	for {
		var msgCh = make(chan EnvelopeOrError, messageBufferSize)

		if mp, ok := s.svc.App.(MessageProducer); ok {
			mp.StartReadingMessages(s, s.store, cp, msgCh)
		} else {
			startReadingMessages(s, cp, msgCh)
		}

		if err = runTransactions(s, cp, msgCh, hintsCh); err != nil {
			return errors.WithMessage(err, "runTransactions")
		}

		// Restore a checkpoint from the shard Store and re-start processing.
		// This may be the same Checkpoint we last committed, but it may not be:
		// that's up to the application.

		cp, err = s.store.RestoreCheckpoint(s)
		if err != nil {
			return errors.WithMessage(err, "restart store.RestoreCheckpoint")
		}
		s.sequencer = message.NewSequencer(pc.FlattenProducerStates(cp), messageRingSize)
	}
}

// waitAndTearDown waits for all outstanding goroutines which are accessing
// the shard, and for all pending Appends to complete, and then tears down
// the shard Store.
func waitAndTearDown(s *shard, done func()) {
	s.wg.Wait()

	if s.store != nil {
		s.store.Destroy()
	}
	done()
}

// updateStatus publishes |status| under the Shard Assignment key in a checked
// transaction. An existing ReplicaStatus is reduced into |status| prior to update.
func updateStatus(s *shard, status pc.ReplicaStatus) error {
	var asn = s.Assignment()
	status.Reduce(asn.Decoded.(allocator.Assignment).AssignmentValue.(*pc.ReplicaStatus))

	var key = string(asn.Raw.Key)
	var val = status.MarshalString()

	var resp, err = s.svc.Etcd.Txn(s.ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", asn.Raw.ModRevision)).
		Then(clientv3.OpPut(key, val, clientv3.WithIgnoreLease())).
		Commit()

	if err == nil && !resp.Succeeded {
		err = errors.Errorf("transaction failed")
	}
	if err == nil {
		// Block until the update is observed in the KeySpace.
		s.svc.State.KS.Mu.RLock()
		_ = s.svc.State.KS.WaitForRevision(s.ctx, resp.Header.Revision)
		s.svc.State.KS.Mu.RUnlock()
	}
	return err
}

// updateStatusWithRetry wraps updateStatus with retry behavior.
func updateStatusWithRetry(s *shard, status pc.ReplicaStatus) {
	for attempt := 0; true; attempt++ {
		if s.ctx.Err() != nil {
			return // Already cancelled.
		}
		select {
		case <-s.ctx.Done():
			return // Cancelled while waiting to retry.
		case <-time.After(backoff(attempt)):
			// Pass.
		}

		var err = updateStatus(s, status)
		if err == nil {
			return
		}

		if attempt != 0 {
			log.WithFields(log.Fields{"err": err, "attempt": attempt}).
				Warn("failed to advertise Etcd shard status (will retry)")
		}
	}
}

// EnvelopeOrError composes an Envelope with its read error.
type EnvelopeOrError struct {
	message.Envelope
	Error error
}

// startReadingMessages from source journals into the provided channel.
func startReadingMessages(s *shard, cp pc.Checkpoint, ch chan<- EnvelopeOrError) {
	for _, src := range s.Spec().Sources {

		// Lower-bound checkpoint offset to the ShardSpec.Source.MinOffset.
		var offset int64
		if s := cp.Sources[src.Journal]; s != nil {
			offset = s.ReadThrough
		}
		if offset < src.MinOffset {
			offset = src.MinOffset
		}

		var it = message.NewReadUncommittedIter(
			client.NewRetryReader(s.ctx, s.ajc, pb.ReadRequest{
				Journal:    src.Journal,
				Offset:     offset,
				Block:      true,
				DoNotProxy: !s.ajc.IsNoopRouter(),
			}), s.svc.App.NewMessage)

		s.wg.Add(1)
		go func(it message.Iterator) {
			defer s.wg.Done()

			var v EnvelopeOrError
			for v.Error == nil {
				v.Envelope, v.Error = it.Next()

				// Attempt to place |v| even if context is cancelled,
				// but don't hang if we're cancelled and buffer is full.
				select {
				case ch <- v:
				default:
					select {
					case ch <- v:
					case <-s.ctx.Done():
						return
					}
				}
			}
		}(it)
	}
}

func backoff(attempt int) time.Duration {
	// The choices of backoff time reflect that we're usually waiting for the
	// cluster to converge on a shared understanding of ownership, and that
	// involves a couple of Nagle-like read delays (~30ms) as Etcd watch
	// updates are applied by participants.
	switch attempt {
	case 0, 1:
		return time.Millisecond * 50
	case 2, 3:
		return time.Millisecond * 100
	case 4, 5:
		return time.Second
	default:
		return 5 * time.Second
	}
}
