package message

import (
	"fmt"
	"io"
	"time"

	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
)

// Sequencer observes read-uncommitted messages from journals and sequences them
// into acknowledged, read-committed messages. Read uncommitted messages are fed
// to QueueUncommitted, after which the client must repeatedly call
// Step to dequeue all acknowledged messages until io.EOF is returned.
//
// In more detail, messages observed by QueueUncommitted may acknowledge one
// or more pending messages previously observed by QueueUncommitted. For example,
// a non-duplicate message with Flag_OUTSIDE_TXN acknowledges itself, and a
// message with Flag_ACK_TXN also acknowledges messages having a lower clock.
// Step will drain the complete set of now-committed messages into field Dequeued,
// and then return io.EOF.
//
// An advantage of the design is that no head-of-line blocking occurs: committed
// messages are immediately dequeued upon observing their corresponding ACK_TXN,
// even if they're interleaved with still-pending messages of other producers.
//
// Sequencer maintains an internal ring buffer of messages, which is usually
// sufficient to directly read committed messages. When recovering from a
// checkpoint, or if a very long sequence or old producer is acknowledged, it
// may be necessary to start a replay of already-read messages. In this case:
//   - QueueUncommitted will return QueueAckCommitReplay.
//   - The client calls ReplayRange to determine the exact offset range required.
//   - The client must then supply an appropriate Iterator to StartReplay.
//
// Having done this, calls to Step may resume to drain messages.
type Sequencer struct {
	// Dequeued is non-nil if (and only if) the Sequencer is in the
	// process of dequeuing an acknowledged sequence of messages.
	// After the client is done inspecting Dequeued, Step() must be
	// called to step to the next Envelope of the sequence, or to
	// complete the sequence (in which case Step returns io.EOF and
	// Dequeued is now nil).
	Dequeued *Envelope
	// Clock of the current non-nil Dequeued, or zero.
	// dequeuedClock is used to tighten the minimum clock bound once
	// Step is called, indicating Dequeued has been processed.
	// We could just ask Dequeued for its UUID, but the application
	// may have changed it out from under us. That's especially easy
	// to do if the application published it (which assigns a new UUID).
	dequeuedClock Clock
	// Offsets of the next (uncommitted) message to process in each journal.
	offsets pb.Offsets
	// partials is partial and un-acknowledged sequences of JournalProducers.
	partials map[JournalProducer]*partialSeq
	// pending is partial sequences which have been started or extended
	// since the last Checkpoint was taken.
	pending map[*partialSeq]struct{}
	// emit is an acknowledged message sequence which is ready for dequeue.
	emit      *partialSeq
	replayIt  Iterator   // Iterator supplied to StartReplay.
	replayEnv Envelope   // Last Envelope read from the replay Iterator.
	ring      []Envelope // Fixed ring buffer of Envelopes.
	next      []int      // Linked-list of Envelopes having the same JournalProducer.
	head      int        // Next |ring| index to be written.
}

// NewSequencer returns a new Sequencer initialized from the given offsets and
// ProducerStates, and with an internal ring buffer of the given size.
func NewSequencer(offsets pb.Offsets, states []ProducerState, buffer int) *Sequencer {
	if buffer <= 0 {
		buffer = 1
	}
	if offsets == nil {
		offsets = make(pb.Offsets)
	}

	var s = &Sequencer{
		offsets:  offsets,
		partials: make(map[JournalProducer]*partialSeq, len(states)),
		pending:  make(map[*partialSeq]struct{}),
		emit:     nil,
		ring:     make([]Envelope, 0, buffer),
		next:     make([]int, 0, buffer),
		head:     0,
	}
	for _, state := range states {
		s.partials[state.JournalProducer] = &partialSeq{
			jp:        state.JournalProducer,
			minClock:  state.LastAck,
			maxClock:  state.LastAck,
			begin:     state.Begin,
			ringStart: -1,
			ringStop:  -1,
		}
	}

	return s
}

// partialSeq is a partially-read transactional sequence.
type partialSeq struct {
	jp                  JournalProducer // JournalProducer which produced this sequence.
	minClock            Clock           // Exclusive-minimum clock of the current sequence.
	maxClock            Clock           // Inclusive-maximum clock of the current sequence.
	begin               pb.Offset       // See ProducerState.Begin.
	ringStart, ringStop int             // Indices (inclusive) of the sequence within |ring|.
}

// QueueOutcome is a queing decision made by Sequencer.QueueUncommitted.
type QueueOutcome int

const (
	// Zero is reserved to mark an invalid outcome.

	// QueueOutsideAlreadyAcked means this OUTSIDE_TXN message was
	// already acknowledged, and is ignored.
	QueueOutsideAlreadyAcked QueueOutcome = iota
	// QueueOutsideCommit means this OUTSIDE_TXN message was committed.
	// The caller must now dequeue via Step.
	QueueOutsideCommit QueueOutcome = iota
	// QueueContinueAlreadyAcked means this CONTINUE_TXN message was
	// already acknowledged, and is ignored.
	QueueContinueAlreadyAcked QueueOutcome = iota
	// QueueContinueTxnClockLarger means this CONTINUE_TXN message
	// is ignored due to a prior, larger Clock in the same transaction.
	QueueContinueTxnClockLarger QueueOutcome = iota
	// QueueContinueBeginSpan means this CONTINUE_TXN message begins
	// a new transactional sequence under this producer.
	QueueContinueBeginSpan QueueOutcome = iota
	// QueueContinueExtendSpan means this CONTINUE_TXN message extends
	// a new transactional sequence under this producer.
	QueueContinueExtendSpan QueueOutcome = iota
	// QueueAckRollback means this ACK_TXN rolled back a partial
	// sequence of messages, re-establishing an earlier Clock
	// for this producer.
	QueueAckRollback QueueOutcome = iota
	// QueueAckEmpty means this ACK_TXN committed without any
	// preceding messages.
	// The caller must now dequeue via Step.
	QueueAckEmpty QueueOutcome = iota
	// QueueAckCommitRing means this ACK_TXN committed a sequence
	// of preceding messages which is fully contained within the
	// Sequencer's ring buffer.
	// The caller must now dequeue via Step.
	QueueAckCommitRing QueueOutcome = iota
	// QueueAckCommitReplay means this ACK_TXN committed a sequence
	// of preceding messages which is only partly contained within the
	// Sequencer's ring buffer.
	// The caller must determine the ReplayRange and StartReplay,
	// and then dequeue via Step.
	QueueAckCommitReplay QueueOutcome = iota
)

// QueueUncommitted applies the next read-uncommitted message Envelope to the
// Sequencer. It panics if called while messages remain to dequeue,
// and otherwise returns a QueueOutcome.
func (w *Sequencer) QueueUncommitted(env Envelope) QueueOutcome {
	if w.emit != nil {
		panic("committed messages remain to dequeue")
	}
	w.evictAtHead()

	var (
		uuid = env.GetUUID()
		jp   = JournalProducer{
			Journal:  env.Journal.Name,
			Producer: GetProducerID(uuid),
		}
		clock   = GetClock(uuid)
		flags   = GetFlags(uuid)
		partial = w.partials[jp]
	)

	// Envelopes with a zero-valued Clock (including UUID{})
	// are treated as immediately committed, and are not indexed.
	if clock == 0 {
		partial = &partialSeq{
			jp:        jp,
			minClock:  0,
			maxClock:  0,
			begin:     -1,
			ringStart: -1,
			ringStop:  -1,
		}

		flags = Flag_OUTSIDE_TXN
		clock = 1
	} else if partial == nil {
		// If this |jp| doesn't have a partialSeq yet, create and index one.
		partial = &partialSeq{
			jp:        jp,
			minClock:  clock - 1,
			maxClock:  clock - 1,
			begin:     -1,
			ringStart: -1,
			ringStop:  -1,
		}
		w.partials[jp] = partial
	}

	// Inspect |flags|, |clock|, and the |partial| sequence to determine an |outcome|.
	// Keep state mutations *outside* of this if/else block.
	var outcome QueueOutcome
	switch flags {
	default:
		w.logError(env, partial, "unexpected UUID flags")
		fallthrough // Handle as Flag_OUTSIDE_TXN.

	case Flag_OUTSIDE_TXN:

		if clock <= partial.minClock {
			outcome = QueueOutsideAlreadyAcked
		} else if partial.begin != -1 {
			w.logError(env, partial, "unexpected OUTSIDE_TXN with a preceding CONTINUE_TXN"+
				" (mixed use of PublishUncommitted and PublishCommitted to the same journal?)")
			outcome = QueueOutsideCommit
		} else {
			outcome = QueueOutsideCommit
		}

	case Flag_CONTINUE_TXN:

		if clock <= partial.minClock {
			outcome = QueueContinueAlreadyAcked
		} else if clock <= partial.maxClock {
			outcome = QueueContinueTxnClockLarger
		} else if partial.begin == -1 {
			outcome = QueueContinueBeginSpan
		} else {
			outcome = QueueContinueExtendSpan
		}

	case Flag_ACK_TXN:

		if clock < partial.minClock {
			// Consumer shards checkpoint ACK intents to their stores before
			// appending them, to ensure that they'll reliably be sent even
			// if the process crashes.
			//
			// To see an earlier ACK means that an upstream consumer shard
			// recovered an earlier checkpoint with respect to the consumer
			// transaction of furthest progress that was previously completed.
			//
			// We allow the SeqNo to reset because future SeqNos we see may not be
			// re-sends of Messages already processed, and to do otherwise runs the
			// risk of failing to effectively process an upstream Message altogether.
			// Resetting effectively falls back back from exactly-once semantics to
			// at-least-once for upstream Messages in the interval from the
			// recovered but out-of-date checkpoint, vs the prior checkpoint of
			// furthest progress.
			w.logError(env, partial,
				"unexpected ACK_TXN which is less than lastACK (loss of exactly-once semantics)")
			outcome = QueueAckRollback
		} else if clock == partial.minClock {
			outcome = QueueAckRollback
		} else if partial.begin == -1 {
			outcome = QueueAckEmpty
		} else if partial.ringStart == -1 {
			outcome = QueueAckCommitReplay // No portion is within the ring.
		} else if partial.begin != w.ring[partial.ringStart].Begin {
			outcome = QueueAckCommitReplay // A trailing portion is in the ring.
		} else {
			outcome = QueueAckCommitRing // The entire sequence is in the ring.
		}
	}

	switch outcome {
	// Note: Each case of this switch must either set
	// |emit| or update |offsets| -- but not both.

	case QueueOutsideAlreadyAcked,
		QueueContinueAlreadyAcked,
		QueueContinueTxnClockLarger:

		// Ignore this message, aside from updating our read-through offset.
		w.offsets[env.Journal.Name] = env.End

	case QueueContinueBeginSpan,
		QueueContinueExtendSpan:

		// Track an uncommitted, transactional span.
		if outcome == QueueContinueBeginSpan {
			partial.begin = env.Begin
			w.pending[partial] = struct{}{} // Mark as pending since the last commit.
		}
		w.addAtHead(env, partial)
		partial.maxClock = clock
		w.offsets[env.Journal.Name] = env.End

	case QueueAckRollback:

		// Roll back to a prior acknowledged clock, discarding
		// any partial transactional span.
		*partial = partialSeq{
			jp:        partial.jp,
			minClock:  clock,
			maxClock:  clock,
			begin:     -1,
			ringStart: -1,
			ringStop:  -1,
		}
		delete(w.pending, partial) // No longer pending.
		w.offsets[env.Journal.Name] = env.End

	case QueueAckEmpty,
		QueueAckCommitRing,
		QueueAckCommitReplay,
		QueueOutsideCommit:

		// Commit a transactional span.
		w.addAtHead(env, partial)

		if outcome == QueueAckEmpty {
			partial.begin = env.Begin // Was -1.
		} else if outcome == QueueOutsideCommit {
			// It's possible there was an uncommitted partial sequence.
			// If so, we deliberately clobber it here, treating as an
			// effective rollback. In all other cases, partial.begin
			// is -1 and this OUTSIDE_TXN message is the only one in
			// the sequence.
			partial.begin = env.Begin            // Was (probably) -1.
			partial.ringStart = partial.ringStop // Was (probably) already ringStop.
		}

		// Commit through |clock|. Which may be less than the maximum clock
		// of the partial sequence! Any higher-clock messages are dropped.
		partial.maxClock = clock
		w.emit = partial

	default:
		panic(fmt.Sprintf("outcome %d", outcome))
	}
	sequencerQueuedTotal.WithLabelValues(
		env.Journal.Name.String(), flags.String(), outcome.String()).Inc()

	return outcome
}

// Step to the next committed message, or return io.EOF if none remain.
// A nil result means that the next message is available as Sequencer.Dequeued.
// Step panics if QueueUncommitted returned QueueAckCommitReplay,
// and the caller didn't first call StartReplay.
func (w *Sequencer) Step() error {
	if w.emit == nil {
		return io.EOF
	}
	if w.Dequeued == nil &&
		w.replayIt == nil &&
		w.emit.begin != w.ring[w.emit.ringStart].Begin {
		panic("caller was required to StartReplay, and didn't")
	}

	if w.Dequeued != nil {
		// Tighten clock to the processed Envelope.
		w.emit.minClock = w.dequeuedClock
		w.Dequeued.Message = nil // Release memory.
	}

	for w.emit.ringStart != -1 {

		if w.replayIt == nil {
			// We're consuming from the ring buffer.
			w.Dequeued = &w.ring[w.emit.ringStart]
			// Step such that ringStart reflects one *past* Dequeued.
			w.emit.ringStart = w.next[w.emit.ringStart]
		} else {
			// We're consuming from a replay Iterator.
			var err error

			if w.replayEnv, err = w.replayIt.Next(); err == io.EOF {
				// Replay finished. Begin consuming from the ring.
				w.replayEnv = Envelope{}
				w.replayIt = nil
				continue
			} else if err != nil {
				return fmt.Errorf("replay reader: %w", err)
			} else if w.replayEnv.Journal.Name != w.emit.jp.Journal {
				return fmt.Errorf("replay of wrong journal (%s; expected %s)",
					w.replayEnv.Journal.Name, w.emit.jp.Journal)
			} else if w.replayEnv.Begin < w.emit.begin {
				return fmt.Errorf("replay has wrong Begin (%d; expected >= %d)",
					w.replayEnv.Begin, w.emit.begin)
			} else if w.replayEnv.End > w.ring[w.emit.ringStart].Begin {
				return fmt.Errorf("replay has wrong End (%d; expected <= %d)",
					w.replayEnv.End, w.ring[w.emit.ringStart].Begin)
			}

			sequencerReplayTotal.WithLabelValues(w.emit.jp.Journal.String()).Inc()

			if GetProducerID(w.replayEnv.GetUUID()) != w.emit.jp.Producer {
				// Not the producer we're replaying for. This is common,
				// as the journal chunk holds the replayed sequence intermixed
				// with other messages.
				continue
			}

			w.Dequeued = &w.replayEnv
		}

		var uuid = w.Dequeued.GetUUID()
		w.dequeuedClock = GetClock(uuid)

		if w.dequeuedClock != 0 && w.dequeuedClock <= w.emit.minClock {
			// We don't allow duplicates within the ring with one exception:
			// messages with zero-valued Clocks are not expected to be
			// consistently ordered on clock (in QueueUncommitted we
			// synthetically assigned a clock value).
			//
			// However:
			// - Duplicated sequences CAN happen during replays.
			// - They can ALSO happen if we dequeued during replay,
			//   and then observe the sequence duplicated again in the ring.
			//
			// While ordinarily we would discard such duplicates during ring
			// maintenance operations, if the duplication straddles a recovered
			// checkpoint boundary then all bets are off because checkpoints
			// track only the last ACK and not the partial minClock.
			continue
		} else if w.dequeuedClock > w.emit.maxClock {
			continue // ACK'd clock tells us not to commit.
		}

		// Tighten offset to the dequeued Envelope.
		w.emit.begin = w.Dequeued.Begin

		return nil
	}

	// The last message processed committed the sequence we just
	// dequeued, and when we originally sequenced it we explicitly
	// *didn't* update journal offsets. Do so now.
	//
	// More details: we didn't update offsets on seeing the ACK to ensure
	// that a recovering sequencer would once again queue it,
	// causing it to emit this message sequence. Further note that we've
	// been tightening offset and clock bounds as we've been going,
	// so that a recovering sequencer would only start from the portion
	// of the sequence that remained to be processed.
	w.offsets[w.emit.jp.Journal] = w.Dequeued.End

	// We've completed consumption of the sequence.
	// Update it to prepare to read the next sequence from this producer.
	*w.emit = partialSeq{
		jp:        w.emit.jp,
		minClock:  w.emit.minClock,
		maxClock:  w.emit.minClock,
		begin:     -1,
		ringStart: -1,
		ringStop:  -1,
	}
	delete(w.pending, w.emit)

	w.Dequeued = nil
	w.emit = nil
	w.dequeuedClock = 0

	return io.EOF
}

// ReplayRange returns the journal and [begin, end) offsets to be replayed
// in order to dequeue committed messages.
// Panics if there are no messages to dequeue.
func (w *Sequencer) ReplayRange() (journal pb.Journal, begin, end pb.Offset) {
	if w.emit == nil {
		panic("emit == nil")
	}
	// Safety: the ring is always >= 1, and a committed message
	// causing |emit| to be set was the last message added.
	return w.emit.jp.Journal, w.emit.begin, w.ring[w.emit.ringStart].Begin
}

// StartReplay sets the read-uncommitted Iterator to read from
// in order to dequeue a committed sequence of messages.
// The Iterator must read from the Journal and offset range last
// returned by ReplayRange.
// Panics if there are no messages to dequeue.
func (w *Sequencer) StartReplay(it Iterator) {
	if w.emit == nil {
		panic("emit == nil")
	}
	w.replayIt = it
}

// HasPending returns true if an uncompleted message sequence has been
// started or extended since the last Checkpoint was taken.
// Assuming liveness of producers, it hints that further messages are
// forthcoming.
func (w *Sequencer) HasPending() bool {
	return len(w.pending) != 0
}

// Checkpoint returns a snapshot of read-through offsets, journal producers,
// and their states. It additionally prunes any producers having surpassed
// |pruneHorizon| in age, relative to the most recent producer within their journal.
// If |pruneHorizon| is zero, no pruning is done.
func (w *Sequencer) Checkpoint(pruneHorizon time.Duration) (pb.Offsets, []ProducerState) {
	// Collect the largest committed Clock seen with each journal.
	var prune = make(map[pb.Journal]Clock)
	for jp, partial := range w.partials {
		if partial.minClock > prune[jp.Journal] {
			prune[jp.Journal] = partial.minClock
		}
	}
	// Convert each to a minimum Clock bound before pruning is applied.
	// Recall Clock units are 100s of nanos, with 4 LSBs of sequence counter.
	for j, clock := range prune {
		if pruneHorizon > 0 {
			prune[j] = clock - Clock((pruneHorizon/100)<<4)
		} else {
			prune[j] = 0
		}
	}
	// Apply pruning and collect remaining states.
	var states = make([]ProducerState, 0, len(w.partials))
	for jp, partial := range w.partials {
		if partial.maxClock >= prune[jp.Journal] {
			states = append(states, ProducerState{
				JournalProducer: jp,
				LastAck:         partial.minClock,
				Begin:           partial.begin,
			})
		} else {
			delete(w.partials, jp)
		}
	}

	// Clear partial sequences which were marked as pending between the last
	// Checkpoint and now such that HasPending will no longer return true.
	// A future consumer transaction will thus not block to wait for their
	// completion, since they already didn't complete in *this* transaction.
	for p := range w.pending {
		delete(w.pending, p)
	}

	return w.offsets, states
}

func (w *Sequencer) evictAtHead() {
	if len(w.ring) != cap(w.ring) {
		// We're still filling the ring.
		w.ring = w.ring[:w.head+1]
		w.next = w.next[:w.head+1]
		return
	} else if w.ring[w.head].Message == nil {
		return // We already evicted at |w.head|.
	}

	var jp = JournalProducer{
		Journal:  w.ring[w.head].Journal.Name,
		Producer: GetProducerID(w.ring[w.head].GetUUID()),
	}

	// We must update a partial sequence that still references |w.head|.
	// It often will not, if the message has already been acknowledged or rolled back.
	if p, ok := w.partials[jp]; ok && p.ringStart == w.head {
		if p.ringStart == p.ringStop && w.next[p.ringStart] == -1 {
			p.ringStart, p.ringStop = -1, -1 // No more entries.
		} else if p.ringStart != p.ringStop && w.next[p.ringStart] != -1 {
			p.ringStart = w.next[p.ringStart] // Step to next entry still in ring.
		} else {
			panic("invariant violated: ringStart == ringStop iff next[ringStart] == -1")
		}
	}
	w.ring[w.head], w.next[w.head] = Envelope{}, -1
}

func (w *Sequencer) addAtHead(env Envelope, partial *partialSeq) {
	w.ring[w.head], w.next[w.head] = env, -1

	if partial.ringStop == -1 {
		partial.ringStart = w.head
	} else {
		w.next[partial.ringStop] = w.head
	}
	partial.ringStop = w.head

	w.head = (w.head + 1) % cap(w.ring)
}

func (w *Sequencer) logError(env Envelope, partial *partialSeq, msg string) {
	log.WithFields(log.Fields{
		"env.Journal":       env.Journal.Name,
		"env.Begin":         env.Begin,
		"env.End":           env.End,
		"env.UUID.Producer": GetProducerID(env.GetUUID()),
		"env.UUID.Clock":    GetClock(env.GetUUID()),
		"env.UUID.Time":     time.Unix(env.GetUUID().Time().UnixTime()),
		"env.UUID.Flags":    GetFlags(env.GetUUID()),
		"partial.minClock":  partial.minClock,
		"partial.maxClock":  partial.maxClock,
		"partial.begin":     partial.begin,
		"partial.ringStart": partial.ringStart,
		"partial.ringStop":  partial.ringStop,
	}).Error(msg)
}

func (o QueueOutcome) String() string {
	switch o {
	case QueueOutsideAlreadyAcked:
		return "outsideAlreadyAcked"
	case QueueOutsideCommit:
		return "outsideCommit"
	case QueueContinueAlreadyAcked:
		return "continueAlreadyAcked"
	case QueueContinueTxnClockLarger:
		return "continueRingClockLarger"
	case QueueContinueBeginSpan:
		return "continueBeginSpan"
	case QueueContinueExtendSpan:
		return "continueExtendSpan"
	case QueueAckRollback:
		return "ackRollback"
	case QueueAckEmpty:
		return "ackEmpty"
	case QueueAckCommitRing:
		return "ackCommitRing"
	case QueueAckCommitReplay:
		return "ackCommitReplay"
	}
	return "invalidOutcome"
}
