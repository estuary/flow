package message

import (
	"io"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
)

// Sequencer observes read-uncommitted messages from journals and sequences them
// into acknowledged, read-committed messages. Read uncommitted messages are fed
// to QueueUncommitted, after which the client must repeatedly call
// DequeCommitted to drain all acknowledged messages until io.EOF is returned.
//
// In more detail, messages observed by QueueUncommitted may acknowledge one
// or more pending messages previously observed by QueueUncommitted. For example,
// a non-duplicate message with Flag_OUTSIDE_TXN acknowledges itself, and a
// message with Flag_ACK_TXN also acknowledges messages having a lower clock.
// DequeCommitted will drain the complete set of now-committed messages,
// and then return io.EOF.
//
// An advantage of the design is that no head-of-line blocking occurs: committed
// messages are immediately deque'd upon observing their corresponding ACK_TXN,
// even if they're interleaved with still-pending messages of other producers.
//
// Sequencer maintains an internal ring buffer of messages, which is usually
// sufficient to directly read committed messages. When recovering from a
// checkpoint, or if a very long sequence or old producer is acknowledged, it
// may be necessary to start a replay of already-read messages. In this case:
//  * DequeCommitted will return ErrMustStartReplay.
//  * ReplayRange will return the exact offset range required.
//  * The client must then supply an appropriate Iterator to StartReplay.
// Having done this, calls to DequeCommitted may resume to drain messages.
type Sequencer struct {
	// partials is partial and un-acknowledged sequences of JournalProducers.
	partials map[JournalProducer]partialSeq
	// emit is an acknowledged message sequence which is ready for deque.
	emit struct {
		jp         JournalProducer // Sequence producer.
		next, last Clock           // Begin and end Clocks of the sequence, inclusive.
		offset     pb.Offset       // Offset to replay. May be earlier than that of |ringIndex|.
		ringIndex  int             // Next sequenced Envelope that's in the ring, or -1 if done.
	}
	replay Iterator   // Iterator supplied to StartReplay().
	ring   []Envelope // Fixed ring buffer of Envelopes.
	next   []int      // Linked-list of Envelopes having the same JournalProducer.
	head   int        // Next |ring| index to be written.
}

// NewSequencer returns a new Sequencer initialized from the given
// ProducerStates, and with an internal ring buffer of the given size.
func NewSequencer(states []ProducerState, buffer int) *Sequencer {
	if buffer <= 0 {
		buffer = 1
	}
	var s = &Sequencer{
		partials: make(map[JournalProducer]partialSeq, len(states)),
		ring:     make([]Envelope, 0, buffer),
		next:     make([]int, 0, buffer),
		head:     0,
	}
	for _, state := range states {
		s.partials[state.JournalProducer] = partialSeq{
			lastACK:   state.LastAck,
			begin:     state.Begin,
			ringStart: -1,
			ringStop:  -1,
		}
	}
	s.emit.ringIndex = -1
	return s
}

// partialSeq is a partially-read transactional sequence.
type partialSeq struct {
	lastACK             Clock     // See ProducerState.LastAck.
	begin               pb.Offset // See ProducerState.Begin.
	ringStart, ringStop int       // Indices (inclusive) of the sequence within |ring|.
}

// QueueUncommitted applies the next read-uncommitted message Envelope to the
// Sequencer. It panics if called while messages remain to dequeue.
// It returns true if the Envelope completes a sequence of messages,
// or is a duplicate or a roll-back, and false if the Envelope extends
// a sequence of messages which is still pending.
func (w *Sequencer) QueueUncommitted(env Envelope) bool {
	if w.emit.ringIndex != -1 {
		panic("committed messages remain to dequeue")
	}
	w.evictAtHead()

	var (
		uuid = env.GetUUID()
		jp   = JournalProducer{
			Journal:  env.Journal.Name,
			Producer: GetProducerID(uuid),
		}
		clock       = GetClock(uuid)
		flags       = GetFlags(uuid)
		partial, ok = w.partials[jp]
	)
	if !ok {
		partial = partialSeq{lastACK: clock - 1, begin: -1, ringStart: -1, ringStop: -1}
	}
	if uuid == (UUID{}) {
		flags = Flag_OUTSIDE_TXN // Emit. Don't track in |w.partials|.
	} else {
		defer func() { w.partials[jp] = partial }()
	}

	switch flags {
	default:
		w.logError(env, partial, "unexpected UUID flags")
		fallthrough // Handle as Flag_OUTSIDE_TXN.

	case Flag_OUTSIDE_TXN:

		// Duplicate of acknowledged message?
		if clock <= partial.lastACK && clock != 0 {
			sequencerQueuedTotal.WithLabelValues(
				env.Journal.Name.String(), "OUTSIDE_TXN", "drop").Inc()
			return true
		}

		if partial.begin != -1 {
			w.logError(env, partial, "unexpected OUTSIDE_TXN with a preceding CONTINUE_TXN"+
				" (mixed use of PublishUncommitted and PublishCommitted to the same journal?)")
			// Handled as an implicit roll-back of preceding messages.
		}

		// Add the message to the ring, and emit it.
		partial = w.addAtHead(env, partial)
		w.emit.jp = jp
		w.emit.next, w.emit.last = clock, clock
		w.emit.offset = env.Begin
		w.emit.ringIndex = partial.ringStop
		partial = partialSeq{lastACK: clock, begin: -1, ringStart: -1, ringStop: -1}

		sequencerQueuedTotal.WithLabelValues(
			env.Journal.Name.String(), "OUTSIDE_TXN", "emit").Inc()
		return true

	case Flag_CONTINUE_TXN:
		// Duplicate of acknowledged message?
		if clock < partial.lastACK {
			sequencerQueuedTotal.WithLabelValues(
				env.Journal.Name.String(), "CONTINUE_TXN", "drop").Inc()
			return true
		}
		// Duplicate of message already in the ring?
		if partial.ringStop != -1 && clock <= GetClock(w.ring[partial.ringStop].GetUUID()) {
			sequencerQueuedTotal.WithLabelValues(
				env.Journal.Name.String(), "CONTINUE_TXN", "drop-ring").Inc()
			return true
		}

		// Does |env| implicitly begin the next span?
		if partial.begin == -1 {
			partial.begin = env.Begin
		}
		partial = w.addAtHead(env, partial)

		sequencerQueuedTotal.WithLabelValues(
			env.Journal.Name.String(), "CONTINUE_TXN", "queue").Inc()
		return false

	case Flag_ACK_TXN:

		if clock < partial.lastACK {
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
		}

		if clock <= partial.lastACK {
			// Roll-back to this acknowledgement.
			partial = partialSeq{lastACK: clock, begin: -1, ringStart: -1, ringStop: -1}

			sequencerQueuedTotal.WithLabelValues(
				env.Journal.Name.String(), "ACK_TXN", "rollback").Inc()
			return true
		}

		if partial.begin == -1 {
			// ACK without a preceding CONTINUE_TXN. Non-standard but permitted.
			partial.begin = env.Begin
		}
		partial = w.addAtHead(env, partial)
		w.emit.jp = jp
		w.emit.next, w.emit.last = partial.lastACK, clock
		w.emit.offset = partial.begin
		w.emit.ringIndex = partial.ringStart
		partial = partialSeq{lastACK: clock, begin: -1, ringStart: -1, ringStop: -1}

		sequencerQueuedTotal.WithLabelValues(
			env.Journal.Name.String(), "ACK_TXN", "emit").Inc()
		return true
	}
	panic("not reached")
}

// DequeCommitted returns the next acknowledged message, or io.EOF if no
// acknowledged messages remain. It must be called repeatedly after
// each QueueUncommitted until it returns io.EOF. If messages are no
// longer within the Sequencer's buffer, it returns ErrMustStartReplay
// and the caller must first StartReplay before trying again.
func (w *Sequencer) DequeCommitted() (env Envelope, err error) {
	for {
		if env, err = w.dequeNext(); err != nil {
			return
		} else if w.replay != nil {
			sequencerReplayTotal.WithLabelValues(env.Journal.Name.String()).Inc()
		}
		var uuid = env.GetUUID()

		if GetProducerID(uuid) != w.emit.jp.Producer {
			// Pass.
		} else if clock := GetClock(uuid); clock < w.emit.next || clock > w.emit.last {
			// Pass.
		} else {
			w.emit.next = clock + 1
			return
		}
	}
}

// ReplayRange returns the [begin, end) exclusive byte offsets to be replayed.
// Panics if ErrMustStartReplay was not returned by DequeCommitted.
func (w *Sequencer) ReplayRange() (begin, end pb.Offset) {
	if w.emit.ringIndex == -1 {
		panic("no messages to deque")
	}
	return w.emit.offset, w.ring[w.emit.ringIndex].Begin
}

// StartReplay is called with a read-uncommitted Iterator over ReplayRange.
// Panics if ErrMustStartReplay was not returned by DequeCommitted.
func (w *Sequencer) StartReplay(it Iterator) {
	if w.emit.ringIndex == -1 {
		panic("no messages to deque")
	}
	w.replay = it
}

// HasPending returns true if any partial sequence has a first offset larger
// than those of the Offsets (eg, the sequence started since |since| was read).
// Assuming liveness of producers, it hints that further messages are
// forthcoming.
func (w *Sequencer) HasPending(since pb.Offsets) bool {
	for jp, partial := range w.partials {
		if partial.begin >= since[jp.Journal] {
			return true
		}
	}
	return false
}

// ProducerStates returns a snapshot of producers and their states, after
// pruning any producers having surpassed pruneHorizon in age relative to
// the most recent producer within their journal. If pruneHorizon is zero,
// no pruning is done. ProducerStates panics if messages still remain to deque.
func (w *Sequencer) ProducerStates(pruneHorizon time.Duration) []ProducerState {
	if w.emit.ringIndex != -1 {
		panic("messages remain to deque")
	}

	// Collect the largest Clock seen with each journal.
	var prune = make(map[pb.Journal]Clock)
	for jp, partial := range w.partials {
		if partial.lastACK > prune[jp.Journal] {
			prune[jp.Journal] = partial.lastACK
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
	var out = make([]ProducerState, 0, len(w.partials))
	for jp, partial := range w.partials {
		if partial.lastACK >= prune[jp.Journal] {
			out = append(out, ProducerState{
				JournalProducer: jp,
				LastAck:         partial.lastACK,
				Begin:           partial.begin,
			})
		} else {
			delete(w.partials, jp)
		}
	}
	return out
}

func (w *Sequencer) dequeNext() (env Envelope, err error) {
	if w.emit.ringIndex == -1 {
		err = io.EOF
		return
	}

	// Can we deque directly from the ring (|replay| must be drained first)?
	if env = w.ring[w.emit.ringIndex]; w.replay == nil && w.emit.offset == env.Begin {
		// We can. Step |emit| to next ring entry before returning.
		w.emit.ringIndex = w.next[w.emit.ringIndex]
		if w.emit.ringIndex != -1 {
			w.emit.offset = w.ring[w.emit.ringIndex].Begin
		}
		return
	}

	// Not within the ring. We must replay the range from w.emit.offset.
	if w.replay == nil {
		err = ErrMustStartReplay
		return
	}

	var ringBegin = env.Begin

	if env, err = w.replay.Next(); err == io.EOF {
		// Finished reading the replay range. Deque remaining entries from the ring.
		w.emit.offset = ringBegin
		w.replay = nil
		env, err = w.dequeNext()
		return
	}

	if err != nil {
		// Pass.
	} else if env.Journal.Name != w.emit.jp.Journal {
		err = errors.Errorf("wrong journal (%s; expected %s)", env.Journal.Name, w.emit.jp.Journal)
	} else if env.Begin < w.emit.offset {
		err = errors.Errorf("wrong Begin (%d; expected >= %d)", env.Begin, w.emit.offset)
	} else if env.End > ringBegin {
		err = errors.Errorf("wrong End (%d; expected <= %d)", env.End, ringBegin)
	} else {
		w.emit.offset = env.End
	}

	if err != nil {
		err = errors.WithMessage(err, "replay reader")
	}
	return
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

	// We must update partial |p| if it still references |w.head|. It often will
	// not, if the the latter has already been acknowledged or rolled back.
	if p, ok := w.partials[jp]; ok && p.ringStart == w.head {
		if p.ringStart == p.ringStop && w.next[p.ringStart] == -1 {
			p.ringStart, p.ringStop = -1, -1 // No more entries.
		} else if p.ringStart != p.ringStop && w.next[p.ringStart] != -1 {
			p.ringStart = w.next[p.ringStart] // Step to next entry still in ring.
		} else {
			panic("invariant violated: ringStart == ringStop iff next[ringStart] == -1")
		}
		w.partials[jp] = p
	}
	w.ring[w.head], w.next[w.head] = Envelope{}, -1
}

func (w *Sequencer) addAtHead(env Envelope, partial partialSeq) partialSeq {
	w.ring[w.head], w.next[w.head] = env, -1

	if partial.ringStop == -1 {
		partial.ringStart = w.head
	} else {
		w.next[partial.ringStop] = w.head
	}
	partial.ringStop = w.head

	w.head = (w.head + 1) % cap(w.ring)
	return partial
}

func (w *Sequencer) logError(env Envelope, partial partialSeq, msg string) {
	log.WithFields(log.Fields{
		"env.Journal":       env.Journal.Name,
		"env.Begin":         env.Begin,
		"env.End":           env.End,
		"env.UUID.Producer": GetProducerID(env.GetUUID()),
		"env.UUID.Clock":    GetClock(env.GetUUID()),
		"env.UUID.Time":     time.Unix(env.GetUUID().Time().UnixTime()),
		"env.UUID.Flags":    GetFlags(env.GetUUID()),
		"partial.lastACK":   partial.lastACK,
		"partial.begin":     partial.begin,
		"partial.ringStart": partial.ringStart,
		"partial.ringStop":  partial.ringStop,
	}).Error(msg)
}

// ErrMustStartReplay is returned by Sequencer to indicate that
// a journal replay must be started before the dequeue may continue.
var ErrMustStartReplay = errors.New("must start reader")
