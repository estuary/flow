package message

import (
	"bytes"
	"io"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

func TestSequencerRingAddAndEvict(t *testing.T) {
	var (
		seq      = NewSequencer(nil, nil, 5)
		generate = newTestMsgGenerator()
		A, B     = NewProducerID(), NewProducerID()
		jpA      = JournalProducer{Journal: "test/journal", Producer: A}
		jpB      = JournalProducer{Journal: "test/journal", Producer: B}
		e1       = generate(A, 100, Flag_CONTINUE_TXN)
		e2       = generate(B, 200, Flag_CONTINUE_TXN)
		e3       = generate(A, 300, Flag_CONTINUE_TXN)
		e4       = generate(A, 400, Flag_CONTINUE_TXN)
		e5       = generate(B, 500, Flag_CONTINUE_TXN)
		e6       = generate(B, 600, Flag_CONTINUE_TXN)
		e7       = generate(B, 700, Flag_CONTINUE_TXN)
		e8       = generate(B, 800, Flag_CONTINUE_TXN)
		e9       = generate(B, 900, Flag_CONTINUE_TXN)
		e10      = generate(B, 1000, Flag_CONTINUE_TXN)
		e11      = generate(B, 1100, Flag_CONTINUE_TXN)
	)
	// Initial ring is empty.
	require.Equal(t, []Envelope{}, seq.ring)
	require.Equal(t, []int{}, seq.next)
	require.Equal(t, 0, seq.head)
	require.Equal(t, map[JournalProducer]*partialSeq{}, seq.partials)
	require.False(t, seq.HasPending())

	require.Equal(t, QueueContinueBeginSpan, seq.QueueUncommitted(e1)) // A.
	require.Equal(t, []Envelope{e1}, seq.ring)
	require.Equal(t, []int{-1}, seq.next)
	require.Equal(t, 1, seq.head)
	require.Equal(t, map[JournalProducer]*partialSeq{
		jpA: {jp: jpA, begin: e1.Begin, ringStart: 0, ringStop: 0, minClock: 99, maxClock: 100},
	}, seq.partials)
	require.True(t, seq.HasPending())

	// Continuations always update read-through offsets.
	require.Equal(t, pb.Offsets{"test/journal": e1.End}, seq.offsets)

	require.Equal(t, QueueContinueBeginSpan, seq.QueueUncommitted(e2)) // B.
	require.Equal(t, []Envelope{e1, e2}, seq.ring)
	require.Equal(t, []int{-1, -1}, seq.next)
	require.Equal(t, 2, seq.head)
	require.Equal(t, map[JournalProducer]*partialSeq{
		jpA: {jp: jpA, begin: e1.Begin, ringStart: 0, ringStop: 0, minClock: 99, maxClock: 100},
		jpB: {jp: jpB, begin: e2.Begin, ringStart: 1, ringStop: 1, minClock: 199, maxClock: 200},
	}, seq.partials)
	require.Len(t, seq.pending, 2) // There are now two pending sequences.

	require.Equal(t, QueueContinueExtendSpan, seq.QueueUncommitted(e3)) // A.
	require.Equal(t, []Envelope{e1, e2, e3}, seq.ring)
	require.Equal(t, []int{2, -1, -1}, seq.next) // e1 => e3.
	require.Equal(t, 3, seq.head)
	require.Equal(t, map[JournalProducer]*partialSeq{
		jpA: {jp: jpA, begin: e1.Begin, ringStart: 0, ringStop: 2, minClock: 99, maxClock: 300},
		jpB: {jp: jpB, begin: e2.Begin, ringStart: 1, ringStop: 1, minClock: 199, maxClock: 200},
	}, seq.partials)

	require.Equal(t, QueueContinueExtendSpan, seq.QueueUncommitted(e4)) // A.
	require.Equal(t, []Envelope{e1, e2, e3, e4}, seq.ring)
	require.Equal(t, []int{2, -1, 3, -1}, seq.next) // e3 => e4.
	require.Equal(t, 4, seq.head)
	require.Equal(t, map[JournalProducer]*partialSeq{
		jpA: {jp: jpA, begin: e1.Begin, ringStart: 0, ringStop: 3, minClock: 99, maxClock: 400},
		jpB: {jp: jpB, begin: e2.Begin, ringStart: 1, ringStop: 1, minClock: 199, maxClock: 200},
	}, seq.partials)

	require.Equal(t, QueueContinueExtendSpan, seq.QueueUncommitted(e5)) // B.
	require.Equal(t, []Envelope{e1, e2, e3, e4, e5}, seq.ring)
	require.Equal(t, []int{2, 4, 3, -1, -1}, seq.next) // e2 => e5.
	require.Equal(t, 0, seq.head)
	require.Equal(t, map[JournalProducer]*partialSeq{
		jpA: {jp: jpA, begin: e1.Begin, ringStart: 0, ringStop: 3, minClock: 99, maxClock: 400},
		jpB: {jp: jpB, begin: e2.Begin, ringStart: 1, ringStop: 4, minClock: 199, maxClock: 500},
	}, seq.partials)

	require.Equal(t, QueueContinueExtendSpan, seq.QueueUncommitted(e6)) // B.
	require.Equal(t, []Envelope{e6, e2, e3, e4, e5}, seq.ring)
	require.Equal(t, []int{-1, 4, 3, -1, 0}, seq.next) // e5 => e6.
	require.Equal(t, 1, seq.head)
	require.Equal(t, map[JournalProducer]*partialSeq{
		jpA: {jp: jpA, begin: e1.Begin, ringStart: 2, ringStop: 3, minClock: 99, maxClock: 400},
		jpB: {jp: jpB, begin: e2.Begin, ringStart: 1, ringStop: 0, minClock: 199, maxClock: 600},
	}, seq.partials)

	require.Equal(t, QueueContinueExtendSpan, seq.QueueUncommitted(e7)) // B.
	require.Equal(t, QueueContinueExtendSpan, seq.QueueUncommitted(e8)) // B.
	require.Equal(t, []Envelope{e6, e7, e8, e4, e5}, seq.ring)
	require.Equal(t, []int{1, 2, -1, -1, 0}, seq.next)
	require.Equal(t, 3, seq.head)
	require.Equal(t, map[JournalProducer]*partialSeq{
		jpA: {jp: jpA, begin: e1.Begin, ringStart: 3, ringStop: 3, minClock: 99, maxClock: 400},
		jpB: {jp: jpB, begin: e2.Begin, ringStart: 4, ringStop: 2, minClock: 199, maxClock: 800},
	}, seq.partials)

	require.Equal(t, QueueContinueExtendSpan, seq.QueueUncommitted(e9)) // B. Evicts final A entry.
	require.Equal(t, []Envelope{e6, e7, e8, e9, e5}, seq.ring)
	require.Equal(t, []int{1, 2, 3, -1, 0}, seq.next)
	require.Equal(t, 4, seq.head)
	require.Equal(t, map[JournalProducer]*partialSeq{
		// A's begin is still tracked, but it's no longer in the ring.
		jpA: {jp: jpA, begin: e1.Begin, ringStart: -1, ringStop: -1, minClock: 99, maxClock: 400},
		jpB: {jp: jpB, begin: e2.Begin, ringStart: 4, ringStop: 3, minClock: 199, maxClock: 900},
	}, seq.partials)

	require.Equal(t, QueueContinueExtendSpan, seq.QueueUncommitted(e10)) // B.
	require.Equal(t, []Envelope{e6, e7, e8, e9, e10}, seq.ring)
	require.Equal(t, []int{1, 2, 3, 4, -1}, seq.next)
	require.Equal(t, 0, seq.head)
	require.Equal(t, map[JournalProducer]*partialSeq{
		jpA: {jp: jpA, begin: e1.Begin, ringStart: -1, ringStop: -1, minClock: 99, maxClock: 400}, // Unchanged.
		jpB: {jp: jpB, begin: e2.Begin, ringStart: 0, ringStop: 4, minClock: 199, maxClock: 1000},
	}, seq.partials)

	require.Equal(t, QueueContinueExtendSpan, seq.QueueUncommitted(e11)) // B.
	require.Equal(t, []Envelope{e11, e7, e8, e9, e10}, seq.ring)
	require.Equal(t, []int{-1, 2, 3, 4, 0}, seq.next)
	require.Equal(t, 1, seq.head)
	require.Equal(t, map[JournalProducer]*partialSeq{
		jpA: {jp: jpA, begin: e1.Begin, ringStart: -1, ringStop: -1, minClock: 99, maxClock: 400}, // Unchanged.
		jpB: {jp: jpB, begin: e2.Begin, ringStart: 1, ringStop: 0, minClock: 199, maxClock: 1100},
	}, seq.partials)

	require.Equal(t, pb.Offsets{"test/journal": e11.End}, seq.offsets)
}

func TestSequencerTxnSequenceCases(t *testing.T) {
	var (
		generate = newTestMsgGenerator()
		seq      = NewSequencer(nil, nil, 3)
		A, B     = NewProducerID(), NewProducerID()
		jpA      = JournalProducer{Journal: "test/journal", Producer: A}
		jpB      = JournalProducer{Journal: "test/journal", Producer: B}
	)

	// Case: Sequence with internal duplicates served from the ring.
	var (
		a1    = generate(A, 1, Flag_CONTINUE_TXN)
		a2    = generate(A, 2, Flag_CONTINUE_TXN)
		a1Dup = generate(A, 1, Flag_CONTINUE_TXN)
		a2Dup = generate(A, 2, Flag_CONTINUE_TXN)
		a3ACK = generate(A, 3, Flag_ACK_TXN)
	)
	require.Equal(t,
		[]QueueOutcome{
			QueueContinueBeginSpan,
			QueueContinueExtendSpan,
			QueueContinueTxnClockLarger,
			QueueContinueTxnClockLarger,
			QueueAckCommitRing,
		},
		queue(seq, a1, a2, a1Dup, a2Dup, a3ACK))
	expectDeque(t, seq, a1, a2, a3ACK)
	require.False(t, seq.HasPending())

	// Case: ACK w/o preceding CONTINUE. Unusual but allowed.
	var a4ACK = generate(A, 4, Flag_ACK_TXN)
	require.Equal(t, []QueueOutcome{QueueAckEmpty}, queue(seq, a4ACK))
	expectDeque(t, seq, a4ACK)

	// Case: Partial ACK of preceding messages.
	var (
		a5      = generate(A, 5, Flag_CONTINUE_TXN)
		a7NoACK = generate(A, 7, Flag_CONTINUE_TXN) // Not included in a6ACK.
		a6ACK   = generate(A, 6, Flag_ACK_TXN)      // Served from ring.
	)
	require.Equal(t,
		[]QueueOutcome{
			QueueContinueBeginSpan,
			QueueContinueExtendSpan,
			QueueAckCommitRing,
		},
		queue(seq, a5, a7NoACK, a6ACK))
	expectDeque(t, seq, a5, a6ACK)
	require.False(t, seq.HasPending())

	// Case: Rollback with interleaved producer B.
	var (
		b1         = generate(B, 1, Flag_CONTINUE_TXN) // Evicted.
		a7Rollback = generate(A, 7, Flag_CONTINUE_TXN) // Evicted.
		a8Rollback = generate(A, 8, Flag_CONTINUE_TXN)
		b2         = generate(B, 2, Flag_CONTINUE_TXN)
		a6Abort    = generate(A, 6, Flag_ACK_TXN) // Aborts back to SeqNo 6.
	)
	require.Equal(t,
		[]QueueOutcome{
			QueueContinueBeginSpan,
			QueueContinueBeginSpan,
			QueueContinueTxnClockLarger,
			QueueContinueExtendSpan,
			QueueContinueExtendSpan,
			QueueAckRollback,
		},
		queue(seq, a7Rollback, b1, a7Rollback, a8Rollback, b2, a6Abort))
	require.Nil(t, seq.emit)          // No messages to dequeue.
	require.True(t, seq.HasPending()) // B is still pending.

	require.Equal(t, map[JournalProducer]*partialSeq{
		jpA: {jp: jpA, begin: -1, ringStart: -1, ringStop: -1, minClock: 6, maxClock: 6},
		jpB: {jp: jpB, begin: b1.Begin, ringStart: 1, ringStop: 1, minClock: 0, maxClock: 2},
	}, seq.partials)

	// Case: Interleaved producer ACKs. A replay is required due to eviction.
	var b3ACK = generate(B, 3, Flag_ACK_TXN)
	require.Equal(t, []QueueOutcome{QueueAckCommitReplay}, queue(seq, b3ACK))
	expectReplay(t, seq, b1.Begin, b2.Begin, b1, a7Rollback, a8Rollback)
	expectDeque(t, seq, b1, b2, b3ACK)
	require.False(t, seq.HasPending())

	// Case: Sequence which requires replay, with duplicates internal
	// to the sequence and from before it, which are encountered in
	// the ring and also during replay.
	var (
		b4    = generate(B, 4, Flag_CONTINUE_TXN) // Evicted.
		b1Dup = generate(B, 1, Flag_CONTINUE_TXN)
		b4Dup = generate(B, 4, Flag_CONTINUE_TXN)
		b5    = generate(B, 5, Flag_CONTINUE_TXN) // Evicted.
		b6    = generate(B, 6, Flag_CONTINUE_TXN)
		b2Dup = generate(B, 2, Flag_CONTINUE_TXN)
		b7    = generate(B, 7, Flag_CONTINUE_TXN)
		b8ACK = generate(B, 8, Flag_ACK_TXN)
	)
	require.Equal(t,
		[]QueueOutcome{
			QueueContinueBeginSpan,
			QueueContinueAlreadyAcked,
			QueueContinueTxnClockLarger,
			QueueContinueExtendSpan,
			QueueContinueExtendSpan,
			QueueContinueAlreadyAcked,
			QueueContinueExtendSpan,
			QueueAckCommitReplay,
		},
		queue(seq, b4, b1Dup, b4Dup, b5, b6, b2Dup, b7, b8ACK))
	expectReplay(t, seq, b4.Begin, b6.Begin, b4, b1Dup, b4Dup, b5)
	expectDeque(t, seq, b4, b5, b6, b7, b8ACK)
	require.False(t, seq.HasPending())

	// Case: Partial rollback where all ring entries are skipped.
	var (
		b9       = generate(B, 9, Flag_CONTINUE_TXN)  // Evicted.
		b11NoACK = generate(B, 11, Flag_CONTINUE_TXN) // Evicted
		b12NoACK = generate(B, 12, Flag_CONTINUE_TXN)
		b13NoACK = generate(B, 13, Flag_CONTINUE_TXN)
		b10ACK   = generate(B, 10, Flag_ACK_TXN)
	)
	require.Equal(t,
		[]QueueOutcome{
			QueueContinueBeginSpan,
			QueueContinueExtendSpan,
			QueueContinueExtendSpan,
			QueueContinueExtendSpan,
			QueueAckCommitReplay,
		},
		queue(seq, b9, b11NoACK, b12NoACK, b13NoACK, b10ACK))
	expectReplay(t, seq, b9.Begin, b12NoACK.Begin, b9, b11NoACK)
	expectDeque(t, seq, b9, b10ACK)
	require.False(t, seq.HasPending())

	// Case: Interleaved ACK'd sequences requiring two replays.
	var (
		b11    = generate(B, 11, Flag_CONTINUE_TXN) // Evicted.
		a7     = generate(A, 7, Flag_CONTINUE_TXN)  // Evicted.
		a8     = generate(A, 8, Flag_CONTINUE_TXN)
		b12    = generate(B, 12, Flag_CONTINUE_TXN)
		a9ACK  = generate(A, 9, Flag_ACK_TXN)
		b13ACK = generate(B, 13, Flag_ACK_TXN)
	)
	require.Equal(t, []QueueOutcome{
		QueueContinueBeginSpan,
		QueueContinueBeginSpan,
		QueueContinueExtendSpan,
		QueueContinueExtendSpan,
		QueueAckCommitReplay,
	},
		queue(seq, b11, a7, a8, b12, a9ACK))
	expectReplay(t, seq, a7.Begin, a8.Begin, a7)
	expectDeque(t, seq, a7, a8, a9ACK)
	require.True(t, seq.HasPending())

	require.Equal(t, []QueueOutcome{QueueAckCommitReplay}, queue(seq, b13ACK))
	expectReplay(t, seq, b11.Begin, b12.Begin, b11, a7, a8)
	expectDeque(t, seq, b11, b12, b13ACK)
	require.False(t, seq.HasPending())

	// Case: Reset to earlier ACK, followed by re-use of SeqNos.
	var (
		b8ACKReset  = generate(B, 8, Flag_ACK_TXN)
		b9Reuse     = generate(B, 9, Flag_CONTINUE_TXN)
		b10ACKReuse = generate(B, 10, Flag_ACK_TXN)
	)

	require.Equal(t,
		[]QueueOutcome{
			QueueAckRollback,
			QueueContinueBeginSpan,
			QueueAckCommitRing,
		},
		queue(seq, b8ACKReset, b9Reuse, b10ACKReuse))
	expectDeque(t, seq, b9Reuse, b10ACKReuse)
	require.False(t, seq.HasPending())
}

func TestSequencerTxnWithoutBuffer(t *testing.T) {
	var (
		generate = newTestMsgGenerator()
		seq      = NewSequencer(nil, nil, 0)
		A, B     = NewProducerID(), NewProducerID()

		a1    = generate(A, 1, Flag_CONTINUE_TXN)
		a2    = generate(A, 2, Flag_CONTINUE_TXN)
		b1    = generate(B, 1, Flag_CONTINUE_TXN)
		a1Dup = generate(A, 1, Flag_CONTINUE_TXN)
		a2Dup = generate(A, 2, Flag_CONTINUE_TXN)
		a3ACK = generate(A, 3, Flag_ACK_TXN)
		b2    = generate(B, 2, Flag_CONTINUE_TXN)
		b3ACK = generate(B, 3, Flag_ACK_TXN)
	)
	require.Equal(t,
		[]QueueOutcome{
			QueueContinueBeginSpan,
			QueueContinueExtendSpan,
			QueueContinueBeginSpan,
			QueueContinueTxnClockLarger,
			QueueContinueTxnClockLarger,
			QueueAckCommitReplay,
		},
		queue(seq, a1, a2, b1, a1Dup, a2Dup, a3ACK))
	expectReplay(t, seq, a1.Begin, a3ACK.Begin, a1, a2, b1, a1Dup, a2Dup)
	expectDeque(t, seq, a1, a2, a3ACK)
	require.True(t, seq.HasPending()) // B still pending.

	require.Equal(t, []QueueOutcome{QueueContinueExtendSpan, QueueAckCommitReplay},
		queue(seq, b2, b3ACK))
	expectReplay(t, seq, b1.Begin, b3ACK.Begin, b1, a1Dup, a2Dup, a3ACK, b2)
	expectDeque(t, seq, b1, b2, b3ACK)
	require.False(t, seq.HasPending())
}

func TestSequencerOutsideTxnCases(t *testing.T) {
	var (
		generate = newTestMsgGenerator()
		seq      = NewSequencer(nil, nil, 0)
		A        = NewProducerID()
		jpA      = JournalProducer{Journal: "test/journal", Producer: A}
	)

	// Case: OUTSIDE_TXN messages immediately dequeue.
	var (
		a1 = generate(A, 1, Flag_OUTSIDE_TXN)
		a2 = generate(A, 2, Flag_OUTSIDE_TXN)
	)
	require.Equal(t, []QueueOutcome{QueueOutsideCommit}, queue(seq, a1))
	expectDeque(t, seq, a1)
	require.Equal(t, []QueueOutcome{QueueOutsideCommit}, queue(seq, a2))
	expectDeque(t, seq, a2)
	require.False(t, seq.HasPending())

	require.Equal(t, pb.Offsets{"test/journal": a2.End}, seq.offsets)

	// Case: Duplicates are ignored.
	var (
		a1Dup = generate(A, 1, Flag_OUTSIDE_TXN)
		a2Dup = generate(A, 2, Flag_OUTSIDE_TXN)
	)
	require.Equal(t,
		[]QueueOutcome{
			QueueOutsideAlreadyAcked,
			QueueOutsideAlreadyAcked,
		}, queue(seq, a1Dup, a2Dup))
	require.Nil(t, seq.emit) // No messages to dequeue.

	require.Equal(t, pb.Offsets{"test/journal": a2Dup.End}, seq.offsets)

	// Case: Any preceding CONTINUE_TXN messages are aborted.
	var (
		a3Discard = generate(A, 3, Flag_CONTINUE_TXN)
		a4Discard = generate(A, 4, Flag_CONTINUE_TXN)
		a5        = generate(A, 5, Flag_OUTSIDE_TXN)
	)
	require.Equal(t,
		[]QueueOutcome{
			QueueContinueBeginSpan,
			QueueContinueExtendSpan,
			QueueOutsideCommit,
		}, queue(seq, a3Discard, a4Discard, a5))
	expectDeque(t, seq, a5)
	require.False(t, seq.HasPending())

	require.Equal(t, pb.Offsets{"test/journal": a5.End}, seq.offsets)

	// Case: Messages with unknown flags are treated as OUTSIDE_TXN.
	var (
		a6Discard    = generate(A, 6, Flag_CONTINUE_TXN)
		a7BadBits    = generate(A, 7, 0x100)
		a7BadBitsDup = generate(A, 7, 0x100)
	)
	require.Equal(t,
		[]QueueOutcome{
			QueueContinueBeginSpan,
			QueueOutsideCommit,
		}, queue(seq, a6Discard, a7BadBits))
	expectDeque(t, seq, a7BadBits)

	require.Equal(t, []QueueOutcome{QueueOutsideAlreadyAcked}, queue(seq, a7BadBitsDup))
	require.Nil(t, seq.emit) // No messages to dequeue.

	// Case: Messages with a zero UUID always dequeue.
	var (
		z1 = generate(ProducerID{}, 0, 0)
		z2 = generate(ProducerID{}, 0, 0)
	)
	z1.SetUUID(UUID{})
	z2.SetUUID(UUID{})

	require.Equal(t, []QueueOutcome{QueueOutsideCommit}, queue(seq, z1))
	expectDeque(t, seq, z1)
	require.Equal(t, []QueueOutcome{QueueOutsideCommit}, queue(seq, z2))
	expectDeque(t, seq, z2)
	require.False(t, seq.HasPending())

	// A producer for the zero-valued UUID is not tracked, but it still updates offsets.
	require.Equal(t, map[JournalProducer]*partialSeq{
		jpA: {jp: jpA, begin: -1, ringStart: -1, ringStop: -1, minClock: 7, maxClock: 7},
	}, seq.partials)
	require.Equal(t, pb.Offsets{"test/journal": z2.End}, seq.offsets)
}

func TestSequencerProducerStatesRoundTrip(t *testing.T) {
	var (
		generate = newTestMsgGenerator()
		seq1     = NewSequencer(nil, nil, 12)
		A, B, C  = NewProducerID(), NewProducerID(), NewProducerID()
		jpA      = JournalProducer{Journal: "test/journal", Producer: A}
		jpB      = JournalProducer{Journal: "test/journal", Producer: B}
		jpC      = JournalProducer{Journal: "test/journal", Producer: C}

		a1         = generate(A, 1, Flag_CONTINUE_TXN)
		a2         = generate(A, 2, Flag_CONTINUE_TXN)
		b1         = generate(B, 1, Flag_CONTINUE_TXN)
		b2         = generate(B, 2, Flag_CONTINUE_TXN)
		c1ACK      = generate(C, 1, Flag_ACK_TXN)
		b3ACK      = generate(B, 3, Flag_ACK_TXN)
		c2         = generate(C, 2, Flag_CONTINUE_TXN)
		c1Rollback = generate(C, 1, Flag_ACK_TXN)
		a3ACK      = generate(A, 3, Flag_ACK_TXN)
		z1         = generate(ProducerID{}, 0, 0)
	)
	z1.SetUUID(UUID{})

	require.Equal(t, []QueueOutcome{
		QueueContinueBeginSpan,
		QueueContinueExtendSpan,
		QueueContinueBeginSpan,
		QueueContinueExtendSpan,
		QueueAckEmpty,
	}, queue(seq1, a1, a2, b1, b2, c1ACK))
	expectDeque(t, seq1, c1ACK)
	require.True(t, seq1.HasPending())

	require.Equal(t, []QueueOutcome{
		QueueOutsideCommit,
	}, queue(seq1, z1))
	expectDeque(t, seq1, z1)

	// Take a checkpoint. The act of taking one causes pending
	// sequences to no longer be pending. To be pending, they must
	// have been updated *since* the last checkpoint was taken.
	require.True(t, seq1.HasPending())
	var offsets, states = seq1.Checkpoint(0)
	require.False(t, seq1.HasPending())

	// Recover a new Sequencer from persisted states & offsets.
	var seq2 = NewSequencer(offsets, states, 12)
	var _, states2 = seq2.Checkpoint(0)
	require.False(t, seq2.HasPending())

	// Expect |seq1| and |seq2| now produce identical states.
	var expect = []ProducerState{
		{JournalProducer: jpA, Begin: a1.Begin, LastAck: 0},
		{JournalProducer: jpB, Begin: b1.Begin, LastAck: 0},
		{JournalProducer: jpC, Begin: -1, LastAck: 1},
	}
	sort.Slice(expect, func(i, j int) bool {
		return bytes.Compare(expect[i].Producer[:], expect[j].Producer[:]) < 0
	})
	for _, states := range [][]ProducerState{states, states2} {
		sort.Slice(states, func(i, j int) bool {
			return bytes.Compare(states[i].Producer[:], states[j].Producer[:]) < 0
		})
		require.Equal(t, expect, states)
		require.Equal(t, pb.Offsets{"test/journal": z1.End}, offsets)
	}

	// Expect both Sequencers produce the same output from here,
	// though |seq2| requires replays while |seq1| does not.
	require.Equal(t, []QueueOutcome{QueueAckCommitRing}, queue(seq1, b3ACK))
	require.Equal(t, []QueueOutcome{QueueAckCommitReplay}, queue(seq2, b3ACK))
	expectReplay(t, seq2, b1.Begin, b3ACK.Begin, b1, b2, c1ACK)

	expectDeque(t, seq1, b1, b2, b3ACK)
	expectDeque(t, seq2, b1, b2, b3ACK)

	require.Equal(t, []QueueOutcome{QueueContinueBeginSpan, QueueAckRollback}, queue(seq1, c2, c1Rollback))
	require.Equal(t, []QueueOutcome{QueueContinueBeginSpan, QueueAckRollback}, queue(seq2, c2, c1Rollback))

	// No messages to dequeue.
	require.Nil(t, seq1.emit)
	require.Nil(t, seq2.emit)

	require.Equal(t, []QueueOutcome{QueueAckCommitRing}, queue(seq1, a3ACK))
	require.Equal(t, []QueueOutcome{QueueAckCommitReplay}, queue(seq2, a3ACK))
	expectReplay(t, seq2, a1.Begin, a3ACK.Begin, a1, a2, b1, b2, c1ACK)

	expectDeque(t, seq1, a1, a2, a3ACK)
	expectDeque(t, seq2, a1, a2, a3ACK)
}

func TestSequencerProducerStatesRoundTripDuringDequeue(t *testing.T) {
	var (
		generate = newTestMsgGenerator()
		seq1     = NewSequencer(nil, nil, 12)
		A        = NewProducerID()
		jpA      = JournalProducer{Journal: "test/journal", Producer: A}

		a1    = generate(A, 1, Flag_CONTINUE_TXN)
		a2    = generate(A, 2, Flag_CONTINUE_TXN)
		a3ACK = generate(A, 3, Flag_ACK_TXN)
		a4    = generate(A, 4, Flag_CONTINUE_TXN)
		a5    = generate(A, 5, Flag_CONTINUE_TXN)
		a6ACK = generate(A, 6, Flag_ACK_TXN)
	)

	require.Equal(t, []QueueOutcome{
		QueueContinueBeginSpan,
		QueueContinueExtendSpan,
		QueueAckCommitRing,
	}, queue(seq1, a1, a2, a3ACK))

	require.NotNil(t, seq1.emit)
	require.NoError(t, seq1.Step()) // Step to a1.

	// Suppose the program crashes here, without having dequeued a1.
	var offsets, states = seq1.Checkpoint(0)

	// Initialize |seq2| a couple of times to verify we round-trip unchanged
	// ProducerStates correctly.
	var seq2 = NewSequencer(offsets.Copy(), states, 12)
	offsets, states = seq2.Checkpoint(0)
	seq2 = NewSequencer(offsets.Copy(), states, 12)

	// Current states are immediately prior to a3ACK being processed.
	for _, seq := range []*Sequencer{seq1, seq2} {
		var offsets, states = seq.Checkpoint(0)
		require.Equal(t, pb.Offsets{"test/journal": a2.End}, offsets)
		require.Equal(t, []ProducerState{
			{JournalProducer: jpA, Begin: a1.Begin, LastAck: 0},
		}, states)
	}

	// |seq2| begins by reading a3ACK again.
	require.Equal(t, []QueueOutcome{QueueAckCommitReplay}, queue(seq2, a3ACK))
	expectReplay(t, seq2, a1.Begin, a3ACK.Begin, a1, a2)

	// Now both sequencers are ready for dequeue.
	require.NoError(t, seq1.Step())       // Step to a2.
	require.NoError(t, seq1.Step())       // Step to a3ACK.
	require.Equal(t, io.EOF, seq1.Step()) // Done.
	expectDeque(t, seq2, a1, a2, a3ACK)

	// Suppose |seq1| generates a checkpoint partway through
	// dequeue of the next sequence.
	require.Equal(t, []QueueOutcome{
		QueueContinueBeginSpan,
		QueueContinueExtendSpan,
		QueueAckCommitRing,
	}, queue(seq1, a4, a5, a6ACK))

	require.NotNil(t, seq1.emit)
	require.NoError(t, seq1.Step()) // Step to a4.
	require.NoError(t, seq1.Step()) // Step to a5. CRASH.

	// Initialize |seq2| to |seq1|'s state checkpoint.
	offsets, states = seq1.Checkpoint(0)
	seq2 = NewSequencer(offsets.Copy(), states, 12)
	offsets, states = seq2.Checkpoint(0)
	seq2 = NewSequencer(offsets.Copy(), states, 12)

	// Current states are immediately prior to a6ACK being processed,
	// but have been tightened to reflect a4's consumption.
	for _, seq := range []*Sequencer{seq1, seq2} {
		var offsets, states = seq.Checkpoint(0)
		require.Equal(t, pb.Offsets{"test/journal": a5.End}, offsets)
		require.Equal(t, []ProducerState{
			{JournalProducer: jpA, Begin: a5.Begin, LastAck: 4},
		}, states)
	}

	// |seq2| begins by reading a6ACK again, then dequeues from a5 (and not a4).
	require.Equal(t, []QueueOutcome{QueueAckCommitReplay}, queue(seq2, a6ACK))
	expectReplay(t, seq2, a5.Begin, a6ACK.Begin, a5)
	expectDeque(t, seq2, a5, a6ACK)
}

func TestSequencerProducerStatesStraddleDuplicate(t *testing.T) {
	var (
		generate = newTestMsgGenerator()
		seq1     = NewSequencer(nil, nil, 12)
		A        = NewProducerID()
		a1       = generate(A, 1, Flag_CONTINUE_TXN)
		a2       = generate(A, 2, Flag_CONTINUE_TXN)
		a3       = generate(A, 3, Flag_CONTINUE_TXN)
		a1Dup    = generate(A, 1, Flag_CONTINUE_TXN)
		a2Dup    = generate(A, 2, Flag_CONTINUE_TXN)
		a3Dup    = generate(A, 3, Flag_CONTINUE_TXN)
		a4ACK    = generate(A, 4, Flag_ACK_TXN)
	)

	require.Equal(t, []QueueOutcome{
		QueueContinueBeginSpan,
		QueueContinueExtendSpan,
		QueueContinueExtendSpan,
	}, queue(seq1, a1, a2, a3))
	require.Nil(t, seq1.emit) // No messages to dequeue.

	// Take a checkpoint and then restore it.
	var offsets, states = seq1.Checkpoint(0)
	var seq2 = NewSequencer(offsets, states, 12)

	require.Equal(t, []QueueOutcome{
		QueueContinueTxnClockLarger,
		QueueContinueTxnClockLarger,
		QueueContinueTxnClockLarger,
		QueueAckCommitRing,
	}, queue(seq1, a1Dup, a2Dup, a3Dup, a4ACK))

	require.Equal(t, []QueueOutcome{
		QueueContinueExtendSpan,
		QueueContinueExtendSpan,
		QueueContinueExtendSpan,
		QueueAckCommitReplay,
	}, queue(seq2, a1Dup, a2Dup, a3Dup, a4ACK))

	expectReplay(t, seq2, a1.Begin, a1Dup.Begin, a1, a2, a3)

	expectDeque(t, seq1, a1, a2, a3, a4ACK)
	expectDeque(t, seq2, a1, a2, a3, a4ACK)
}

func TestSequencerProducerPruning(t *testing.T) {
	var (
		generate = newTestMsgGenerator()
		seq1     = NewSequencer(nil, nil, 12)
		A, B, C  = NewProducerID(), NewProducerID(), NewProducerID()
		jpA      = JournalProducer{Journal: "test/journal", Producer: A}
		jpB      = JournalProducer{Journal: "test/journal", Producer: B}
		jpC      = JournalProducer{Journal: "test/journal", Producer: C}

		// Clocks units are 100s of nanos, with 4 LSBs of sequence counter.
		aCont = generate(A, 10<<4, Flag_CONTINUE_TXN)
		bCont = generate(B, 19<<4, Flag_CONTINUE_TXN)
		bACK  = generate(B, 20<<4, Flag_ACK_TXN)
		cCont = generate(C, 30<<4, Flag_CONTINUE_TXN)
	)
	require.Equal(t, []QueueOutcome{
		QueueContinueBeginSpan,
		QueueContinueBeginSpan,
		QueueAckCommitRing,
	}, queue(seq1, aCont, bCont, bACK))
	expectDeque(t, seq1, bCont, bACK)

	require.Equal(t, []QueueOutcome{QueueContinueBeginSpan}, queue(seq1, cCont))

	var expect = func(a, b []ProducerState) {
		sort.Slice(a, func(i, j int) bool {
			return bytes.Compare(a[i].Producer[:], a[j].Producer[:]) < 0
		})
		sort.Slice(b, func(i, j int) bool {
			return bytes.Compare(b[i].Producer[:], b[j].Producer[:]) < 0
		})
		require.Equal(t, a, b)
	}

	// Horizon prunes no producers: all states returned.
	var _, states = seq1.Checkpoint(20 * 100)
	expect([]ProducerState{
		{JournalProducer: jpA, Begin: aCont.Begin, LastAck: (10 << 4) - 1},
		{JournalProducer: jpB, Begin: -1, LastAck: 20 << 4},
		{JournalProducer: jpC, Begin: cCont.Begin, LastAck: (30 << 4) - 1},
	}, states)
	require.Len(t, seq1.partials, 3)

	// Expect A is pruned.
	_, states = seq1.Checkpoint(19 * 100)
	expect([]ProducerState{
		{JournalProducer: jpB, Begin: -1, LastAck: 20 << 4},
		{JournalProducer: jpC, Begin: cCont.Begin, LastAck: (30 << 4) - 1},
	}, states)
	require.Len(t, seq1.partials, 2)

	// Expect B is pruned.
	_, states = seq1.Checkpoint(1)
	expect([]ProducerState{
		{JournalProducer: jpC, Begin: cCont.Begin, LastAck: (30 << 4) - 1},
	}, states)
	require.Len(t, seq1.partials, 1)
}

func TestSequencerReplayReaderErrors(t *testing.T) {
	var A, B = NewProducerID(), NewProducerID()
	var cases = []struct {
		wrap   func(Iterator) func() (Envelope, error)
		expect string
	}{
		{ // No error.
			wrap:   func(it Iterator) func() (Envelope, error) { return it.Next },
			expect: "",
		},
		{ // Reader errors are passed through.
			wrap: func(Iterator) func() (Envelope, error) {
				return func() (Envelope, error) {
					return Envelope{}, io.ErrUnexpectedEOF
				}
			},
			expect: "replay reader: unexpected EOF",
		},
		{ // Returns Envelope of the wrong journal.
			wrap: func(it Iterator) func() (Envelope, error) {
				return func() (env Envelope, err error) {
					env, err = it.Next()
					env.Journal.Name = "wrong/journal"
					return
				}
			},
			expect: "replay of wrong journal (wrong/journal; expected test/journal)",
		},
		{ // Returns a Begin that's before the ReplayRange.
			wrap: func(it Iterator) func() (Envelope, error) {
				return func() (env Envelope, err error) {
					env, err = it.Next()
					env.Begin -= 1
					return
				}
			},
			expect: "replay has wrong Begin (101; expected >= 102)",
		},
		{ // Returns an End that's after the ReplayRange.
			wrap: func(it Iterator) func() (Envelope, error) {
				return func() (env Envelope, err error) {
					env, err = it.Next()
					env.End += 100
					return
				}
			},
			expect: "replay has wrong End (302; expected <= 204)",
		},
	}
	for _, tc := range cases {
		var (
			generate = newTestMsgGenerator()
			seq      = NewSequencer(nil, nil, 0)
			b1       = generate(B, 1, Flag_CONTINUE_TXN)
			a2       = generate(A, 2, Flag_CONTINUE_TXN)
			a3ACK    = generate(A, 3, Flag_ACK_TXN)
		)
		require.Equal(t, []QueueOutcome{
			QueueContinueBeginSpan,
			QueueContinueBeginSpan,
			QueueAckCommitReplay,
		}, queue(seq, b1, a2, a3ACK))
		expectReplay(t, seq, a2.Begin, a3ACK.Begin, a2)

		seq.replayIt = fnIterator(tc.wrap(seq.replayIt))
		var err = seq.Step()

		if tc.expect == "" {
			require.NoError(t, err)
		} else {
			require.EqualError(t, err, tc.expect)
		}
	}
}

type fnIterator func() (Envelope, error)

func (fn fnIterator) Next() (Envelope, error) { return fn() }

func queue(seq *Sequencer, envs ...Envelope) []QueueOutcome {
	var out = make([]QueueOutcome, len(envs))
	for i, e := range envs {
		out[i] = seq.QueueUncommitted(e)
	}
	return out
}

func expectDeque(t *testing.T, seq *Sequencer, expect ...Envelope) {
	for i := 0; i != len(expect); i++ {
		require.NoError(t, seq.Step())
		require.Equal(t, &expect[i], seq.Dequeued)

		require.Equal(t, expect[i].Begin, seq.emit.begin)
		if i > 0 {
			require.Equal(t, GetClock(expect[i-1].GetUUID()), seq.emit.minClock)
		}
	}

	require.Equal(t, io.EOF, seq.Step())
	require.Nil(t, seq.Dequeued)

	var last = expect[len(expect)-1]
	var jp = JournalProducer{
		Journal:  last.Journal.Name,
		Producer: GetProducerID(last.GetUUID()),
	}
	var clock = GetClock(last.GetUUID())

	// Expect internal tracked partial sequence was cleared & prepared
	// for next message sequence.
	if clock != 0 {
		require.Equal(t, partialSeq{
			jp:        jp,
			minClock:  clock,
			maxClock:  clock,
			begin:     -1,
			ringStart: -1,
			ringStop:  -1,
		}, *seq.partials[jp])
	}

	// Read-through of the ACK is tracked in offsets.
	require.Equal(t, last.End, seq.offsets[last.Journal.Name])
}

func expectReplay(t *testing.T, seq *Sequencer, expectBegin, expectEnd pb.Offset, envs ...Envelope) {
	var journal, begin, end = seq.ReplayRange()
	require.Equal(t, envs[0].Journal.Name, journal)
	require.Equal(t, expectBegin, begin)
	require.Equal(t, expectEnd, end)

	seq.StartReplay(fnIterator(func() (env Envelope, err error) {
		if envs == nil {
			panic("unexpected extra replay call")
		} else if len(envs) == 0 {
			envs, err = nil, io.EOF
			return
		} else {
			env, envs = envs[0], envs[1:]
			return
		}
	}))
}

func newTestMsgGenerator() func(p ProducerID, clock Clock, flags Flags) Envelope {
	var offset pb.Offset

	return func(p ProducerID, clock Clock, flags Flags) (e Envelope) {
		e = Envelope{
			Journal: &pb.JournalSpec{Name: "test/journal"},
			Begin:   offset,
			End:     offset + 100,
			Message: &testMsg{
				UUID: BuildUUID(p, clock, flags),
				Str:  strconv.Itoa(int(clock)),
			},
		}
		// Leave 2 bytes of dead space. Sequencer must handle non-contiguous envelopes.
		offset += 102
		return
	}
}

// testMsg meets the Message, Validator, & NewMessageFunc interfaces.
type testMsg struct {
	UUID UUID
	Str  string `json:",omitempty"`
	err  error
}

func newTestMsg(*pb.JournalSpec) (Message, error)        { return new(testMsg), nil }
func (m *testMsg) GetUUID() UUID                         { return m.UUID }
func (m *testMsg) SetUUID(uuid UUID)                     { m.UUID = uuid }
func (m *testMsg) NewAcknowledgement(pb.Journal) Message { return new(testMsg) }
func (m *testMsg) Validate() error                       { return m.err }
