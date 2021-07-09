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
		seq      = NewSequencer(nil, 5)
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
	require.Equal(t, map[JournalProducer]partialSeq{}, seq.partials)

	require.False(t, seq.QueueUncommitted(e1)) // A.
	require.Equal(t, []Envelope{e1}, seq.ring)
	require.Equal(t, []int{-1}, seq.next)
	require.Equal(t, 1, seq.head)
	require.Equal(t, map[JournalProducer]partialSeq{
		jpA: {begin: e1.Begin, ringStart: 0, ringStop: 0, lastACK: 99},
	}, seq.partials)

	require.False(t, seq.QueueUncommitted(e2)) // B.
	require.Equal(t, []Envelope{e1, e2}, seq.ring)
	require.Equal(t, []int{-1, -1}, seq.next)
	require.Equal(t, 2, seq.head)
	require.Equal(t, map[JournalProducer]partialSeq{
		jpA: {begin: e1.Begin, ringStart: 0, ringStop: 0, lastACK: 99},
		jpB: {begin: e2.Begin, ringStart: 1, ringStop: 1, lastACK: 199},
	}, seq.partials)

	require.False(t, seq.QueueUncommitted(e3)) // A.
	require.Equal(t, []Envelope{e1, e2, e3}, seq.ring)
	require.Equal(t, []int{2, -1, -1}, seq.next) // e1 => e3.
	require.Equal(t, 3, seq.head)
	require.Equal(t, map[JournalProducer]partialSeq{
		jpA: {begin: e1.Begin, ringStart: 0, ringStop: 2, lastACK: 99},
		jpB: {begin: e2.Begin, ringStart: 1, ringStop: 1, lastACK: 199},
	}, seq.partials)

	require.False(t, seq.QueueUncommitted(e4)) // A.
	require.Equal(t, []Envelope{e1, e2, e3, e4}, seq.ring)
	require.Equal(t, []int{2, -1, 3, -1}, seq.next) // e3 => e4.
	require.Equal(t, 4, seq.head)
	require.Equal(t, map[JournalProducer]partialSeq{
		jpA: {begin: e1.Begin, ringStart: 0, ringStop: 3, lastACK: 99},
		jpB: {begin: e2.Begin, ringStart: 1, ringStop: 1, lastACK: 199},
	}, seq.partials)

	require.False(t, seq.QueueUncommitted(e5)) // B.
	require.Equal(t, []Envelope{e1, e2, e3, e4, e5}, seq.ring)
	require.Equal(t, []int{2, 4, 3, -1, -1}, seq.next) // e2 => e5.
	require.Equal(t, 0, seq.head)
	require.Equal(t, map[JournalProducer]partialSeq{
		jpA: {begin: e1.Begin, ringStart: 0, ringStop: 3, lastACK: 99},
		jpB: {begin: e2.Begin, ringStart: 1, ringStop: 4, lastACK: 199},
	}, seq.partials)

	require.False(t, seq.QueueUncommitted(e6)) // B.
	require.Equal(t, []Envelope{e6, e2, e3, e4, e5}, seq.ring)
	require.Equal(t, []int{-1, 4, 3, -1, 0}, seq.next) // e5 => e6.
	require.Equal(t, 1, seq.head)
	require.Equal(t, map[JournalProducer]partialSeq{
		jpA: {begin: e1.Begin, ringStart: 2, ringStop: 3, lastACK: 99},
		jpB: {begin: e2.Begin, ringStart: 1, ringStop: 0, lastACK: 199},
	}, seq.partials)

	require.False(t, seq.QueueUncommitted(e7)) // B.
	require.False(t, seq.QueueUncommitted(e8)) // B.
	require.Equal(t, []Envelope{e6, e7, e8, e4, e5}, seq.ring)
	require.Equal(t, []int{1, 2, -1, -1, 0}, seq.next)
	require.Equal(t, 3, seq.head)
	require.Equal(t, map[JournalProducer]partialSeq{
		jpA: {begin: e1.Begin, ringStart: 3, ringStop: 3, lastACK: 99},
		jpB: {begin: e2.Begin, ringStart: 4, ringStop: 2, lastACK: 199},
	}, seq.partials)

	require.False(t, seq.QueueUncommitted(e9)) // B. Evicts final A entry.
	require.Equal(t, []Envelope{e6, e7, e8, e9, e5}, seq.ring)
	require.Equal(t, []int{1, 2, 3, -1, 0}, seq.next)
	require.Equal(t, 4, seq.head)
	require.Equal(t, map[JournalProducer]partialSeq{
		// A's begin is still tracked, but it's no longer in the ring.
		jpA: {begin: e1.Begin, ringStart: -1, ringStop: -1, lastACK: 99},
		jpB: {begin: e2.Begin, ringStart: 4, ringStop: 3, lastACK: 199},
	}, seq.partials)

	require.False(t, seq.QueueUncommitted(e10)) // B.
	require.Equal(t, []Envelope{e6, e7, e8, e9, e10}, seq.ring)
	require.Equal(t, []int{1, 2, 3, 4, -1}, seq.next)
	require.Equal(t, 0, seq.head)
	require.Equal(t, map[JournalProducer]partialSeq{
		jpA: {begin: e1.Begin, ringStart: -1, ringStop: -1, lastACK: 99}, // Unchanged.
		jpB: {begin: e2.Begin, ringStart: 0, ringStop: 4, lastACK: 199},
	}, seq.partials)

	require.False(t, seq.QueueUncommitted(e11)) // B.
	require.Equal(t, []Envelope{e11, e7, e8, e9, e10}, seq.ring)
	require.Equal(t, []int{-1, 2, 3, 4, 0}, seq.next)
	require.Equal(t, 1, seq.head)
	require.Equal(t, map[JournalProducer]partialSeq{
		jpA: {begin: e1.Begin, ringStart: -1, ringStop: -1, lastACK: 99}, // Unchanged.
		jpB: {begin: e2.Begin, ringStart: 1, ringStop: 0, lastACK: 199},
	}, seq.partials)
}

func TestSequencerTxnSequenceCases(t *testing.T) {
	var (
		generate = newTestMsgGenerator()
		seq      = NewSequencer(nil, 3)
		A, B     = NewProducerID(), NewProducerID()
	)

	// Case: Sequence with internal duplicates served from the ring.
	var (
		a1    = generate(A, 1, Flag_CONTINUE_TXN)
		a2    = generate(A, 2, Flag_CONTINUE_TXN)
		a1Dup = generate(A, 1, Flag_CONTINUE_TXN)
		a2Dup = generate(A, 2, Flag_CONTINUE_TXN)
		a3ACK = generate(A, 3, Flag_ACK_TXN)
	)
	require.Equal(t, []bool{false, false, true, true, true},
		queue(seq, a1, a2, a1Dup, a2Dup, a3ACK))
	expectDeque(t, seq, a1, a2, a3ACK)

	// Case: ACK w/o preceding CONTINUE. Unusual but allowed.
	var a4ACK = generate(A, 4, Flag_ACK_TXN)
	require.Equal(t, []bool{true}, queue(seq, a4ACK))
	expectDeque(t, seq, a4ACK)

	// Case: Partial ACK of preceding messages.
	var (
		a5      = generate(A, 5, Flag_CONTINUE_TXN)
		a7NoACK = generate(A, 7, Flag_CONTINUE_TXN) // Not included in a6ACK.
		a6ACK   = generate(A, 6, Flag_ACK_TXN)      // Served from ring.
	)
	require.Equal(t, []bool{false, false, true},
		queue(seq, a5, a7NoACK, a6ACK))
	expectDeque(t, seq, a5, a6ACK)

	// Case: Rollback with interleaved producer B.
	var (
		b1         = generate(B, 1, Flag_CONTINUE_TXN) // Evicted.
		a7Rollback = generate(A, 7, Flag_CONTINUE_TXN) // Evicted.
		a8Rollback = generate(A, 8, Flag_CONTINUE_TXN)
		b2         = generate(B, 2, Flag_CONTINUE_TXN)
		a6Abort    = generate(A, 6, Flag_ACK_TXN) // Aborts back to SeqNo 6.
	)
	require.Equal(t, []bool{false, false, true, false, false, true},
		queue(seq, a7Rollback, b1, a7Rollback, a8Rollback, b2, a6Abort))
	expectDeque(t, seq) // No messages deque.

	// Case: Interleaved producer ACKs. A replay is required due to eviction.
	var b3ACK = generate(B, 3, Flag_ACK_TXN)
	require.Equal(t, []bool{true}, queue(seq, b3ACK))
	expectReplay(t, seq, b1.Begin, b2.Begin, b1, a7Rollback, a8Rollback)
	expectDeque(t, seq, b1, b2, b3ACK)

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
	require.Equal(t, []bool{false, true, true, false, false, true, false, true},
		queue(seq, b4, b1Dup, b4Dup, b5, b6, b2Dup, b7, b8ACK))
	expectReplay(t, seq, b4.Begin, b6.Begin, b4, b1Dup, b4Dup, b5)
	expectDeque(t, seq, b4, b5, b6, b7, b8ACK)

	// Case: Partial rollback where all ring entries are skipped.
	var (
		b9       = generate(B, 9, Flag_CONTINUE_TXN)  // Evicted.
		b11NoACK = generate(B, 11, Flag_CONTINUE_TXN) // Evicted
		b12NoACK = generate(B, 12, Flag_CONTINUE_TXN)
		b13NoACK = generate(B, 13, Flag_CONTINUE_TXN)
		b10ACK   = generate(B, 10, Flag_ACK_TXN)
	)
	require.Equal(t, []bool{false, false, false, false, true},
		queue(seq, b9, b11NoACK, b12NoACK, b13NoACK, b10ACK))
	expectReplay(t, seq, b9.Begin, b12NoACK.Begin, b9, b11NoACK)
	expectDeque(t, seq, b9, b10ACK)

	// Case: Interleaved ACK'd sequences requiring two replays.
	var (
		b11    = generate(B, 11, Flag_CONTINUE_TXN) // Evicted.
		a7     = generate(A, 7, Flag_CONTINUE_TXN)  // Evicted.
		a8     = generate(A, 8, Flag_CONTINUE_TXN)
		b12    = generate(B, 12, Flag_CONTINUE_TXN)
		a9ACK  = generate(A, 9, Flag_ACK_TXN)
		b13ACK = generate(B, 13, Flag_ACK_TXN)
	)
	require.Equal(t, []bool{false, false, false, false, true},
		queue(seq, b11, a7, a8, b12, a9ACK))
	expectReplay(t, seq, a7.Begin, a8.Begin, a7)
	expectDeque(t, seq, a7, a8, a9ACK)

	require.Equal(t, []bool{true}, queue(seq, b13ACK))
	expectReplay(t, seq, b11.Begin, b12.Begin, b11, a7, a8)
	expectDeque(t, seq, b11, b12, b13ACK)

	// Case: Reset to earlier ACK, followed by re-use of SeqNos.
	var (
		b8ACKReset  = generate(B, 8, Flag_ACK_TXN)
		b9Reuse     = generate(B, 9, Flag_CONTINUE_TXN)
		b10ACKReuse = generate(B, 10, Flag_ACK_TXN)
	)

	require.Equal(t, []bool{true, false, true},
		queue(seq, b8ACKReset, b9Reuse, b10ACKReuse))
	expectDeque(t, seq, b9Reuse, b10ACKReuse)
}

func TestSequencerTxnWithoutBuffer(t *testing.T) {
	var (
		generate = newTestMsgGenerator()
		seq      = NewSequencer(nil, 0)
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
	require.Equal(t, []bool{false, false, false, false, false, true},
		queue(seq, a1, a2, b1, a1Dup, a2Dup, a3ACK))
	expectReplay(t, seq, a1.Begin, a3ACK.Begin, a1, a2, b1, a1Dup, a2Dup)
	expectDeque(t, seq, a1, a2, a3ACK)

	require.Equal(t, []bool{false, true},
		queue(seq, b2, b3ACK))
	expectReplay(t, seq, b1.Begin, b3ACK.Begin, b1, a1Dup, a2Dup, a3ACK, b2)
	expectDeque(t, seq, b1, b2, b3ACK)
}

func TestSequencerOutsideTxnCases(t *testing.T) {
	var (
		generate = newTestMsgGenerator()
		seq      = NewSequencer(nil, 0)
		A        = NewProducerID()
	)

	// Case: OUTSIDE_TXN messages immediately deque.
	var (
		a1 = generate(A, 1, Flag_OUTSIDE_TXN)
		a2 = generate(A, 2, Flag_OUTSIDE_TXN)
	)
	require.Equal(t, []bool{true}, queue(seq, a1))
	expectDeque(t, seq, a1)
	require.Equal(t, []bool{true}, queue(seq, a2))
	expectDeque(t, seq, a2)

	// Case: Duplicates are ignored.
	var (
		a1Dup = generate(A, 1, Flag_OUTSIDE_TXN)
		a2Dup = generate(A, 2, Flag_OUTSIDE_TXN)
	)
	require.Equal(t, []bool{true, true}, queue(seq, a1Dup, a2Dup))
	expectDeque(t, seq)

	// Case: Any preceding CONTINUE_TXN messages are aborted.
	var (
		a3Discard = generate(A, 3, Flag_CONTINUE_TXN)
		a4Discard = generate(A, 4, Flag_CONTINUE_TXN)
		a5        = generate(A, 5, Flag_OUTSIDE_TXN)
	)
	require.Equal(t, []bool{false, false, true},
		queue(seq, a3Discard, a4Discard, a5))
	expectDeque(t, seq, a5)

	// Case: Messages with unknown flags are treated as OUTSIDE_TXN.
	var (
		a6Discard    = generate(A, 6, Flag_CONTINUE_TXN)
		a7BadBits    = generate(A, 7, 0x100)
		a7BadBitsDup = generate(A, 7, 0x100)
	)
	require.Equal(t, []bool{false, true}, queue(seq, a6Discard, a7BadBits))
	expectDeque(t, seq, a7BadBits)
	require.Equal(t, []bool{true}, queue(seq, a7BadBitsDup))
	expectDeque(t, seq)

	// Case: Messages with a zero UUID always deque.
	var (
		z1 = generate(ProducerID{}, 0, 0)
		z2 = generate(ProducerID{}, 0, 0)
	)
	z1.SetUUID(UUID{})
	z2.SetUUID(UUID{})

	require.Equal(t, []bool{true}, queue(seq, z1))
	expectDeque(t, seq, z1)
	require.Equal(t, []bool{true}, queue(seq, z2))
	expectDeque(t, seq, z2)
}

func TestSequencerProducerStatesRoundTrip(t *testing.T) {
	var (
		generate = newTestMsgGenerator()
		seq1     = NewSequencer(nil, 12)
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
	)
	require.Equal(t, []bool{false, false, false, false, true},
		queue(seq1, a1, a2, b1, b2, c1ACK))
	expectDeque(t, seq1, c1ACK)

	var states = seq1.ProducerStates(0)
	var expect = []ProducerState{
		{JournalProducer: jpA, Begin: a1.Begin, LastAck: 0},
		{JournalProducer: jpB, Begin: b1.Begin, LastAck: 0},
		{JournalProducer: jpC, Begin: -1, LastAck: 1},
	}
	sort.Slice(states, func(i, j int) bool {
		return bytes.Compare(states[i].Producer[:], states[j].Producer[:]) < 0
	})
	sort.Slice(expect, func(i, j int) bool {
		return bytes.Compare(expect[i].Producer[:], expect[j].Producer[:]) < 0
	})
	require.Equal(t, expect, states)

	// Recover Sequencer from persisted states.
	var seq2 = NewSequencer(states, 12)

	// Expect both Sequencers produce the same output from here,
	// though |seq2| requires replays while |seq1| does not.
	require.Equal(t, []bool{true}, queue(seq1, b3ACK))
	require.Equal(t, []bool{true}, queue(seq2, b3ACK))
	expectReplay(t, seq2, b1.Begin, b3ACK.Begin, b1, b2, c1ACK)

	expectDeque(t, seq1, b1, b2, b3ACK)
	expectDeque(t, seq2, b1, b2, b3ACK)

	require.Equal(t, []bool{false, true}, queue(seq1, c2, c1Rollback))
	require.Equal(t, []bool{false, true}, queue(seq2, c2, c1Rollback))

	expectDeque(t, seq1)
	expectDeque(t, seq2)

	require.Equal(t, []bool{true}, queue(seq1, a3ACK))
	require.Equal(t, []bool{true}, queue(seq2, a3ACK))
	expectReplay(t, seq2, a1.Begin, a3ACK.Begin, a1, a2, b1, b2, c1ACK)

	expectDeque(t, seq1, a1, a2, a3ACK)
	expectDeque(t, seq2, a1, a2, a3ACK)
}

func TestSequencerProducerPruning(t *testing.T) {
	var (
		generate = newTestMsgGenerator()
		seq1     = NewSequencer(nil, 12)
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
	require.Equal(t, []bool{false, false, true}, queue(seq1, aCont, bCont, bACK))
	expectDeque(t, seq1, bCont, bACK)
	require.Equal(t, []bool{false}, queue(seq1, cCont))

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
	expect([]ProducerState{
		{JournalProducer: jpA, Begin: aCont.Begin, LastAck: (10 << 4) - 1},
		{JournalProducer: jpB, Begin: -1, LastAck: 20 << 4},
		{JournalProducer: jpC, Begin: cCont.Begin, LastAck: (30 << 4) - 1},
	}, seq1.ProducerStates(20*100))
	require.Len(t, seq1.partials, 3)

	// Expect A is pruned.
	expect([]ProducerState{
		{JournalProducer: jpB, Begin: -1, LastAck: 20 << 4},
		{JournalProducer: jpC, Begin: cCont.Begin, LastAck: (30 << 4) - 1},
	}, seq1.ProducerStates(19*100))
	require.Len(t, seq1.partials, 2)

	// Expect B is pruned.
	expect([]ProducerState{
		{JournalProducer: jpC, Begin: cCont.Begin, LastAck: (30 << 4) - 1},
	}, seq1.ProducerStates(1))
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
			expect: "replay reader: wrong journal (wrong/journal; expected test/journal)",
		},
		{ // Returns a Begin that's before the ReplayRange.
			wrap: func(it Iterator) func() (Envelope, error) {
				return func() (env Envelope, err error) {
					env, err = it.Next()
					env.Begin -= 1
					return
				}
			},
			expect: "replay reader: wrong Begin (101; expected >= 102)",
		},
		{ // Returns an End that's after the ReplayRange.
			wrap: func(it Iterator) func() (Envelope, error) {
				return func() (env Envelope, err error) {
					env, err = it.Next()
					env.End += 100
					return
				}
			},
			expect: "replay reader: wrong End (302; expected <= 204)",
		},
	}
	for _, tc := range cases {
		var (
			generate = newTestMsgGenerator()
			seq      = NewSequencer(nil, 0)
			b1       = generate(B, 1, Flag_CONTINUE_TXN)
			a2       = generate(A, 2, Flag_CONTINUE_TXN)
			a3ACK    = generate(A, 3, Flag_ACK_TXN)
		)
		require.Equal(t, []bool{false, false, true}, queue(seq, b1, a2, a3ACK))
		expectReplay(t, seq, a2.Begin, a3ACK.Begin, a2)

		seq.replay = fnIterator(tc.wrap(seq.replay))
		var _, err = seq.DequeCommitted()

		if tc.expect == "" {
			require.NoError(t, err)
		} else {
			require.EqualError(t, err, tc.expect)
		}
	}
}

type fnIterator func() (Envelope, error)

func (fn fnIterator) Next() (Envelope, error) { return fn() }

func queue(seq *Sequencer, envs ...Envelope) []bool {
	var out = make([]bool, len(envs))
	for i, e := range envs {
		out[i] = seq.QueueUncommitted(e)
	}
	return out
}

func expectDeque(t *testing.T, seq *Sequencer, expect ...Envelope) {
	for len(expect) != 0 {
		var env, err = seq.DequeCommitted()

		require.NoError(t, err)
		require.Equal(t, expect[0], env)
		expect = expect[1:]
	}
	var _, err = seq.DequeCommitted()
	require.Equal(t, io.EOF, err)
}

func expectReplay(t *testing.T, seq *Sequencer, expectBegin, expectEnd pb.Offset, envs ...Envelope) {
	var _, err = seq.DequeCommitted()
	require.Equal(t, ErrMustStartReplay, err)

	var begin, end = seq.ReplayRange()
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
