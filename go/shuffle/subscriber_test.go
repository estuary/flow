package shuffle

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
	"golang.org/x/net/context"
)

func simpleResponseFixture() pf.ShuffleResponse {
	var resp = pf.ShuffleResponse{
		ReadThrough: 400,
		WriteHead:   600,
		Begin:       []pb.Offset{200},
		End:         []pb.Offset{300},
		UuidParts: []pf.UUIDParts{
			{Clock: 1001, ProducerAndFlags: uint64(message.Flag_CONTINUE_TXN)},
		},
	}
	resp.DocsJson = resp.Arena.AddAll([]byte("content"))
	resp.PackedKey = resp.Arena.AddAll([]byte("bb-cc-key"))
	return resp
}

func TestSubscriberInitAndStage(t *testing.T) {
	var sub = subscriber{
		// Start from a populated fixture, to test re-initialization.
		staged: simpleResponseFixture(),
	}

	// Initializing and then staging all docs should re-produce the fixture.
	var from = simpleResponseFixture()
	sub.initStaged(&from)
	sub.stageDoc(&from, 0)
	require.Equal(t, sub.staged, simpleResponseFixture())

	// If the Request provides offset restrictions, then expect only those documents are staged.
	sub.Offset, sub.EndOffset = 0, 200 // Ends before fixture.
	sub.initStaged(&from)
	sub.stageDoc(&from, 0)
	require.Equal(t, sub.staged.Begin, []pb.Offset{})

	sub.Offset, sub.EndOffset = 300, 400 // Starts after fixture.
	sub.initStaged(&from)
	sub.stageDoc(&from, 0)
	require.Equal(t, sub.staged.Begin, []pb.Offset{})

	sub.Offset, sub.EndOffset = 100, 500 // Fixture is within range.
	sub.initStaged(&from)
	sub.stageDoc(&from, 0)
	require.Equal(t, sub.staged.Begin, []pb.Offset{200})

	from.TerminalError = "an error" // Error is copied through on staging.
	sub.initStaged(&from)
	require.Equal(t, sub.staged.TerminalError, "an error")
}

func TestSubscriberKeyRangesWithShuffledAdd(t *testing.T) {
	var mk = func(a, b string) subscriber {
		var ranges = pf.RangeSpec{
			KeyBegin:  []byte(a),
			KeyEnd:    []byte(b),
			RClockEnd: (1 << 64) - 1,
		}
		if err := ranges.Validate(); err != nil {
			panic(err)
		}
		return subscriber{ShuffleRequest: pf.ShuffleRequest{Range: ranges}}
	}

	var fixtures = []subscriber{
		mk("a", "b"), // 0
		mk("a", "b"), // 1
		mk("a", "b"), // 2
		mk("c", "d"), // 3
		mk("d", "e"), // 4
		mk("d", "e"), // 5
		mk("g", "h"), // 6
		mk("g", "h"), // 7
		mk("m", "n"), // 8
	}
	// Perturb fixtures randomly; we should not depend on order.
	rand.Shuffle(len(fixtures), func(i, j int) {
		fixtures[i], fixtures[j] = fixtures[j], fixtures[i]
	})

	var s subscribers
	for _, sub := range fixtures {
		s.add(sub)
	}

	// Test keySpan cases.
	for _, tc := range []struct {
		k           string
		start, stop int
	}{
		{"a", 0, 3},
		{"aabb", 0, 3},
		{"b", 3, 3},
		{"bbb", 3, 3},
		{"ccc", 3, 4},
		{"ddcc", 4, 6},
		{"eeee", 6, 6},
		{"f", 6, 6},
		{"g", 6, 8},
		{"gzz", 6, 8},
		{"hh", 8, 8},
		{"mmnn", 8, 9},
		{"n", 9, 9},
		{"zzzz", 9, 9},
	} {
		var start, stop = keySpan(s, []byte(tc.k))
		require.Equal(t, tc.start, start)
		require.Equal(t, tc.stop, stop)
	}

	// Test rangeSpan cases.
	for _, tc := range []struct {
		begin, end  string
		start, stop int
	}{
		// Exact matches of ranges.
		{"a", "b", 0, 3},
		{"g", "h", 6, 8},
		// Partial overlap of single entry at list begin & end.
		{"", "aa", 0, 3},
		{"mm", "zz", 8, 9},
		// Overlaps of multiple entries.
		{"", "cc", 0, 4},   // Begin.
		{"", "d", 0, 4},    // Begin.
		{"bb", "dd", 3, 6}, // Middle.
		{"c", "e", 3, 6},   // Middle.
		{"c", "g", 3, 6},   // Middle.
		{"gg", "n", 6, 9},  // End.
		{"g", "nn", 6, 9},  // End.
	} {
		var start, stop = rangeSpan(s, []byte(tc.begin), []byte(tc.end))
		require.Equal(t, tc.start, start)
		require.Equal(t, tc.stop, stop)
	}

	for _, tc := range []struct {
		begin, end string
		index      int
	}{
		// Repetitions of key-ranges are fine.
		{"a", "b", 3},
		{"d", "e", 6},
		// As are insertions into list middle.
		{"b", "c", 3},
		{"f", "g", 6},
		// Or begining.
		{"", "a", 0},
		// Or end.
		{"n", "o", 9},

		// Overlaps are not a okay, at beginning.
		{"", "b", -1},
		// Or middle.
		{"a", "c", -1},
		{"b", "d", -1},
		{"d", "f", -1},
		// Or end.
		{"m", "o", -1},
		{"mm", "o", -1},
	} {
		var ind, err = insertionIndex(s, []byte(tc.begin), []byte(tc.end))
		if tc.index != -1 {
			require.NoError(t, err)
			require.Equal(t, tc.index, ind)
		} else {
			require.Error(t, err)
		}
	}
}

func TestClockRotationRegression(t *testing.T) {
	var c message.Clock
	require.Equal(t, rotateClock(c), uint64(0b00000000))

	// Increasing a clock's sequence modulates the MSBs of the output.
	c++
	require.Equal(t, rotateClock(c), uint64(0b10000000)<<56)
	c++
	require.Equal(t, rotateClock(c), uint64(0b01000000)<<56)
	c++
	require.Equal(t, rotateClock(c), uint64(0b11000000)<<56)
	c++
	require.Equal(t, rotateClock(c), uint64(0b00100000)<<56)

	// Timestamps are folded with sequence-counter updates.
	// Each sequence increment leads to a large jump in semiring location.
	c.Update(time.Unix(0, 0x1234567891))
	require.Equal(t, rotateClock(c), uint64(0xef6dd8424bb84d80))
	c++
	require.Equal(t, rotateClock(c), uint64(0x6f6dd8424bb84d80))
	c++
	require.Equal(t, rotateClock(c), uint64(0xaf6dd8424bb84d80))

	// Successive timestamps having small nano-second changes also
	// result in large jumps in semiring location.
	// (Recall Clocks have resolution of 100ns).
	c.Update(time.Unix(0, 0x1234567900))
	require.Equal(t, rotateClock(c), uint64(0x1f6dd8424bb84d80))
	c.Update(time.Unix(0, 0x1234568000))
	require.Equal(t, rotateClock(c), uint64(0x50edd8424bb84d80))
	c.Update(time.Unix(0, 0x1234568100))
	require.Equal(t, rotateClock(c), uint64(0x30edd8424bb84d80))
}

func TestSubscriberResponseStaging(t *testing.T) {
	// Subscriber fixtures:

	var requests = []pf.ShuffleRequest{
		{
			Shuffle: pf.JournalShuffle{Shuffle: &pf.Shuffle{FilterRClocks: true}},
			Range: pf.RangeSpec{
				KeyBegin:    []byte("a"),
				KeyEnd:      []byte("g"),
				RClockBegin: 0,
				RClockEnd:   1 << 63,
			},
		},
		{
			Shuffle: pf.JournalShuffle{Shuffle: &pf.Shuffle{FilterRClocks: true}},
			Range: pf.RangeSpec{
				KeyBegin:    []byte("a"),
				KeyEnd:      []byte("g"),
				RClockBegin: 1 << 63,
				RClockEnd:   (1 << 64) - 1,
			},
		},
		{
			Shuffle: pf.JournalShuffle{Shuffle: &pf.Shuffle{FilterRClocks: false}},
			Range: pf.RangeSpec{
				KeyBegin: []byte("l"),
				KeyEnd:   []byte("p"),
				// RClock range matches no fixtures, but is ignored since !IsPublishOnly.
				RClockBegin: 0,
				RClockEnd:   1,
			},
		},
	}
	var s subscribers
	for _, r := range requests {
		s.add(subscriber{
			ShuffleRequest: r,
			staged:         simpleResponseFixture(), // Test re-initialization.
		})
	}

	var tokens = bytes.Split([]byte("c/low c/high ACK lmn q"), []byte{' '})
	var fixture = pf.ShuffleResponse{
		ReadThrough: 1000,
		WriteHead:   2000,
		Begin:       []pb.Offset{200, 300, 400, 500, 600},
		End:         []pb.Offset{300, 400, 500, 600, 700},
		UuidParts: []pf.UUIDParts{
			{Clock: 10000 << 4, ProducerAndFlags: uint64(message.Flag_CONTINUE_TXN)},
			{Clock: 10001 << 4, ProducerAndFlags: uint64(message.Flag_CONTINUE_TXN)},
			{Clock: 10002 << 4, ProducerAndFlags: uint64(message.Flag_ACK_TXN)},
			{Clock: 10003 << 4, ProducerAndFlags: uint64(message.Flag_CONTINUE_TXN)},
			{Clock: 10004 << 4, ProducerAndFlags: uint64(message.Flag_CONTINUE_TXN)},
		},
	}
	fixture.DocsJson = fixture.Arena.AddAll(tokens...)
	fixture.PackedKey = fixture.Arena.AddAll(tokens...)

	s.stageResponses(&fixture)

	// Subscriber 0 sees c/low & ACK.
	require.Equal(t, s[0].staged.Begin, []pb.Offset{200, 400})
	// Subscriber 1 sees c/high & ACK.
	require.Equal(t, s[1].staged.Begin, []pb.Offset{300, 400})
	// Subscriber 2 sees ACK & lmn.
	require.Equal(t, s[2].staged.Begin, []pb.Offset{400, 500})
	// No subscribers see q.
}

func TestSubscriberSendAndPruneCases(t *testing.T) {
	var ch = make(chan error, 10)
	var sends int

	var sendMsg = func(interface{}) error {
		sends++
		return nil
	}
	var errContext, cancel = context.WithCancel(context.Background())
	cancel()

	var s = subscribers{
		{ // Send completes this subscriber's response.
			ShuffleRequest: pf.ShuffleRequest{Offset: 100, EndOffset: 200},
			staged:         pf.ShuffleResponse{ReadThrough: 300, WriteHead: 300},
			sendMsg:        sendMsg,
			doneCh:         ch,
		},
		{ // Nothing to send (subscriber is already tailing).
			ShuffleRequest: pf.ShuffleRequest{Offset: 300},
			staged:         pf.ShuffleResponse{ReadThrough: 300, WriteHead: 300},
			sendCtx:        context.Background(),
			sendMsg:        sendMsg,
			sentTailing:    true,
			doneCh:         ch,
		},
		{ // Nothing to send (not tailing yet as 299 != 300).
			ShuffleRequest: pf.ShuffleRequest{Offset: 100},
			staged:         pf.ShuffleResponse{ReadThrough: 299, WriteHead: 300},
			sendCtx:        context.Background(),
			sendMsg:        sendMsg,
			doneCh:         ch,
		},
		{ // Send tailing, but results in an error.
			staged:  pf.ShuffleResponse{ReadThrough: 300, WriteHead: 300},
			sendMsg: func(m interface{}) error { return fmt.Errorf("an-error") },
			doneCh:  ch,
		},
		{ // Nothing to send (already tailing), but context is error'd.
			staged:      pf.ShuffleResponse{ReadThrough: 300, WriteHead: 300},
			sendCtx:     errContext,
			sentTailing: true,
			doneCh:      ch,
		},
	}

	// Verify fixture expectations of which subscribers have responses to send.
	for i, e := range []bool{true, false, false, true, false} {
		require.Equal(t, e, s[i].shouldSendResponse())
	}
	s.sendResponses()

	require.Equal(t, 1, sends)        // Sent to 0 only.
	require.True(t, s[0].sentTailing) // 0 marked as having sent Tailing.
	sends = 0                         // Re-zero for next case.

	// Send to 0 finishes response, resulting in EOF.
	require.Nil(t, <-ch)
	// Send to 3 is an error.
	require.EqualError(t, <-ch, "an-error")
	// No send to 4, but context is error'd.
	require.EqualError(t, <-ch, context.Canceled.Error())

	// Only two subscribers are left now.
	require.Len(t, s, 2)

	s[1].staged.ReadThrough = 300              // Now tailing.
	require.True(t, s[1].shouldSendResponse()) // Toggles Tailing.
	s.sendResponses()

	require.Equal(t, 1, sends)        // Sent to 1 only.
	require.True(t, s[1].sentTailing) // 1 marked as having sent Tailing.
	sends = 0

	// Case: Terminal error staged, and sent to 0 & 1.
	for i := range s {
		s[i].staged.TerminalError = "foobar"
		require.True(t, s[i].shouldSendResponse()) // Error always sends.
	}
	s.sendResponses()
	require.Equal(t, 2, sends)

	// Both subscribers were notified of EOF.
	require.Len(t, s, 0)
	require.Nil(t, <-ch)
	require.Nil(t, <-ch)
}

func TestSubscriberAddCases(t *testing.T) {
	// Case: First subscriber, at offset 0.
	// Starts a read at offset zero.
	var s subscribers

	var sub = subscriber{
		ShuffleRequest: pf.ShuffleRequest{
			Shuffle: pf.JournalShuffle{Journal: "a/journal"},
			Range: pf.RangeSpec{
				KeyBegin:  []byte("a"),
				KeyEnd:    []byte("b"),
				RClockEnd: (1 << 64) - 1,
			},
			Offset: 0,
		},
		doneCh: make(chan error, 1),
	}
	require.Equal(t, &pb.ReadRequest{Journal: "a/journal"}, s.add(sub))

	// Case: First subscriber, at non-zero Offset & EndOffset.
	// Starts a read at the request offset.
	s = subscribers{}

	sub.ShuffleRequest.Offset = 456
	sub.ShuffleRequest.EndOffset = 789
	require.Equal(t,
		&pb.ReadRequest{
			Journal:   "a/journal",
			Offset:    456,
			EndOffset: 0, // Never EOFs.
		}, s.add(sub))

	// Case: Second subscriber, at a lower offset.
	// Starts a catch-up read which ends at the already-started read.
	sub.ShuffleRequest.Offset = 123
	require.Equal(t,
		&pb.ReadRequest{
			Journal:   "a/journal",
			Offset:    123,
			EndOffset: 456,
		}, s.add(sub))

	// Case: Third subscriber, at a higher Offset.
	// Doesn't start a new read.
	sub.ShuffleRequest.Offset = 789
	require.Nil(t, s.add(sub))

	// Case: Subscriber partially overlaps with existing key-range.
	sub.ShuffleRequest.Range.KeyEnd = []byte("bb")
	require.Nil(t, s.add(sub))

	require.EqualError(t, <-sub.doneCh, `range ["a", "bb") overlaps with existing range ["a", "b")`)
}

func TestSubscriberMinOffset(t *testing.T) {
	var s subscribers

	// Case: Empty list.
	var o, ok = s.minOffset()
	require.Equal(t, pb.Offset(0), o)
	require.Equal(t, false, ok)

	s = make(subscribers, 2)
	s[0].Offset = 456
	s[1].Offset = 123

	o, ok = s.minOffset()
	require.Equal(t, pb.Offset(123), o)
	require.Equal(t, true, ok)
}
