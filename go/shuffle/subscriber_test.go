package shuffle

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

func simpleResponseFixture() *pf.ShuffleResponse {
	var resp = &pf.ShuffleResponse{
		ReadThrough: 400,
		WriteHead:   600,
		Offsets:     []pb.Offset{200, 300, 300, 400},
		UuidParts: []pf.UUIDParts{
			{Clock: 1001, Node: uint64(message.Flag_CONTINUE_TXN)},
			{Clock: 1002, Node: uint64(message.Flag_CONTINUE_TXN)},
		},
	}
	resp.Docs = resp.Arena.AddAll([]byte("one"), []byte("two"))
	resp.PackedKey = resp.Arena.AddAll([]byte("bb-cc-key"), []byte("more-key"))
	return resp
}

func TestSubscriberDocStaging(t *testing.T) {
	var sub subscriber

	// Initializing and then staging all docs should re-produce the fixture.
	var from = simpleResponseFixture()
	sub.staged = newStagedResponse(0, 0)
	sub.stageDoc(from, 0)
	sub.stageDoc(from, 1)

	require.Equal(t, sub.staged.Offsets, []pb.Offset{200, 300, 300, 400})
	require.Equal(t, sub.staged.UuidParts, []pf.UUIDParts{
		{Clock: 1001, Node: uint64(message.Flag_CONTINUE_TXN)},
		{Clock: 1002, Node: uint64(message.Flag_CONTINUE_TXN)},
	})
	require.Equal(t, [][]byte{[]byte("one"), []byte("two")},
		sub.staged.Arena.AllBytes(sub.staged.Docs...))
	require.Equal(t, [][]byte{[]byte("bb-cc-key"), []byte("more-key")},
		sub.staged.Arena.AllBytes(sub.staged.PackedKey...))

	sub.staged = newStagedResponse(0, 0)

	// If the Request provides offset restrictions, then expect only those documents are staged.
	sub.Offset, sub.EndOffset = 0, 200 // Ends before fixture.
	sub.stageDoc(from, 0)
	sub.stageDoc(from, 1)
	require.Equal(t, sub.staged.Offsets, []pb.Offset{})

	sub.Offset, sub.EndOffset = 400, 500 // Starts after fixture.
	sub.stageDoc(from, 0)
	sub.stageDoc(from, 1)
	require.Equal(t, sub.staged.Offsets, []pb.Offset{})

	sub.Offset, sub.EndOffset = 250, 500 // Fixture is partially within range.
	sub.stageDoc(from, 0)
	sub.stageDoc(from, 1)
	require.Equal(t, sub.staged.Offsets, []pb.Offset{300, 400})
}

func TestSubscriberKeyRangesWithShuffledAdd(t *testing.T) {
	var mk = func(a, b uint32) subscriber {
		var ranges = pf.RangeSpec{
			KeyBegin:  a,
			KeyEnd:    b,
			RClockEnd: math.MaxUint32,
		}
		if err := ranges.Validate(); err != nil {
			panic(err)
		}
		return subscriber{ShuffleRequest: pf.ShuffleRequest{Range: ranges}}
	}

	var fixtures = []subscriber{
		mk(10, 100),  // 0
		mk(10, 100),  // 1
		mk(10, 100),  // 2
		mk(200, 299), // 3
		mk(300, 399), // 4
		mk(300, 399), // 5
		mk(600, 699), // 6
		mk(600, 699), // 7
		mk(900, 999), // 8
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
		k           uint32
		start, stop int
	}{
		{0, 0, 0},
		{10, 0, 3},
		{50, 0, 3},
		{100, 0, 3},
		{111, 3, 3},
		{200, 3, 4},
		{350, 4, 6},
		{410, 6, 6},
		{500, 6, 6},
		{550, 6, 6},
		{600, 6, 8},
		{690, 6, 8},
		{899, 8, 8},
		{950, 8, 9},
		{999, 8, 9},
		{10000, 9, 9},
	} {
		var start, stop = s.keySpan(tc.k)
		require.Equalf(t, tc.start, start, "tc: %#v, actualStart: %v", tc, start)
		require.Equalf(t, tc.stop, stop, "tc: %#v, actualStop: %v", tc, stop)
	}

	for _, tc := range []struct {
		begin, end uint32
		index      int
	}{
		// Repetitions of key-ranges are fine.
		{10, 100, 3},
		{300, 399, 6},
		// As are insertions into list middle.
		{101, 199, 3},
		{450, 460, 6},
		// Or begining.
		{0, 9, 0},
		// Or end.
		{1000, 1001, 9},

		// Overlaps are not a okay, at beginning.
		{0, 11, -1},
		// Or middle.
		{100, 300, -1},
		{399, 600, -1},
		// Or end.
		{910, 999, -1},
		{998, 1000, -1},
	} {
		var ind, err = s.insertionIndex(pf.RangeSpec{
			KeyBegin:  tc.begin,
			KeyEnd:    tc.end,
			RClockEnd: math.MaxUint32,
		})
		if tc.index != -1 {
			require.NoErrorf(t, err, "tc: %#v", tc)
			require.Equal(t, tc.index, ind)
		} else {
			require.Errorf(t, err, "tc: %#v, index: %v", tc, ind)
		}
	}
}

func TestClockRotationRegression(t *testing.T) {
	var c message.Clock
	require.Equal(t, rotateClock(c), uint32(0b00000000))

	// Increasing a clock's sequence modulates the MSBs of the output.
	c++
	require.Equal(t, rotateClock(c), uint32(0b10000000)<<24)
	c++
	require.Equal(t, rotateClock(c), uint32(0b01000000)<<24)
	c++
	require.Equal(t, rotateClock(c), uint32(0b11000000)<<24)
	c++
	require.Equal(t, rotateClock(c), uint32(0b00100000)<<24)

	// Timestamps are folded with sequence-counter updates.
	// Each sequence increment leads to a large jump in semiring location.
	c.Update(time.Unix(0, 0x1234567891))
	require.Equal(t, rotateClock(c), uint32(0xef6dd842))
	c++
	require.Equal(t, rotateClock(c), uint32(0x6f6dd842))
	c++
	require.Equal(t, rotateClock(c), uint32(0xaf6dd842))

	// Successive timestamps having small nano-second changes also
	// result in large jumps in semiring location.
	// (Recall Clocks have resolution of 100ns).
	c.Update(time.Unix(0, 0x1234567900))
	require.Equal(t, rotateClock(c), uint32(0x1f6dd842))
	c.Update(time.Unix(0, 0x1234568000))
	require.Equal(t, rotateClock(c), uint32(0x50edd842))
	c.Update(time.Unix(0, 0x1234568100))
	require.Equal(t, rotateClock(c), uint32(0x30edd842))
}

func TestSubscriberResponseStaging(t *testing.T) {
	var requests = []pf.ShuffleRequest{
		{ // Subscriber sees first half of keyspace, and first half of clocks.
			Shuffle: pf.JournalShuffle{Shuffle: &pf.Shuffle{FilterRClocks: true}},
			Range: pf.RangeSpec{
				KeyBegin:    0,
				KeyEnd:      1<<31 - 1,
				RClockBegin: 0,
				RClockEnd:   1<<31 - 1,
			},
		},
		{ // Sees first half of keyspace, and second half of clocks.
			Shuffle: pf.JournalShuffle{Shuffle: &pf.Shuffle{FilterRClocks: true}},
			Range: pf.RangeSpec{
				KeyBegin:    0x00000000,
				KeyEnd:      1<<31 - 1,
				RClockBegin: 1 << 31,
				RClockEnd:   1<<32 - 1,
			},
		},
		{ // Sees keyspace 0x8 through 0xa, and clocks are ignored since !FilterRClocks.
			Shuffle: pf.JournalShuffle{Shuffle: &pf.Shuffle{FilterRClocks: false}},
			Range: pf.RangeSpec{
				KeyBegin:    1 << 31,
				KeyEnd:      0xa0000000,
				RClockBegin: 0,
				RClockEnd:   1,
			},
		},
	}

	var s subscribers
	for i, r := range requests {
		var read = s.add(subscriber{ShuffleRequest: r})
		// First add starts a read at offset 0.
		require.Equal(t, i == 0, read != nil)
	}

	// Confirm the hash values of "packed key" tokens we'll use.
	var tokenHashRegresionCheck = []struct {
		hash  uint32
		token string
	}{
		{0x64ecbab1, "bar"}, // Low half.
		{0x38ad3674, "qib"}, // Low half.
		{0x8cad4162, "foo"}, // High.
		{0xa08b7e30, "fub"}, // High (out of fixture range).
	}
	for _, tc := range tokenHashRegresionCheck {
		require.Equal(t, tc.hash, flow.PackedKeyHash_HH64([]byte(tc.token)))
	}

	var tokens = bytes.Split([]byte("bar qib ACK foo fub"), []byte{' '})
	var fixture = pf.ShuffleResponse{
		TerminalError: "an error",
		ReadThrough:   1000,
		WriteHead:     2000,
		Offsets:       []pb.Offset{200, 201, 300, 301, 400, 401, 500, 501, 600, 601},
		UuidParts: []pf.UUIDParts{
			{Clock: 10000 << 4, Node: uint64(message.Flag_CONTINUE_TXN)},
			{Clock: 10001 << 4, Node: uint64(message.Flag_CONTINUE_TXN)},
			{Clock: 10002 << 4, Node: uint64(message.Flag_ACK_TXN)},
			{Clock: 10003 << 4, Node: uint64(message.Flag_CONTINUE_TXN)},
			{Clock: 10004 << 4, Node: uint64(message.Flag_CONTINUE_TXN)},
		},
	}
	fixture.Docs = fixture.Arena.AddAll(tokens...)
	fixture.PackedKey = fixture.Arena.AddAll(tokens...)

	s.stageResponses(&fixture)

	// Subscriber 0 sees bar & ACK.
	require.Equal(t, s[0].staged.Offsets, []pb.Offset{200, 201, 400, 401})
	// Subscriber 1 sees qib & ACK.
	require.Equal(t, s[1].staged.Offsets, []pb.Offset{300, 301, 400, 401})
	// Subscriber 2 sees ACK & foo.
	require.Equal(t, s[2].staged.Offsets, []pb.Offset{400, 401, 500, 501})
	// No subscribers see fub.

	var snap bytes.Buffer
	for _, sub := range s {
		require.NoError(t, proto.MarshalText(&snap, sub.staged))
	}
	cupaloy.SnapshotT(t, snap.String())
}

func TestSubscriberSendAndPruneCases(t *testing.T) {
	var errCh = make(chan error, 10)
	var sends int

	var callback = func(m *pf.ShuffleResponse, err error) error {
		if err != nil {
			errCh <- err
		} else {
			sends++
		}
		return nil
	}
	var errContext, cancel = context.WithCancel(context.Background())
	cancel()

	var s = subscribers{
		{ // A: Send completes this subscriber's response.
			ShuffleRequest: pf.ShuffleRequest{Offset: 100, EndOffset: 200},
			staged:         &pf.ShuffleResponse{ReadThrough: 300, WriteHead: 300},
			callback:       callback,
			sentTailing:    true,
		},
		{ // B: Nothing to send (subscriber is already tailing).
			ShuffleRequest: pf.ShuffleRequest{Offset: 300},
			staged:         &pf.ShuffleResponse{ReadThrough: 300, WriteHead: 300},
			ctx:            context.Background(),
			callback:       callback,
			sentTailing:    true,
		},
		{ // C: Nothing to send (not tailing yet as 299 != 300).
			ShuffleRequest: pf.ShuffleRequest{Offset: 100},
			staged:         &pf.ShuffleResponse{ReadThrough: 299, WriteHead: 300},
			ctx:            context.Background(),
			callback:       callback,
		},
		{ // D: Send tailing, but results in an error.
			staged: &pf.ShuffleResponse{ReadThrough: 300, WriteHead: 300},
			callback: func(m *pf.ShuffleResponse, err error) error {
				_ = callback(m, err)
				return fmt.Errorf("an-error")
			},
		},
		{ // E: Nothing to send (already tailing), but context is error'd.
			staged:      &pf.ShuffleResponse{ReadThrough: 300, WriteHead: 300},
			ctx:         errContext,
			callback:    callback,
			sentTailing: true,
		},
		{ // F: Queued document to send.
			staged:   simpleResponseFixture(),
			ctx:      context.Background(),
			callback: callback,
		},
	}

	// Verify fixture expectations of which subscribers have responses to send.
	for i, e := range []bool{true, false, false, true, false, true} {
		require.Equal(t, e, s[i].shouldSendResponse())
	}
	s.sendResponses()

	require.Equal(t, 3, sends) // Sent to A, D, & F.
	sends = 0                  // Re-zero for next case.

	// Send to A finishes response, resulting in EOF.
	require.Equal(t, io.EOF, <-errCh)
	// Send to D is an error.
	require.EqualError(t, <-errCh, "an-error")
	// No send to E, but context is error'd.
	require.EqualError(t, <-errCh, context.Canceled.Error())

	// Three subscribers are left now: B, C, & F.
	require.Len(t, s, 3)
	// Send to F cleared its staged response.
	require.Equal(t, s[2].staged, newStagedResponse(0, 0))

	s[1].staged.ReadThrough = 300                                       // C is now tailing.
	s[2].staged = &pf.ShuffleResponse{ReadThrough: 400, WriteHead: 500} // F is not tailing.

	for i, e := range []bool{false, true, false} {
		require.Equal(t, e, s[i].shouldSendResponse())
	}
	s.sendResponses()

	require.Equal(t, 1, sends)        // Sent to C only.
	require.True(t, s[1].sentTailing) // C marked as having sent Tailing.
	sends = 0

	// Case: Terminal error staged, and sent to B / C / F.
	for i := range s {
		s[i].staged.TerminalError = "foobar"
		require.True(t, s[i].shouldSendResponse()) // Error always sends.
	}
	s.sendResponses()
	require.Equal(t, 3, sends)

	// All subscribers were notified of EOF.
	require.Len(t, s, 0)
	require.Equal(t, io.EOF, <-errCh)
	require.Equal(t, io.EOF, <-errCh)
	require.Equal(t, io.EOF, <-errCh)
}

func TestSubscriberAddCases(t *testing.T) {
	// Retain callback error.
	var callbackErr error
	var callback = func(_ *pf.ShuffleResponse, err error) error {
		callbackErr = err
		return nil
	}

	// Case: First subscriber, at offset 0.
	// Starts a read at offset zero.
	var s subscribers

	var sub = subscriber{
		ShuffleRequest: pf.ShuffleRequest{
			Shuffle: pf.JournalShuffle{Journal: "a/journal"},
			Range: pf.RangeSpec{
				KeyBegin:  0x100,
				KeyEnd:    0x200,
				RClockEnd: (1 << 32) - 1,
			},
			Offset: 0,
		},
		callback: callback,
	}
	require.Equal(t, &pb.ReadRequest{Journal: "a/journal"}, s.add(sub))
	require.NoError(t, callbackErr)

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
	require.NoError(t, callbackErr)

	// Case: Second subscriber, at a lower offset.
	// Starts a catch-up read which ends at the already-started read.
	sub.ShuffleRequest.Offset = 123
	require.Equal(t,
		&pb.ReadRequest{
			Journal:   "a/journal",
			Offset:    123,
			EndOffset: 456,
		}, s.add(sub))
	require.NoError(t, callbackErr)

	// Case: Third subscriber, at a higher Offset.
	// Doesn't start a new read.
	sub.ShuffleRequest.Offset = 789
	require.Nil(t, s.add(sub))
	require.NoError(t, callbackErr)

	// Case: Subscriber partially overlaps with existing key-range.
	sub.ShuffleRequest.Range.KeyEnd = 0x300
	require.Nil(t, s.add(sub))
	require.EqualError(t, callbackErr,
		"range key:00000100-00000300;r-clock:00000000-ffffffff overlaps with existing range key:00000100-00000200;r-clock:00000000-ffffffff")
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

func TestPOW2CapacityEstimates(t *testing.T) {
	var cases = []struct{ v, e int }{
		{0, 8},
		{7, 8},
		{8, 8},
		{9, 16},
		{128, 128},
		{1025, 2048},
		{1 << 16, 1 << 16},
		{1<<16 + 1, 1 << 17},
		{1 << 19, 1 << 19},
		{1<<19 + 1, 1 << 20},
		{1<<20 + 1, 1 << 20},
		{1 << 30, 1 << 20},
	}
	for _, tc := range cases {
		require.Equal(t, tc.e, roundUpPow2(tc.v, 8, 1<<20))
	}
}
