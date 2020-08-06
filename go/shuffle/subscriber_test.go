package shuffle

import (
	"fmt"
	"testing"

	pf "github.com/estuary/flow/go/protocol"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

func TestSubscriberResponseStaging(t *testing.T) {

	var buildFixture = func() pf.ShuffleResponse {
		var resp = pf.ShuffleResponse{
			ReadThrough: 400,
			WriteHead:   600,
			Transform:   "a-transform",
			ContentType: pf.ContentType_JSON,
			Begin:       []pb.Offset{100, 200, 300},
			End:         []pb.Offset{200, 300, 400},
			UuidParts: []pf.UUIDParts{
				{Clock: 1000, ProducerAndFlags: uint64(message.Flag_CONTINUE_TXN)},
				{Clock: 2000, ProducerAndFlags: uint64(message.Flag_ACK_TXN)},
				{Clock: 3000, ProducerAndFlags: uint64(message.Flag_CONTINUE_TXN)},
			},
			// Hashes are tweaked such that index 0 => 3, and index 2 => 0.
			ShuffleHashesHigh: []uint64{0xaaaaaaaaaaaaaaaa, 0x0, 0x0555555555555555},
			ShuffleHashesLow:  []uint64{0x10, 0x20, 0x30},
		}
		resp.Content = resp.Arena.AddAll([]byte("one"), []byte("two"), []byte("three"))
		resp.ShuffleKey = []pf.Field{
			{Values: []pf.Field_Value{
				{Kind: pf.Field_Value_STRING, Bytes: resp.Arena.Add([]byte("abc"))},
				{Kind: pf.Field_Value_UNSIGNED, Unsigned: 32},
				{Kind: pf.Field_Value_DOUBLE, Double: 3.14},
			}},
			{Values: []pf.Field_Value{
				{Kind: pf.Field_Value_STRING, Bytes: resp.Arena.Add([]byte("xyz"))},
				{Kind: pf.Field_Value_UNSIGNED, Unsigned: 42},
				{Kind: pf.Field_Value_DOUBLE, Double: 8.67},
			}},
		}
		return resp
	}

	var rendezvous = newRendezvous(
		pf.ShuffleConfig{
			Journal: "a/journal",
			Ring: pf.Ring{
				Name:    "a-ring",
				Members: []pf.Ring_Member{{}, {}, {}, {}},
			},
			Shuffle: pf.Shuffle{
				ShuffleKeyPtr: []string{"/foo"},
				BroadcastTo:   1,
			},
		})

	// Initialize each of our subscribers with pre-populated Response fixtures.
	// This verifies correct response truncation, ahead of staging the next response.
	var s = subscribers{
		{
			request:  pf.ShuffleRequest{EndOffset: 300},
			response: buildFixture(),
			next: &subscriber{
				request: pf.ShuffleRequest{Offset: 300},
			},
		},
		{
			response: buildFixture(),
		},
		{
			request:  pf.ShuffleRequest{Offset: 500},
			response: buildFixture(),
		},
		{
			request:  pf.ShuffleRequest{},
			response: buildFixture(),
		},
	}

	var fixture = buildFixture()
	s.stageResponses(&fixture, &rendezvous)

	// Expect subscriber 0 sees documents 2 & 3.
	require.Equal(t, subscriber{
		request: pf.ShuffleRequest{EndOffset: 300},
		response: pf.ShuffleResponse{
			Arena:       pf.Arena("two"),
			ReadThrough: 400,
			WriteHead:   600,
			Transform:   "a-transform",
			ContentType: pf.ContentType_JSON,
			Content:     []pf.Slice{{Begin: 0, End: 3}},
			Begin:       []pb.Offset{200},
			End:         []pb.Offset{300},
			UuidParts: []pf.UUIDParts{
				{Clock: 2000, ProducerAndFlags: uint64(message.Flag_ACK_TXN)},
			},
			ShuffleHashesHigh: []uint64{0x0},
			ShuffleHashesLow:  []uint64{0x20},
			ShuffleKey: []pf.Field{
				{Values: []pf.Field_Value{
					{Kind: pf.Field_Value_UNSIGNED, Unsigned: 32},
				}},
				{Values: []pf.Field_Value{
					{Kind: pf.Field_Value_UNSIGNED, Unsigned: 42},
				}},
			},
		},
		next: &subscriber{
			request: pf.ShuffleRequest{Offset: 300},
			response: pf.ShuffleResponse{
				Arena:       pf.Arena("three"),
				ReadThrough: 400,
				WriteHead:   600,
				Transform:   "a-transform",
				ContentType: pf.ContentType_JSON,
				Content:     []pf.Slice{{Begin: 0, End: 5}},
				Begin:       []pb.Offset{300},
				End:         []pb.Offset{400},
				UuidParts: []pf.UUIDParts{
					{Clock: 3000, ProducerAndFlags: uint64(message.Flag_CONTINUE_TXN)},
				},
				// Hashes are tweaked such that index 0 => 3, and index 2 => 0.
				ShuffleHashesHigh: []uint64{0x0555555555555555},
				ShuffleHashesLow:  []uint64{0x30},
				ShuffleKey: []pf.Field{
					{Values: []pf.Field_Value{
						{Kind: pf.Field_Value_DOUBLE, Double: 3.14},
					}},
					{Values: []pf.Field_Value{
						{Kind: pf.Field_Value_DOUBLE, Double: 8.67},
					}},
				},
			},
		},
	}, s[0])

	// Meanwhile, subscriber 3 sees documents 0 & 1.
	require.Equal(t, subscriber{
		request: pf.ShuffleRequest{},
		response: pf.ShuffleResponse{
			Arena:       pf.Arena("oneabcxyztwo"),
			ReadThrough: 400,
			WriteHead:   600,
			Transform:   "a-transform",
			ContentType: pf.ContentType_JSON,
			Content:     []pf.Slice{{Begin: 0, End: 3}, {Begin: 9, End: 12}},
			Begin:       []pb.Offset{100, 200},
			End:         []pb.Offset{200, 300},
			UuidParts: []pf.UUIDParts{
				{Clock: 1000, ProducerAndFlags: uint64(message.Flag_CONTINUE_TXN)},
				{Clock: 2000, ProducerAndFlags: uint64(message.Flag_ACK_TXN)},
			},
			ShuffleHashesHigh: []uint64{0xaaaaaaaaaaaaaaaa, 0x0},
			ShuffleHashesLow:  []uint64{0x10, 0x20},
			ShuffleKey: []pf.Field{
				{Values: []pf.Field_Value{
					{Kind: pf.Field_Value_STRING, Bytes: pf.Slice{Begin: 3, End: 6}},
					{Kind: pf.Field_Value_UNSIGNED, Unsigned: 32},
				}},
				{Values: []pf.Field_Value{
					{Kind: pf.Field_Value_STRING, Bytes: pf.Slice{Begin: 6, End: 9}},
					{Kind: pf.Field_Value_UNSIGNED, Unsigned: 42},
				}},
			},
		},
	}, s[3])

	// Subscriber 2 sees no documents (offset is too high).
	require.Equal(t, subscriber{
		request: pf.ShuffleRequest{Offset: 500}, // Unchanged.
		response: pf.ShuffleResponse{
			Arena:             pf.Arena{},
			ReadThrough:       400,
			WriteHead:         600,
			Transform:         "a-transform",
			ContentType:       pf.ContentType_JSON,
			Content:           []pf.Slice{},
			Begin:             []pb.Offset{},
			End:               []pb.Offset{},
			UuidParts:         []pf.UUIDParts{},
			ShuffleHashesHigh: []uint64{},
			ShuffleHashesLow:  []uint64{},
			ShuffleKey: []pf.Field{
				{Values: []pf.Field_Value{}},
				{Values: []pf.Field_Value{}},
			},
		},
	}, s[2])

	// Expect that a TerminalError is staged to all subscribers.
	var errResponse = pf.ShuffleResponse{TerminalError: "an error"}
	s = subscribers{
		{next: &subscriber{}},
		{},
	}
	s.stageResponses(&errResponse, &rendezvous)

	require.Equal(t, subscribers{
		{
			response: errResponse,
			next:     &subscriber{response: errResponse},
		},
		{response: errResponse},
	}, s)
}

func TestSubscriberSendAndPruneCases(t *testing.T) {
	var s0a = make(chan error, 1)
	var s0b = make(chan error, 1)
	var s1 = make(chan error, 1)
	var s2 = make(chan error, 1)
	var sends int

	var sendMsg = func(interface{}) error {
		sends++
		return nil
	}

	var s = subscribers{
		{
			request:  pf.ShuffleRequest{Offset: 100, EndOffset: 200},
			response: pf.ShuffleResponse{ReadThrough: 300, WriteHead: 300},
			sendMsg:  sendMsg,
			doneCh:   s0a,

			next: &subscriber{
				request:     pf.ShuffleRequest{Offset: 300},
				response:    pf.ShuffleResponse{ReadThrough: 300, WriteHead: 300},
				doneCh:      s0b,
				sendMsg:     sendMsg,
				sentTailing: true,
			},
		},
		{
			request:  pf.ShuffleRequest{Offset: 100},
			response: pf.ShuffleResponse{ReadThrough: 299, WriteHead: 300}, // Not tailing.
			sendMsg:  sendMsg,
			doneCh:   s1,
		},
		{
			response: pf.ShuffleResponse{ReadThrough: 300, WriteHead: 300},
			sendMsg:  func(m interface{}) error { return fmt.Errorf("an-error") },
			doneCh:   s2,
		},
	}

	// Case: Message sent to 0, omitted from 1, and causes 2 to error.
	require.True(t, s[0].shouldSendResponse())
	require.False(t, s[1].shouldSendResponse())
	require.True(t, s[2].shouldSendResponse())
	require.Equal(t, 3, s.sendResponses(0))

	require.Equal(t, 1, sends)        // Sent to 0 only.
	require.True(t, s[0].sentTailing) // 0 marked as having sent Tailing.
	sends = 0                         // Re-zero for next case.
	require.EqualError(t, <-s2, "an-error")
	require.Nil(t, s[2].doneCh) // Expect reset.

	// Case: Message is trivial for 0, which completes, and is sent to 1.
	s[1].response.ReadThrough = 300             // Now tailing.
	require.False(t, s[0].shouldSendResponse()) // Already Tailing.
	require.True(t, s[1].shouldSendResponse())  // Toggles Tailing.
	require.Equal(t, 2, s.sendResponses(s[0].request.EndOffset))

	require.Equal(t, 1, sends) // Sent to 1.
	sends = 0                  // Re-zero for next case.
	require.Nil(t, <-s0a)      // Notified of EOF.
	require.Nil(t, s[0].next)  // Expect child was promoted.

	// Case: Terminal error staged, and sent to 0 & 1.
	for i := range s {
		s[i].response.TerminalError = "foobar"
	}
	require.True(t, s[0].shouldSendResponse()) // Error always sends.
	require.True(t, s[1].shouldSendResponse()) // Error always sends.
	require.Equal(t, 0, s.sendResponses(0))    // No more subscribers.

	require.Equal(t, 2, sends) // Sent to 0 & 1.
	require.Nil(t, <-s0b)      // Notified of EOF.
	require.Nil(t, <-s1)       // Notified of EOF.
}

func TestSubscriberAddCases(t *testing.T) {
	// Case: First subscriber, at offset 0.
	var s = subscribers{{}, {}, {}}

	var sub = subscriber{
		request: pf.ShuffleRequest{
			Config:    pf.ShuffleConfig{Journal: "a/journal"},
			RingIndex: 1,
			Offset:    0,
		},
		doneCh: make(chan error, 1),
	}
	require.Equal(t,
		&pb.ReadRequest{
			Journal: "a/journal",
		}, s.add(sub))

	// Case: First subscriber, at non-zero offset.
	s = subscribers{{}, {}, {}}

	sub.request.Offset = 456
	require.Equal(t,
		&pb.ReadRequest{
			Journal:   "a/journal",
			Offset:    456,
			EndOffset: 0, // Never EOFs.
		}, s.add(sub))

	// Case: Second subscriber, at a lower offset.
	sub.request.RingIndex = 0
	sub.request.Offset = 0
	require.Equal(t,
		&pb.ReadRequest{
			Journal:   "a/journal",
			Offset:    0,
			EndOffset: 456,
		}, s.add(sub))

	// Case: Third subscriber, at a higher Offset, and with an EndOffset.
	sub.request.RingIndex = 2
	sub.request.Offset = 789
	sub.request.EndOffset = 1011
	require.Nil(t, s.add(sub))

	// Case: A second read of an existing subscriber may be added.
	sub = subscriber{
		request: pf.ShuffleRequest{
			Offset:    123,
			EndOffset: 456,
			RingIndex: 1,
		},
		doneCh: make(chan error, 1),
	}
	require.Nil(t, s.add(sub))

	// Expect the prior subscriber at this index was pushed into |next|.
	require.Equal(t, s[1].next, &subscriber{
		request: pf.ShuffleRequest{
			Config:    pf.ShuffleConfig{Journal: "a/journal"},
			Offset:    456,
			RingIndex: 1,
		},
		doneCh: s[1].next.doneCh,
	})
}

func TestSubscriberMinOffset(t *testing.T) {
	var s subscribers

	// Case: Empty list.
	var o, ok = s.minOffset()
	require.Equal(t, pb.Offset(0), o)
	require.Equal(t, false, ok)

	// Case: None initialized.
	s = append(s, subscriber{}, subscriber{}, subscriber{})
	o, ok = s.minOffset()
	require.Equal(t, pb.Offset(0), o)
	require.Equal(t, false, ok)

	// Case: Single entry.
	s[2].doneCh = make(chan error)
	s[2].request.Offset = 123
	o, ok = s.minOffset()
	require.Equal(t, pb.Offset(123), o)
	require.Equal(t, true, ok)

	// Case: Multiple entries.
	s[1].doneCh = make(chan error)
	s[1].request.Offset = 456
	o, ok = s.minOffset()
	require.Equal(t, pb.Offset(123), o)
	require.Equal(t, true, ok)

	// Case: Linked entries.
	s[1].next = &subscriber{
		doneCh:  make(chan error),
		request: pf.ShuffleRequest{Offset: 96},
	}
	o, ok = s.minOffset()
	require.Equal(t, pb.Offset(96), o)
	require.Equal(t, true, ok)

	// Case: Multiple, with zero offset.
	s[0].doneCh = make(chan error)
	s[0].request.Offset = 0
	o, ok = s.minOffset()
	require.Equal(t, pb.Offset(0), o)
	require.Equal(t, true, ok)
}
