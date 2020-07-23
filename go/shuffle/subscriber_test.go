package shuffle

import (
	"fmt"
	"testing"

	pf "github.com/estuary/flow/go/protocol"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

func TestSubscriberResponseStaging(t *testing.T) {
	// Document fixtures with Shuffle outcomes, to be staged for sending.
	var docs = []pf.Document{
		{
			Shuffles: []pf.Document_Shuffle{
				{RingIndex: 1, TransformId: 10},
				{RingIndex: 1, TransformId: 20},
				{RingIndex: 2, TransformId: 10}, // Filtered.
			},
			Content: []byte("one"),
			Begin:   100,
			End:     200,
		},
		{
			Content: []byte("two"),
			Begin:   200,
			End:     300,
		},
		{
			Shuffles: []pf.Document_Shuffle{
				{RingIndex: 0, TransformId: 5},
				{RingIndex: 0, TransformId: 6},
				{RingIndex: 1, TransformId: 6},
			},
			Content: []byte("three"),
			Begin:   300,
			End:     400,
		},
	}

	var s = subscribers{
		{
			request: pf.ShuffleRequest{EndOffset: 300},
			next: &subscriber{
				request: pf.ShuffleRequest{Offset: 300},
			},
		},
		{},
		{request: pf.ShuffleRequest{Offset: 200}},
		{request: pf.ShuffleRequest{Offset: 500}},
	}
	s.stageResponses(pf.ShuffleResponse{Documents: docs})

	// Expected staged outcomes.
	require.Equal(t, subscribers{
		{
			request: pf.ShuffleRequest{Offset: 300, EndOffset: 300},
			response: pf.ShuffleResponse{
				Documents: []pf.Document{docs[1]},
			},
			next: &subscriber{
				request: pf.ShuffleRequest{Offset: 400},
				response: pf.ShuffleResponse{
					Documents: []pf.Document{docs[2]},
				},
			},
		},
		{
			request: pf.ShuffleRequest{Offset: 400},
			response: pf.ShuffleResponse{
				Documents: []pf.Document{docs[0], docs[1], docs[2]},
			}},
		{
			request: pf.ShuffleRequest{Offset: 300},
			response: pf.ShuffleResponse{
				// docs[0] matches, but is filtered by the requested offest.
				Documents: []pf.Document{docs[1]},
			}},
		{
			request: pf.ShuffleRequest{Offset: 500},
			// docs[1] is filtered by the requested offset.
		},
	}, s)

	docs = []pf.Document{
		{
			Shuffles: []pf.Document_Shuffle{
				{RingIndex: 0, TransformId: 7},
				{RingIndex: 3, TransformId: 8},
			},
			Content: []byte("four"),
			Begin:   400,
			End:     500,
		},
	}
	// Expect the next staged response clears the prior.
	s.stageResponses(pf.ShuffleResponse{Documents: docs})

	require.Equal(t, subscribers{
		{
			request: pf.ShuffleRequest{Offset: 300, EndOffset: 300},
			response: pf.ShuffleResponse{
				Documents: []pf.Document{},
			},
			next: &subscriber{
				request:  pf.ShuffleRequest{Offset: 500},
				response: pf.ShuffleResponse{Documents: docs},
			},
		},
		{
			request:  pf.ShuffleRequest{Offset: 400},
			response: pf.ShuffleResponse{Documents: []pf.Document{}},
		},
		{
			request:  pf.ShuffleRequest{Offset: 300},
			response: pf.ShuffleResponse{Documents: []pf.Document{}},
		},
		{request: pf.ShuffleRequest{Offset: 500}}, // Still filtered by offset.
	}, s)

	// Expect that a TerminalError is staged to all subscribers.
	var errResponse = pf.ShuffleResponse{TerminalError: "an error"}
	s = subscribers{
		{next: &subscriber{}},
		{},
	}
	s.stageResponses(errResponse)

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
			request: pf.ShuffleRequest{Offset: 0, EndOffset: 200},
			sendMsg: sendMsg,
			doneCh:  s0a,

			next: &subscriber{
				request: pf.ShuffleRequest{Offset: 200},
				doneCh:  s0b,
				sendMsg: sendMsg,
			},
		},
		{
			request: pf.ShuffleRequest{Offset: 100},
			sendMsg: sendMsg,
			doneCh:  s1,
		},
		{
			sendMsg: func(m interface{}) error { return fmt.Errorf("an-error") },
			doneCh:  s2,
		},
	}

	// Case: Message sent to 0, omitted from 1, and causes 2 to error.
	s.stageResponses(pf.ShuffleResponse{
		Documents: []pf.Document{{Begin: 0, End: 100}},
	})
	require.True(t, s.sendResponses())

	require.Equal(t, 1, sends) // Sent to 0 only.
	sends = 0                  // Re-zero for next case.
	require.EqualError(t, <-s2, "an-error")
	require.Nil(t, s[2].doneCh) // Expect reset.

	// Case: Message sent to 0, which completes, and 1.
	s.stageResponses(pf.ShuffleResponse{
		Documents: []pf.Document{{Begin: 100, End: 200}},
	})
	require.True(t, s.sendResponses())

	require.Equal(t, 2, sends) // Sent to 0 & 1.
	sends = 0                  // Re-zero for next case.
	require.Nil(t, <-s0a)      // Notified of EOF.
	require.Nil(t, s[0].next)  // Expect child was promoted.

	// Case: Terminal error sent to 0 & 1.
	s.stageResponses(pf.ShuffleResponse{
		TerminalError: "foobar",
	})
	require.False(t, s.sendResponses()) // No more subscribers.

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

	// Case: Third subscriber, at a higher offset, but with an unexpected endOffset.
	sub.request.RingIndex = 2
	sub.request.Offset = 789
	sub.request.EndOffset = 1011

	sub.sendMsg = func(m interface{}) error {
		require.Equal(t, m.(*pf.ShuffleResponse).TerminalError,
			"unexpected EndOffset 1011 (no other subscriber at ring index 2)")
		return fmt.Errorf("send-err-is-plumbed")
	}
	require.Nil(t, s.add(sub))
	require.EqualError(t, <-sub.doneCh, "send-err-is-plumbed")
	sub.sendMsg = nil

	// Case: Third subscriber again, without an EndOffset.
	sub.request.EndOffset = 0
	require.Nil(t, s.add(sub))

	// Case: Add of subscriber that exists with a conflicting offset range.
	sub = subscriber{
		request: pf.ShuffleRequest{
			Offset:    123,
			RingIndex: 1,
		},
		sendMsg: func(m interface{}) error {
			require.Equal(t, m.(*pf.ShuffleResponse).TerminalError,
				"existing subscriber at ring index 1 (offset 456) overlaps with request range [123, 0)")
			return fmt.Errorf("other-send-err-is-plumbed")
		},
		doneCh: make(chan error, 1),
	}
	require.Nil(t, s.add(sub))
	require.EqualError(t, <-sub.doneCh, "other-send-err-is-plumbed")
	sub.sendMsg = nil

	// Case: A second read of an existing subscriber may be added
	// *if* it's a lower offset range.
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

	// Case: Multiple, with zero offset.
	s[0].doneCh = make(chan error)
	s[0].request.Offset = 0
	o, ok = s.minOffset()
	require.Equal(t, pb.Offset(0), o)
	require.Equal(t, true, ok)
}
