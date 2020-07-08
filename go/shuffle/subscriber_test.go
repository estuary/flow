package shuffle

import (
	"testing"

	pf "github.com/estuary/flow/go/protocol"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

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
			Journal:    "a/journal",
			Block:      true,
			DoNotProxy: true,
		}, s.add(sub))

	// Case: First subscriber, at non-zero offset.
	s = subscribers{{}, {}, {}}

	sub.request.Offset = 456
	require.Equal(t,
		&pb.ReadRequest{
			Journal:    "a/journal",
			Offset:     456,
			EndOffset:  0, // Never EOFs.
			Block:      true,
			DoNotProxy: true,
		}, s.add(sub))

	// Case: Second subscriber, at a lower offset.
	sub.request.RingIndex = 0
	sub.request.Offset = 0
	require.Equal(t,
		&pb.ReadRequest{
			Journal:    "a/journal",
			Offset:     0,
			EndOffset:  456,
			Block:      true,
			DoNotProxy: true,
		}, s.add(sub))

	// Case: Third subscriber, at a higher offset.
	sub.request.RingIndex = 2
	sub.request.Offset = 789
	require.Nil(t, s.add(sub))

	// Case: Add of subscriber that exists.
	sub = subscriber{
		request: pf.ShuffleRequest{RingIndex: 0},
		doneCh:  make(chan error, 1),
	}
	require.Nil(t, s.add(sub))
	require.EqualError(t, <-sub.doneCh, "subscriber at ring index 0 already exists")
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
