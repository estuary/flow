package message

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestUUIDProducerUniqueness(t *testing.T) {
	// Sanity check that the distribution of NewProducerID is highly random.
	var set = make(map[ProducerID]struct{}, 1000)
	for i := 0; i != 1000; i++ {
		set[NewProducerID()] = struct{}{}
	}
	require.Len(t, set, 1000)
}

func TestUUIDClock(t *testing.T) {
	var clock Clock

	// Each Tick produces a new value, incrementing by 1 microsecond (160 units).
	require.Equal(t, clock.Tick(), Clock(160))
	require.Equal(t, clock.Tick(), Clock(320))

	clock.Update(time.Unix(12, 300))

	// Clock representation is precise to 100ns.
	require.Equal(t, clock, NewClock(time.Unix(12, 399)))
	require.Equal(t, clock.AsTime(), time.Unix(12, 300))
	require.True(t, clock < NewClock(time.Unix(12, 400)))
	require.True(t, clock > NewClock(time.Unix(12, 299)))

	// After a Tick, clock advances by 1 microsecond.
	require.Equal(t, clock, NewClock(time.Unix(12, 300)))
	clock.Tick()
	// Clock should now be at time.Unix(12, 1300) (300ns + 1000ns)
	require.Equal(t, clock.AsTime(), time.Unix(12, 1300))
	require.True(t, clock > NewClock(time.Unix(12, 1299)))
	require.True(t, clock < NewClock(time.Unix(12, 1400)))

	// Update must never decrease the clock value.
	clock.Update(time.Unix(11, 100))
	require.Equal(t, clock.AsTime(), time.Unix(12, 1300))

	// Sequence bits are reset if the clock timestamp is updated.
	clock.Update(time.Unix(12, 5000))
	require.Equal(t, clock, NewClock(time.Unix(12, 5000)))
	require.Equal(t, clock.AsTime(), time.Unix(12, 5000))
}

func TestUUIDBuilding(t *testing.T) {
	var producer = NewProducerID()

	// Craft an interesting Clock fixture which uses the full bit-range
	// and includes clock sequence increments.
	var clock Clock
	const expectSecs, expectNanos = 1567304621, 981273734
	clock.Update(time.Unix(expectSecs, expectNanos))
	clock.Tick()
	clock.Tick()

	const expectFlags = 682 // 0b1010101010

	var id = BuildUUID(producer, clock, expectFlags)
	require.Equal(t, clock, GetClock(id))
	require.Equal(t, producer, GetProducerID(id))
	require.Equal(t, Flags(expectFlags), GetFlags(id))

	// Verify compatibility with github.com/google/uuid package.
	require.Equal(t, uuid.Version(1), id.Version())
	require.Equal(t, uuid.RFC4122, id.Variant())
	require.Equal(t, producer[:], id.NodeID())
	require.Equal(t, uuid.Time(clock>>4), id.Time())

	// Expect package extracts our time-point. After two ticks of 1 microsecond each,
	// we should have added 2000ns to the original time.
	var sec, nsec = id.Time().UnixTime()
	require.Equal(t, int64(expectSecs), sec)
	// Original nanos (981273734) rounded to 100ns (981273700) + 2000ns from 2 ticks = 981275700
	require.Equal(t, int64(981275700), nsec)
}
