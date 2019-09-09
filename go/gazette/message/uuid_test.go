package message

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestUUIDProducerUniqueness(t *testing.T) {
	// Sanity check that the distribution of NewProducerID is highly random.
	var set = make(map[ProducerID]struct{}, 1000)
	for i := 0; i != 1000; i++ {
		set[NewProducerID()] = struct{}{}
	}
	assert.Len(t, set, 1000)
}

func TestUUIDClock(t *testing.T) {
	var clock Clock

	// Each Tick produces a new value.
	assert.Equal(t, clock.Tick(), Clock(1))
	assert.Equal(t, clock.Tick(), Clock(2))

	clock.Update(time.Unix(12, 300))

	// Clock representation is precise to 100ns.
	assert.Equal(t, clock, NewClock(time.Unix(12, 399)))
	assert.True(t, clock < NewClock(time.Unix(12, 400)))
	assert.True(t, clock > NewClock(time.Unix(12, 299)))

	// Within a 100ns interval, Clock uses remaining bits to expand the
	// distinguishable sequence.
	assert.Equal(t, clock, NewClock(time.Unix(12, 300)))
	clock.Tick()
	assert.True(t, clock > NewClock(time.Unix(12, 399)))

	// Just 4 bits are available. If they're overflowed, Tick will spill
	// over to update the timestamp.
	for i := 0; i != 14; i++ {
		clock.Tick()
		assert.True(t, clock < NewClock(time.Unix(12, 400)))
	}
	clock.Tick() // 16th tick.
	assert.Equal(t, clock, NewClock(time.Unix(12, 400)))

	// Update must never decrease the clock value.
	clock.Update(time.Unix(11, 100))
	assert.Equal(t, clock, NewClock(time.Unix(12, 400)))

	// That includes sequence bits.
	clock.Tick()
	clock.Update(time.Unix(12, 400))
	assert.True(t, clock > NewClock(time.Unix(12, 400)))

	// Sequence bits are reset if the clock timestamp is updated.
	clock.Update(time.Unix(12, 500))
	assert.Equal(t, clock, NewClock(time.Unix(12, 500)))
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
	assert.Equal(t, clock, GetClock(id))
	assert.Equal(t, producer, GetProducerID(id))
	assert.Equal(t, Flags(expectFlags), GetFlags(id))

	// Verify compatibility with github.com/google/uuid package.
	assert.Equal(t, uuid.Version(1), id.Version())
	assert.Equal(t, uuid.RFC4122, id.Variant())
	assert.Equal(t, producer[:], id.NodeID())
	assert.Equal(t, uuid.Time(clock>>4), id.Time())

	// Expect package extracts our time-point, rounded to 100ns precision.
	var sec, nsec = id.Time().UnixTime()
	assert.Equal(t, int64(expectSecs), sec)
	assert.Equal(t, int64((expectNanos/100)*100), nsec)
}
