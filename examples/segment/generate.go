package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"
)

const (
	// Universe of segments from which we sample.
	SegmentCardinality = 10000
	// Universe of vendors, mapped to via modulo from each segment.
	VendorCardinality = 10
	// Zipfian skew of the generated segment distribution. Must be > 1.
	// Smaller values sample more universally from the universe of segments,
	// while larger values increase the relative frequency of some segments
	// over others. Try values in range 1.0001 to 1.5.
	SegmentSkew = 1.1
	// Universe of users from which we sample.
	UserCardinality = 1000000
	// UserStdDevClip is the multiple of the standard deviation by which we
	// scale and clip the distribution. Smaller values sample more universally
	// from the universe of users, while larger values increase the relative
	// frequency of some users over others. Try values in range 5.0 to 20.0.
	UserStdDevClip = 10.0
	// Each event is "add" or "remove" sampled with uniform probability density.
	// Adds are more frequent than removes.
	AddProbability = 0.7
	// Each event is novel or a repetition of previous event, sampled uniformly.
	// Novel events are more frequent, but there are many repeats (e.x. browser reload).
	RepeatProability = 0.4
	// Size of the reservoir from which we select repeated events.
	RepetitionReservoirSize = 20000
)

type sample struct {
	segment, user int
	add           bool
}

func main() {
	var rnd = rand.New(rand.NewSource(8675309))
	var rndSegment = rand.NewZipf(rnd, SegmentSkew, 1, SegmentCardinality)
	var reservoir = make([]sample, 0, RepetitionReservoirSize)

	// We'll generate timestamps seeded from the present time,
	// but uniformly incremented by 10ms with each generated event.
	var now = time.Now()
	var tickCh = time.NewTicker(time.Second)

	var enc = json.NewEncoder(os.Stdout)

	for n := 0; true; n++ {
		var cur sample

		if n == 0 || rnd.Float32() > RepeatProability {
			// Generate a novel sample.
			cur = sample{
				// Sample segment ∈ [0, SegmentCardinality].
				segment: int(rndSegment.Uint64()),
				// Sample from positive half of the normal distribution, then scaled and clip to p ∈ [0, 1].
				// Then, project to user ∈ [0, UserCardinality].
				user: int(UserCardinality * math.Min(1.0, math.Abs(rnd.NormFloat64())/UserStdDevClip)),
				// Sample uniformly ∈ [0, 1] and project to "add" or "remove".
				add: rnd.Float32() < AddProbability,
			}
		} else {
			// Sample from reservoir.
			cur = reservoir[rnd.Intn(len(reservoir))]
		}

		// Update sample reservoir.
		if len(reservoir) == cap(reservoir) {
			reservoir[rnd.Intn(len(reservoir))] = cur
		} else {
			reservoir = append(reservoir, cur)
		}

		// Read a random UUID.
		var uuid [16]byte
		rnd.Read(uuid[:])

		// Maybe update current event time.
		select {
		case now = <-tickCh.C:
		default:
		}

		var event = struct {
			EventID   string `json:"event"`
			Timestamp string `json:"timestamp"`
			User      string `json:"user"`
			Segment   struct {
				Vendor int    `json:"vendor"`
				Name   string `json:"name"`
			} `json:"segment"`
			Remove bool `json:"remove,omitempty"`
		}{
			EventID:   encodeHexUUID(uuid),
			Timestamp: now.Format(time.RFC3339),
			User:      fmt.Sprintf("usr-%06x", cur.user),
			Remove:    !cur.add,
		}

		event.Segment.Vendor = 1 + (cur.segment % VendorCardinality)
		event.Segment.Name = fmt.Sprintf("seg-%X", cur.segment)

		if err := enc.Encode(event); err != nil {
			panic(err)
		}
	}
}

func encodeHexUUID(uuid [16]byte) string {
	var buf [36]byte

	hex.Encode(buf[0:8], uuid[:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], uuid[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], uuid[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], uuid[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:], uuid[10:])

	return string(buf[:])
}
