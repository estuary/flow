package message

import (
	"crypto/rand"
	"encoding/binary"
	"math"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// UUID is a RFC 4122 v1 variant Universally Unique Identifier which uniquely
// identifies each message. As a v1 UUID, it incorporates a clock timestamp
// and sequence, as well as a node identifier (which, within the context of
// Gazette, is also known as a ProducerID).
//
// Each sequence of UUIDs produced by Gazette use a strongly random ProducerID,
// and as such the RFC 4122 purpose of the clock sequence isn't required.
// Instead, Gazette uses clock sequence bits of UUIDs it generates in the
// following way:
//
// * The first 2 bits are reserved to represent the variant, as per RFC 4122.
// * The next 4 bits extend the 60 bit timestamp with a counter, which allows
//   for a per-producer UUID generation rate of 160M UUIDs / second before
//   running ahead of wall-clock time. The timestamp and counter are monotonic,
//   and together provide a total ordering of UUIDs from each ProducerID.
// * The remaining 10 bits are flags, eg for representing transaction semantics.
type UUID = uuid.UUID

// ProducerID is the unique node identifier portion of a v1 UUID.
type ProducerID [6]byte

// NewProducerID returns a cryptographically random ProducerID which is very,
// very likely to be unique (47 bits of entropy, a space of ~141 trillion)
// provided that each ProducerID has a reasonably long lifetime (eg on the
// order of a process, not of a request).
func NewProducerID() ProducerID {
	var i, err = rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(err)
	}
	var id ProducerID
	copy(id[:], i.Bytes())

	// Per RFC 4122, set multicast bit to mark this is not a real MAC address.
	id[0] |= 0x01
	return id
}

// GetProducerID returns the node identifier of a UUID as a ProducerID.
func GetProducerID(uuid UUID) ProducerID {
	var id ProducerID
	copy(id[:], uuid[10:])
	return id
}

// Clock is a v1 UUID 60-bit timestamp (60 MSBs), followed by 4 bits of sequence
// counter. Both the timestamp and counter are monotonic (will never decrease),
// and each Tick increments the Clock. For UUID generation, Clock provides a
// total ordering over UUIDs of a given ProducerID.
type Clock uint64

// NewClock returns a Clock initialized to the given Time.
func NewClock(t time.Time) Clock {
	var c Clock
	c.Update(t)
	return c
}

// Update the Clock given a recent time observation. If |t| has a wall time
// which is less than the current Clock, no update occurs (in order to
// maintain monotonicity). Update is safe for concurrent use.
func (c *Clock) Update(t time.Time) {
	var next = uint64(t.UnixNano()/100+g1582ns100) << 4
	for {
		var prev = atomic.LoadUint64((*uint64)(c))
		if next <= prev || atomic.CompareAndSwapUint64((*uint64)(c), prev, next) {
			break
		}
	}
}

// Tick increments the Clock by one and returns the result.
// It is safe for concurrent use.
func (c *Clock) Tick() Clock {
	return Clock(atomic.AddUint64((*uint64)(c), 1))
}

// GetClock returns the clock timestamp and sequence as a Clock.
func GetClock(uuid UUID) Clock {
	var t = uint64(binary.BigEndian.Uint32(uuid[0:4])) << 4    // Clock low bits.
	t |= uint64(binary.BigEndian.Uint16(uuid[4:6])) << 36      // Clock middle bits.
	t |= uint64(binary.BigEndian.Uint16(uuid[6:8])) << 52      // Clock high bits.
	t |= uint64(binary.BigEndian.Uint16(uuid[8:10])>>10) & 0xf // Clock sequence.
	return Clock(t)
}

// Flags are the 10 least-significant bits of the v1 UUID clock sequence,
// which are reserved for flags.
type Flags uint16

// GetFlags returns the 10 least-significant bits of the clock sequence.
func GetFlags(uuid UUID) Flags {
	return Flags(binary.BigEndian.Uint16(uuid[8:10])) & 0x3ff
}

// BuildUUID builds v1 UUIDs per RFC 4122.
func BuildUUID(id ProducerID, clock Clock, flags Flags) UUID {
	if flags > 0x3ff {
		panic("only 10 low bits may be used for flags")
	}
	var out UUID
	binary.BigEndian.PutUint32(out[0:], uint32(clock>>4))                              // Clock low bits.
	binary.BigEndian.PutUint16(out[4:], uint16(clock>>36))                             // Clock middle bits.
	binary.BigEndian.PutUint16(out[6:], uint16(clock>>52)|0x1000)                      // Clock high bits + version 1.
	binary.BigEndian.PutUint16(out[8:], uint16(clock<<10)&0x3c00|uint16(flags)|0x8000) // Clock sequence + flags + variant 1.
	copy(out[10:], id[:])

	return out
}

const (
	// Flag_OUTSIDE_TXN indicates the message is not a participant in a
	// transaction and should be processed immediately.
	Flag_OUTSIDE_TXN Flags = 0x0
	// Flag_CONTINUE_TXN indicates the message implicitly begins or continues a
	// transaction. The accompanying message should be processed only after
	// reading a Flag_ACK_TXN having a larger clock.
	Flag_CONTINUE_TXN Flags = 0x1
	// Flag_ACK_TXN indicates the message acknowledges the commit of all
	// Flag_CONTINUE_TXN messages before it and having smaller clocks, allowing
	// those messages to be processed.
	//
	// A Flag_ACK_TXN may have a clock *earlier* than prior Flag_CONTINUE_TXNs,
	// in which case those Messages are to be considered "rolled back" and should
	// be discarded without processing.
	//
	// A read Flag_ACK_TXN clock should generally not be less than a prior read
	// Flag_ACK_TXN, as each such message is confirmed to have committed before
	// the next is written. Should the clock be less, it indicates that an
	// upstream store checkpoint was rolled-back to a prior version (eg, due to
	// disaster or missing WAL). When this happens, the upstream producer will
	// re-process some number of messages, and may publish Messages under new
	// UUIDs which partially or wholly duplicate messages published before.
	// In other words, the processing guarantee in this case is weakened from
	// exactly-once to at-least-once until the upstream producer catches up to
	// the progress of the furthest checkpoint ever achieved.
	Flag_ACK_TXN Flags = 0x2

	// g1582ns100 is the time interval between 15 Oct 1582 (RFC 4122)
	// and 1 Jan 1970 (Unix epoch), in units of 100 nanoseconds.
	g1582ns100 = 122192928000000000
)
