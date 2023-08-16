package shuffle

import (
	"context"
	"fmt"
	"io"
	"math/bits"
	"sort"

	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	pr "github.com/estuary/flow/go/protocols/runtime"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

// subscriber is an active gRPC stream to which shuffled Documents are dispatched.
type subscriber struct {
	// Context of the subscriber.
	ctx context.Context
	// Request of this subscriber.
	pr.ShuffleRequest
	// Unpacked details of how the shuffle is to be performed.
	shuffle shuffle
	// Callback which is invoked with each new ShuffleResponse, or with a final non-nil error.
	// If a callback returns an error, that error is passed back in the final callback.
	callback func(*pr.ShuffleResponse, error) error
	// Next ShuffleResponse to be sent to the subscriber.
	staged *pr.ShuffleResponse
	// sentTailing is true if the previously sent |response| had Tailing set.
	sentTailing bool
}

// stageDoc stages the document into the subscriber-specific response.
func (s *subscriber) stageDoc(response *pr.ShuffleResponse, doc int) {
	var offset = response.Offsets[2*doc]

	if offset >= s.Offset && (s.EndOffset == 0 || offset < s.EndOffset) {
		s.staged.Docs = append(s.staged.Docs,
			s.staged.Arena.Add(response.Arena.Bytes(response.Docs[doc])))
		s.staged.Offsets = append(s.staged.Offsets, offset, response.Offsets[2*doc+1])
		s.staged.UuidParts = append(s.staged.UuidParts, response.UuidParts[doc])
		s.staged.PackedKey = append(s.staged.PackedKey,
			s.staged.Arena.Add(response.Arena.Bytes(response.PackedKey[doc])))
	}
}

// shouldSendResponse returns whether the staged |response| is trivial and
// should be filtered from the client's response stream.
func (s *subscriber) shouldSendResponse() bool {
	if len(s.staged.UuidParts) != 0 {
		return true
	} else if s.staged.TerminalError != "" {
		return true
	} else if s.staged.Tailing() && !s.sentTailing {
		return true // Send to inform the client we're now Tailing.
	} else if s.EndOffset != 0 && s.EndOffset <= s.staged.ReadThrough {
		return true // Completes bounded client read.
	} else {
		return false
	}
}

// Subscribers is a set of subscriber instances. It's ordered on monotonically
// increasing RangeSpec. Sibling subscribers are allowed to have exactly equal RangeSpecs,
// but partially overlapping RangeSpecs are disallowed.
type subscribers []subscriber

// keySpan locates the span of indices having ranges which cover the given key.
func (s subscribers) keySpan(key uint32) (start, stop int) {
	// Find the index of the first subscriber having |key| <= KeyEnd.
	start = sort.Search(len(s), func(i int) bool {
		return key <= s[i].Range.KeyEnd
	})
	// Walk forwards while KeyBegin <= |key|.
	for stop = start; stop != len(s) && s[stop].Range.KeyBegin <= key; stop++ {
	}
	return
}

// insertionIndex returns the index at which a subscriber with the given key
// range could be inserted, or an error if it would result in a partial overlap.
func (s subscribers) insertionIndex(range_ pf.RangeSpec) (int, error) {
	// Find the first |index| having |range_| < subscribers[index].Range.
	// I.e, this is the last index at which |range_| could be inserted.
	var index = sort.Search(len(s), func(i int) bool {
		return range_.Less(&s[i].Range)
	})

	// Ensure left neighbor is less-than or equal to this range.
	if index == 0 {
		// No left neighbor.
	} else if l := index - 1; s[l].Range.Less(&range_) || range_.Equal(&s[l].Range) {
		// Left neighbor ends before |begin| starts.
	} else {
		return 0, fmt.Errorf("range %s overlaps with existing range %s", range_, s[l].Range)
	}

	// Ensure right neighbor doesn't exist, or has |range_| <= subscribers[right].Range.
	if index == len(s) {
		// No right neighbor.
	} else if r := index; range_.Less(&s[r].Range) {
		// Okay: |end| is before neighbor begins.
	} else {
		return 0, fmt.Errorf("range %s overlaps with existing range %s", range_, s[r].Range)
	}
	return index, nil
}

// Rotate a Clock into a high-entropy sequence by shifting the high-60 bits
// of timestamp down by 4, XOR-ed with the low 4 bits of sequence counter,
// and then rotating to a 32-bit result such that the LSB is now the MSB.
func rotateClock(c message.Clock) uint32 {
	return bits.Reverse32(uint32((c >> 4) ^ (c & 0xf)))
}

// stageResponses distributes Documents of this ShuffleResponse into the staged
// ShuffleResponses of each subscriber.
func (s subscribers) stageResponses(from *pr.ShuffleResponse) {
	for i := range s {
		s[i].staged.TerminalError = from.TerminalError
		s[i].staged.ReadThrough = from.ReadThrough
		s[i].staged.WriteHead = from.WriteHead
	}
	for doc, uuid := range from.UuidParts {
		if message.Flags(uuid.Node) == message.Flag_ACK_TXN {
			// ACK documents are always broadcast to every subscriber.
			for i := range s {
				s[i].stageDoc(from, doc)
			}
			continue
		}

		var keyHash = flow.PackedKeyHash_HH64(from.Arena.Bytes(from.PackedKey[doc]))
		var start, stop = s.keySpan(keyHash)
		var rClock = rotateClock(uuid.Clock)

		for i := start; i != stop; i++ {
			// We're not filtering on r-clock values, or we are, but the document's r-clock is within the reader's range.
			var rClockOkay = !s[i].shuffle.filterRClocks || (rClock >= s[i].Range.RClockBegin && rClock < s[i].Range.RClockEnd)
			// The UUID Clock is within the allowed [notBefore, notAfter) range.
			var timeOkay = uuid.Clock >= s[i].shuffle.notBefore && uuid.Clock < s[i].shuffle.notAfter

			if rClockOkay && timeOkay {
				s[i].stageDoc(from, doc)
			}
		}
	}
}

// sendResponses sends staged ShuffleResponses to each subscriber,
// and removes subscribers which have finished or had an error.
func (s *subscribers) sendResponses() {
	var index = 0

	for index != len(*s) {
		var sub = &(*s)[index]
		var err error
		var sent = sub.staged

		if sub.shouldSendResponse() {
			err = sub.callback(sent, nil)
			sub.sentTailing = sent.Tailing()
			sub.staged = newStagedResponse(len(sent.Arena), len(sent.UuidParts))
		} else {
			// Though we're not sending, still poll for context cancellation.
			err = sub.ctx.Err()
		}

		// This subscriber is still active if we haven't seen an error, and a requested
		// EndOffset (if present) hasn't been reached.
		if err == nil &&
			sent.TerminalError == "" &&
			(sub.EndOffset == 0 || sub.EndOffset > sent.ReadThrough) {

			// Update request Offset to reflect |readThrough|.
			if sub.Offset < sent.ReadThrough {
				sub.Offset = sent.ReadThrough
			}
			index++
			continue
		}

		// Inform subscriber of shutdown, and prune.
		if err == nil {
			err = io.EOF
		}
		_ = sub.callback(nil, err)

		// Prune this subscriber.
		*s = append((*s)[:index], (*s)[index+1:]...)
	}
}

// Add a subscriber to the subscribers set. Iff this subscriber requires that
// a new read be started, a corresponding non-nil ReadRequest is returned.
func (s *subscribers) add(add subscriber) *pb.ReadRequest {
	var index, err = s.insertionIndex(add.Range)
	if err != nil {
		add.callback(nil, err)
		return nil
	}

	var rr *pb.ReadRequest
	// If this is the first subscriber (!ok), start a base read with
	// EndOffset: 0 which will never EOF. Or, if this subscriber has a
	// lower offset than the current minimum, start a read of the difference
	// which will EOF on reaching the prior minimum.
	if offset, ok := s.minOffset(); !ok || add.Offset < offset {
		rr = &pb.ReadRequest{
			Journal:   add.Journal,
			Offset:    add.Offset,
			EndOffset: offset,
		}
		if add.shuffle.notBefore != 0 {
			rr.BeginModTime = add.shuffle.notBefore.AsTime().Unix()
		}
	}

	// Allocate initial staged ShuffleResponse.
	add.staged = newStagedResponse(0, 0)
	// Splice |add| into the subscriber list.
	*s = append((*s)[:index], append(subscribers{add}, (*s)[index:]...)...)
	return rr
}

// minOffset is the minimum request Offset among active subscribers.
func (s *subscribers) minOffset() (offset pb.Offset, ok bool) {
	for i := range *s {
		if i == 0 {
			offset = (*s)[i].Offset
			ok = true
		} else if (*s)[i].Offset < offset {
			offset = (*s)[i].Offset
		}
	}
	return offset, ok
}

// prune subscribers which have failed contexts.
func (s *subscribers) prune() {
	var index = 0

	for index != len(*s) {
		var sub = &(*s)[index]

		if err := sub.ctx.Err(); err != nil {
			_ = sub.callback(nil, err)
			*s = append((*s)[:index], (*s)[index+1:]...)
		} else {
			index++
		}
	}
}

// newStagedResponse builds an empty ShuffleResponse with pre-allocated
// slice memory, according to provided estimates of arena & docs utilization.
// It differs from newReadResponse in that it also pre-allocates extracted fields.
func newStagedResponse(arenaEstimate, docsEstimate int) *pr.ShuffleResponse {
	var arenaCap = roundUpPow2(arenaEstimate, arenaCapMin, arenaCapMax)
	var docsCap = roundUpPow2(docsEstimate, docsCapMin, docsCapMax)

	return &pr.ShuffleResponse{
		Arena:     make([]byte, 0, arenaCap),
		Docs:      make([]pf.Slice, 0, docsCap),
		Offsets:   make([]int64, 0, 2*docsCap),
		UuidParts: make([]pf.UUIDParts, 0, docsCap),
		PackedKey: make([]pf.Slice, 0, docsCap),
	}
}

// roundUpPow2 |v| into the next higher power-of-two, subject to a |min| / |max| bound.
func roundUpPow2(v, min, max int) int {
	if v < min {
		return min
	} else if v > max {
		return max
	}

	// https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
	v = (v - 1) | 0x7 // Lower-bound of 8
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++

	return v
}

const (
	arenaCapMin = 4096
	arenaCapMax = 1 << 20 // 1MB
	docsCapMin  = 8
	docsCapMax  = 1024
)
