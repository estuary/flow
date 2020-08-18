package shuffle

import (
	"bytes"
	"context"
	"fmt"
	"math/bits"
	"sort"

	pf "github.com/estuary/flow/go/protocol"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

// subscriber is an active gRPC stream to which shuffled Documents are dispatched.
type subscriber struct {
	// Request of this subscriber.
	pf.ShuffleRequest
	// Staged, re-used ShuffleResponse to be sent to the subscriber.
	staged pf.ShuffleResponse
	// sentTailing is true if the previously sent |response| had Tailing set.
	sentTailing bool
	// Compare to gRPC's ServerStream.SendMsg(). We use a method closure to facilitate testing.
	sendMsg func(m interface{}) error
	// Compare to gRPC's ServerStream.Context().
	sendCtx context.Context
	// Channel to notify gRPC handler of stream completion.
	doneCh chan error
}

func (s *subscriber) initStaged(from *pf.ShuffleResponse) {
	// Clear previous staged responses, retaining allocations for re-use.
	// If a TerminalError is set, pass it through to all subscribers.
	s.staged = pf.ShuffleResponse{
		TerminalError: from.TerminalError,
		ReadThrough:   from.ReadThrough,
		WriteHead:     from.WriteHead,
		Transform:     from.Transform,
		ContentType:   from.ContentType,

		// Truncate per-document slices.
		Arena:     s.staged.Arena[:0],
		Content:   s.staged.Content[:0],
		Begin:     s.staged.Begin[:0],
		End:       s.staged.End[:0],
		UuidParts: s.staged.UuidParts[:0],
		PackedKey: s.staged.PackedKey[:0],

		// ShuffleKey is a column of values per shuffle key component.
		ShuffleKey: s.staged.ShuffleKey,
	}
	if l := len(from.ShuffleKey); l != len(s.staged.ShuffleKey) {
		s.staged.ShuffleKey = make([]pf.Field, l)
	}
	for i := range s.staged.ShuffleKey {
		s.staged.ShuffleKey[i] = pf.Field{Values: s.staged.ShuffleKey[i].Values[:0]}
	}
}

// stageDoc stages the document into the subscriber-specific response.
func (s *subscriber) stageDoc(response *pf.ShuffleResponse, doc int) {
	var offset = response.Begin[doc]

	if offset >= s.Offset && (s.EndOffset == 0 || offset < s.EndOffset) {
		s.staged.Content = append(s.staged.Content,
			s.staged.Arena.Add(response.Arena.Bytes(response.Content[doc])))
		s.staged.Begin = append(s.staged.Begin, offset)
		s.staged.End = append(s.staged.End, response.End[doc])
		s.staged.UuidParts = append(s.staged.UuidParts, response.UuidParts[doc])
		s.staged.PackedKey = append(s.staged.PackedKey,
			s.staged.Arena.Add(response.Arena.Bytes(response.PackedKey[doc])))

		for f := range s.staged.ShuffleKey {
			s.staged.ShuffleKey[f].AppendValue(&response.Arena, &s.staged.Arena,
				response.ShuffleKey[f].Values[doc])
		}
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
	} else {
		return false
	}
}

// Subscribers is a set of subscriber instances. It's ordered on ascending
// (KeyBegin, KeyEnd) to allow for binary searches into the subscriber(s) which
// match a given shuffle key.
//
// Subscribers may *exactly* match the (KeyBegin, KeyEnd) of another subscriber,
// but may not partially overlap in their ranges. Put differently, KeyBegin
// and KeyEnd of subscribers are always monotonic.
type subscribers []subscriber

type orderedRanges interface {
	len() int
	getKeyBegin(int) []byte
	getKeyEnd(int) []byte
}

// keySpan locates the span of indices having ranges which cover the given key.
func keySpan(s orderedRanges, k []byte) (start, stop int) {
	// Find the index of the first subscriber having |k| < KeyEnd.
	start = sort.Search(s.len(), func(i int) bool {
		return bytes.Compare(k, s.getKeyEnd(i)) < 0
	})
	// Walk forwards while KeyBegin <= |k|.
	for stop = start; stop != s.len() && bytes.Compare(s.getKeyBegin(stop), k) <= 0; stop++ {
	}
	return
}

// rangeSpan locates the span of indices having ranges which cover the given range.
func rangeSpan(s orderedRanges, begin, end []byte) (start, stop int) {
	// Find the index of the first subscriber having |begin| < KeyEnd.
	start = sort.Search(s.len(), func(i int) bool {
		return bytes.Compare(begin, s.getKeyEnd(i)) < 0
	})
	// Walk forwards while KeyBegin < |end|.
	for stop = start; stop != s.len() && bytes.Compare(s.getKeyBegin(stop), end) < 0; stop++ {
	}
	return
}

// insertionIndex returns the index at which a subscriber with the given key
// range could be inserted, or an error if it would result in a partial overlap.
func insertionIndex(s orderedRanges, begin, end []byte) (int, error) {
	// Find the first |index| having a KeyBegin > |begin|.
	// I.e, this is the last index at which [begin, end) could be inserted.
	var index = sort.Search(s.len(), func(i int) bool {
		return bytes.Compare(begin, s.getKeyBegin(i)) < 0
	})

	// Ensure left neighbor equals this range, or has KeyEnd <= |begin|.
	if index == 0 {
		// No left neighbor.
	} else if l := index - 1; bytes.Equal(begin, s.getKeyBegin(l)) && bytes.Equal(end, s.getKeyEnd(l)) {
		// Exactly equals left neighbor.
	} else if bytes.Compare(s.getKeyEnd(l), begin) <= 0 {
		// Left neighbor ends before |begin| starts.
	} else {
		return 0, fmt.Errorf("range [%q, %q) overlaps with existing range [%q, %q)",
			begin, end, s.getKeyBegin(l), s.getKeyEnd(l))
	}

	// Ensure right neighbor doesn't exist, or has |end| <= KeyBegin.
	if index == s.len() {
		// No right neighbor.
	} else if bytes.Compare(end, s.getKeyBegin(index)) <= 0 {
		// Okay: |end| is before neighbor begins.
	} else {
		return 0, fmt.Errorf("range [%q, %q) overlaps with existing range [%q, %q)",
			begin, end, s.getKeyBegin(index), s.getKeyEnd(index))
	}
	return index, nil
}

func (s subscribers) len() int                 { return len(s) }
func (s subscribers) getKeyBegin(i int) []byte { return s[i].Range.KeyBegin }
func (s subscribers) getKeyEnd(i int) []byte   { return s[i].Range.KeyEnd }

// Rotate a Clock into a high-entropy sequence by:
//  * Shifting the top-60 timestamp bits onto the 4 sequence bits,
//    XOR-ing in the current sequence counter.
func rotateClock(c message.Clock) uint64 {
	return bits.Reverse64(uint64((c >> 4) ^ (c & 0xf)))
}

// stageResponses distributes Documents of this ShuffleResponse into the staged
// ShuffleResponses of each subscriber.
func (s subscribers) stageResponses(from *pf.ShuffleResponse) {
	for i := range s {
		s[i].initStaged(from)
	}
	for doc, uuid := range from.UuidParts {
		if message.Flags(uuid.ProducerAndFlags) == message.Flag_ACK_TXN {
			// ACK documents are always broadcast to every subscriber.
			for i := range s {
				s[i].stageDoc(from, doc)
			}
			continue
		}

		var start, stop = keySpan(s, from.Arena.Bytes(from.PackedKey[doc]))
		var rClock = bits.Reverse64(uint64(uuid.Clock))

		for i := start; i != stop; i++ {
			// Stage to the reader if:
			// * We're not filtering on r-clock values, or
			// * We are, but the document's r-clock is within the reader's range.
			if !s[i].Shuffle.Shuffle.FilterRClocks ||
				rClock >= s[i].Range.RClockBegin && rClock < s[i].Range.RClockEnd {
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

		if sub.shouldSendResponse() {
			err = sub.sendMsg(&sub.staged)
			sub.sentTailing = sub.staged.Tailing()
		} else {
			// Though we're not sending, still poll for context cancellation.
			err = sub.sendCtx.Err()
		}

		// This subscriber is still active if we haven't seen an error, and a requested
		// EndOffset (if present) hasn't been reached.
		if err == nil &&
			sub.staged.TerminalError == "" &&
			(sub.EndOffset == 0 || sub.EndOffset > sub.staged.ReadThrough) {

			// Update request Offset to reflect |readThrough|.
			if sub.Offset < sub.staged.ReadThrough {
				sub.Offset = sub.staged.ReadThrough
			}
			index++
			continue
		}

		// Prune this subscriber.
		sub.doneCh <- err
		*s = append((*s)[:index], (*s)[index+1:]...)
	}
}

// sendEOF notifies all active subscribers of EOF.
func (s subscribers) sendEOF() {
	for i := range s {
		s[i].doneCh <- nil
	}
}

// Add a subscriber to the subscribers set. Iff this subscriber requires that
// a new read be started, a corresponding non-nil ReadRequest is returned.
func (s *subscribers) add(add subscriber) *pb.ReadRequest {
	var index, err = insertionIndex(s, add.Range.KeyBegin, add.Range.KeyEnd)
	if err != nil {
		add.doneCh <- err
		return nil
	}

	var rr *pb.ReadRequest
	// If this is the first subscriber (!ok), start a base read with
	// EndOffset: 0 which will never EOF. Or, if this subscriber has a
	// lower offset than the current minimum, start a read of the difference
	// which will EOF on reaching the prior minimum.
	if offset, ok := s.minOffset(); !ok || add.Offset < offset {
		rr = &pb.ReadRequest{
			Journal:   add.Shuffle.Journal,
			Offset:    add.Offset,
			EndOffset: offset,
		}
	}
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
