package shuffle

import (
	"fmt"

	pf "github.com/estuary/flow/go/protocol"
	pb "go.gazette.dev/core/broker/protocol"
)

// subscriber is an active gRPC stream to which shuffled Documents are dispatched.
type subscriber struct {
	// Request of this subscriber.
	request pf.ShuffleRequest
	// Staged, re-used response to be sent to the subscriber.
	response pf.ShuffleResponse
	// Compare to gRPC's ServerStream.SendMsg(). We use a method closure to facilitate testing.
	sendMsg func(m interface{}) error
	// Channel to notify gRPC handler of stream completion.
	doneCh chan error
	// Linked-list of a next request from this ring member, which must have an
	// offset at or above the EndOffset of this |request|.
	next *subscriber
}

// Subscribers is a set of subscriber instances. It's sized to the number of
// ring members, and a given subscriber is indexed by it's ring index.
type subscribers []subscriber

// stageDoc stages the Document into the subscriber's response.
func (s *subscriber) stageDoc(doc pf.Document) {
	if s.next != nil && doc.Begin >= s.request.EndOffset {
		s.next.stageDoc(doc)
	} else if doc.Begin >= s.request.Offset {
		s.response.Documents = append(s.response.Documents, doc)
		s.request.Offset = doc.End
	}
}

// stageResponses distributes Documents of this ShuffleResponse into the staged
// ShuffleResponses of each subscriber.
func (s subscribers) stageResponses(response pf.ShuffleResponse) {
	// Clear previous staged responses, retaining slices for re-use.
	// If a TerminalError is set, pass it through to all subscribers.
	for i := range s {
		for sub := &s[i]; sub != nil; sub = sub.next {
			sub.response.Documents = sub.response.Documents[:0]
			sub.response.TerminalError = response.TerminalError
		}
	}
	for _, doc := range response.Documents {
		// ACKs (indicated here by having no shuffles) are broadcast to all members.
		if doc.Shuffles == nil {
			for i := range s {
				s[i].stageDoc(doc)
			}
		} else {
			for _, shuffle := range doc.Shuffles {
				s[shuffle.RingIndex].stageDoc(doc)
			}
		}
	}
}

// sendResponses sends staged ShuffleResponses to each subscriber, and prunes
// subscribers which have finished. It returns true iff an active subscriber
// still remains on return (and false if no subscribers remain).
func (s subscribers) sendResponses() bool {
	var atLeastOneActive = false

	for i := range s {
		for sub := &s[i]; sub != nil; sub = sub.next {
			if sub.doneCh == nil {
				continue // No current subscriber at this ring index.
			}

			// Is this a trivial ShuffleResponse? If so, skip sending.
			if len(sub.response.Documents) == 0 && sub.response.TerminalError == "" {
				atLeastOneActive = true
				continue
			}
			var err = sub.sendMsg(&sub.response)

			// This subscriber is still active if we haven't seen an error, and a requested
			// EndOffset (if present) hasn't been reached.
			if err == nil &&
				sub.response.TerminalError == "" &&
				(sub.request.EndOffset == 0 || sub.request.Offset < sub.request.EndOffset) {
				atLeastOneActive = true
				continue
			}

			// Prune this subscriber.
			sub.doneCh <- err

			if sub.next != nil {
				*sub = *sub.next
			} else {
				*sub = subscriber{}
			}
		}
	}
	return atLeastOneActive
}

// Add a subscriber to the subscribers set. Iff this subscriber requires that
// a new read be started, a corresponding non-nil ReadRequest is returned.
func (s subscribers) add(add subscriber) *pb.ReadRequest {
	var rr *pb.ReadRequest

	// If this is the first subscriber (!ok), start a base read with
	// EndOffset: 0 which will never EOF. Or, if this subscriber has a
	// lower offset than the current minimum, start a read of the difference
	// which will EOF on reaching the prior minimum.
	if offset, ok := s.minOffset(); !ok || add.request.Offset < offset {
		rr = &pb.ReadRequest{
			Journal:   add.request.Config.Journal,
			Offset:    add.request.Offset,
			EndOffset: offset,
		}
	}

	var prev = s[add.request.RingIndex]

	if prev.doneCh != nil {
		// A subscriber exists at this ring index already. This is allowed *if*
		// this request is over a closed, earlier offset range than that reader.
		if add.request.EndOffset == 0 || add.request.EndOffset > prev.request.Offset {
			add.doneCh <- add.sendMsg(&pf.ShuffleResponse{
				TerminalError: fmt.Sprintf(
					"existing subscriber at ring index %d (offset %d) overlaps with request range [%d, %d)",
					add.request.RingIndex, prev.request.Offset, add.request.Offset, add.request.EndOffset),
			})
			return nil
		}
		add.next = new(subscriber)
		*add.next = prev
	} else if add.request.EndOffset != 0 {
		add.doneCh <- add.sendMsg(&pf.ShuffleResponse{
			TerminalError: fmt.Sprintf(
				"unexpected EndOffset %d (no other subscriber at ring index %d)",
				add.request.EndOffset, add.request.RingIndex),
		})
		return nil
	}

	s[add.request.RingIndex] = add
	return rr
}

// minOffset is the minimum request Offset among active subscribers.
func (s *subscribers) minOffset() (offset pb.Offset, ok bool) {
	for _, ss := range *s {
		if ss.doneCh == nil {
			continue
		} else if !ok {
			offset = ss.request.Offset
			ok = true
		} else if offset > ss.request.Offset {
			offset = ss.request.Offset
		}
	}
	return offset, ok
}
