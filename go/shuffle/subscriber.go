package shuffle

import (
	pf "github.com/estuary/flow/go/protocol"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

// subscriber is an active gRPC stream to which shuffled Documents are dispatched.
type subscriber struct {
	// Request of this subscriber.
	request pf.ShuffleRequest
	// Staged, re-used response to be sent to the subscriber.
	response pf.ShuffleResponse
	// sentTailing is true if the previously sent |response| had Tailing set.
	sentTailing bool
	// Compare to gRPC's ServerStream.SendMsg(). We use a method closure to facilitate testing.
	sendMsg func(m interface{}) error
	// Channel to notify gRPC handler of stream completion.
	doneCh chan error
	// Linked list of a next request from this ring member. Typically this is nil,
	// unless this ring member is replaying a segment of the journal.
	next *subscriber
}

// Subscribers is a set of subscriber instances. It's sized to the number of
// ring members, and a given subscriber is indexed by ring index.
type subscribers []subscriber

// stageDoc stages the document into the subscriber-specific response.
func (s *subscriber) stageDoc(staged *pf.ShuffleResponse, doc int) {
	var offset = staged.Begin[doc]

	for cur := s; cur != nil; cur = cur.next {
		if offset >= cur.request.Offset && (cur.request.EndOffset == 0 || offset < cur.request.EndOffset) {
			cur.response.Content = append(cur.response.Content,
				cur.response.Arena.Add(staged.Arena.Bytes(staged.Content[doc])))
			cur.response.Begin = append(cur.response.Begin, offset)
			cur.response.End = append(cur.response.End, staged.End[doc])
			cur.response.UuidParts = append(cur.response.UuidParts, staged.UuidParts[doc])
			cur.response.ShuffleHashesLow = append(cur.response.ShuffleHashesLow, staged.ShuffleHashesLow[doc])
			cur.response.ShuffleHashesHigh = append(cur.response.ShuffleHashesHigh, staged.ShuffleHashesHigh[doc])

			for f := range cur.response.ShuffleKey {
				cur.response.ShuffleKey[f].AppendValue(&staged.Arena, &cur.response.Arena,
					staged.ShuffleKey[f].Values[doc])
			}
		}
	}
}

// shouldSendResponse returns whether the staged |response| is trivial and
// should be filtered from the client's response stream.
func (s *subscriber) shouldSendResponse() bool {
	if len(s.response.UuidParts) != 0 {
		return true
	} else if s.response.TerminalError != "" {
		return true
	} else if s.response.Tailing() && !s.sentTailing {
		return true // Send to inform the client we're now Tailing.
	} else {
		return false
	}
}

// stageResponses distributes Documents of this ShuffleResponse into the staged
// ShuffleResponses of each subscriber.
func (s subscribers) stageResponses(staged *pf.ShuffleResponse, rendezvous *rendezvous) {
	_ = rendezvous.ranks // Elide later nil checks.

	for i := range s {
		// Clear previous staged responses, retaining allocations for re-use.
		// If a TerminalError is set, pass it through to all subscribers.
		for ss := &s[i]; ss != nil; ss = ss.next {
			ss.response = pf.ShuffleResponse{
				TerminalError: staged.TerminalError,
				ReadThrough:   staged.ReadThrough,
				WriteHead:     staged.WriteHead,
				Transform:     staged.Transform,
				ContentType:   staged.ContentType,

				// Truncate per-document slices.
				Arena:             ss.response.Arena[:0],
				Content:           ss.response.Content[:0],
				Begin:             ss.response.Begin[:0],
				End:               ss.response.End[:0],
				UuidParts:         ss.response.UuidParts[:0],
				ShuffleHashesLow:  ss.response.ShuffleHashesLow[:0],
				ShuffleHashesHigh: ss.response.ShuffleHashesHigh[:0],

				// ShuffleKey is a column of values per shuffle key component.
				ShuffleKey: ss.response.ShuffleKey,
			}
			if l := len(staged.ShuffleKey); l != len(ss.response.ShuffleKey) {
				ss.response.ShuffleKey = make([]pf.Field, l)
			}
			for i := range ss.response.ShuffleKey {
				ss.response.ShuffleKey[i] = pf.Field{Values: ss.response.ShuffleKey[i].Values[:0]}
			}
		}
	}
	for doc, uuid := range staged.UuidParts {
		if message.Flags(uuid.ProducerAndFlags) == message.Flag_ACK_TXN {
			// ACK documents are broadcast to all members.
			for i := range s {
				s[i].stageDoc(staged, doc)
			}
		} else {
			// Otherwise, send only to ranked subcribers.
			for _, rank := range rendezvous.pick(staged.ShuffleHashesHigh[doc], uuid.Clock) {
				s[rank.index].stageDoc(staged, doc)
			}
		}
	}
}

// sendResponses sends staged ShuffleResponses to each subscriber, and prunes
// subscribers which have finished. It returns the number of an active subscribers
// which remain on return (and zero if no subscribers remain).
func (s subscribers) sendResponses(readThrough pb.Offset) int {
	var active int

	for i := range s {
		for sub := &s[i]; sub != nil; {
			if sub.doneCh == nil {
				break // No current subscriber at this ring index.
			}

			var err error
			if sub.shouldSendResponse() {
				err = sub.sendMsg(&sub.response)
				sub.sentTailing = sub.response.Tailing()
			}

			// This subscriber is still active if we haven't seen an error, and a requested
			// EndOffset (if present) hasn't been reached.
			if err == nil &&
				sub.response.TerminalError == "" &&
				(sub.request.EndOffset == 0 || readThrough < sub.request.EndOffset) {

				// Update request Offset to reflect |readThrough|.
				if sub.request.Offset < readThrough {
					sub.request.Offset = readThrough
				}
				sub, active = sub.next, active+1
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
	return active
}

// sendEOF notifies all active subscribers of EOF.
func (s subscribers) sendEOF() {
	for i := range s {
		for sub := &s[i]; sub != nil; sub = sub.next {
			if sub.doneCh != nil {
				sub.doneCh <- nil
			}
		}
	}
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
		// A subscriber exists at this ring index already.
		//  Add as a new link in the subscriber list.
		add.next = new(subscriber)
		*add.next = prev
	}

	s[add.request.RingIndex] = add
	return rr
}

// minOffset is the minimum request Offset among active subscribers.
func (s subscribers) minOffset() (offset pb.Offset, ok bool) {
	for i := range s {
		for ss := &s[i]; ss != nil; ss = ss.next {
			if ss.doneCh == nil {
				continue // Not active.
			} else if !ok {
				offset = ss.request.Offset
				ok = true
			} else if ss.request.Offset < offset {
				offset = ss.request.Offset
			}
		}
	}
	return offset, ok
}
