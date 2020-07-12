package shuffle

import (
	"fmt"

	pf "github.com/estuary/flow/go/protocol"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
	"google.golang.org/grpc"
)

// TODO - hoist to ring, and/or invert to drive with extract responses?
func distribute(
	cfg pf.ShuffleConfig,
	rendezvous rendezvous,
	docs []pf.Document,
	uuids []pf.UUIDParts,
	hashes []pf.Hash,
) {
	for d := range docs {
		docs[d].UuidParts = uuids[d]

		if message.Flags(docs[d].UuidParts.ProducerAndFlags) == message.Flag_ACK_TXN {
			// ACK documents have no shuffles, and go to all readers.
			continue
		}
		for h := range hashes {
			docs[d].Shuffles = rendezvous.pick(h,
				hashes[h].Values[d],
				docs[d].UuidParts.Clock,
				docs[d].Shuffles)
		}
	}
}

type subscriber struct {
	request  pf.ShuffleRequest
	response pf.ShuffleResponse
	stream   grpc.ServerStream
	doneCh   chan error
	next     *subscriber
}

func (s *subscriber) stageDoc(doc pf.Document) {
	if s.next != nil && doc.Begin >= s.request.EndOffset {
		s.next.stageDoc(doc)
	} else if doc.Begin >= s.request.Offset {
		s.response.Documents = append(s.response.Documents, doc)
		s.request.Offset = doc.End
	}
}

type subscribers []subscriber

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

func (s subscribers) add(add subscriber) *pb.ReadRequest {
	var rr *pb.ReadRequest

	// If this is the first subscriber (!ok), start a base read with
	// EndOffset: 0 which will never EOF. Or, if this subscriber has a
	// lower offset than the current minimum, start a read of the difference
	// which will EOF on reaching the prior minimum.
	if offset, ok := s.minOffset(); !ok || add.request.Offset < offset {
		rr = &pb.ReadRequest{
			Journal:    add.request.Config.Journal,
			Offset:     add.request.Offset,
			EndOffset:  offset,
			Block:      true,
			DoNotProxy: true,
		}
	}

	var prev = s[add.request.RingIndex]

	if prev.doneCh != nil {
		// A subscriber exists at this ring index already. This is allowed *if*
		// this request is over a closed, earlier offset range than that reader.
		if add.request.EndOffset == 0 || add.request.EndOffset > prev.request.Offset {
			add.doneCh <- fmt.Errorf(
				"existing subscriber at ring index %d (offset %d) overlaps with request range [%d, %d)",
				add.request.RingIndex, prev.request.Offset, add.request.Offset, add.request.EndOffset)
			return nil
		}
		add.next = new(subscriber)
		*add.next = prev
	} else if add.request.EndOffset != 0 {
		add.doneCh <- fmt.Errorf(
			"unexpected EndOffset %d (no other subscriber at ring index %d)",
			add.request.EndOffset, add.request.RingIndex)
		return nil
	}

	s[add.request.RingIndex] = add
	return rr
}

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
