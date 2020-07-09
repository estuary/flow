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
	extracts []pf.ExtractResponse,
) {
	for d := range docs {
		docs[d].UuidParts = extracts[0].Documents[d].UuidParts

		if message.Flags(docs[d].UuidParts.ProducerAndFlags) == message.Flag_ACK_TXN {
			// ACK documents have no shuffles, and go to all readers.
			continue
		}
		for e := range extracts {
			docs[d].Shuffles = rendezvous.pick(e,
				uint32(extracts[e].Documents[d].HashKey),
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
}

func (s *subscriber) stageDoc(doc pf.Document) {
	if s.request.Offset <= doc.Begin {
		s.response.Documents = append(s.response.Documents, doc)
		s.request.Offset = doc.End
	}
}

type subscribers []subscriber

func (s subscribers) stageResponses(response pf.ShuffleResponse) {
	// Clear previous staged responses, retaining slices for re-use.
	// Also pass-through any terminal error to all subscribers.
	for ind := range s {
		s[ind].response.Documents = s[ind].response.Documents[:0]
		s[ind].response.TerminalError = response.TerminalError
	}
	for _, doc := range response.Documents {
		// ACKs (indicated here by having no shuffles) are broadcast to all members.
		if doc.Shuffles == nil {
			for ind := range s {
				s[ind].stageDoc(doc)
			}
			continue
		}
		// Add each document to each shuffled member -- but only add once
		// (a doc may have multiple transforms for a single ring member).
		var last uint32 = uint32(len(s))
		for ind, shuffle := range doc.Shuffles {
			if ind != 0 && doc.Shuffles[ind].Less(doc.Shuffles[ind-1]) {
				panic("shuffles are not ordered")
			}
			if shuffle.RingIndex != last {
				s[shuffle.RingIndex].stageDoc(doc)
				last = shuffle.RingIndex
			}
		}
	}
}

func (s *subscribers) add(sub subscriber) *pb.ReadRequest {
	if (*s)[sub.request.RingIndex].doneCh != nil {
		sub.doneCh <- fmt.Errorf("subscriber at ring index %d already exists",
			sub.request.RingIndex)
		return nil
	}
	var rr *pb.ReadRequest

	// If this is the first subscriber (!ok), start a base read with
	// EndOffset: 0 which will never EOF. Or, if this subscriber has a
	// lower offset than the current minimum, start a read of the difference
	// which will EOF on reaching the prior minimum.
	if offset, ok := s.minOffset(); !ok || sub.request.Offset < offset {
		rr = &pb.ReadRequest{
			Journal:    sub.request.Config.Journal,
			Offset:     sub.request.Offset,
			EndOffset:  offset,
			Block:      true,
			DoNotProxy: true,
		}
	}
	(*s)[sub.request.RingIndex] = sub

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
