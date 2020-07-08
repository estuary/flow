package shuffle

import (
	"fmt"

	pf "github.com/estuary/flow/go/protocol"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc"
)

type subscriber struct {
	request  pf.ShuffleRequest
	response pf.ShuffleResponse
	stream   grpc.ServerStream
	doneCh   chan error
}

type subscribers []subscriber

/*
func distribute(
	cfg pf.ShuffleConfig,
	subscribers subscribers,
	rendezvous rendezvous,
	docs []pf.Document,
	extracts []pf.ExtractResponse,
) {
	for s := range subscribers {
		subscribers[s].response.Documents = subscribers[s].response.Documents[:0]
	}

	var ranks []rank

	for d := range docs {
		docs[d].UuidParts = extracts[0].Documents[d].UuidParts

		if message.Flags(docs[d].UuidParts.ProducerAndFlags) == message.Flag_ACK_TXN {
			// ACK documents go to all subscribers, with no transforms marked.
			for s := range subscribers {
				subscribers[s].response.Documents = append(
					subscribers[s].response.Documents, docs[d])
			}
			continue
		}

		// Collect rankings of this document.
		ranks = ranks[:0]

		for e := range extracts {
			ranks = rendezvous.pick(e,
				uint32(extracts[e].Documents[d].HashKey),
				docs[d].UuidParts.Clock,
				ranks)
		}
		sort.Slice(ranks, func(i, j int) bool {
			return ranks[i].ind < ranks[j].ind
		})

		var ind int32
		var transformIds []int64

		for _, rank := range ranks {
			// Did we finish collecting all transforms of the prior index?
			if rank.ind != ind {
				if transformIds != nil {
					var doc = docs[d]
					doc.TransformIds = transformIds

					subscribers[ind].response.Documents = append(
						subscribers[ind].response.Documents, doc)
				}
				ind, transformIds = rank.ind, nil
			}
			transformIds = append(transformIds, rank.shuffle)
		}
		var doc = docs[d]
		doc.TransformIds = transformIds

		subscribers[ind].response.Documents = append(
			subscribers[ind].response.Documents, doc)

	}

}
*/

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
