package message

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"sync"

	"github.com/gazette/gazette/v2/client"
	"github.com/gazette/gazette/v2/labels"
	pb "github.com/gazette/gazette/v2/protocol"
)

// Publish maps the Message to its target journal and begins an Append of the
// Message's marshaled content under the mapped journal framing. If Message
// implements Validate, the message is first validated and any error returned.
func Publish(broker client.AsyncJournalClient, mapping MappingFunc, msg Message) (*client.AsyncAppend, error) {
	if v, ok := msg.(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return nil, err
		}
	}
	var journal, framing, err = mapping(msg)
	if err != nil {
		return nil, err
	}
	var aa = broker.StartAppend(journal)
	aa.Require(framing.Marshal(msg, aa.Writer()))

	if err = aa.Release(); err != nil {
		return nil, err
	}
	return aa, nil
}

// FramingByContentType returns the Framing having the corresponding |contentType|,
// or returns an error if none match.
func FramingByContentType(contentType string) (Framing, error) {
	switch contentType {
	case labels.ContentType_ProtoFixed:
		return FixedFraming, nil
	case labels.ContentType_JSONLines:
		return JSONFraming, nil
	default:
		return nil, fmt.Errorf(`unrecognized %s (%s)`, labels.ContentType, contentType)
	}
}

// UnpackLine returns bytes through to the first encountered newline "\n". If
// the complete line is in the Reader buffer, no alloc or copy is needed.
func UnpackLine(r *bufio.Reader) ([]byte, error) {
	// Fast path: a line is fully contained in the buffer.
	var line, err = r.ReadSlice('\n')

	if err == bufio.ErrBufferFull {
		// Slow path: the line spills across multiple buffer fills.
		err = nil

		line = append([]byte(nil), line...) // Copy as |line| references an internal buffer.
		var rest []byte

		if rest, err = r.ReadBytes('\n'); err == nil {
			line = append(line, rest...)
		}
	}

	if err == io.EOF && len(line) != 0 {
		// If we read at least one byte, then an EOF is unexpected (it should
		// occur only on whole-message boundaries).
		err = io.ErrUnexpectedEOF
	}
	return line, err
}

// RandomMapping returns a MappingFunc which maps a Message to a randomly
// selected Journal of the PartitionsFunc.
func RandomMapping(partitions PartitionsFunc) MappingFunc {
	return func(msg Message) (journal pb.Journal, framing Framing, err error) {
		var parts = partitions()
		if len(parts.Journals) == 0 {
			err = ErrEmptyListResponse
			return
		}

		var ind = rand.Intn(len(parts.Journals))
		journal = parts.Journals[ind].Spec.Name

		var ct = parts.Journals[ind].Spec.LabelSet.ValueOf(labels.ContentType)
		framing, err = FramingByContentType(ct)
		return
	}
}

// ModuloMapping returns a MappingFunc which maps a Message into a stable
// Journal of the PartitionsFunc, selected via 32-bit FNV-1a of the
// MappingKeyFunc and modulo arithmetic.
func ModuloMapping(key MappingKeyFunc, partitions PartitionsFunc) MappingFunc {
	return func(msg Message) (journal pb.Journal, framing Framing, err error) {
		var parts = partitions()
		if len(parts.Journals) == 0 {
			err = ErrEmptyListResponse
			return
		}

		var h = fnv.New32a()
		_, _ = h.Write(key(msg, make([]byte, 0, 32)))

		var ind = int(h.Sum32()) % len(parts.Journals)
		journal = parts.Journals[ind].Spec.Name

		var ct = parts.Journals[ind].Spec.LabelSet.ValueOf(labels.ContentType)
		framing, err = FramingByContentType(ct)
		return
	}
}

// RendezvousMapping returns a MappingFunc which maps a Message into a stable
// Journal of the PartitionsFunc, selected via 32-bit FNV-1a of the
// MappingKeyFunc and Highest Random Weight (aka "rendezvous") hashing. HRW is
// more expensive to compute than using modulo arithmetic, but is still efficient
// and minimizes reassignments which occur when journals are added or removed.
func RendezvousMapping(key MappingKeyFunc, partitions PartitionsFunc) MappingFunc {
	// We cache hashes derived from ListResponses. So long as the PartitionsFunc
	// result is pointer-equal, derived hashes can be cheaply re-used.
	var lastLR *pb.ListResponse
	var lastHashes []uint32
	var mu sync.Mutex

	var partitionsAndHashes = func() (lr *pb.ListResponse, hashes []uint32) {
		lr = partitions()

		mu.Lock()
		if lr != lastLR {
			// Recompute hashes of each journal name.
			lastLR, lastHashes = lr, make([]uint32, len(lr.Journals))

			for i, journal := range lr.Journals {
				var h = fnv.New32a()
				_, _ = h.Write([]byte(journal.Spec.Name))
				lastHashes[i] = h.Sum32()
			}
		}
		hashes = lastHashes
		mu.Unlock()

		return
	}

	return func(msg Message) (journal pb.Journal, framing Framing, err error) {
		var lr, hashes = partitionsAndHashes()

		if len(lr.Journals) == 0 {
			err = ErrEmptyListResponse
			return
		}

		var h = fnv.New32a()
		_, _ = h.Write(key(msg, make([]byte, 0, 32)))
		var sum = h.Sum32()

		var hrw uint32
		var ind int

		for i := range lr.Journals {
			if w := sum ^ hashes[i]; w > hrw {
				hrw, ind = w, i
			}
		}
		journal = lr.Journals[ind].Spec.Name

		var ct = lr.Journals[ind].Spec.LabelSet.ValueOf(labels.ContentType)
		framing, err = FramingByContentType(ct)
		return
	}
}
