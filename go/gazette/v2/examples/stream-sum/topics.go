package stream_sum

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"hash/crc64"
	"io"
	"sync"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
)

// StreamID uniquely identifies a stream.
type StreamID [16]byte

// NewStreamID returns a new, globally unique StreamID.
func NewStreamID() StreamID {
	var id StreamID
	FillPRNG(id[:])
	return id
}

// Chunk is an ordered slice of stream content.
type Chunk struct {
	ID    StreamID // Unique ID of the stream.
	SeqNo int      // Monotonic sequence number. One begins a new stream under this unique StreamID.
	Data  []byte   // Raw data included in the Value. If empty, this is the stream's final chunk.
}

func ChunkMappingKey(m message.Message, b []byte) []byte {
	return append(b, m.(*Chunk).ID[:]...)
}

// Sum represents a partial or final CRC64 sum of a stream.
type Sum struct {
	ID    StreamID // Unique ID of the stream.
	SeqNo int      // SeqNo of last Chunk summed.
	Value uint64   // Computed sum through SeqNo.
}

// Update folds a Chunk into this Sum, returning whether this is the last Chunk of the Stream.
// Update requires that SeqNo be totally ordered, however replays of previous SeqNo are ignored.
func (s *Sum) Update(chunk Chunk) (done bool, err error) {
	if chunk.SeqNo <= s.SeqNo {
		return false, nil // Replay of older message. Ignore.
	} else if chunk.SeqNo > s.SeqNo+1 {
		return true, fmt.Errorf("invalid chunk.SeqNo (%d; sum.SeqNo %d; id %x)",
			chunk.SeqNo, s.SeqNo, chunk.ID)
	} else if chunk.ID != s.ID {
		return true, fmt.Errorf("invalid chunk.ID (%x; sum.ID %x)", chunk.ID, s.ID)
	}

	s.SeqNo = chunk.SeqNo
	s.Value = crc64.Update(s.Value, ecmaTable, chunk.Data)
	return len(chunk.Data) == 0, nil
}

func NewChunkMapping(ctx context.Context, jc pb.JournalClient) (message.MappingFunc, error) {
	var chunkParts, err = client.NewPolledList(context.Background(), jc, time.Minute,
		pb.ListRequest{
			Selector: pb.LabelSelector{
				Include: pb.MustLabelSet("topic", "examples/stream-sum/chunks"),
			},
		})

	if err != nil {
		return nil, err
	}
	return message.ModuloMapping(ChunkMappingKey, chunkParts.List), nil
}

const SumsJournal pb.Journal = "examples/stream-sum/sums"

var SumsMapping message.MappingFunc = func(msg message.Message) (pb.Journal, message.Framing, error) {
	return SumsJournal, message.JSONFraming, nil
}

// FillPRNG generates fast but high-quality random entropy into the provided byte slice.
func FillPRNG(b []byte) []byte {
	var stream = prngSource.Get().(cipher.Stream)
	stream.XORKeyStream(b[:], b[:])
	prngSource.Put(stream)
	return b
}

var prngSource = sync.Pool{
	New: func() interface{} {
		var key [32]byte
		var iv [aes.BlockSize]byte

		// Generate a random AES key and initialization vector.
		for _, b := range [][]byte{key[:], iv[:]} {
			if _, err := io.ReadFull(rand.Reader, b); err != nil {
				panic(err) // rand.Reader should never error.
			}
		}
		if aesCipher, err := aes.NewCipher(key[:]); err != nil {
			panic(err) // Should never error (given correct |key| size).
		} else {
			return cipher.NewCTR(aesCipher, iv[:])
		}
	},
}

var ecmaTable = crc64.MakeTable(crc64.ECMA)
