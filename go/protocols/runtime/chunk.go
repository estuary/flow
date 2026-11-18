package runtime

import (
	"fmt"
	"io"
)

// Chunked transport of Shard messages too large for one gRPC frame.
// This mirrors the Rust `proto_grpc::chunk` module, which documents the
// protocol: a chunk message sets only `chunk` and `remaining`, the bytes of
// the encoding which follow it.

// MaxFrame is the largest encoded message which Go sends as one frame.
// Go's only chunked peer is the Rust Shard service of its own build, so Go
// chunks from the first release in which both sides can reassemble.
const MaxFrame = 4 << 20

// MaxReassembled is the largest encoded message which may be sent or received
// in chunks.
const MaxReassembled = 1 << 30

// ChunkOverhead bounds the bytes which a chunk message adds around its payload.
const ChunkOverhead = 16

// Chunk is the `chunk` and `remaining` of one chunk message.
type Chunk struct {
	Chunk     []byte
	Remaining uint32
}

// Chunks of `encoded` which send it in frames of at most `maxFrame` bytes.
// An encoding larger than MaxReassembled is sent instead as two one-byte
// sentinels, which fail the receiver's reassembly whatever its limit.
func Chunks(encoded []byte, maxFrame int) []Chunk {
	var sentinel = []byte{0}

	if len(encoded) > MaxReassembled {
		var remaining = uint32(min(len(encoded), int(^uint32(0))))
		return []Chunk{{sentinel, remaining}, {sentinel, 0}}
	}

	var payload = maxFrame - ChunkOverhead
	var chunks []Chunk

	for offset := 0; offset != len(encoded); {
		var end = min(len(encoded), offset+payload)
		chunks = append(chunks, Chunk{encoded[offset:end], uint32(len(encoded) - end)})
		offset = end
	}
	return chunks
}

// Reassembly is the receive-side state of a chunked message.
type Reassembly struct {
	// Bytes of the message received so far, if a chunk sequence is underway.
	buf []byte
	// Bytes which the last chunk said remain.
	remaining uint32
}

type chunked[T any] interface {
	*T
	ProtoSize() int
	Unmarshal([]byte) error
	chunkFields() (*[]byte, *uint32)
}

func (m *Capture) chunkFields() (*[]byte, *uint32)     { return &m.Chunk, &m.Remaining }
func (m *Derive) chunkFields() (*[]byte, *uint32)      { return &m.Chunk, &m.Remaining }
func (m *Materialize) chunkFields() (*[]byte, *uint32) { return &m.Chunk, &m.Remaining }

// RecvChunked calls `recv` until it yields a whole message, reassembling
// chunks into `r`. A stream which ends within a chunk sequence is an error.
func RecvChunked[T any, P chunked[T]](r *Reassembly, recv func() (P, error)) (P, error) {
	for {
		var message, err = recv()
		if err == io.EOF && r.buf != nil {
			return nil, chunkError("stream ended while %d bytes of a chunked %T remain", r.remaining, message)
		} else if err != nil {
			return nil, err
		}

		if message, err = pushChunk(r, message); err != nil || message != nil {
			return message, err
		}
	}
}

// pushChunk pushes a received message into `r`, returning a whole message
// once one is ready.
func pushChunk[T any, P chunked[T]](r *Reassembly, message P) (P, error) {
	var chunkPtr, remainingPtr = message.chunkFields()
	var chunk, remaining = *chunkPtr, *remainingPtr
	*chunkPtr, *remainingPtr = nil, 0

	if len(chunk) == 0 {
		if remaining != 0 {
			return nil, chunkError("%T sets `remaining` (%d) without a `chunk`", message, remaining)
		} else if r.buf != nil {
			return nil, chunkError("received a whole %T while %d bytes of a chunked %T remain", message, r.remaining, message)
		}
		return message, nil
	}

	if message.ProtoSize() != 0 {
		return nil, chunkError("chunk of %T sets fields other than `chunk` and `remaining`", message)
	}

	if r.buf == nil {
		var total = len(chunk) + int(remaining)
		if total > MaxReassembled {
			return nil, fmt.Errorf("received chunked %T of %d bytes, which exceeds the %d-byte limit of chunked messages", message, total, MaxReassembled)
		}
		r.buf = make([]byte, 0, total)
	} else if int(r.remaining)-len(chunk) != int(remaining) {
		return nil, chunkError("chunk of %T having %d bytes and %d remaining doesn't follow its predecessor's %d remaining (the peer may have exceeded its own limit of chunked messages)", message, len(chunk), remaining, r.remaining)
	}
	r.buf = append(r.buf, chunk...)
	r.remaining = remaining

	if remaining != 0 {
		return nil, nil
	}

	var buf = r.buf
	r.buf = nil

	var whole P = new(T)
	if err := whole.Unmarshal(buf); err != nil {
		return nil, chunkError("failed to decode reassembled %T: %s", message, err)
	}
	if chunk, remaining := whole.chunkFields(); len(*chunk) != 0 || *remaining != 0 {
		return nil, chunkError("reassembled %T is itself a chunk", message)
	}
	return whole, nil
}

func chunkError(format string, args ...any) error {
	return fmt.Errorf("chunk protocol error: "+format, args...)
}
