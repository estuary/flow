package runtime

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func chunkFixture(specLen int) *Materialize {
	return &Materialize{Task: &Task{Spec: []byte(strings.Repeat("s", specLen)), SqliteVfsUri: "file://vfs"}}
}

func chunkFrames(t *testing.T, message *Materialize, maxFrame int) []*Materialize {
	var encoded, err = message.Marshal()
	require.NoError(t, err)

	var frames []*Materialize
	for _, chunk := range Chunks(encoded, maxFrame) {
		frames = append(frames, &Materialize{Chunk: chunk.Chunk, Remaining: chunk.Remaining})
	}
	return frames
}

// reassemble frames, returning whole messages and the terminal error (if not io.EOF).
func reassemble(frames []*Materialize) ([]*Materialize, error) {
	var reassembly Reassembly
	var recv = func() (*Materialize, error) {
		if len(frames) == 0 {
			return nil, io.EOF
		}
		var next = frames[0]
		frames = frames[1:]
		return next, nil
	}

	var out []*Materialize
	for {
		var message, err = RecvChunked(&reassembly, recv)
		if err == io.EOF {
			return out, nil
		} else if err != nil {
			return out, err
		}
		out = append(out, message)
	}
}

func TestChunkRoundTrip(t *testing.T) {
	var message = chunkFixture(100)
	var size = message.ProtoSize()

	// Chunks always chunks. At `size + ChunkOverhead` the message fits one
	// chunk, one byte less takes two, and 64 takes several.
	var describe []string
	for _, maxFrame := range []int{size + ChunkOverhead, size + ChunkOverhead - 1, 64} {
		var frames = chunkFrames(t, message, maxFrame)
		describe = append(describe, fmt.Sprintf("maxFrame %d:", maxFrame))

		for _, frame := range frames {
			require.LessOrEqual(t, frame.ProtoSize(), maxFrame)
			describe = append(describe, fmt.Sprintf("  chunk: %d bytes, remaining: %d, frame: %d bytes",
				len(frame.Chunk), frame.Remaining, frame.ProtoSize()))
		}

		// Interleave with whole messages.
		var small = chunkFixture(1)
		var out, err = reassemble(append(append([]*Materialize{small}, frames...), small))
		require.NoError(t, err)
		require.Equal(t, []*Materialize{small, message, small}, out)
	}
	cupaloy.SnapshotT(t, strings.Join(describe, "\n"))
}

func TestChunkErrors(t *testing.T) {
	var chunks = chunkFrames(t, chunkFixture(100), 64)
	require.Len(t, chunks, 3)

	var cases = []struct {
		name   string
		frames []*Materialize
	}{
		{"other fields", []*Materialize{chunks[0], {Chunk: chunks[1].Chunk, Remaining: chunks[1].Remaining, Stop: &Stop{}}}},
		{"interleaved", []*Materialize{chunks[0], chunkFixture(1)}},
		{"truncated", chunks[:2]},
		{"dropped", []*Materialize{chunks[0], chunks[2]}},
		{"stray remaining", []*Materialize{{Remaining: 7}}},
		{"nested", chunkFrames(t, chunks[0], 32)},
		{"sentinels", []*Materialize{{Chunk: []byte{0}, Remaining: 1 << 30}, {Chunk: []byte{0}}}},
	}

	var describe []string
	for _, tc := range cases {
		// Copy frames, as RecvChunked clears the chunk fields of each.
		var frames []*Materialize
		for _, frame := range tc.frames {
			var copied = *frame
			frames = append(frames, &copied)
		}
		var _, err = reassemble(frames)
		require.Error(t, err, tc.name)
		describe = append(describe, fmt.Sprintf("%s: %s", tc.name, err))
	}
	cupaloy.SnapshotT(t, strings.Join(describe, "\n"))
}

func TestChunkOversizeSentinels(t *testing.T) {
	var chunks = Chunks(make([]byte, MaxReassembled+1), MaxFrame)
	require.Equal(t, []Chunk{{[]byte{0}, MaxReassembled + 1}, {[]byte{0}, 0}}, chunks)
}
