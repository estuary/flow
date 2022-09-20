package connector

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"math/rand"
	"testing"

	"github.com/estuary/flow/go/protocols/flow"
	"github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestProtoRecordBreaks(t *testing.T) {
	var fixture bytes.Buffer
	require.NoError(t, io.NewUint32DelimitedWriter(&fixture, binary.LittleEndian).
		WriteMsg(&flow.CollectionSpec{
			Collection: "a/collection",
			KeyPtrs:    []string{"/a", "/b", "/c"},
		}))

	// Create a decoder of CollectionSpec, which expects to see parsed
	// copies of our fixture, and counts its number of invocations.
	var verifyCount int
	var verifyEmptyCount int
	var s = NewProtoOutput(
		func() proto.Message { return new(flow.CollectionSpec) },
		func(m proto.Message) error {
			if m.String() != "" {
				require.Equal(t, m, &flow.CollectionSpec{
					Collection: "a/collection",
					KeyPtrs:    []string{"/a", "/b", "/c"},
				})
				verifyCount++
			} else {
				verifyEmptyCount++
			}
			return nil
		})

	var w = func(p []byte) {
		var n, err = s.Write(p)
		require.NoError(t, err)
		require.Equal(t, len(p), n)
	}

	// Complete message.
	w(fixture.Bytes())
	require.Equal(t, verifyCount, 1)
	require.Equal(t, verifyEmptyCount, 0)
	verifyCount = 0

	// Multiple writes for each of header & message.
	w(fixture.Bytes()[0:1])
	w(fixture.Bytes()[1:3])
	w(fixture.Bytes()[3:10]) // Length is complete.
	require.Equal(t, verifyCount, 0)
	w(fixture.Bytes()[10:15])
	w(fixture.Bytes()[15:]) // Message is complete.
	require.Equal(t, verifyCount, 1)
	require.Equal(t, verifyEmptyCount, 0)
	verifyCount = 0

	// Multiple messages in a single write.
	var multi = bytes.Repeat(fixture.Bytes(), 9)
	w(multi[:len(multi)/2])
	w(multi[len(multi)/2:])
	require.Equal(t, verifyCount, 9)
	require.Equal(t, verifyEmptyCount, 0)
	verifyCount = 0

	// Again, but use randomized chunking.
	for len(multi) != 0 {
		var n = rand.Intn(len(multi)) + 1
		w(multi[:n])
		multi = multi[n:]
	}
	require.Equal(t, verifyCount, 9)
	require.Equal(t, verifyEmptyCount, 0)
	verifyCount = 0

	// Multiple messages with empty messages.
	var emptyFixture bytes.Buffer
	require.NoError(t, io.NewUint32DelimitedWriter(&emptyFixture, binary.LittleEndian).
		WriteMsg(&flow.CollectionSpec{}))

	w(emptyFixture.Bytes()) // Write an empty message as a whole.
	require.Equal(t, verifyEmptyCount, 1)
	require.Equal(t, verifyCount, 0)

	w(emptyFixture.Bytes()[:2])
	w(emptyFixture.Bytes()[2:]) // Write an empty message(length only) in chunks.
	require.Equal(t, verifyEmptyCount, 2)
	require.Equal(t, verifyCount, 0)

	w(fixture.Bytes()) // Write a non-empty message.
	require.Equal(t, verifyEmptyCount, 2)
	require.Equal(t, verifyCount, 1)

	// If the message header is too large, we error
	// (rather than attempting to allocate it).
	var n, err = s.Write([]byte{0xff, 0xff, 0xff, 0xff})
	require.EqualError(t, err, "go.estuary.dev/E108: message is too large: 4294967295")
	require.Equal(t, 0, n)
}

func TestStderrCapture(t *testing.T) {
	var s = connectorStderr{delegate: ioutil.Discard}

	var n, err = s.Write([]byte("whoops"))
	require.Equal(t, 6, n)
	require.NoError(t, err)
	require.Equal(t, "whoops", s.buffer.String())

	// Expect it caps the amount of output collected.
	s.Write(bytes.Repeat([]byte("x"), maxStderrBytes))
	require.Equal(t, maxStderrBytes, s.buffer.Len())
}
