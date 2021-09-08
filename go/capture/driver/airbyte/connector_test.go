package airbyte

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/estuary/protocols/flow"
	"github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestJSONRecordBreaks(t *testing.T) {
	var all []string

	var s = &jsonOutput{
		newRecord: func() interface{} { return new(string) },
		onDecode: func(i interface{}) error {
			all = append(all, *i.(*string))
			return nil
		},
	}

	var w = func(p string) {
		var n, err = s.Write([]byte(p))
		require.Equal(t, len(p), n)
		require.NoError(t, err)
	}

	var verify = func(v []string) {
		require.Equal(t, v, all)
		all = nil
	}

	// Single line.
	w("\"one\"\n")
	// Multiple writes for one line.
	w("\"two")
	w("three")
	w("four\"\n")
	// Multiple linebreaks in one write.
	w("\"five\"\n\"six\"\n\"seven\"\n")

	verify([]string{"one", "twothreefour", "five", "six", "seven"})

	// Worst-case line breaks.
	w("\"one")
	w("two\"\n\"three\"\n\"four")
	w("five\"\n\"six\"\n\"seven")

	verify([]string{"onetwo", "three", "fourfive", "six"})

	w("eight\"\n\"")
	w("nine\"")
	w("\n")
	verify([]string{"seveneight", "nine"})

	// A Close on a newline is okay.
	require.NoError(t, s.Close())
	// But a Close with partial data errors.
	w("\"extra")
	require.EqualError(t, s.Close(),
		"connector stdout closed without a final newline: \"\\\"extra\"")

	// Attempting to process a too-large message errors.
	var manyOnes = bytes.Repeat([]byte("1"), maxMessageSize/2)

	// First one works (not at threshold yet).
	_, err := s.Write([]byte(manyOnes))
	require.NoError(t, err)
	// Second does not.
	_, err = s.Write([]byte(manyOnes))
	require.EqualError(t, err, "message is too large (8388614 bytes without a newline)")

	// If onDecode errors, it's returned.
	var errFixture = fmt.Errorf("error!")
	s.onDecode = func(i interface{}) error { return errFixture }

	_, err = s.Write([]byte("\"\n"))
	require.Equal(t, errFixture, err)
}

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
	var s = NewConnectorProtoOutput(new(flow.CollectionSpec), func(m proto.Message) error {
		require.Equal(t, m, &flow.CollectionSpec{
			Collection: "a/collection",
			KeyPtrs:    []string{"/a", "/b", "/c"},
		})
		verifyCount++
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
	verifyCount = 0

	// Multiple writes for each of header & message.
	w(fixture.Bytes()[0:1])
	w(fixture.Bytes()[1:3])
	w(fixture.Bytes()[3:10]) // Length is complete.
	require.Equal(t, verifyCount, 0)
	w(fixture.Bytes()[10:15])
	w(fixture.Bytes()[15:]) // Message is complete.
	require.Equal(t, verifyCount, 1)
	verifyCount = 0

	// Multiple messages in a single write.
	var multi = bytes.Repeat(fixture.Bytes(), 9)
	w(multi[:len(multi)/2])
	w(multi[len(multi)/2:])
	require.Equal(t, verifyCount, 9)
	verifyCount = 0

	// Again, but use randomized chunking.
	for len(multi) != 0 {
		var n = rand.Intn(len(multi)) + 1
		w(multi[:n])
		multi = multi[n:]
	}
	require.Equal(t, verifyCount, 9)

	// If the message header is too large, we error
	// (rather than attempting to allocate it).
	var n, err = s.Write([]byte{0xff, 0xff, 0xff, 0xff})
	require.EqualError(t, err, "message is too large: 4294967295")
	require.Equal(t, 0, n)
}

func TestStderrCapture(t *testing.T) {
	var s = new(connectorStderr)

	var n, err = s.Write([]byte("whoops"))
	require.Equal(t, 6, n)
	require.NoError(t, err)
	require.Equal(t, "whoops", s.err.String())

	// Expect it caps the amount of output collected.
	s.Write(bytes.Repeat([]byte("x"), maxStderrBytes))
	require.Equal(t, maxStderrBytes, s.err.Len())
}
