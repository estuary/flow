package connector

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/estuary/protocols/flow"
	"github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestJSONRecordBreaks(t *testing.T) {
	var all []string
	var errors []string

	var s = &jsonOutput{
		newRecord: func() interface{} { return new(string) },
		onDecodeSuccess: func(i interface{}) error {
			all = append(all, *i.(*string))
			return nil
		},
		onDecodeError: func(b []byte, _ error) error {
			errors = append(errors, string(b))
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
	var verifyErrors = func(expect ...string) {
		require.Equal(t, expect, errors)
		errors = nil
	}

	// Single line.
	w("\"one\"\n")
	// Multiple writes for one line.
	w("\"two")
	w("three")
	w("four\"\n")
	// a line that can't be parsed into a string
	w("123.45\n")
	// Multiple linebreaks in one write.
	w("\"five\"\n\"six\"\n\"seven\"\n")

	verify([]string{"one", "twothreefour", "five", "six", "seven"})
	verifyErrors("123.45")

	// Worst-case line breaks.
	w("\"one")
	w("two\"\n\"three\"\n\"four")
	w("five\"\n\"six\"\n\"seven")

	verify([]string{"onetwo", "three", "fourfive", "six"})

	w("eight\"\n\"")
	w("nine\"")
	w("\n")
	verify([]string{"seveneight", "nine"})

	// Invalid json in the middle of valid json. This is kind of a weird corner case, where the
	// entire line will get logged because the invalid portion was in the middle of it.
	w("\"uno\"dos\"tres\"\n")
	verify([]string{"uno"})
	verifyErrors("\"uno\"dos\"tres\"")

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
	s.onDecodeSuccess = func(i interface{}) error { return errFixture }

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
	require.EqualError(t, err, "message is too large: 4294967295")
	require.Equal(t, 0, n)
}

func TestFIFOFiles(t *testing.T) {
	// Verify the garden path of a ready reader.
	var path = filepath.Join(t.TempDir(), "test-fifo")
	require.NoError(t, unix.Mkfifo(path, 0644))

	var wg sync.WaitGroup
	var recovered string

	wg.Add(1)
	go func() {
		defer wg.Done()

		var f, err = os.OpenFile(path, os.O_RDONLY, os.ModeNamedPipe)
		require.NoError(t, err)

		b, err := ioutil.ReadAll(f)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		recovered = string(b)
	}()

	var input = []byte("hello")
	require.NoError(t, fifoSend(path, input, time.Minute))
	wg.Wait()
	require.Equal(t, "hello", recovered)

	// Expect input was zeroed.
	require.Equal(t, []byte{0, 0, 0, 0, 0}, input)

	// Again, but this time there is no reader.
	// Expect fifoSend doesn't block and returns an error.
	input = []byte("world")
	require.Regexp(t, "writing to FIFO: write .*: i\\/o timeout",
		fifoSend(path, input, time.Millisecond).Error())

	// Input was zeroed on error as well.
	require.Equal(t, []byte{0, 0, 0, 0, 0}, input)
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
