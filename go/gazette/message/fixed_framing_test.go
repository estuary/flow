package message

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestFixedFramingMarshalWithFixtures(t *testing.T) {
	var buf bytes.Buffer
	var bw = bufio.NewWriter(&buf)

	assert.NoError(t, FixedFraming.Marshal(frameablestring("test message content"), bw))
	_ = bw.Flush()
	assert.Equal(t, []byte{0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't'}, buf.Bytes())

	// Append another message.
	assert.NoError(t, FixedFraming.Marshal(frameablestring("foo message"), bw))
	_ = bw.Flush()
	assert.Equal(t, []byte{0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't',
		0x66, 0x33, 0x93, 0x36, 0xb, 0x0, 0x0, 0x0, 'f', 'o', 'o',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e'}, buf.Bytes())
}

func TestFixedFramingMarshalError(t *testing.T) {
	assert.EqualError(t, FixedFraming.Marshal(frameableerror("test message"), nil), "error!")
}

func TestFixedFramingEncodeWithFixtures(t *testing.T) {
	var b, err = FixedFraming.Encode(frameablestring("foo"), nil)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0x66, 0x33, 0x93, 0x36, 0x03, 0x0, 0x0, 0x0, 'f', 'o', 'o'}, b)

	// Expect a following message is appended to |b|.
	b, err = FixedFraming.Encode(frameablestring("bar"), b)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0x66, 0x33, 0x93, 0x36, 0x03, 0x0, 0x0, 0x0, 'f', 'o', 'o',
		0x66, 0x33, 0x93, 0x36, 0x03, 0x0, 0x0, 0x0, 'b', 'a', 'r'}, b)

	// New message with a truncated buffer.
	bb, err := FixedFraming.Encode(frameablestring("baz"), b[:0])
	assert.NoError(t, err)
	assert.Equal(t, []byte{0x66, 0x33, 0x93, 0x36, 0x03, 0x0, 0x0, 0x0, 'b', 'a', 'z'}, bb)

	assert.Equal(t, &b[0], &bb[0]) // Expect it didn't re-allocate.
}

func TestFixedFramingDecodeWithFixture(t *testing.T) {
	var fixture = []byte{
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't',
		'e', 'x', 't', 'r', 'a'}

	var frame, err = FixedFraming.Unpack(testReader(fixture))
	assert.NoError(t, err)
	assert.Equal(t, fixture[:len(fixture)-len("extra")], frame)

	var msg frameablestring
	assert.NoError(t, FixedFraming.Unmarshal(frame, &msg))
	assert.Equal(t, "test message content", string(msg))
}

func TestFixedFramingDecodingError(t *testing.T) {
	var fixture = []byte{
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't'}

	var frame, err = FixedFraming.Unpack(testReader(fixture))
	assert.NoError(t, err)
	assert.Equal(t, fixture, frame)

	var msg frameableerror
	assert.EqualError(t, FixedFraming.Unmarshal(frame, &msg), "error!")
	assert.Equal(t, "test messa", string(msg))
}

func TestFixedFramingDecodeTrailingNewLine(t *testing.T) {
	var fixture = []byte{
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't',
		'\n'}

	var bufReader = testReader(fixture)
	var frame, err = FixedFraming.Unpack(bufReader)
	assert.NoError(t, err)
	assert.Equal(t, fixture[:len(fixture)-1], frame)

	var msg frameablestring
	assert.NoError(t, FixedFraming.Unmarshal(frame, &msg))
	assert.Equal(t, "test message content", string(msg))

	// Read offset should now be at the \n, ensure this returns an EOF.
	_, err = FixedFraming.Unpack(bufReader)
	assert.Equal(t, io.EOF, err)
}

func TestFixedFramingDesyncHandling(t *testing.T) {
	var fixture = []byte{'f', 'o', 'o', 'b', 'a', 'r',
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't'}

	var r = testReader(fixture)

	// Expect leading invalid range is returned on first unpack.
	var frame, err = FixedFraming.Unpack(r)
	assert.NoError(t, err)
	assert.Equal(t, []byte{'f', 'o', 'o', 'b', 'a', 'r'}, frame)

	// Attempting to unmarshal returns an error.
	var msg frameablestring
	assert.Equal(t, ErrDesyncDetected, FixedFraming.Unmarshal(frame, &msg))

	// However, the next frame may be unpacked and unmarshaled.
	frame, err = FixedFraming.Unpack(r)
	assert.NoError(t, err)
	assert.NoError(t, FixedFraming.Unmarshal(frame, &msg))
	assert.Equal(t, "test message content", string(msg))
}

func TestFixedFramingIncompleteBufferHandling(t *testing.T) {
	var fixture = []byte{'f', 'o', 'o',
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't'}

	// EOF at first byte.
	var _, err = FixedFraming.Unpack(testReader(fixture[3:3]))
	assert.Equal(t, io.EOF, err)

	// EOF partway through header.
	_, err = FixedFraming.Unpack(testReader(fixture[3:10]))
	assert.Equal(t, io.ErrUnexpectedEOF, errors.Cause(err))

	// EOF while scanning for de-sync'd header.
	_, err = FixedFraming.Unpack(testReader(fixture[0:6]))
	assert.Equal(t, io.ErrUnexpectedEOF, errors.Cause(err))

	// EOF just after reading complete header.
	_, err = FixedFraming.Unpack(testReader(fixture[3:11]))
	assert.Equal(t, io.ErrUnexpectedEOF, errors.Cause(err))

	// EOF partway through the message.
	_, err = FixedFraming.Unpack(testReader(fixture[3:22]))
	assert.Equal(t, io.ErrUnexpectedEOF, errors.Cause(err))

	// Full message. Success.
	_, err = FixedFraming.Unpack(testReader(fixture[3:]))
	assert.NoError(t, err)
}

func TestFixedFramingUnderflowReadingDesyncHeader(t *testing.T) {
	var fixture = append(bytes.Repeat([]byte{'x'}, 13+8),
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't')
	var r = bufio.NewReaderSize(bytes.NewReader(fixture), 16)

	// Reads and returns 13 bytes of desync'd content, with no magic word.
	var b, err = FixedFraming.Unpack(r)
	assert.NoError(t, err)
	assert.Equal(t, fixture[0:13], b)

	// Next read returns only 8 bytes, because the following bytes are valid header.
	b, err = FixedFraming.Unpack(r)
	assert.NoError(t, err)
	assert.Equal(t, fixture[13:13+8], b)

	// Final read returns a full frame.
	b, err = FixedFraming.Unpack(r)
	assert.NoError(t, err)
	assert.Equal(t, fixture[13+8:], b)
}

func testReader(t []byte) *bufio.Reader {
	// Using a small buffered reader forces the message content Peek
	// underflow handling / ReadFull path.
	switch os.Getpid() % 2 {
	case 1:
		return bufio.NewReaderSize(bytes.NewReader(t), 16) // Minimum size bufio.Reader allows.
	default:
		return bufio.NewReader(bytes.NewReader(t))
	}
}

// Marshals and parses strings.
type frameablestring string

func (f frameablestring) MarshalTo(buf []byte) (int, error) {
	return copy(buf, f), nil
}

func (f frameablestring) ProtoSize() int {
	return len(f)
}

func (f *frameablestring) Unmarshal(buf []byte) error {
	*f = frameablestring(buf)
	return nil
}

// Marshals and parses half a message, then fails.
type frameableerror string

func (f frameableerror) MarshalTo(buf []byte) (int, error) {
	return copy(buf[:len(buf)/2], f), errors.New("error!")
}

func (f frameableerror) ProtoSize() int {
	return len(f)
}

func (f *frameableerror) Unmarshal(buf []byte) error {
	*f = frameableerror(buf[:len(buf)/2])
	return errors.New("error!")
}
