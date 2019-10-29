package message

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"go.gazette.dev/core/labels"
)

func TestFixedFramingMarshalWithFixtures(t *testing.T) {
	var buf bytes.Buffer
	var bw = bufio.NewWriter(&buf)
	var f, _ = FramingByContentType(labels.ContentType_ProtoFixed)

	assert.NoError(t, f.Marshal(&frameablestring{"test message content"}, bw))
	_ = bw.Flush()
	assert.Equal(t, []byte{0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't'}, buf.Bytes())

	// Append another message.
	assert.NoError(t, f.Marshal(&frameablestring{"foo message"}, bw))
	_ = bw.Flush()
	assert.Equal(t, []byte{0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't',
		0x66, 0x33, 0x93, 0x36, 0xb, 0x0, 0x0, 0x0, 'f', 'o', 'o',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e'}, buf.Bytes())
}

func TestFixedFramingMarshalError(t *testing.T) {
	var f, _ = FramingByContentType(labels.ContentType_ProtoFixed)
	assert.EqualError(t, f.Marshal(&frameableerror{"test message"}, nil), "error!")
}

func TestFixedFramingEncodeWithFixtures(t *testing.T) {
	var b, err = EncodeFixedProtoFrame(&frameablestring{"foo"}, nil)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0x66, 0x33, 0x93, 0x36, 0x03, 0x0, 0x0, 0x0, 'f', 'o', 'o'}, b)

	// Expect a following message is appended to |b|.
	b, err = EncodeFixedProtoFrame(&frameablestring{"bar"}, b)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0x66, 0x33, 0x93, 0x36, 0x03, 0x0, 0x0, 0x0, 'f', 'o', 'o',
		0x66, 0x33, 0x93, 0x36, 0x03, 0x0, 0x0, 0x0, 'b', 'a', 'r'}, b)

	// New message with a truncated buffer.
	bb, err := EncodeFixedProtoFrame(&frameablestring{"baz"}, b[:0])
	assert.NoError(t, err)
	assert.Equal(t, []byte{0x66, 0x33, 0x93, 0x36, 0x03, 0x0, 0x0, 0x0, 'b', 'a', 'z'}, bb)

	assert.Equal(t, &b[0], &bb[0]) // Expect it didn't re-allocate.
}

func TestFixedFramingDecodeWithFixture(t *testing.T) {
	var f, _ = FramingByContentType(labels.ContentType_ProtoFixed)
	var fixture = []byte{
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't',
		'e', 'x', 't', 'r', 'a'}

	var br = testReader(fixture)
	var msg frameablestring

	assert.NoError(t, f.NewUnmarshalFunc(br)(&msg))
	assert.Equal(t, "test message content", msg.s)
	var extra, _ = ioutil.ReadAll(br) // Expect the precise frame was consumed.
	assert.Equal(t, "extra", string(extra))
}

func TestFixedFramingDecodingError(t *testing.T) {
	var f, _ = FramingByContentType(labels.ContentType_ProtoFixed)
	var fixture = []byte{
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't'}

	var br = testReader(fixture)
	var msg frameableerror

	assert.EqualError(t, f.NewUnmarshalFunc(br)(&msg), "error!")
	assert.Equal(t, "test messa", msg.s)
}

func TestFixedFramingDecodeTrailingNewLine(t *testing.T) {
	var f, _ = FramingByContentType(labels.ContentType_ProtoFixed)
	var fixture = []byte{
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't',
		'\n'}

	var msg frameablestring
	var br = testReader(fixture)
	var unmarshal = f.NewUnmarshalFunc(br)

	assert.NoError(t, unmarshal(&msg))
	assert.Equal(t, "test message content", msg.s)

	// Read offset should now be at the \n.
	var b, _ = br.Peek(1)
	assert.Equal(t, []byte("\n"), b)
	// Expect this returns an EOF.
	assert.Equal(t, io.EOF, unmarshal(&msg))
}

func TestFixedFramingDesyncDetection(t *testing.T) {
	var f, _ = FramingByContentType(labels.ContentType_ProtoFixed)
	var fixture = []byte{'f', 'o', 'o', 'b', 'a', 'r',
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't'}

	var r = testReader(fixture)
	var msg frameablestring

	// Expect leading invalid range is returned on first unpack.
	var frame, err = UnpackFixedFrame(r)
	assert.Equal(t, ErrDesyncDetected, err)
	assert.Equal(t, []byte{'f', 'o', 'o', 'b', 'a', 'r'}, frame)

	// However, the next frame may be unpacked and unmarshaled.
	assert.NoError(t, f.NewUnmarshalFunc(r)(&msg))
	assert.Equal(t, "test message content", msg.s)
}

func TestFixedFramingIncompleteBufferHandling(t *testing.T) {
	var f, _ = FramingByContentType(labels.ContentType_ProtoFixed)
	var fixture = []byte{'f', 'o', 'o',
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't'}

	var msg frameablestring

	// EOF at first byte.
	assert.Equal(t, io.EOF,
		f.NewUnmarshalFunc(testReader(fixture[3:3]))(&msg))

	// EOF partway through header.
	assert.EqualError(t, f.NewUnmarshalFunc(testReader(fixture[3:10]))(&msg),
		"Peek(FixedFrameHeaderLength): unexpected EOF")

	// EOF partway through de-sync'd header.
	assert.EqualError(t, f.NewUnmarshalFunc(testReader(fixture[0:7]))(&msg),
		"Peek(FixedFrameHeaderLength): unexpected EOF")

	// EOF just after reading complete header.
	assert.EqualError(t, f.NewUnmarshalFunc(testReader(fixture[3:11]))(&msg),
		"reading frame (size 28): unexpected EOF")

	// EOF partway through the message.
	assert.EqualError(t, f.NewUnmarshalFunc(testReader(fixture[3:22]))(&msg),
		"reading frame (size 28): unexpected EOF")

	// Full message. Success.
	assert.NoError(t, f.NewUnmarshalFunc(testReader(fixture[3:]))(&msg))
	assert.Equal(t, "test message content", msg.s)
}

func TestFixedFramingUnderflowReadingDesyncHeader(t *testing.T) {
	var fixture = append(bytes.Repeat([]byte{'x'}, 13+8),
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't')
	var r = bufio.NewReaderSize(bytes.NewReader(fixture), 16)

	// Reads and returns 13 bytes of desync'd content, with no magic word.
	var b, err = UnpackFixedFrame(r)
	assert.Equal(t, ErrDesyncDetected, err)
	assert.Equal(t, fixture[0:13], b)

	// Next read returns only 8 bytes, because the following bytes are valid header.
	b, err = UnpackFixedFrame(r)
	assert.Equal(t, ErrDesyncDetected, err)
	assert.Equal(t, fixture[13:13+8], b)

	// Final read returns a full frame.
	b, err = UnpackFixedFrame(r)
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
type frameablestring struct{ s string }

func (f frameablestring) MarshalTo(buf []byte) (int, error) {
	return copy(buf, f.s), nil
}

func (f frameablestring) ProtoSize() int { return len(f.s) }

func (f *frameablestring) Unmarshal(buf []byte) error {
	f.s = string(buf)
	return nil
}

// Marshals and parses half a message, then fails.
type frameableerror struct{ s string }

func (f frameableerror) MarshalTo(buf []byte) (int, error) {
	return copy(buf[:len(buf)/2], f.s), errors.New("error!")
}

func (f frameableerror) ProtoSize() int { return len(f.s) }

func (f *frameableerror) Unmarshal(buf []byte) error {
	f.s = string(buf[:len(buf)/2])
	return errors.New("error!")
}
