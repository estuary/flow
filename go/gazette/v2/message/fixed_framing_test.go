package message

import (
	"bufio"
	"bytes"
	"io"
	"os"

	gc "github.com/go-check/check"
	"github.com/pkg/errors"
)

type FixedFramingSuite struct{}

func (s *FixedFramingSuite) TestImplementsFraming(c *gc.C) {
	// Verified by the compiler.
	var _ Framing = FixedFraming
	c.Succeed()
}

func (s *FixedFramingSuite) TestMarshalWithFixtures(c *gc.C) {
	var buf bytes.Buffer
	var bw = bufio.NewWriter(&buf)

	c.Check(FixedFraming.Marshal(frameablestring("test message content"), bw), gc.IsNil)
	_ = bw.Flush()
	c.Check(buf.Bytes(), gc.DeepEquals, []byte{
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't'})

	// Append another message.
	c.Check(FixedFraming.Marshal(frameablestring("foo message"), bw), gc.IsNil)
	_ = bw.Flush()
	c.Check(buf.Bytes(), gc.DeepEquals, []byte{
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't',
		0x66, 0x33, 0x93, 0x36, 0xb, 0x0, 0x0, 0x0, 'f', 'o', 'o',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e'})
}

func (s *FixedFramingSuite) TestMarshalError(c *gc.C) {
	c.Check(FixedFraming.Marshal(frameableerror("test message"), nil), gc.ErrorMatches, "error!")
}

func (s *FixedFramingSuite) TestEncodeWithFixtures(c *gc.C) {
	var b, err = FixedFraming.Encode(frameablestring("foo"), nil)
	c.Check(err, gc.IsNil)
	c.Check(b, gc.DeepEquals, []byte{0x66, 0x33, 0x93, 0x36, 0x03, 0x0, 0x0, 0x0, 'f', 'o', 'o'})

	// Expect a following message is appended to |b|.
	b, err = FixedFraming.Encode(frameablestring("bar"), b)
	c.Check(err, gc.IsNil)
	c.Check(b, gc.DeepEquals, []byte{0x66, 0x33, 0x93, 0x36, 0x03, 0x0, 0x0, 0x0, 'f', 'o', 'o',
		0x66, 0x33, 0x93, 0x36, 0x03, 0x0, 0x0, 0x0, 'b', 'a', 'r'})

	// New message with a truncated buffer.
	bb, err := FixedFraming.Encode(frameablestring("baz"), b[:0])
	c.Check(err, gc.IsNil)
	c.Check(bb, gc.DeepEquals, []byte{0x66, 0x33, 0x93, 0x36, 0x03, 0x0, 0x0, 0x0, 'b', 'a', 'z'})

	c.Check(&bb[0], gc.Equals, &b[0]) // Expect it didn't re-allocate.
}

func (s *FixedFramingSuite) TestDecodeWithFixture(c *gc.C) {
	var fixture = []byte{
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't',
		'e', 'x', 't', 'r', 'a'}

	var frame, err = FixedFraming.Unpack(testReader(fixture))
	c.Check(err, gc.IsNil)
	c.Check(frame, gc.DeepEquals, fixture[:len(fixture)-len("extra")])

	var msg frameablestring
	c.Check(FixedFraming.Unmarshal(frame, &msg), gc.IsNil)
	c.Check(string(msg), gc.Equals, "test message content")
}

func (s *FixedFramingSuite) TestDecodingError(c *gc.C) {
	var fixture = []byte{
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't'}

	var frame, err = FixedFraming.Unpack(testReader(fixture))
	c.Check(err, gc.IsNil)
	c.Check(frame, gc.DeepEquals, fixture)

	var msg frameableerror
	c.Check(FixedFraming.Unmarshal(frame, &msg), gc.ErrorMatches, "error!")
	c.Check(string(msg), gc.Equals, "test messa")
}

func (s *FixedFramingSuite) TestDecodeTrailingNewLine(c *gc.C) {
	var fixture = []byte{
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't',
		'\n'}

	var bufReader = testReader(fixture)
	var frame, err = FixedFraming.Unpack(bufReader)
	c.Check(err, gc.IsNil)
	c.Check(frame, gc.DeepEquals, fixture[:len(fixture)-1])

	var msg frameablestring
	c.Check(FixedFraming.Unmarshal(frame, &msg), gc.IsNil)
	c.Check(string(msg), gc.Equals, "test message content")

	// Read offset should now be at the \n, ensure this returns an EOF.
	var _, eofErr = FixedFraming.Unpack(bufReader)
	c.Check(errors.Cause(eofErr), gc.Equals, io.EOF)
}

func (s *FixedFramingSuite) TestDesyncHandling(c *gc.C) {
	var fixture = []byte{'f', 'o', 'o', 'b', 'a', 'r',
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't'}

	var r = testReader(fixture)

	// Expect leading invalid range is returned on first unpack.
	var frame, err = FixedFraming.Unpack(r)
	c.Check(err, gc.IsNil)
	c.Check(frame, gc.DeepEquals, []byte{'f', 'o', 'o', 'b', 'a', 'r'})

	// Attempting to unmarshal returns an error.
	var msg frameablestring
	c.Check(FixedFraming.Unmarshal(frame, &msg), gc.Equals, ErrDesyncDetected)

	// However, the next frame may be unpacked and unmarshaled.
	frame, err = FixedFraming.Unpack(r)
	c.Check(err, gc.IsNil)
	c.Check(FixedFraming.Unmarshal(frame, &msg), gc.IsNil)
	c.Check(string(msg), gc.Equals, "test message content")
}

func (s *FixedFramingSuite) TestIncompleteBufferHandling(c *gc.C) {
	var fixture = []byte{'f', 'o', 'o',
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't'}

	// EOF at first byte.
	var _, err = FixedFraming.Unpack(testReader(fixture[3:3]))
	c.Check(errors.Cause(err), gc.Equals, io.EOF)

	// EOF partway through header.
	_, err = FixedFraming.Unpack(testReader(fixture[3:10]))
	c.Check(errors.Cause(err), gc.Equals, io.ErrUnexpectedEOF)

	// EOF while scanning for de-sync'd header.
	_, err = FixedFraming.Unpack(testReader(fixture[0:6]))
	c.Check(errors.Cause(err), gc.Equals, io.ErrUnexpectedEOF)

	// EOF just after reading complete header.
	_, err = FixedFraming.Unpack(testReader(fixture[3:11]))
	c.Check(errors.Cause(err), gc.Equals, io.ErrUnexpectedEOF)

	// EOF partway through the message.
	_, err = FixedFraming.Unpack(testReader(fixture[3:22]))
	c.Check(errors.Cause(err), gc.Equals, io.ErrUnexpectedEOF)

	// Full message. Success.
	_, err = FixedFraming.Unpack(testReader(fixture[3:]))
	c.Check(err, gc.IsNil)
}

func (s *FixedFramingSuite) TestUnderflowReadingDesyncHeader(c *gc.C) {
	var fixture = append(bytes.Repeat([]byte{'x'}, 13+8),
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't')
	var r = bufio.NewReaderSize(bytes.NewReader(fixture), 16)

	// Reads and returns 13 bytes of desync'd content, with no magic word.
	var b, err = FixedFraming.Unpack(r)
	c.Check(err, gc.IsNil)
	c.Check(b, gc.DeepEquals, fixture[0:13])

	// Next read returns only 8 bytes, because the following bytes are valid header.
	b, err = FixedFraming.Unpack(r)
	c.Check(err, gc.IsNil)
	c.Check(b, gc.DeepEquals, fixture[13:13+8])

	// Final read returns a full frame.
	b, err = FixedFraming.Unpack(r)
	c.Check(err, gc.IsNil)
	c.Check(b, gc.DeepEquals, fixture[13+8:])
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

var (
	expectFixedIsFraming Framing = new(fixedFraming)
	_                            = gc.Suite(&FixedFramingSuite{})
)
