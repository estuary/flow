package topic

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"

	gc "github.com/go-check/check"
)

type FixedFramingSuite struct{}

func (s *FixedFramingSuite) TestFramingWithFixture(c *gc.C) {
	var buf, err = FixedFraming.Encode(frameablestring("test message content"), nil)
	c.Check(err, gc.IsNil)
	c.Check(buf, gc.DeepEquals, []byte{
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't'})

	// Append another message.
	buf, err = FixedFraming.Encode(frameablestring("foo message"), buf)
	c.Check(err, gc.IsNil)
	c.Check(buf, gc.DeepEquals, []byte{
		0x66, 0x33, 0x93, 0x36, 0x14, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't',
		0x66, 0x33, 0x93, 0x36, 0xb, 0x0, 0x0, 0x0, 'f', 'o', 'o',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e'})
}

func (s *FixedFramingSuite) TestEncodingError(c *gc.C) {
	var buf, err = FixedFraming.Encode(frameableerror("test message"), nil)
	c.Check(err, gc.ErrorMatches, "error!")
	c.Check(buf, gc.HasLen, 0)
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
	c.Check(err, gc.Equals, io.EOF)

	// EOF partway through header.
	_, err = FixedFraming.Unpack(testReader(fixture[3:10]))
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)

	// EOF while scanning for de-sync'd header.
	_, err = FixedFraming.Unpack(testReader(fixture[0:6]))
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)

	// EOF just after reading complete header.
	_, err = FixedFraming.Unpack(testReader(fixture[3:11]))
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)

	// EOF partway through the message.
	_, err = FixedFraming.Unpack(testReader(fixture[3:22]))
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)

	// Full message. Success.
	_, err = FixedFraming.Unpack(testReader(fixture[3:]))
	c.Check(err, gc.IsNil)
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

func (f frameablestring) Size() int {
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

func (f frameableerror) Size() int {
	return len(f)
}

func (f *frameableerror) Unmarshal(buf []byte) error {
	*f = frameableerror(buf[:len(buf)/2])
	return errors.New("error!")
}

var _ = gc.Suite(&FixedFramingSuite{})
