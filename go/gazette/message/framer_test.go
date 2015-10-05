package message

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
	"testing/iotest"

	gc "github.com/go-check/check"
)

type FramerSuite struct{}

// Marshalls and parses strings.
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

// Marshalls and parses half a message, then fails.
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

func (s *FramerSuite) reader(t []byte) io.Reader {
	// Swap out different kinds of test readers on each run.
	switch os.Getpid() % 3 {
	case 1:
		return iotest.HalfReader(bytes.NewReader(t))
	case 2:
		return iotest.OneByteReader(bytes.NewReader(t))
	default:
		return bytes.NewReader(t)
	}
}

func (s *FramerSuite) TestFramingWithFixture(c *gc.C) {
	var buf []byte

	c.Check(Frame(frameablestring("test message"), &buf), gc.IsNil)
	c.Check(buf, gc.DeepEquals, []byte{
		0x66, 0x33, 0x93, 0x36, 0xc, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e'})

	// Another message which fits within |buf|'s allocation.
	orig := buf
	c.Check(Frame(frameablestring("foo message"), &buf), gc.IsNil)
	c.Check(buf, gc.DeepEquals, []byte{
		0x66, 0x33, 0x93, 0x36, 0xb, 0x0, 0x0, 0x0, 'f', 'o', 'o',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e'})

	c.Check(&orig[0], gc.Equals, &buf[0]) // No reallocation occurred.
}

func (s *FramerSuite) TestFramingMarshallingError(c *gc.C) {
	var buf []byte

	c.Check(Frame(frameableerror("test message"), &buf),
		gc.ErrorMatches, "error!")
	c.Check(buf, gc.HasLen, 0)
}

func (s *FramerSuite) TestParsingWithFixture(c *gc.C) {
	fixture := []byte{0x66, 0x33, 0x93, 0x36, 0xc, 0x0, 0x0, 0x0,
		't', 'e', 's', 't', ' ', 'm', 'e', 's', 's', 'a', 'g', 'e'}

	var msg frameablestring
	var buf []byte
	delta, err := Parse(&msg, s.reader(fixture), &buf)

	c.Check(delta, gc.Equals, len(fixture))
	c.Check(string(msg), gc.Equals, "test message")
	c.Check(err, gc.IsNil)

	// |buf| was sized in-place.
	c.Check(buf, gc.HasLen, len(fixture)-kHeaderLength)
}

func (s *FramerSuite) TestParseErrorHandling(c *gc.C) {
	fixture := []byte{0x66, 0x33, 0x93, 0x36, 0xc, 0x0, 0x0, 0x0,
		't', 'e', 's', 't', ' ', 'm', 'e', 's', 's', 'a', 'g', 'e'}

	var msg frameableerror
	var buf []byte
	delta, err := Parse(&msg, s.reader(fixture), &buf)

	// Though a parse error occurred, message delta is still returned.
	// It's up to the client whether to skip the message or not.
	c.Check(delta, gc.Equals, len(fixture))
	c.Check(string(msg), gc.Equals, "test m")
	c.Check(err, gc.ErrorMatches, "error!")

	c.Check(buf, gc.HasLen, len(fixture)-kHeaderLength)
}

func (s *FramerSuite) TestParseDesyncHandling(c *gc.C) {
	fixture := []byte{'f', 'o', 'o', 'b', 'a', 'r',
		0x66, 0x33, 0x93, 0x36, 0xc, 0x0, 0x0, 0x0,
		't', 'e', 's', 't', ' ', 'm', 'e', 's', 's', 'a', 'g', 'e'}

	var msg frameablestring
	var buf []byte
	delta, err := Parse(&msg, s.reader(fixture), &buf)

	c.Check(delta, gc.Equals, len(fixture))
	c.Check(string(msg), gc.Equals, "test message")
	c.Check(err, gc.Equals, ErrDesyncDetected)
}

func (s *FramerSuite) TestIncompleteBufferHandling(c *gc.C) {
	fixture := []byte{'f', 'o', 'o', 0x66, 0x33, 0x93, 0x36, 0xc, 0x0, 0x0, 0x0,
		't', 'e', 's', 't', ' ', 'm', 'e', 's', 's', 'a', 'g', 'e'}

	var msg frameablestring
	var buf []byte

	// EOF at first byte.
	delta, err := Parse(&msg, s.reader(fixture[3:3]), &buf)
	c.Check(delta, gc.Equals, 0)
	c.Check(msg, gc.Equals, frameablestring(""))
	c.Check(err, gc.Equals, io.EOF)

	// EOF partway through header.
	delta, err = Parse(&msg, s.reader(fixture[3:5]), &buf)
	c.Check(delta, gc.Equals, 0)
	c.Check(msg, gc.Equals, frameablestring(""))
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)

	// EOF while scanning for de-sync'd header.
	delta, err = Parse(&msg, s.reader(fixture[0:10]), &buf)
	c.Check(delta, gc.Equals, 0)
	c.Check(msg, gc.Equals, frameablestring(""))
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)

	// EOF just after reading complete header.
	delta, err = Parse(&msg, s.reader(fixture[3:11]), &buf)
	c.Check(delta, gc.Equals, 0)
	c.Check(msg, gc.Equals, frameablestring(""))
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)

	// EOF partway through the message.
	delta, err = Parse(&msg, s.reader(fixture[3:22]), &buf)
	c.Check(delta, gc.Equals, 0)
	c.Check(msg, gc.Equals, frameablestring(""))
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)

	// Full message. Success (with desync error).
	delta, err = Parse(&msg, s.reader(fixture), &buf)
	c.Check(delta, gc.Equals, len(fixture))
	c.Check(msg, gc.Equals, frameablestring("test message"))
	c.Check(err, gc.Equals, ErrDesyncDetected)
}

func (s *FramerSuite) TestBufferSizing(c *gc.C) {
	fixture := []byte{'f', 'o', 'o', '0', '0', '0'}
	out := fixture

	sizeBuffer(&out, 3)
	c.Check(&out[0], gc.Equals, &fixture[0]) // No reallocation.
	c.Check(len(out), gc.Equals, 3)

	sizeBuffer(&out, 9)
	c.Check(&out[0], gc.Not(gc.Equals), &fixture[0]) // Reallocation occurred.
	c.Check(cap(out), gc.Equals, 15)
	c.Check(len(out), gc.Equals, 9)
}

var _ = gc.Suite(&FramerSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
