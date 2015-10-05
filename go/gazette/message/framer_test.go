package topic

import (
	"bytes"
	"errors"
	gc "github.com/go-check/check"
	"io"
	"os"
	"testing"
	"testing/iotest"
)

type IoSuite struct{}

func (s *IoSuite) stringTopic() *TopicDescription {
	// Marshalls and parses strings.
	return &TopicDescription{
		Name: "test topic",
		MarshalTo: func(msg interface{}, to []byte) error {
			in := msg.(string)
			copy(to, in)
			return nil
		},
		Size: func(msg interface{}) int {
			return len(msg.(string))
		},
		Unmarshal: func(buffer []byte) (interface{}, error) {
			return string(buffer), nil
		},
	}
}
func (s *IoSuite) errorTopic() *TopicDescription {
	// Marshalls or parses half of a message, then fails.
	return &TopicDescription{
		Name: "test topic",
		MarshalTo: func(msg interface{}, to []byte) error {
			in := msg.(string)
			copy(to[:len(in)/2], in)
			return errors.New("error!")
		},
		Size: func(msg interface{}) int {
			return len(msg.(string))
		},
		Unmarshal: func(buffer []byte) (interface{}, error) {
			return string(buffer[:len(buffer)/2]), errors.New("error!")
		},
	}
}
func (s *IoSuite) reader(t []byte) io.Reader {
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

func (s *IoSuite) TestFramingWithFixture(c *gc.C) {
	d := s.stringTopic()
	var buf []byte

	c.Check(frame(d, "test message", &buf), gc.IsNil)
	c.Check(buf, gc.DeepEquals, []byte{
		0x66, 0x33, 0x93, 0x36, 0xc, 0x0, 0x0, 0x0, 't', 'e', 's', 't',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e'})

	// Another message which fits within |buf|'s allocation.
	orig := buf
	c.Check(frame(d, "foo message", &buf), gc.IsNil)
	c.Check(buf, gc.DeepEquals, []byte{
		0x66, 0x33, 0x93, 0x36, 0xb, 0x0, 0x0, 0x0, 'f', 'o', 'o',
		' ', 'm', 'e', 's', 's', 'a', 'g', 'e'})

	c.Check(&orig[0], gc.Equals, &buf[0]) // No reallocation occurred.
}

func (s *IoSuite) TestFramingMarshallingError(c *gc.C) {
	d := s.errorTopic()
	var buf []byte

	c.Check(frame(d, "test message", &buf), gc.ErrorMatches, "error!")
	c.Check(buf, gc.HasLen, 0)
}

func (s *IoSuite) TestParsingWithFixture(c *gc.C) {
	fixture := []byte{0x66, 0x33, 0x93, 0x36, 0xc, 0x0, 0x0, 0x0,
		't', 'e', 's', 't', ' ', 'm', 'e', 's', 's', 'a', 'g', 'e'}

	var buf []byte
	delta, msg, err := parse(s.reader(fixture), s.stringTopic(), &buf)

	c.Check(delta, gc.Equals, len(fixture))
	c.Check(msg.(string), gc.Equals, "test message")
	c.Check(err, gc.IsNil)

	// |buf| was sized in-place.
	c.Check(buf, gc.HasLen, 0)
	c.Check(cap(buf), gc.Not(gc.Equals), 0)
}

func (s *IoSuite) TestParseErrorHandling(c *gc.C) {
	fixture := []byte{0x66, 0x33, 0x93, 0x36, 0xc, 0x0, 0x0, 0x0,
		't', 'e', 's', 't', ' ', 'm', 'e', 's', 's', 'a', 'g', 'e'}

	var buf []byte
	delta, msg, err := parse(s.reader(fixture), s.errorTopic(), &buf)

	// Though a parse error occurred, message delta is still returned.
	// It's up to the client whether to skip the message or not.
	c.Check(delta, gc.Equals, len(fixture))
	c.Check(msg, gc.IsNil)
	c.Check(err, gc.ErrorMatches, "error!")
}

func (s *IoSuite) TestParseDesyncHandling(c *gc.C) {
	fixture := []byte{'f', 'o', 'o', 'b', 'a', 'r',
		0x66, 0x33, 0x93, 0x36, 0xc, 0x0, 0x0, 0x0,
		't', 'e', 's', 't', ' ', 'm', 'e', 's', 's', 'a', 'g', 'e'}

	var buf []byte
	delta, msg, err := parse(s.reader(fixture), s.stringTopic(), &buf)

	c.Check(delta, gc.Equals, len(fixture))
	c.Check(msg.(string), gc.Equals, "test message")
	c.Check(err, gc.ErrorMatches, "detected de-synchronization")
}

func (s *IoSuite) TestIncompleteBufferHandling(c *gc.C) {
	fixture := []byte{'f', 'o', 'o', 0x66, 0x33, 0x93, 0x36, 0xc, 0x0, 0x0, 0x0,
		't', 'e', 's', 't', ' ', 'm', 'e', 's', 's', 'a', 'g', 'e'}
	var buf []byte

	// EOF at first byte.
	delta, msg, err := parse(s.reader(fixture[3:3]), s.stringTopic(), &buf)
	c.Check(delta, gc.Equals, 0)
	c.Check(msg, gc.IsNil)
	c.Check(err, gc.Equals, io.EOF)

	// EOF partway through header.
	delta, msg, err = parse(s.reader(fixture[3:5]), s.stringTopic(), &buf)
	c.Check(delta, gc.Equals, 0)
	c.Check(msg, gc.IsNil)
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)

	// EOF while scanning for de-sync'd header.
	delta, msg, err = parse(s.reader(fixture[0:10]), s.stringTopic(), &buf)
	c.Check(delta, gc.Equals, 0)
	c.Check(msg, gc.IsNil)
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)

	// EOF just after reading complete header.
	delta, msg, err = parse(s.reader(fixture[3:11]), s.stringTopic(), &buf)
	c.Check(delta, gc.Equals, 0)
	c.Check(msg, gc.IsNil)
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)

	// EOF partway through the message.
	delta, msg, err = parse(s.reader(fixture[3:22]), s.stringTopic(), &buf)
	c.Check(delta, gc.Equals, 0)
	c.Check(msg, gc.IsNil)
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)
}

func (s *IoSuite) TestBufferSizing(c *gc.C) {
	fixture := []byte{'f', 'o', 'o', '0', '0', '0'}
	in := fixture[:3]

	out := sizeBuffer(in, 3) // No reallocation.
	c.Check(&out[0], gc.Equals, &in[0])

	out = sizeBuffer(in, 9) // Doubling re-allocation occurs.
	c.Check(out, gc.DeepEquals, []byte{'f', 'o', 'o'})
	c.Check(&out[0], gc.Not(gc.Equals), &in[0])
	c.Check(cap(out), gc.Equals, 12)

	out = sizeBuffer(in, 10) // Sized re-allocation occurs.
	c.Check(out, gc.DeepEquals, []byte{'f', 'o', 'o'})
	c.Check(&out[0], gc.Not(gc.Equals), &in[0])
	c.Check(cap(out), gc.Equals, 13)
}

var _ = gc.Suite(&IoSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
