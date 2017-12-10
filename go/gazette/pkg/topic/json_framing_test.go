package topic

import (
	"io"

	gc "github.com/go-check/check"
)

type JsonFramingSuite struct{}

func (s *JsonFramingSuite) TestImplementsFraming(c *gc.C) {
	// Verified by the compiler.
	var _ Framing = JsonFraming
	c.Succeed()
}

func (s *JsonFramingSuite) TestFramingWithFixture(c *gc.C) {
	var buf, err = JsonFraming.Encode(struct {
		A       int
		B       string
		ignored int
	}{42, "the answer", 53}, nil)

	c.Check(err, gc.IsNil)
	c.Check(string(buf), gc.Equals, `{"A":42,"B":"the answer"}`+"\n")

	// Append another message.
	var orig = buf
	buf, err = JsonFraming.Encode(struct{ Bar int }{63}, buf)

	c.Check(err, gc.IsNil)
	c.Check(string(buf), gc.Equals, `{"A":42,"B":"the answer"}`+"\n"+`{"Bar":63}`+"\n")

	c.Check(&orig[0], gc.Equals, &buf[0]) // No reallocation occurred.
}

func (s *JsonFramingSuite) TestEncodingError(c *gc.C) {
	var buf, err = JsonFraming.Encode(struct {
		Unencodable chan struct{}
	}{}, nil)

	c.Check(err, gc.ErrorMatches, "json: unsupported type: chan struct {}")
	c.Check(buf, gc.HasLen, 0)
}

func (s *JsonFramingSuite) TestDecodeWithFixture(c *gc.C) {
	var fixture = []byte(`{"A":42,"B":"test message content"}` + "\nextra")

	var frame, err = JsonFraming.Unpack(testReader(fixture))
	c.Check(err, gc.IsNil)
	c.Check(len(frame), gc.Equals, len(fixture)-len("extra"))

	var msg struct{ B string }
	c.Check(JsonFraming.Unmarshal(frame, &msg), gc.IsNil)
	c.Check(msg.B, gc.Equals, "test message content")
}

func (s *JsonFramingSuite) TestUnexpectedEOF(c *gc.C) {
	var fixture = []byte(`{"A":42,"B":"missing trailing newline"}`)

	var _, err = JsonFraming.Unpack(testReader(fixture))
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)
}

func (s *JsonFramingSuite) TestMessageDecodeError(c *gc.C) {
	var fixture = []byte(`{"A":42,"B":"missing quote but including newline}` + "\nextra")

	var frame, err = JsonFraming.Unpack(testReader(fixture))
	c.Check(err, gc.IsNil)

	var msg struct{ B string }
	c.Check(JsonFraming.Unmarshal(frame, &msg), gc.ErrorMatches, "invalid character .*")
}

var _ = gc.Suite(&JsonFramingSuite{})
