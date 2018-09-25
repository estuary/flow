package message

import (
	"bufio"
	"bytes"
	"io"

	gc "github.com/go-check/check"
)

type JsonFramingSuite struct{}

func (s *JsonFramingSuite) TestImplementsFraming(c *gc.C) {
	// Verified by the compiler.
	var _ Framing = JSONFraming
	c.Succeed()
}

func (s *JsonFramingSuite) TestMarshalWithFixtures(c *gc.C) {
	var buf bytes.Buffer
	var bw = bufio.NewWriter(&buf)

	var err = JSONFraming.Marshal(struct {
		A       int
		B       string
		ignored int
	}{42, "the answer", 53}, bw)

	c.Check(err, gc.IsNil)
	bw.Flush()
	c.Check(buf.String(), gc.Equals, `{"A":42,"B":"the answer"}`+"\n")

	// Append another message.
	err = JSONFraming.Marshal(struct{ Bar int }{63}, bw)

	c.Check(err, gc.IsNil)
	bw.Flush()
	c.Check(buf.String(), gc.Equals, `{"A":42,"B":"the answer"}`+"\n"+`{"Bar":63}`+"\n")
}

func (s *JsonFramingSuite) TestMarshalError(c *gc.C) {
	var err = JSONFraming.Marshal(struct {
		Unencodable chan struct{}
	}{}, nil)

	c.Check(err, gc.ErrorMatches, "json: unsupported type: chan struct {}")
}
func (s *JsonFramingSuite) TestDecodeWithFixture(c *gc.C) {
	var fixture = []byte(`{"A":42,"B":"test message content"}` + "\nextra")

	var frame, err = JSONFraming.Unpack(testReader(fixture))
	c.Check(err, gc.IsNil)
	c.Check(len(frame), gc.Equals, len(fixture)-len("extra"))

	var msg struct{ B string }
	c.Check(JSONFraming.Unmarshal(frame, &msg), gc.IsNil)
	c.Check(msg.B, gc.Equals, "test message content")
}

func (s *JsonFramingSuite) TestUnexpectedEOF(c *gc.C) {
	var fixture = []byte(`{"A":42,"B":"missing trailing newline"}`)

	var _, err = JSONFraming.Unpack(testReader(fixture))
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)
}

func (s *JsonFramingSuite) TestMessageDecodeError(c *gc.C) {
	var fixture = []byte(`{"A":42,"B":"missing quote but including newline}` + "\nextra")

	var frame, err = JSONFraming.Unpack(testReader(fixture))
	c.Check(err, gc.IsNil)
	c.Check(len(frame), gc.Equals, len(fixture)-len("extra"))

	var msg struct{ B string }
	c.Check(JSONFraming.Unmarshal(frame, &msg), gc.ErrorMatches, "invalid character .*")
}

var _ = gc.Suite(&JsonFramingSuite{})
