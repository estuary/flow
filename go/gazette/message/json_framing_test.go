package message

import (
	"bufio"
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJSONFramingMarshalWithFixtures(t *testing.T) {
	var buf bytes.Buffer
	var bw = bufio.NewWriter(&buf)

	var err = JSONFraming.Marshal(struct {
		A       int
		B       string
		ignored int
	}{42, "the answer", 53}, bw)

	assert.NoError(t, err)
	_ = bw.Flush()
	assert.Equal(t, `{"A":42,"B":"the answer"}`+"\n", buf.String())

	// Append another message.
	err = JSONFraming.Marshal(struct{ Bar int }{63}, bw)

	assert.NoError(t, err)
	_ = bw.Flush()
	assert.Equal(t, `{"A":42,"B":"the answer"}`+"\n"+`{"Bar":63}`+"\n", buf.String())
}

func TestJSONFramingMarshalError(t *testing.T) {
	var err = JSONFraming.Marshal(struct {
		Unencodable chan struct{}
	}{}, nil)

	assert.EqualError(t, err, "json: unsupported type: chan struct {}")
}

func TestJSONFramingDecodeWithFixture(t *testing.T) {
	var fixture = []byte(`{"A":42,"B":"test message content"}` + "\n")

	var r = testReader(fixture)
	var frame, err = JSONFraming.Unpack(r)
	assert.NoError(t, err)
	assert.Len(t, frame, len(fixture))

	var msg struct{ B string }
	assert.NoError(t, JSONFraming.Unmarshal(frame, &msg))
	assert.Equal(t, "test message content", msg.B)

	// EOF read on message boundary is returned as EOF.
	frame, err = JSONFraming.Unpack(r)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, []byte{}, frame)
}

func TestJSONFramingUnexpectedEOF(t *testing.T) {
	var fixture = []byte(`{"A":42,"B":"missing trailing newline"}`)

	var _, err = JSONFraming.Unpack(testReader(fixture))
	assert.Equal(t, io.ErrUnexpectedEOF, err)
}

func TestJSONFramingMessageDecodeError(t *testing.T) {
	var fixture = []byte(`{"A":42,"B":"missing quote but including newline}` + "\nextra")

	var frame, err = JSONFraming.Unpack(testReader(fixture))
	assert.NoError(t, err)
	assert.Len(t, frame, len(fixture)-len("extra"))

	var msg struct{ B string }
	assert.Regexp(t, "invalid character .*", JSONFraming.Unmarshal(frame, &msg))
}
