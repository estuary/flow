package message

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.gazette.dev/core/labels"
)

func TestJSONFramingMarshalWithFixtures(t *testing.T) {
	var f, _ = FramingByContentType(labels.ContentType_JSONLines)
	var buf bytes.Buffer
	var bw = bufio.NewWriter(&buf)

	var err = f.Marshal(struct {
		A       int
		B       string
		ignored int
	}{42, "the answer", 53}, bw)

	assert.NoError(t, err)
	_ = bw.Flush()
	assert.Equal(t, `{"A":42,"B":"the answer"}`+"\n", buf.String())

	// Append another message.
	assert.NoError(t, f.Marshal(struct{ Bar int }{63}, bw))
	_ = bw.Flush()
	assert.Equal(t, `{"A":42,"B":"the answer"}`+"\n"+`{"Bar":63}`+"\n", buf.String())
}

func TestJSONFramingMarshalError(t *testing.T) {
	var f, _ = FramingByContentType(labels.ContentType_JSONLines)
	var err = f.Marshal(struct {
		Unencodable chan struct{}
	}{}, nil)

	assert.EqualError(t, err, "json: unsupported type: chan struct {}")
}

func TestJSONFramingDecodeWithFixture(t *testing.T) {
	var f, _ = FramingByContentType(labels.ContentType_JSONLines)
	var fixture = []byte(`{"A":42,"B":"test message content"}` + "\n")

	var msg struct{ B string }
	var unmarshal = f.NewUnmarshalFunc(testReader(fixture))

	assert.NoError(t, unmarshal(&msg))
	assert.Equal(t, "test message content", msg.B)

	// EOF read on message boundary is returned as EOF.
	assert.Equal(t, io.EOF, unmarshal(&msg))
}

func TestJSONFramingUnexpectedEOF(t *testing.T) {
	var f, _ = FramingByContentType(labels.ContentType_JSONLines)
	var fixture = []byte(`{"A":42,"B":"missing trailing newline"}`)

	var msg struct{ B string }
	var unmarshal = f.NewUnmarshalFunc(testReader(fixture))

	assert.Equal(t, io.ErrUnexpectedEOF, unmarshal(&msg))
}

func TestJSONFramingMessageDecodeError(t *testing.T) {
	var f, _ = FramingByContentType(labels.ContentType_JSONLines)
	var fixture = []byte(`{"A":42,"B":"missing quote but including newline}` + "\nextra")

	var msg struct{}
	var br = testReader(fixture)
	var unmarshal = f.NewUnmarshalFunc(br)

	assert.Regexp(t, "invalid character .*", unmarshal(&msg))

	var extra, _ = ioutil.ReadAll(br) // Expect the precise frame was consumed.
	assert.Equal(t, "extra", string(extra))
}
