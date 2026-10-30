package message

import (
	"bufio"
	"bytes"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/labels"
)

func TestCSVFramingMarshalWithFixtures(t *testing.T) {
	var f, _ = FramingByContentType(labels.ContentType_CSV)
	var buf bytes.Buffer
	var bw = bufio.NewWriter(&buf)
	var uuid = uuid.New()

	// Expect encoding/csv gracefully handles quoting.
	require.NoError(t, f.Marshal(&CSVRecord{uuid.String(), "123", `qu"ote`}, bw))
	_ = bw.Flush()
	require.Equal(t, uuid.String()+`,123,"qu""ote"`+"\n", buf.String())

	require.NoError(t, f.Marshal(&CSVRecord{uuid.String(), "567,891"}, bw))
	_ = bw.Flush()
	require.Equal(t, uuid.String()+`,123,"qu""ote"`+"\n"+uuid.String()+`,"567,891"`+"\n", buf.String())
}

func TestCSVFramingDecodeWithFixture(t *testing.T) {
	var f, _ = FramingByContentType(labels.ContentType_CSV)
	var uuid = uuid.New()
	var fixture = []byte(uuid.String() + `,bar,baz` + "\n")

	var msg CSVRecord
	var unmarshal = f.NewUnmarshalFunc(testReader(fixture))

	// Read single message.
	require.NoError(t, unmarshal(&msg))
	require.Equal(t, CSVRecord{uuid.String(), "bar", "baz"}, msg)

	// EOF read on message boundary is returned as EOF.
	require.Equal(t, io.EOF, unmarshal(&msg))
}

func TestCSVFramingErrorsIfFieldsChange(t *testing.T) {
	var f, _ = FramingByContentType(labels.ContentType_CSV)
	var uuid = uuid.New()
	var fixture = []byte(uuid.String() + ",foo,bar\n" + uuid.String() + ",baz\n")

	var msg CSVRecord
	var unmarshal = f.NewUnmarshalFunc(testReader(fixture))

	require.NoError(t, unmarshal(&msg))
	require.Equal(t, CSVRecord{uuid.String(), "foo", "bar"}, msg)

	require.EqualError(t, unmarshal(&msg), "record on line 2: wrong number of fields")
}

func TestCSVFramingDecodeError(t *testing.T) {
	var f, _ = FramingByContentType(labels.ContentType_CSV)
	var fixture = []byte("not-a-uuid, bar, baz\n")

	var msg CSVRecord
	var unmarshal = f.NewUnmarshalFunc(testReader(fixture))

	require.EqualError(t, unmarshal(&msg), "invalid UUID length: 10")
}
