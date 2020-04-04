package bridge

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.gazette.dev/core/message"
)

func TestStatus(t *testing.T) {
	var b, err = NewBuilder("/_hdr/uuid")
	assert.Nil(t, err)

	var m = b.Build()

	var zero message.UUID
	var f1 = uuid.MustParse("7367f4f3-7668-4370-b06f-021c828d6ed8")
	var f2 = uuid.MustParse("18cd0685-c97f-470b-a585-ed951ada17cf")

	assert.Equal(t, m.GetUUID(), zero)

	m.SetUUID(f1)
	assert.Equal(t, m.GetUUID(), f1)
	m.SetUUID(f2)
	assert.Equal(t, m.GetUUID(), f2)

	var buf bytes.Buffer
	var bw = bufio.NewWriter(&buf)

	_, err = m.MarshalJSONTo(bw)
	assert.Nil(t, err)
	bw.Flush()

	assert.Equal(t, buf.String(), `{"_hdr":{"uuid":"`+f2.String()+`"}}`)

	m.Drop()
	b.Drop()
}
