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
	var ptr, err = NewJSONPointer("/_hdr/uuid")
	assert.Nil(t, err)

	var zero message.UUID
	var f1 = uuid.MustParse("7367f4f3-7668-4370-b06f-021c828d6ed8")
	var f2 = uuid.MustParse("18cd0685-c97f-470b-a585-ed951ada17cf")

	var m = NewMessage(ptr)
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
	ptr.Drop()
}

func TestMarshalJSON(t *testing.T) {
	var ptr, err = NewJSONPointer("/_meta/uuid")
	assert.Nil(t, err)

	var m = NewMessage(ptr)
	m.SetUUID(uuid.MustParse("7367f4f3-7668-4370-b06f-021c828d6ed8"))
	var expect = `{"_meta":{"uuid":"7367f4f3-7668-4370-b06f-021c828d6ed8"}}`

	// Marshal to a too-short buffer.
	var b = make([]byte, 12)
	assert.Equal(t, m.MarshalJSONInPlace(b), len(expect))
	assert.Equal(t, string(b), expect[:12])

	b = make([]byte, 128)
	assert.Equal(t, string(b[:m.MarshalJSONInPlace(b)]), expect)

	var bw bytes.Buffer
	var bbw = bufio.NewWriter(&bw)
	l, err := m.MarshalJSONTo(bbw)
	assert.Nil(t, err)
	assert.Nil(t, bbw.Flush())
	assert.Equal(t, l, len(expect))
	assert.Equal(t, bw.String(), expect)
}

func TestRoundUp(t *testing.T) {
	assert.Equal(t, 2, roundUp(1))
	assert.Equal(t, 4, roundUp(2))
	assert.Equal(t, 4, roundUp(3))
	assert.Equal(t, 16, roundUp(15))
}

func TestExtract(t *testing.T) {
	var ptr1 = MustJSONPointer("/_meta/uuid")
	var ptr2 = MustJSONPointer("/other")
	defer ptr1.Drop()
	defer ptr2.Drop()

	var m = NewMessage(ptr1)
	m.SetUUID(uuid.MustParse("7367f4f3-7668-4370-b06f-021c828d6ed8"))

	var out debugVisitor
	m.VisitFields(&out, ptr2, ptr1, ptr1)
	assert.Equal(t, debugVisitor{
		"dne",
		"7367f4f3-7668-4370-b06f-021c828d6ed8",
		"7367f4f3-7668-4370-b06f-021c828d6ed8",
	}, out)
}

type debugVisitor []interface{}

func (v *debugVisitor) VisitDoesNotExist(i int) {
	*v = append(*v, "dne")
}
func (v *debugVisitor) VisitNull(i int) {
	*v = append(*v, nil)
}
func (v *debugVisitor) VisitBool(i int, val bool) {
	*v = append(*v, val)
}
func (v *debugVisitor) VisitUnsigned(i int, val uint64) {
	*v = append(*v, val)
}
func (v *debugVisitor) VisitSigned(i int, val int64) {
	*v = append(*v, val)
}
func (v *debugVisitor) VisitFloat(i int, val float64) {
	*v = append(*v, val)
}
func (v *debugVisitor) VisitString(i int, val []byte) {
	*v = append(*v, string(val))
}
func (v *debugVisitor) VisitObject(i int, val []byte) {
	*v = append(*v, string(val))
}
func (v *debugVisitor) VisitArray(i int, val []byte) {
	*v = append(*v, string(val))
}
