package bridge

import (
	"bufio"
	"bytes"
	"sync"
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

	// Excercise pessimstic re-allocation case.
	bufferPool = sync.Pool{New: func() interface{} { return make([]byte, 4) }}

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

func TestUnmarshalAndExtract(t *testing.T) {
	// Note whitespace of "arr" and "obj". We expect it's compacted in extractions.
	var fixture = `
	{
		"uuid": "7367f4f3-7668-4370-b06f-021c828d6ed8",
		"arr": [
			{"true": true},
			false
		],
		"obj": {
			"null": null
		},
		"nums": {"u": 23, "s": -42, "f": 42.5}
	}
	`
	var ptrs = []JSONPointer{
		MustJSONPointer("/uuid"),          // String.
		MustJSONPointer("/arr/0/true"),    // True.
		MustJSONPointer("/arr/1"),         // False.
		MustJSONPointer("/arr/0/missing"), // Missing.
		MustJSONPointer("/obj/null"),      // Null.
		MustJSONPointer("/nums/u"),        // Unsigned.
		MustJSONPointer("/nums/s"),        // Signed.
		MustJSONPointer("/nums/f"),        // Float.
		MustJSONPointer("/arr"),           // Array.
		MustJSONPointer("/obj"),           // Object.
	}
	defer func() {
		for _, p := range ptrs {
			p.Drop()
		}
	}()

	var m = NewMessage(ptrs[0])
	defer m.Drop()
	assert.Nil(t, m.UnmarshalJSON([]byte(fixture)))

	// Excercise pessimstic re-allocation case.
	bufferPool = sync.Pool{New: func() interface{} { return make([]byte, 4) }}

	var out debugVisitor
	m.VisitFields(&out, ptrs...)

	assert.Equal(t, debugVisitor{
		"7367f4f3-7668-4370-b06f-021c828d6ed8",
		true,
		false,
		doesNotExist{},
		nil,
		uint64(23),
		int64(-42),
		float64(42.5),
		[]byte(`[{"true":true},false]`),
		[]byte(`{"null":null}`),
	}, out)
}

func TestUnmarshalCases(t *testing.T) {
	var ptr = MustJSONPointer("/_meta/uuid")
	defer ptr.Drop()

	var cases = []struct {
		input  string
		expect string
	}{
		{`{"_meta": {"uuid": "7367f4f3-7668-4370-b06f-021c828d6ed8"}, "bar": 1}`, ""}, // Valid.
		{`{"_meta": {"uuid": null}, "bar": 1}`, ""},                                   // Null UUID is valid.
		{`{"_meta": {}, "bar": 1}`, ""},                                               // Missing UUID is valid.
		{`{"bar": 1}`, ""},

		{`{bad json`, "EST_MSG_JSON_PARSE_ERROR"},
		{`{"_meta": []}`, "EST_MSG_UUID_BAD_LOCATION"},                  // UUID pointer cannot exist.
		{`{"_meta": {"uuid": 42}}`, "EST_MSG_UUID_NOT_A_STRING"},        // UUID is not a string.
		{`{"_meta": {"uuid": "bad uuid"}}`, "EST_MSG_UUID_PARSE_ERROR"}, // UUID doesn't parse.
	}
	for _, tc := range cases {
		var m = NewMessage(ptr)

		if err := m.UnmarshalJSON([]byte(tc.input)); tc.expect == "" {
			assert.Nil(t, err)
		} else {
			assert.NotNil(t, err)
			assert.Equal(t, err.Error(), tc.expect)
		}
		m.Drop()
	}
}

type debugVisitor []interface{}

type doesNotExist struct{}

func (v *debugVisitor) VisitDoesNotExist(i int) {
	*v = append(*v, doesNotExist{})
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
	*v = append(*v, val)
}
func (v *debugVisitor) VisitArray(i int, val []byte) {
	*v = append(*v, val)
}
