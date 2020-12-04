package bindings

import (
	"testing"

	pf "github.com/estuary/flow/go/protocol"
	"github.com/stretchr/testify/assert"
)

func TestExtractor(t *testing.T) {
	var e, err = NewExtractor("/0", []string{"/1", "/2"})
	assert.NoError(t, err)

	e.SendDocument([]byte(`["9f2952f3-c6a3-11ea-8802-080607050309", 42, "a-string"]`))
	e.SendDocument([]byte(`["9f2952f3-c6a3-12fb-8802-080607050309", 52, "other-string"]`))

	arena, uuids, fields, err := e.Poll()
	assert.NoError(t, err)

	assert.Equal(t, []pf.UUIDParts{
		{
			ProducerAndFlags: 0x0806070503090000 + 0x02,
			Clock:            0x1eac6a39f2952f32,
		},
		{
			ProducerAndFlags: 0x0806070503090000 + 0x02,
			Clock:            0x2fbc6a39f2952f32,
		},
	}, uuids)

	var s1 = pf.Slice{Begin: 0x16, End: 0x1e}
	var s2 = pf.Slice{Begin: 0x3c, End: 0x48}

	assert.Equal(t, []pf.Field{
		{Values: []pf.Field_Value{
			{

				Kind:     pf.Field_Value_UNSIGNED,
				Unsigned: 42,
			},
			{

				Kind:     pf.Field_Value_UNSIGNED,
				Unsigned: 52,
			},
		}},
		{Values: []pf.Field_Value{
			{

				Kind:  pf.Field_Value_STRING,
				Bytes: s1,
			},
			{

				Kind:  pf.Field_Value_STRING,
				Bytes: s2,
			},
		}},
	}, fields)

	assert.Equal(t, string(arena.Bytes(s1)), "a-string")
	assert.Equal(t, string(arena.Bytes(s2)), "other-string")
}
