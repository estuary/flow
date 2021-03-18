package bindings

import (
	"math"
	"strconv"
	"testing"

	"github.com/estuary/flow/go/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/assert"
)

func TestExtractorBasic(t *testing.T) {
	var ex, err = NewExtractor("/0", []string{"/1", "/2", "/3"})
	assert.NoError(t, err)
	defer ex.Destroy()

	ex.Document([]byte(`["9f2952f3-c6a3-11ea-8802-080607050309", 42, "a-string", [true, null]]`))
	ex.Document([]byte(`["9f2952f3-c6a3-12fb-8802-080607050309", 52, "other-string", {"k": "v"}]`))

	uuids, packed, err := ex.Extract()
	assert.NoError(t, err)

	var tuples []tuple.Tuple
	for _, p := range packed {
		var tuple, err = tuple.Unpack(p)
		assert.NoError(t, err)
		tuples = append(tuples, tuple)
	}

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

	assert.EqualValues(t, []tuple.Tuple{
		[]tuple.TupleElement{int64(42), "a-string", []byte("[true,null]")},
		[]tuple.TupleElement{int64(52), "other-string", []byte(`{"k":"v"}`)},
	}, tuples)
}

func TestExtractorIntegerBoundaryCases(t *testing.T) {
	var ex, err = NewExtractor("/0", []string{"/1"})
	assert.NoError(t, err)
	defer ex.Destroy()

	var minInt64 = strconv.FormatInt(math.MinInt64, 10)
	var maxInt64 = strconv.FormatInt(math.MaxInt64, 10)
	var maxUint64 = strconv.FormatUint(math.MaxUint64, 10)

	ex.Document([]byte(`["9f2952f3-c6a3-11ea-8802-080607050309", 0]`))
	ex.Document([]byte(`["9f2952f3-c6a3-11ea-8802-080607050309", ` + minInt64 + `]`))
	ex.Document([]byte(`["9f2952f3-c6a3-11ea-8802-080607050309", ` + maxInt64 + `]`))
	ex.Document([]byte(`["9f2952f3-c6a3-11ea-8802-080607050309", ` + maxUint64 + `]`))

	_, packed, err := ex.Extract()
	assert.NoError(t, err)

	var tuples []tuple.Tuple
	for _, p := range packed {
		var tuple, err = tuple.Unpack(p)
		assert.NoError(t, err)
		tuples = append(tuples, tuple)
	}

	assert.EqualValues(t, []tuple.Tuple{
		[]tuple.TupleElement{int64(0)},
		[]tuple.TupleElement{int64(math.MinInt64)},
		[]tuple.TupleElement{int64(math.MaxInt64)},
		[]tuple.TupleElement{uint64(math.MaxUint64)},
	}, tuples)
}

func TestExtractorEmpty(t *testing.T) {
	var ex, err = NewExtractor("/0", []string{"/1"})
	assert.NoError(t, err)
	defer ex.Destroy()

	uuids, packed, err := ex.Extract()
	assert.NoError(t, err)
	assert.Empty(t, uuids)
	assert.Empty(t, packed)
}
