package bindings

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/ops"
	"github.com/estuary/flow/go/protocols/catalog"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/message"
)

func TestExtractorBasic(t *testing.T) {
	var ex, err = NewExtractor(localPublisher)
	require.NoError(t, err)
	require.NoError(t, ex.Configure("/0", []string{"/1", "/2", "/3"}, nil))
	ex.Document([]byte(`["9f2952f3-c6a3-11ea-8802-080607050309", 42, "a-string", [true, null]]`))
	ex.Document([]byte(`["9f2952f3-c6a3-12fb-8801-080607050309", 52, "other-string", {"k": "v"}]`))

	uuids, packed, err := ex.Extract()
	require.NoError(t, err)

	var tuples []tuple.Tuple
	for _, p := range packed {
		var tuple, err = tuple.Unpack(p)
		require.NoError(t, err)
		tuples = append(tuples, tuple)
	}

	require.Equal(t, []pf.UUIDParts{
		{
			ProducerAndFlags: 0x0806070503090000 + uint64(message.Flag_ACK_TXN),
			Clock:            0x1eac6a39f2952f32,
		},
		{
			ProducerAndFlags: 0x0806070503090000 + uint64(message.Flag_CONTINUE_TXN),
			Clock:            0x2fbc6a39f2952f32,
		},
	}, uuids)

	require.EqualValues(t, []tuple.Tuple{
		[]tuple.TupleElement{int64(42), "a-string", []byte("[true,null]")},
		[]tuple.TupleElement{int64(52), "other-string", []byte(`{"k":"v"}`)},
	}, tuples)
}

func TestExtractorValidation(t *testing.T) {
	var args = BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "fixture",
			Directory:  t.TempDir(),
			Source:     "file:///int-string.flow.yaml",
			SourceType: pf.ContentType_CATALOG,
		}}
	require.NoError(t, BuildCatalog(args))

	var collection *pf.CollectionSpec

	require.NoError(t, catalog.Extract(args.OutputPath(), func(db *sql.DB) (err error) {
		if collection, err = catalog.LoadCollection(db, "int-string"); err != nil {
			return fmt.Errorf("loading collection: %w", err)
		}
		return nil
	}))

	var opsLogs = make(chan ops.Log)
	var ex, err = NewExtractor(newChanPublisher(opsLogs, pf.LogLevel_warn))
	require.NoError(t, err)
	require.NoError(t, ex.Configure("/uuid", []string{"/s"}, collection.SchemaJson))

	ex.Document([]byte(`{"uuid": "9f2952f3-c6a3-12fb-8801-080607050309", "i": 32, "s": "valid"}`))         // Valid.
	ex.Document([]byte(`{"uuid": "9f2952f3-c6a3-11ea-8802-080607050309", "i": "not a string but ACK"}`))   // Invalid but ACK.
	ex.Document([]byte(`{"uuid": "9f2952f3-c6a3-12fb-8801-080607050309", "i": "not a string and fails"}`)) // Invalid.

	_, _, err = ex.Extract()

	var opsLog = <-opsLogs
	cupaloy.SnapshotT(t, err, opsLog.Level, opsLog.Message, string(opsLog.Fields))
}

func TestExtractorIntegerBoundaryCases(t *testing.T) {
	var ex, err = NewExtractor(localPublisher)
	require.NoError(t, err)
	require.NoError(t, ex.Configure("/0", []string{"/1"}, nil))

	var minInt64 = strconv.FormatInt(math.MinInt64, 10)
	var maxInt64 = strconv.FormatInt(math.MaxInt64, 10)
	var maxUint64 = strconv.FormatUint(math.MaxUint64, 10)

	ex.Document([]byte(`["9f2952f3-c6a3-11ea-8802-080607050309", 0]`))
	ex.Document([]byte(`["9f2952f3-c6a3-11ea-8802-080607050309", ` + minInt64 + `]`))
	ex.Document([]byte(`["9f2952f3-c6a3-11ea-8802-080607050309", ` + maxInt64 + `]`))
	ex.Document([]byte(`["9f2952f3-c6a3-11ea-8802-080607050309", ` + maxUint64 + `]`))

	_, packed, err := ex.Extract()
	require.NoError(t, err)

	var tuples []tuple.Tuple
	for _, p := range packed {
		var tuple, err = tuple.Unpack(p)
		require.NoError(t, err)
		tuples = append(tuples, tuple)
	}

	require.EqualValues(t, []tuple.Tuple{
		[]tuple.TupleElement{int64(0)},
		[]tuple.TupleElement{int64(math.MinInt64)},
		[]tuple.TupleElement{int64(math.MaxInt64)},
		[]tuple.TupleElement{uint64(math.MaxUint64)},
	}, tuples)
}

func TestExtractorEmpty(t *testing.T) {
	var ex, err = NewExtractor(localPublisher)
	require.NoError(t, err)
	require.NoError(t, ex.Configure("/0", []string{"/1"}, nil))

	uuids, packed, err := ex.Extract()
	require.NoError(t, err)
	require.Empty(t, uuids)
	require.Empty(t, packed)
}
