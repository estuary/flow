package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"path"
	"path/filepath"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/fdb/tuple"
	"github.com/estuary/flow/go/materialize"
	sqlDriver "github.com/estuary/flow/go/materialize/driver/sql2"
	"github.com/estuary/flow/go/materialize/driver/sqlite"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestSQLGeneration(t *testing.T) {
	var built, err = bindings.BuildCatalog(bindings.BuildArgs{
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			Directory:   "testdata",
			Source:      "file:///sql-gen.yaml",
			SourceType:  pf.ContentType_CATALOG_SPEC,
			CatalogPath: filepath.Join(t.TempDir(), "catalog.db"),
		},
		MaterializeDriverFn: materialize.NewDriver,
	})
	require.NoError(t, err)
	require.Empty(t, built.Errors)

	var gen = sqlDriver.SQLiteSQLGenerator()
	var spec = &built.Materializations[0]
	var table = sqlDriver.TableForMaterialization("test_table", "", &gen.IdentifierQuotes, spec.Bindings[0])

	keyCreate, keyInsert, keyJoin, keyTruncate, err := sqlite.BuildSQL(
		&gen, 123, table, spec.Bindings[0].FieldSelection)
	require.NoError(t, err)

	require.Equal(t, `
		CREATE TABLE load.keys_123 (
			key1 INTEGER NOT NULL, key2 BOOLEAN NOT NULL
		);`, keyCreate)

	require.Equal(t, `
		INSERT INTO load.keys_123 (
			key1, key2
		) VALUES (
			?, ?
		);`, keyInsert)

	// Note the intentional missing semicolon, as this is a subquery.
	require.Equal(t, `
		SELECT 123, l.flow_document
			FROM test_table AS l
			JOIN load.keys_123 AS r
			ON l.key1 = r.key1 AND l.key2 = r.key2
		`, keyJoin)

	require.Equal(t, `DELETE FROM load.keys_123 ;`, keyTruncate)
}

func TestSQLiteDriver(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	var ctx = context.Background()
	var driver = materialize.AdaptServerToClient(sqlite.NewSQLiteDriver())

	var built, err = bindings.BuildCatalog(bindings.BuildArgs{
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			Directory:   "testdata",
			Source:      "file:///driver-steps.yaml",
			SourceType:  pf.ContentType_CATALOG_SPEC,
			CatalogPath: filepath.Join(t.TempDir(), "catalog.db"),
		},
		MaterializeDriverFn: materialize.NewDriver,
	})
	require.NoError(t, err)
	require.Empty(t, built.Errors)

	// Model MaterializationSpec we'll *mostly* use, but vary slightly in this test.
	var model = built.Materializations[0]

	// Config fixture which matches schema of ParseConfig.
	var endpointConfig = struct {
		Path string
	}{Path: "file://" + path.Join(t.TempDir(), "target.db")}
	var endpointJSON, _ = json.Marshal(endpointConfig)

	// Validate should return constraints for a non-existant materialization
	var validateReq = pm.ValidateRequest{
		Materialization:  built.Materializations[0].Materialization,
		EndpointType:     pf.EndpointType_SQLITE,
		EndpointSpecJson: json.RawMessage(endpointJSON),
		Bindings: []*pm.ValidateRequest_Binding{
			{
				Collection:       model.Bindings[0].Collection,
				ResourceSpecJson: model.Bindings[0].ResourceSpecJson,
			},
		},
	}

	validateResp, err := driver.Validate(ctx, &validateReq)
	require.NoError(t, err)
	// There should be a constraint for every projection
	require.Equal(t, &pm.ValidateResponse_Binding{
		Constraints: map[string]*pm.Constraint{
			"array":         {Type: pm.Constraint_FIELD_OPTIONAL, Reason: "This field is able to be materialized"},
			"bool":          {Type: pm.Constraint_LOCATION_RECOMMENDED, Reason: "The projection has a single scalar type"},
			"flow_document": {Type: pm.Constraint_LOCATION_REQUIRED, Reason: "The root document must be materialized"},
			"int":           {Type: pm.Constraint_LOCATION_RECOMMENDED, Reason: "The projection has a single scalar type"},
			"number":        {Type: pm.Constraint_LOCATION_RECOMMENDED, Reason: "The projection has a single scalar type"},
			"object":        {Type: pm.Constraint_FIELD_OPTIONAL, Reason: "This field is able to be materialized"},
			"string":        {Type: pm.Constraint_LOCATION_RECOMMENDED, Reason: "The projection has a single scalar type"},
			"theKey":        {Type: pm.Constraint_LOCATION_REQUIRED, Reason: "All Locations that are part of the collections key are required"},
		},
		DeltaUpdates: false,
		ResourcePath: model.Bindings[0].ResourcePath,
	}, validateResp.Bindings[0])

	// Select some fields and Apply the materialization
	var fields = pf.FieldSelection{
		Keys:     []string{"theKey"},
		Values:   []string{"bool", "int", "string"}, // intentionally missing "number" field
		Document: "flow_document",
	}
	var applyReq = pm.ApplyRequest{
		Materialization: &pf.MaterializationSpec{
			Materialization:  built.Materializations[0].Materialization,
			EndpointType:     pf.EndpointType_SQLITE,
			EndpointSpecJson: json.RawMessage(endpointJSON),
			Bindings: []*pf.MaterializationSpec_Binding{
				{
					Collection:       model.Bindings[0].Collection,
					FieldSelection:   fields,
					ResourcePath:     model.Bindings[0].ResourcePath,
					ResourceSpecJson: model.Bindings[0].ResourceSpecJson,
					DeltaUpdates:     false,
					Shuffle:          model.Bindings[0].Shuffle,
				},
			},
		},
		DryRun: true,
	}

	applyResp, err := driver.Apply(ctx, &applyReq)
	require.NoError(t, err)
	require.NotEmpty(t, applyResp.ActionDescription)

	applyReq.DryRun = false
	applyResp, err = driver.Apply(ctx, &applyReq)
	require.NoError(t, err)
	require.NotEmpty(t, applyResp.ActionDescription)

	// Now that we've applied, call Validate again to ensure the existing fields are accounted for
	validateResp, err = driver.Validate(ctx, &validateReq)
	require.NoError(t, err)

	// Expect a constraint was returned for each projection.
	require.Equal(t,
		len(model.Bindings[0].Collection.Projections),
		len(validateResp.Bindings[0].Constraints))

	for _, field := range fields.AllFields() {
		var actual = validateResp.Bindings[0].Constraints[field].Type
		require.Equal(
			t,
			pm.Constraint_FIELD_REQUIRED,
			actual,
			"wrong constraint for field: %s, expected FIELD_REQUIRED, got %s",
			field,
			actual,
		)
	}
	// The "number" field should be forbidden because it was not included in the FieldSelection that
	// was applied.
	require.Equal(t, pm.Constraint_FIELD_FORBIDDEN, validateResp.Bindings[0].Constraints["number"].Type)

	// Insert a fixture into the `flow_checkpoints` table which we'll fence
	// and draw a checkpoint from, and then insert a more-specific checkpoint
	// that reflects our transaction request range fixture.
	{
		var db, err = sql.Open("sqlite3", endpointConfig.Path)
		require.NoError(t, err)

		_, err = db.Exec(`INSERT INTO flow_checkpoints_v1
			(materialization, key_begin, key_end, fence, checkpoint)
			VALUES (?, 0, ?, 5, ?)
		;`,
			applyReq.Materialization.Materialization,
			math.MaxUint32,
			base64.StdEncoding.EncodeToString([]byte("initial checkpoint fixture")),
		)
		require.NoError(t, err)
		require.NoError(t, db.Close())
	}

	transaction, err := driver.Transactions(ctx)
	require.NoError(t, err)

	// Send open.
	err = transaction.Send(&pm.TransactionRequest{
		Open: &pm.TransactionRequest_Open{
			Materialization:      applyReq.Materialization,
			KeyBegin:             100,
			KeyEnd:               200,
			DriverCheckpointJson: nil,
		},
	})
	require.NoError(t, err)

	// Receive Opened.
	opened, err := transaction.Recv()
	require.NoError(t, err)
	require.Equal(t, &pm.TransactionResponse_Opened{
		FlowCheckpoint: []byte("initial checkpoint fixture"),
	}, opened.Opened)

	// Test Load with keys that don't exist yet
	var key1 = tuple.Tuple{"key1Value"}
	var key2 = tuple.Tuple{"key2Value"}
	err = transaction.Send(&pm.TransactionRequest{
		Load: newLoadReq(key1.Pack(), key2.Pack()),
	})
	require.NoError(t, err)
	var key3 = tuple.Tuple{"key3Value"}
	err = transaction.Send(&pm.TransactionRequest{
		Load: newLoadReq(key3.Pack()),
	})
	require.NoError(t, err)

	var checkpoint1 = []byte("first checkpoint value")
	err = transaction.Send(&pm.TransactionRequest{
		Prepare: &pm.TransactionRequest_Prepare{
			FlowCheckpoint: checkpoint1,
		},
	})
	require.NoError(t, err)

	// Receive Prepared, which indicates that none of the documents exist
	prepared, err := transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, prepared.Prepared, "unexpected message: %v+", prepared)
	require.Empty(t, prepared.Prepared.DriverCheckpointJson)

	// Test Store to add those keys
	require.NoError(t, err)
	var doc1 = `{ "theKey": "key1Value", "string": "foo", "bool": true, "int": 77, "number": 12.34 }`
	var doc2 = `{ "theKey": "key2Value", "string": "bar", "bool": false, "int": 88, "number": 56.78 }`
	var doc3 = `{ "theKey": "key3Value", "string": "baz", "bool": false, "int": 99, "number": 0 }`

	var store1 = pm.TransactionRequest_Store{}
	store1.DocsJson = store1.Arena.AddAll([]byte(doc1), []byte(doc2))
	store1.PackedKeys = store1.Arena.AddAll(key1.Pack(), key2.Pack())
	store1.PackedValues = store1.Arena.AddAll(
		tuple.Tuple{"foo", true, 77}.Pack(),
		tuple.Tuple{"bar", false, 88}.Pack(),
	)
	store1.Exists = []bool{false, false}
	err = transaction.Send(&pm.TransactionRequest{
		Store: &store1,
	})
	require.NoError(t, err)

	var store2 = pm.TransactionRequest_Store{}
	store2.DocsJson = store2.Arena.AddAll([]byte(doc3))
	store2.PackedKeys = store2.Arena.AddAll(key3.Pack())
	store2.PackedValues = store2.Arena.AddAll(
		tuple.Tuple{"baz", false, 99}.Pack(),
	)
	store2.Exists = []bool{false}
	err = transaction.Send(&pm.TransactionRequest{
		Store: &store2,
	})
	require.NoError(t, err)

	err = transaction.Send(&pm.TransactionRequest{
		Commit: &pm.TransactionRequest_Commit{},
	})
	require.NoError(t, err)

	committed, err := transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, committed.Committed)

	// Next transaction.
	err = transaction.Send(&pm.TransactionRequest{
		Load: newLoadReq(key1.Pack(), key2.Pack(), key3.Pack()),
	})
	require.NoError(t, err)

	var checkpoint2 = []byte("second checkpoint value")
	err = transaction.Send(&pm.TransactionRequest{
		Prepare: &pm.TransactionRequest_Prepare{
			FlowCheckpoint: checkpoint2,
		},
	})
	require.NoError(t, err)

	// Receive Loaded response, which is expected to contain our 3 documents.
	loaded, err := transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, loaded.Loaded)
	require.Equal(t, 3, len(loaded.Loaded.DocsJson))

	for i, expected := range []string{doc1, doc2, doc3} {
		var actual = loaded.Loaded.Arena.Bytes(loaded.Loaded.DocsJson[i])
		require.Equal(t, expected, string(actual))
	}

	// Receive Prepared
	prepared, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, prepared.Prepared, "unexpected message: %v+", prepared)

	// This store will update one document and add a new one.
	var newDoc1 = `{ "theKey": "key1Value", "string": "notthesame", "bool": false, "int": 33, "number": 2 }`
	var key4 = tuple.Tuple{"key4Value"}
	var doc4 = `{ "theKey": "key4Value" }`

	var storeReq = pm.TransactionRequest_Store{}
	storeReq.Exists = []bool{true, false}
	storeReq.PackedKeys = storeReq.Arena.AddAll(key1.Pack(), key4.Pack())
	storeReq.PackedValues = storeReq.Arena.AddAll(
		tuple.Tuple{"totally different", false, 33}.Pack(),
		tuple.Tuple{nil, nil, nil}.Pack(),
	)
	storeReq.DocsJson = storeReq.Arena.AddAll([]byte(newDoc1), []byte(doc4))

	err = transaction.Send(&pm.TransactionRequest{
		Store: &storeReq,
	})
	require.NoError(t, err)

	// Commit transaction and assert we get a Committed.
	err = transaction.Send(&pm.TransactionRequest{
		Commit: &pm.TransactionRequest_Commit{},
	})
	require.NoError(t, err)

	committed, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, committed.Committed)

	// One more transaction just to verify the updated documents
	err = transaction.Send(&pm.TransactionRequest{
		Load: newLoadReq(key1.Pack(), key2.Pack(), key3.Pack(), key4.Pack()),
	})
	require.NoError(t, err)

	var checkpoint3 = []byte("third checkpoint value")
	err = transaction.Send(&pm.TransactionRequest{
		Prepare: &pm.TransactionRequest_Prepare{
			FlowCheckpoint: checkpoint3,
		},
	})
	require.NoError(t, err)

	// Receive LoadResponse, which is expected to contain 4 documents.

	loaded, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, loaded.Loaded)
	require.Equal(t, 4, len(loaded.Loaded.DocsJson))

	for i, expected := range []string{newDoc1, doc2, doc3, doc4} {
		var actual = loaded.Loaded.Arena.Bytes(loaded.Loaded.DocsJson[i])
		require.Equal(t, expected, string(actual))
	}

	// Receive Prepared
	prepared, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, prepared.Prepared, "unexpected message: %v+", prepared)

	// Send and receive Commit / Committed.
	err = transaction.Send(&pm.TransactionRequest{
		Commit: &pm.TransactionRequest_Commit{},
	})
	require.NoError(t, err)

	committed, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, committed.Committed)

	// Shut down stream.
	require.NoError(t, transaction.CloseSend())
	_, err = transaction.Recv()
	require.Equal(t, io.EOF, err)

	// Last thing is to snapshot the database tables we care about.
	var quotes = sqlDriver.DoubleQuotes()
	var tab = sqlDriver.TableForMaterialization(
		"test_target", // Matches fixture in testdata/driver-steps.yaml
		"", &quotes,
		&pf.MaterializationSpec_Binding{
			Collection:     model.Bindings[0].Collection,
			FieldSelection: fields,
		})
	var dump = dumpTables(t, endpointConfig.Path, tab,
		sqlDriver.FlowCheckpointsTable(sqlDriver.DefaultFlowCheckpoints))
	cupaloy.SnapshotT(t, dump)
}

func dumpTables(t *testing.T, uri string, tables ...*sqlDriver.Table) string {
	uri = fmt.Sprintf("%s?mode=ro", uri)
	var db, err = sql.Open("sqlite3", uri)
	require.NoError(t, err)
	defer db.Close()

	out, err := sqlDriver.DumpTables(db, tables...)
	require.NoError(t, err)

	return out
}

func newLoadReq(keys ...[]byte) *pm.TransactionRequest_Load {
	var arena pf.Arena
	var packedKeys = arena.AddAll(keys...)
	return &pm.TransactionRequest_Load{
		Arena:      arena,
		PackedKeys: packedKeys,
	}
}
