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
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/materialize/driver/sqlite"
	"github.com/estuary/flow/go/protocols/catalog"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	sqlDriver "github.com/estuary/flow/go/protocols/materialize/sql"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

func TestSQLGeneration(t *testing.T) {
	pb.RegisterGRPCDispatcher("local")

	var args = bindings.BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "fixture",
			BuildDb:    path.Join(t.TempDir(), "build.db"),
			Source:     "file:///sql-gen.yaml",
			SourceType: pf.ContentType_CATALOG,
		},
	}
	require.NoError(t, bindings.BuildCatalog(args))

	var spec *pf.MaterializationSpec
	require.NoError(t, catalog.Extract(args.BuildDb, func(db *sql.DB) (err error) {
		spec, err = catalog.LoadMaterialization(db, "test/sqlite")
		return err
	}))

	var gen = sqlDriver.SQLiteSQLGenerator()
	var table = sqlDriver.TableForMaterialization("test_table", "", gen.IdentifierRenderer, spec.Bindings[0])

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
	pb.RegisterGRPCDispatcher("local")

	var args = bindings.BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "fixture",
			BuildDb:    path.Join(t.TempDir(), "build.db"),
			Source:     "file:///driver-steps.yaml",
			SourceType: pf.ContentType_CATALOG,
		},
	}
	require.NoError(t, bindings.BuildCatalog(args))

	// Model MaterializationSpec we'll *mostly* use, but vary slightly in this test.
	var model *pf.MaterializationSpec
	require.NoError(t, catalog.Extract(args.BuildDb, func(db *sql.DB) (err error) {
		model, err = catalog.LoadMaterialization(db, "a/materialization")
		return err
	}))

	var server, err = sqlite.NewInProcessServer(context.Background())
	require.NoError(t, err)

	var driver = server.Client()
	var ctx = pb.WithDispatchDefault(context.Background())

	transaction, err := driver.Materialize(ctx)
	require.NoError(t, err)

	// Config fixture which matches schema of ParseConfig.
	var endpointConfig = struct {
		Path string
	}{Path: "file://" + path.Join(t.TempDir(), "target.db")}
	var endpointJSON, _ = json.Marshal(endpointConfig)

	// Validate should return constraints for a non-existant materialization
	var validateReq = pm.Request_Validate{
		Name:          model.Name,
		ConnectorType: pf.MaterializationSpec_SQLITE,
		ConfigJson:    json.RawMessage(endpointJSON),
		Bindings: []*pm.Request_Validate_Binding{
			{
				Collection:         model.Bindings[0].Collection,
				ResourceConfigJson: model.Bindings[0].ResourceConfigJson,
			},
		},
	}
	require.NoError(t, transaction.Send(&pm.Request{Validate: &validateReq}))

	validateResp, err := transaction.Recv()
	require.NoError(t, err)
	// There should be a constraint for every projection
	require.Equal(t, &pm.Response_Validated_Binding{
		Constraints: map[string]*pm.Response_Validated_Constraint{
			"array":         {Type: pm.Response_Validated_Constraint_FIELD_OPTIONAL, Reason: "This field is able to be materialized"},
			"bool":          {Type: pm.Response_Validated_Constraint_LOCATION_RECOMMENDED, Reason: "The projection has a single scalar type"},
			"flow_document": {Type: pm.Response_Validated_Constraint_LOCATION_REQUIRED, Reason: "The root document must be materialized"},
			"int":           {Type: pm.Response_Validated_Constraint_LOCATION_RECOMMENDED, Reason: "The projection has a single scalar type"},
			"number":        {Type: pm.Response_Validated_Constraint_LOCATION_RECOMMENDED, Reason: "The projection has a single scalar type"},
			"object":        {Type: pm.Response_Validated_Constraint_FIELD_OPTIONAL, Reason: "This field is able to be materialized"},
			"string":        {Type: pm.Response_Validated_Constraint_LOCATION_RECOMMENDED, Reason: "The projection has a single scalar type"},
			"theKey":        {Type: pm.Response_Validated_Constraint_LOCATION_REQUIRED, Reason: "All Locations that are part of the collections key are required"},
		},
		DeltaUpdates: false,
		ResourcePath: model.Bindings[0].ResourcePath,
	}, validateResp.Validated.Bindings[0])

	// Select some fields and Apply the materialization
	var fields = pf.FieldSelection{
		Keys:     []string{"theKey"},
		Values:   []string{"bool", "int", "string"}, // intentionally missing "number" field
		Document: "flow_document",
	}
	var applyReq = pm.Request_Apply{
		Materialization: &pf.MaterializationSpec{
			Name:          model.Name,
			ConnectorType: pf.MaterializationSpec_SQLITE,
			ConfigJson:    json.RawMessage(endpointJSON),
			Bindings: []*pf.MaterializationSpec_Binding{
				{
					Collection:         model.Bindings[0].Collection,
					FieldSelection:     fields,
					ResourcePath:       model.Bindings[0].ResourcePath,
					ResourceConfigJson: model.Bindings[0].ResourceConfigJson,
					DeltaUpdates:       false,
					PartitionSelector:  model.Bindings[0].PartitionSelector,
				},
			},
			ShardTemplate:       model.ShardTemplate,
			RecoveryLogTemplate: model.RecoveryLogTemplate,
		},
		Version: "the-version",
	}
	require.NoError(t, transaction.Send(&pm.Request{Apply: &applyReq}))

	applyResp, err := transaction.Recv()
	require.NoError(t, err)
	require.NotEmpty(t, applyResp.Applied.ActionDescription)

	// Now that we've applied, call Validate again to ensure the existing fields are accounted for
	require.NoError(t, transaction.Send(&pm.Request{Validate: &validateReq}))
	validateResp, err = transaction.Recv()
	require.NoError(t, err)

	// Expect a constraint was returned for each projection.
	require.Equal(t,
		len(model.Bindings[0].Collection.Projections),
		len(validateResp.Validated.Bindings[0].Constraints))

	for _, field := range fields.AllFields() {
		var actual = validateResp.Validated.Bindings[0].Constraints[field].Type
		require.Equal(
			t,
			pm.Response_Validated_Constraint_FIELD_REQUIRED,
			actual,
			"wrong constraint for field: %s, expected FIELD_REQUIRED, got %s",
			field,
			actual,
		)
	}
	// The "number" field should be forbidden because it was not included in the FieldSelection that
	// was applied.
	require.Equal(t, pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
		validateResp.Validated.Bindings[0].Constraints["number"].Type)

	// Insert a fixture into the `flow_checkpoints` table which we'll fence
	// and draw a checkpoint from, and then insert a more-specific checkpoint
	// that reflects our transaction request range fixture.
	{
		var db, err = sql.Open("sqlite3", endpointConfig.Path)
		require.NoError(t, err)

		var cp = &pf.Checkpoint{
			Sources: map[pf.Journal]pc.Checkpoint_Source{"a/journal": {ReadThrough: 1}},
		}
		var cpBytes, _ = cp.Marshal()

		_, err = db.Exec(`INSERT INTO flow_checkpoints_v1
			(materialization, key_begin, key_end, fence, checkpoint)
			VALUES (?, 0, ?, 5, ?)
		;`,
			applyReq.Materialization.Name,
			math.MaxUint32,
			base64.StdEncoding.EncodeToString(cpBytes),
		)
		require.NoError(t, err)
		require.NoError(t, db.Close())
	}

	// Send open.
	err = transaction.Send(&pm.Request{
		Open: &pm.Request_Open{
			Materialization: applyReq.Materialization,
			Version:         "the-version",
			Range: &pf.RangeSpec{
				KeyBegin: 100,
				KeyEnd:   200,
			},
			StateJson: nil,
		},
	})
	require.NoError(t, err)

	// Receive Opened.
	opened, err := transaction.Recv()
	require.NoError(t, err)
	require.Contains(t, opened.Opened.RuntimeCheckpoint.Sources, pb.Journal("a/journal"))

	// Send & receive Acknowledge.
	require.NoError(t, transaction.Send(&pm.Request{
		Acknowledge: &pm.Request_Acknowledge{},
	}))
	acknowledged, err := transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, acknowledged.Acknowledged, acknowledged)

	// Test Load with keys that don't exist yet
	var key1 = tuple.Tuple{"key1Value"}
	var key2 = tuple.Tuple{"key2Value"}
	var key3 = tuple.Tuple{"key3Value"}
	transaction.Send(&pm.Request{Load: &pm.Request_Load{KeyPacked: key1.Pack()}})
	transaction.Send(&pm.Request{Load: &pm.Request_Load{KeyPacked: key2.Pack()}})
	transaction.Send(&pm.Request{Load: &pm.Request_Load{KeyPacked: key3.Pack()}})

	// Send Flush, which ends the Load phase.
	err = transaction.Send(&pm.Request{Flush: &pm.Request_Flush{}})
	require.NoError(t, err)

	// Receive Flushed, which indicates that none of the documents exist
	flushed, err := transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, flushed.Flushed, "unexpected message: %v+", flushed)

	// Build and send Store requests with these documents.
	var doc1 = `{ "theKey": "key1Value", "string": "foo", "bool": true, "int": 77, "number": 12.34 }`
	var doc2 = `{ "theKey": "key2Value", "string": "bar", "bool": false, "int": 88, "number": 56.78 }`
	var doc3 = `{ "theKey": "key3Value", "string": "baz", "bool": false, "int": 99, "number": 0 }`

	transaction.Send(&pm.Request{Store: &pm.Request_Store{
		KeyPacked:    key1.Pack(),
		ValuesPacked: tuple.Tuple{"foo", true, 77}.Pack(),
		DocJson:      []byte(doc1),
		Exists:       false,
	}})
	transaction.Send(&pm.Request{Store: &pm.Request_Store{
		KeyPacked:    key2.Pack(),
		ValuesPacked: tuple.Tuple{"bar", false, 88}.Pack(),
		DocJson:      []byte(doc2),
		Exists:       false,
	}})
	transaction.Send(&pm.Request{Store: &pm.Request_Store{
		KeyPacked:    key3.Pack(),
		ValuesPacked: tuple.Tuple{"baz", false, 99}.Pack(),
		DocJson:      []byte(doc3),
		Exists:       false,
	}})

	// Send StartCommit and receive StartedCommit.
	var checkpoint1 = &pf.Checkpoint{
		Sources: map[pf.Journal]pc.Checkpoint_Source{"a/journal": {ReadThrough: 111}},
	}
	err = transaction.Send(&pm.Request{
		StartCommit: &pm.Request_StartCommit{
			RuntimeCheckpoint: checkpoint1,
		},
	})
	require.NoError(t, err)

	startedCommit, err := transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, startedCommit.StartedCommit)

	// Send & receive Acknowledge.
	require.NoError(t, transaction.Send(&pm.Request{Acknowledge: &pm.Request_Acknowledge{}}))
	acknowledged, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, acknowledged.Acknowledged, acknowledged)

	// Next transaction. Send some loads.
	transaction.Send(&pm.Request{Load: &pm.Request_Load{KeyPacked: key1.Pack()}})
	transaction.Send(&pm.Request{Load: &pm.Request_Load{KeyPacked: key2.Pack()}})
	transaction.Send(&pm.Request{Load: &pm.Request_Load{KeyPacked: key3.Pack()}})

	// Send Flush to drain the load phase.
	err = transaction.Send(&pm.Request{Flush: &pm.Request_Flush{}})
	require.NoError(t, err)

	// Receive Loaded response, which is expected to contain our 3 documents.
	for _, expected := range []string{doc1, doc2, doc3} {
		loaded, err := transaction.Recv()
		require.NoError(t, err)
		require.Equal(t, expected, string(loaded.Loaded.DocJson))
	}

	// Receive Flushed
	flushed, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, flushed.Flushed, "unexpected message: %v+", flushed)

	// This store will update one document and add a new one.
	var newDoc1 = `{ "theKey": "key1Value", "string": "notthesame", "bool": false, "int": 33, "number": 2 }`
	var key4 = tuple.Tuple{"key4Value"}
	var doc4 = `{ "theKey": "key4Value" }`

	transaction.Send(&pm.Request{Store: &pm.Request_Store{
		KeyPacked:    key1.Pack(),
		ValuesPacked: tuple.Tuple{"totally different", false, 33}.Pack(),
		DocJson:      []byte(newDoc1),
		Exists:       true,
	}})
	transaction.Send(&pm.Request{Store: &pm.Request_Store{
		KeyPacked:    key4.Pack(),
		ValuesPacked: tuple.Tuple{nil, nil, nil}.Pack(),
		DocJson:      []byte(doc4),
		Exists:       false,
	}})

	// Commit transaction and assert we get a Committed.
	var checkpoint2 = &pf.Checkpoint{
		Sources: map[pf.Journal]pc.Checkpoint_Source{"a/journal": {ReadThrough: 222}},
	}
	err = transaction.Send(&pm.Request{
		StartCommit: &pm.Request_StartCommit{
			RuntimeCheckpoint: checkpoint2,
		},
	})
	require.NoError(t, err)

	startedCommit, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, startedCommit.StartedCommit)

	// Send & receive Acknowledge.
	require.NoError(t, transaction.Send(&pm.Request{
		Acknowledge: &pm.Request_Acknowledge{},
	}))
	acknowledged, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, acknowledged.Acknowledged, acknowledged)

	// One more transaction just to verify the updated documents
	for _, key := range []tuple.Tuple{key1, key2, key3, key4} {
		transaction.Send(&pm.Request{Load: &pm.Request_Load{KeyPacked: key.Pack()}})
	}

	// Send Flush.
	err = transaction.Send(&pm.Request{Flush: &pm.Request_Flush{}})
	require.NoError(t, err)

	// Receive loads, and expect it contains 4 documents.
	for _, expected := range []string{newDoc1, doc2, doc3, doc4} {
		loaded, err := transaction.Recv()
		require.NoError(t, err)
		require.Equal(t, expected, string(loaded.Loaded.DocJson))
	}

	// Receive Flushed
	flushed, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, flushed.Flushed, "unexpected message: %v+", flushed)

	// Send and receive StartCommit / StartedCommit.
	var checkpoint3 = &pf.Checkpoint{
		Sources: map[pf.Journal]pc.Checkpoint_Source{"a/journal": {ReadThrough: 333}},
	}
	require.NoError(t, transaction.Send(&pm.Request{
		StartCommit: &pm.Request_StartCommit{
			RuntimeCheckpoint: checkpoint3,
		},
	}))
	startedCommit, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, startedCommit.StartedCommit)

	// Send & receive a final Acknowledge.
	require.NoError(t, transaction.Send(&pm.Request{
		Acknowledge: &pm.Request_Acknowledge{},
	}))
	acknowledged, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, acknowledged.Acknowledged, acknowledged)

	// Gracefully shut down the stream.
	require.NoError(t, transaction.CloseSend())
	_, err = transaction.Recv()
	require.Equal(t, io.EOF, err)

	// Snapshot database tables and verify them against our expectation.
	var identifierRenderer = sqlDriver.NewRenderer(nil, sqlDriver.DoubleQuotesWrapper(), sqlDriver.DefaultUnwrappedIdentifiers)
	var tab = sqlDriver.TableForMaterialization(
		"test_target", // Matches fixture in testdata/driver-steps.yaml
		"", identifierRenderer,
		&pf.MaterializationSpec_Binding{
			Collection:     model.Bindings[0].Collection,
			FieldSelection: fields,
		})
	var dump = dumpTables(t, endpointConfig.Path, tab,
		sqlDriver.FlowCheckpointsTable(sqlDriver.DefaultFlowCheckpoints))
	cupaloy.SnapshotT(t, dump)

	// Next we'll verify the deletion of connector table states.
	var verifyTableStatus = func(expect error) {
		db, err := sql.Open("sqlite3", endpointConfig.Path)
		require.NoError(t, err)

		require.Equal(t, expect, db.QueryRow(
			fmt.Sprintf("SELECT 1 FROM %s;", sqlDriver.DefaultFlowCheckpoints)).Scan(new(int)))
		require.Equal(t, expect, db.QueryRow(
			fmt.Sprintf("SELECT 1 FROM %s;", sqlDriver.DefaultFlowMaterializations)).Scan(new(int)))
		require.Equal(t, expect, db.QueryRow(
			"SELECT 1 FROM sqlite_master WHERE type='table' AND tbl_name='test_target';").Scan(new(int)))

		require.NoError(t, db.Close())
	}

	// Precondition: table states exist.
	verifyTableStatus(nil)
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
