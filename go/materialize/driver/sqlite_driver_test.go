package driver

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/fdb/tuple"
	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestSQLiteDriver(t *testing.T) {
	var driver = NewSQLiteDriver()

	var ctx = context.Background()

	const bufSize = 1024 * 1024

	var lis *bufconn.Listener
	var bufDialer = func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	pm.RegisterDriverServer(s, driver)
	var done = make(chan error, 1)
	go func() {
		var e = s.Serve(lis)
		done <- e
	}()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	var client = pm.NewDriverClient(conn)

	doTestSQLite(t, client)
	s.GracefulStop()
	err = <-done
	require.NoError(t, err)
}

func doTestSQLite(t *testing.T, driver pm.DriverClient) {
	var ctx = context.Background()
	var tempdir, err = ioutil.TempDir("", "sqlite-driver-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempdir)

	var testCaller = "canary"
	var endpoint = path.Join(tempdir, "target.db")
	var tableName = "test_target"
	var startSession = pm.SessionRequest{
		ShardId:     testCaller,
		EndpointUrl: endpoint,
		Target:      tableName,
	}

	sessionResponse, err := driver.StartSession(ctx, &startSession)
	require.NoError(t, err)

	cat, err := flow.NewCatalog("../../../catalog.db", tempdir)
	require.NoError(t, err)

	collection, err := cat.LoadCollection("weird-types/optionals")
	require.NoError(t, err)

	// Validate should return constraints for a non-existant materialization
	var validateReq = pm.ValidateRequest{
		Handle:     sessionResponse.Handle,
		Collection: &collection,
	}

	validateResp, err := driver.Validate(ctx, &validateReq)
	require.NoError(t, err)
	// There should be a constraint for every projection
	require.Equal(t, len(collection.Projections), len(validateResp.Constraints))

	require.Equal(t, pm.Constraint_LOCATION_REQUIRED, validateResp.Constraints["theKey"].Type)
	require.Equal(t, pm.Constraint_LOCATION_REQUIRED, validateResp.Constraints["flow_document"].Type)

	require.Equal(t, pm.Constraint_LOCATION_RECOMMENDED, validateResp.Constraints["string"].Type)
	require.Equal(t, pm.Constraint_LOCATION_RECOMMENDED, validateResp.Constraints["bool"].Type)
	require.Equal(t, pm.Constraint_LOCATION_RECOMMENDED, validateResp.Constraints["int"].Type)
	require.Equal(t, pm.Constraint_LOCATION_RECOMMENDED, validateResp.Constraints["number"].Type)
	// TODO: we can add these assertions once this is integrated with the catalog spec changes,
	// since that PR also generates projections for object and array fields.
	//require.Equal(t, pm.Constraint_FIELD_OPTIONAL, validateResp.Constraints["object"].Type)
	//require.Equal(t, pm.Constraint_FIELD_OPTIONAL, validateResp.Constraints["array"].Type)

	// Select some fields and Apply the materialization
	var fields = pm.FieldSelection{
		Keys:     []string{"theKey"},
		Values:   []string{"string", "bool", "int"}, // intentionally missing "number" field
		Document: "flow_document",
	}
	var applyReq = pm.ApplyRequest{
		Handle:     sessionResponse.Handle,
		Collection: &collection,
		Fields:     &fields,
		DryRun:     true,
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
	require.Equal(t, len(collection.Projections), len(validateResp.Constraints))
	for _, field := range fields.AllFields() {
		require.Equal(t, pm.Constraint_FIELD_REQUIRED, validateResp.Constraints[field].Type)
	}
	// The "number" field should be forbidden because it was not included in the FieldSelection that
	// was applied.
	require.Equal(t, pm.Constraint_FIELD_FORBIDDEN, validateResp.Constraints["number"].Type)

	// Test the initial Fence, which should return an empty checkpoint
	fencResp, err := driver.Fence(ctx, &pm.FenceRequest{
		Handle: sessionResponse.Handle,
	})
	require.NoError(t, err)
	require.Empty(t, fencResp.FlowCheckpoint)

	// Test Load with keys that don't exist yet
	var key1 = tuple.Tuple{"key1Value"}
	var key2 = tuple.Tuple{"key2Value"}
	var key3 = tuple.Tuple{"key3Value"}
	var loadReq = newLoadReq(sessionResponse.Handle, key1.Pack(), key2.Pack(), key3.Pack())
	loadResp, err := driver.Load(ctx, &loadReq)
	require.NoError(t, err)
	require.Equal(t, 3, len(loadResp.DocsJson))
	for _, doc := range loadResp.DocsJson {
		require.Empty(t, loadResp.Arena.Bytes(doc))
	}

	// Test Store to add those keys
	storeClient, err := driver.Store(ctx)
	require.NoError(t, err)
	var doc1 = `{ "theKey": "key1Value", "string": "foo", "bool": true, "int": 77, "number": 12.34 }`
	var doc2 = `{ "theKey": "key2Value", "string": "bar", "bool": false, "int": 88, "number": 56.78 }`
	var doc3 = `{ "theKey": "key3Value", "string": "baz", "bool": false, "int": 99, "number": 0 }`

	var checkpoint1 = []byte("first checkpoint value")
	var storeStart = pm.StoreRequest_Start{
		Handle:         sessionResponse.Handle,
		Fields:         &fields,
		FlowCheckpoint: checkpoint1,
	}
	storeClient.Send(&pm.StoreRequest{Start: &storeStart})

	var continueReq = new(pm.StoreRequest_Continue)
	continueReq.Exists = []bool{false, false}
	continueReq.PackedKeys = continueReq.Arena.AddAll(key1.Pack(), key2.Pack())
	continueReq.PackedValues = continueReq.Arena.AddAll(
		tuple.Tuple{"foo", true, 77}.Pack(),
		tuple.Tuple{"bar", false, 88}.Pack(),
	)
	continueReq.DocsJson = continueReq.Arena.AddAll([]byte(doc1), []byte(doc2))
	storeClient.Send(&pm.StoreRequest{Continue: continueReq})

	continueReq = new(pm.StoreRequest_Continue)
	continueReq.Exists = []bool{false}
	continueReq.PackedKeys = continueReq.Arena.AddAll(key3.Pack())
	continueReq.PackedValues = continueReq.Arena.AddAll(
		tuple.Tuple{"baz", false, 99}.Pack(),
	)
	continueReq.DocsJson = continueReq.Arena.AddAll([]byte(doc3))
	storeClient.Send(&pm.StoreRequest{Continue: continueReq})

	storeResp, err := storeClient.CloseAndRecv()
	require.NoError(t, err)
	require.Empty(t, storeResp.DriverCheckpoint)

	loadResp, err = driver.Load(ctx, &loadReq)
	require.NoError(t, err)
	require.Equal(t, 3, len(loadResp.DocsJson))

	var allDocs = []string{doc1, doc2, doc3}
	for i, slice := range loadResp.DocsJson {
		var actual = string(loadResp.Arena.Bytes(slice))
		require.Equal(t, allDocs[i], actual)
	}

	// Start a new Session and assert that a new call to Fence returns the expected checkpoint
	newSession, err := driver.StartSession(ctx, &pm.SessionRequest{
		EndpointUrl: endpoint,
		Target:      tableName,
		ShardId:     testCaller,
	})
	require.NoError(t, err)

	newFence, err := driver.Fence(ctx, &pm.FenceRequest{
		Handle: newSession.Handle,
	})
	require.NoError(t, err)
	require.Equal(t, checkpoint1, newFence.FlowCheckpoint)

	// Make another store request that mixes an update in with a new document, and verify that the
	// final set of documents is correct.
	var checkpoint2 = []byte("second checkpoint value")
	var newDoc1 = `{ "theKey": "key1Value", "string": "notthesame", "bool": false, "int": 33, "number": 2 }`
	var key4 = tuple.Tuple{"key4Value"}
	var doc4 = `{ "theKey": "key4Value" }`
	storeClient, err = driver.Store(ctx)
	require.NoError(t, err)
	storeClient.Send(&pm.StoreRequest{
		Start: &pm.StoreRequest_Start{
			Handle:         newSession.Handle,
			Fields:         &fields,
			FlowCheckpoint: checkpoint2,
		},
	})
	continueReq = new(pm.StoreRequest_Continue)
	continueReq.Exists = []bool{true, false}
	continueReq.PackedKeys = continueReq.Arena.AddAll(key1.Pack(), key4.Pack())
	continueReq.PackedValues = continueReq.Arena.AddAll(
		tuple.Tuple{"totally different", false, 33}.Pack(),
		tuple.Tuple{nil, nil, nil}.Pack(),
	)
	continueReq.DocsJson = continueReq.Arena.AddAll([]byte(newDoc1), []byte(doc4))
	err = storeClient.Send(&pm.StoreRequest{
		Continue: continueReq,
	})
	require.NoError(t, err)
	storeResp, err = storeClient.CloseAndRecv()
	require.NoError(t, err)
	require.Empty(t, storeResp.DriverCheckpoint)

	loadReq = newLoadReq(newSession.Handle, key1.Pack(), key2.Pack(), key3.Pack(), key4.Pack())
	loadResp, err = driver.Load(ctx, &loadReq)
	require.NoError(t, err)
	require.Equal(t, 4, len(loadResp.DocsJson))

	allDocs = []string{newDoc1, doc2, doc3, doc4}
	for i, slice := range loadResp.DocsJson {
		var actual = string(loadResp.Arena.Bytes(slice))
		require.Equal(t, allDocs[i], actual)
	}

	// Last thing is to snapshot the database tables we care about
	var tab = tableForMaterialization(tableName, "", &MaterializationSpec{Collection: collection, Fields: fields})
	var dump = dumpTables(t, endpoint, tab)
	cupaloy.SnapshotT(t, dump)
}

type AnyCol string

func (col *AnyCol) Scan(i interface{}) error {
	var sval string
	if b, ok := i.([]byte); ok {
		sval = string(b)
	} else {
		sval = fmt.Sprint(i)
	}
	*col = AnyCol(sval)
	return nil
}
func (col AnyCol) String() string {
	return string(col)
}

func dumpTables(t *testing.T, uri string, tables ...*Table) string {
	uri = fmt.Sprintf("%s?mode=ro", uri)
	var db, err = sql.Open("sqlite3", uri)
	require.NoError(t, err)
	defer db.Close()

	var builder strings.Builder
	for tn, table := range tables {
		if tn > 0 {
			builder.WriteString("\n\n") // make it more readable
		}
		var colNames strings.Builder
		for i, col := range table.Columns {
			if i > 0 {
				colNames.WriteString(", ")
			}
			colNames.WriteString(col.Name)
		}

		var sql = fmt.Sprintf("SELECT %s FROM %s;", colNames.String(), table.Name)
		rows, err := db.Query(sql)
		require.NoError(t, err)
		defer rows.Close()

		fmt.Fprintf(&builder, "%s:\n", table.Name)
		builder.WriteString(colNames.String())

		for rows.Next() {
			var data = make([]AnyCol, len(table.Columns))
			var ptrs = make([]interface{}, len(table.Columns))
			for i := range data {
				ptrs[i] = &data[i]
			}
			err = rows.Scan(ptrs...)
			require.NoError(t, err)
			builder.WriteString("\n")
			for i, v := range ptrs {
				if i > 0 {
					builder.WriteString(", ")
				}
				var val = v.(*AnyCol)
				builder.WriteString(val.String())
			}
		}
	}
	return builder.String()
}

func newLoadReq(handle []byte, keys ...[]byte) pm.LoadRequest {
	var arena pf.Arena
	var packedKeys = arena.AddAll(keys...)
	return pm.LoadRequest{
		Handle:     handle,
		Arena:      arena,
		PackedKeys: packedKeys,
	}
}
