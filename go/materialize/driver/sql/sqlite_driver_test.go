package sql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
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
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestSQLiteDriver(t *testing.T) {
	log.SetLevel(log.DebugLevel)
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

	cat, err := flow.NewCatalog("../../../../catalog.db", tempdir)
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
		var actual = validateResp.Constraints[field].Type
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
	require.Equal(t, pm.Constraint_FIELD_FORBIDDEN, validateResp.Constraints["number"].Type)

	// Test the initial Fence, which should return an empty checkpoint
	fencResp, err := driver.Fence(ctx, &pm.FenceRequest{
		Handle: sessionResponse.Handle,
	})
	require.NoError(t, err)
	require.Empty(t, fencResp.FlowCheckpoint)

	transaction, err := driver.Transaction(ctx)
	require.NoError(t, err)

	var checkpoint1 = []byte("first checkpoint value")
	err = transaction.Send(&pm.TransactionRequest{
		Start: &pm.TransactionRequest_Start{
			Handle:         sessionResponse.Handle,
			Fields:         &fields,
			FlowCheckpoint: checkpoint1,
		},
	})
	require.NoError(t, err)

	// Test Load with keys that don't exist yet
	var key1 = tuple.Tuple{"key1Value"}
	var key2 = tuple.Tuple{"key2Value"}
	var loadReq = newLoadReq(key1.Pack(), key2.Pack())
	err = transaction.Send(&pm.TransactionRequest{
		Load: &loadReq,
	})
	require.NoError(t, err)
	var key3 = tuple.Tuple{"key3Value"}
	loadReq = newLoadReq(key3.Pack())
	err = transaction.Send(&pm.TransactionRequest{
		Load: &loadReq,
	})
	require.NoError(t, err)

	err = transaction.Send(&pm.TransactionRequest{
		LoadEOF: &pm.LoadEOF{},
	})
	require.NoError(t, err)

	// Receive Load EOF, which indicates that none of the documents exist
	resp, err := transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, resp.LoadEOF, "unexpected message: %v+", resp)

	// Test Store to add those keys
	require.NoError(t, err)
	var doc1 = `{ "theKey": "key1Value", "string": "foo", "bool": true, "int": 77, "number": 12.34 }`
	var doc2 = `{ "theKey": "key2Value", "string": "bar", "bool": false, "int": 88, "number": 56.78 }`
	var doc3 = `{ "theKey": "key3Value", "string": "baz", "bool": false, "int": 99, "number": 0 }`

	var store1 = pm.TransactionRequest_StoreRequest{}
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

	var store2 = pm.TransactionRequest_StoreRequest{}
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
	err = transaction.CloseSend()
	require.NoError(t, err)

	storeResp, err := transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, storeResp.StoreResponse)
	require.Empty(t, storeResp.StoreResponse.DriverCheckpoint)

	_, err = transaction.Recv()
	require.Equal(t, io.EOF, err)

	// Now we start a new session and go through the whole thing again

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

	transaction, err = driver.Transaction(ctx)
	require.NoError(t, err)

	var checkpoint2 = []byte("second checkpoint value")
	err = transaction.Send(&pm.TransactionRequest{
		Start: &pm.TransactionRequest_Start{
			Handle:         newSession.Handle,
			Fields:         &fields,
			FlowCheckpoint: checkpoint2,
		},
	})
	require.NoError(t, err)

	loadReq = newLoadReq(key1.Pack(), key2.Pack(), key3.Pack())
	err = transaction.Send(&pm.TransactionRequest{
		Load: &loadReq,
	})
	require.NoError(t, err)
	err = transaction.Send(&pm.TransactionRequest{
		LoadEOF: &pm.LoadEOF{},
	})
	require.NoError(t, err)

	// Receive LoadResponse, which is expected to contain our 3 documents.
	resp, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, resp.LoadResponse)
	require.Equal(t, 3, len(resp.LoadResponse.DocsJson))

	for i, expected := range []string{doc1, doc2, doc3} {
		var actual = resp.LoadResponse.Arena.Bytes(resp.LoadResponse.DocsJson[i])
		require.Equal(t, expected, string(actual))
	}

	// Receive Load EOF
	resp, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, resp.LoadEOF)

	// This store will update one document and add a new one.
	var newDoc1 = `{ "theKey": "key1Value", "string": "notthesame", "bool": false, "int": 33, "number": 2 }`
	var key4 = tuple.Tuple{"key4Value"}
	var doc4 = `{ "theKey": "key4Value" }`

	var storeReq = pm.TransactionRequest_StoreRequest{}
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

	// Close transaction and assert we get a storeResponse followed by end of stream
	err = transaction.CloseSend()
	require.NoError(t, err)

	storeResp, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, storeResp.StoreResponse)
	require.Empty(t, storeResp.StoreResponse.DriverCheckpoint)

	_, err = transaction.Recv()
	require.Equal(t, io.EOF, err)

	// One more transaction just to verify the updated documents
	transaction, err = driver.Transaction(ctx)
	require.NoError(t, err)

	var checkpoint3 = []byte("third checkpoint value")
	err = transaction.Send(&pm.TransactionRequest{
		Start: &pm.TransactionRequest_Start{
			Handle:         newSession.Handle,
			Fields:         &fields,
			FlowCheckpoint: checkpoint3,
		},
	})
	require.NoError(t, err)

	loadReq = newLoadReq(key1.Pack(), key2.Pack(), key3.Pack(), key4.Pack())
	err = transaction.Send(&pm.TransactionRequest{
		Load: &loadReq,
	})
	require.NoError(t, err)
	err = transaction.Send(&pm.TransactionRequest{
		LoadEOF: &pm.LoadEOF{},
	})
	require.NoError(t, err)

	// Receive LoadResponse, which is expected to contain 4 documents.
	resp, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, resp.LoadResponse)
	require.Equal(t, 4, len(resp.LoadResponse.DocsJson))

	for i, expected := range []string{newDoc1, doc2, doc3, doc4} {
		var actual = resp.LoadResponse.Arena.Bytes(resp.LoadResponse.DocsJson[i])
		require.Equal(t, expected, string(actual))
	}

	err = transaction.CloseSend()
	require.NoError(t, err)

	resp, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, resp.LoadEOF, "expected LoadEOF, got: %+v", resp)

	storeResp, err = transaction.Recv()
	require.NoError(t, err)
	require.NotNil(t, storeResp.StoreResponse, "Expected final StoreResponse, got: %+v", storeResp)

	_, err = transaction.Recv()
	require.Equal(t, io.EOF, err)

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

func newLoadReq(keys ...[]byte) pm.TransactionRequest_LoadRequest {
	var arena pf.Arena
	var packedKeys = arena.AddAll(keys...)
	return pm.TransactionRequest_LoadRequest{
		Arena:      arena,
		PackedKeys: packedKeys,
	}
}
