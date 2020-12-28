package materialize

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/estuary/flow/go/fdb/tuple"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestSqliteMaterialization(t *testing.T) {
	dbfile := tempFilename("sqlitetest")
	defer os.Remove(dbfile)

	db, err := sql.Open("sqlite3", dbfile)
	if err != nil {
		t.Fatal(err)
	}

	execSQL(db, t, createFlowMaterializationsTable)

	execSQL(db, t, `
        INSERT INTO flow_materializations (table_name, config_json)
        VALUES ('good_table', '{
            "name": "testCollectionName",
            "schema_uri": "test://test/schema.json",
            "projections": [
                { "field": "a", "ptr": "/a", "is_primary_key": true },
                { "field": "b", "ptr": "/b", "is_primary_key": true },
                { "field": "x", "ptr": "/x", "is_primary_key": false },
                { "field": "y", "ptr": "/y", "is_primary_key": false },
                { "field": "z", "ptr": "/z", "is_primary_key": false }
            ]
        }');
    `)
	execSQL(db, t, `CREATE TABLE good_table (a, b, x, y, z, flow_document, PRIMARY KEY (a, b));`)
	// close the database, since we're done with setup
	require.Nil(t, db.Close(), "db error on close")

	materialization := Materialization{
		CatalogDBID: 1,
		TargetName:  "testSqlite",
		TargetURI:   dbfile,
		TableName:   "good_table",
		TargetType:  "sqlite",
	}
	target, err := NewMaterializationTarget(&materialization)
	if err != nil {
		t.Fatalf("Failed to initialize materialization target: %v", err)
	}

	expectedPointers := []string{"/a", "/b", "/x", "/y", "/z"}
	require.Equal(t, expectedPointers, target.ProjectionPointers(), "invalid field pointers")

	expectedPKIndexes := []int{0, 1}
	require.Equal(t, expectedPKIndexes, target.PrimaryKeyFieldIndexes(), "invalid PK indexes")

	transaction, err := target.BeginTxn(context.Background())

	shouldBeNil, err := transaction.FetchExistingDocument(tuple.Tuple{"foo", "bar"})
	require.Nil(t, err, "error fetching missing document")
	require.Nil(t, shouldBeNil, "expected nil document")

	fullDoc := json.RawMessage("testDocument")
	err = transaction.Store(fullDoc, nil, tuple.Tuple{"someA", "someB", "someX", "someY", "someZ"})
	require.Nil(t, err, "error storing document")

	returnedDoc, err := transaction.FetchExistingDocument(tuple.Tuple{"someA", "someB"})
	require.Nil(t, err, "failed to fetch existing document")
	require.Equal(t, fullDoc, returnedDoc, json.RawMessage("invalid flow_document"))

	err = transaction.Store(json.RawMessage("differentDoc"), nil,
		tuple.Tuple{"someA", "someB", "diffX", "diffY", "diffZ"})
	require.Nil(t, err, "error updating document")

	updated, err := transaction.FetchExistingDocument(tuple.Tuple{"someA", "someB"})
	require.Nil(t, err, "failed to fetch updated doc")
	require.Equal(t, json.RawMessage("differentDoc"), updated, "invalid updated flow_document")
}

func assertEq(t *testing.T, expected interface{}, actual interface{}) {
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected: %v, actual: %v", expected, actual)
	}
}

const createFlowMaterializationsTable = `
CREATE TABLE IF NOT EXISTS flow_materializations
(
    table_name TEXT NOT NULL PRIMARY KEY,
    config_json TEXT NOT NULL
);`

func execSQL(db *sql.DB, t *testing.T, sql string, args ...interface{}) {
	_, err := db.Exec(sql, args...)
	if err != nil {
		t.Fatalf("Failed to execute sql: %q, err: %v", sql, err)
	}
}

func tempFilename(name string) string {
	ts := time.Now().UnixNano()
	filename := fmt.Sprintf("%s-%s", name, strconv.FormatInt(ts, 10)[:6])
	return path.Join(os.TempDir(), filename)
}
