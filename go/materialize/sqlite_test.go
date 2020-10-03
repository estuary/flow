package materialize

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestSqliteMaterialization(t *testing.T) {
	dbfile := tempFilename("sqlitetest")
	defer os.Remove(dbfile)

	db, err := sql.Open("sqlite3", dbfile)
	if err != nil {
		t.Fatal(err)
	}

	execSql(db, t, createFlowMaterializationsTable)

	execSql(db, t, `
        INSERT INTO flow_materializations (table_name, config_json)
        VALUES ('good_table', '{
            "fields": [
                { "field": "a", "locationPtr": "/a", "primaryKey": true },
                { "field": "b", "locationPtr": "/b", "primaryKey": true },
                { "field": "x", "locationPtr": "/x", "primaryKey": false },
                { "field": "y", "locationPtr": "/y", "primaryKey": false },
                { "field": "z", "locationPtr": "/z", "primaryKey": false }
            ]
        }');
    `)
	execSql(db, t, `CREATE TABLE good_table (a, b, x, y, z, flow_document, PRIMARY KEY (a, b));`)
	// close the database, since we're done with setup
	require.Nil(t, db.Close(), "db error on close")

	materialization := Materialization{
		CatalogDbId:         1,
		MaterializationName: "testSqlite",
		TargetUri:           dbfile,
		TableName:           "good_table",
		TargetType:          "sqlite",
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

	shouldBeNil, err := transaction.FetchExistingDocument([]interface{}{"foo", "bar"})
	require.Nil(t, err, "error fetching missing document")
	require.Nil(t, shouldBeNil, "expected nil document")

	fullDoc := json.RawMessage("testDocument")
	err = transaction.Store([]interface{}{"someA", "someB", "someX", "someY", "someZ"}, fullDoc)
	require.Nil(t, err, "error storing document")

	returnedDoc, err := transaction.FetchExistingDocument([]interface{}{"someA", "someB"})
	require.Nil(t, err, "failed to fetch existing document")
	require.Equal(t, fullDoc, returnedDoc, json.RawMessage("invalid flow_document"))

	err = transaction.Store([]interface{}{"someA", "someB", "diffX", "diffY", "diffZ"}, json.RawMessage("differentDoc"))
	require.Nil(t, err, "error updating document")

	updated, err := transaction.FetchExistingDocument([]interface{}{"someA", "someB"})
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

func execSql(db *sql.DB, t *testing.T, sql string, args ...interface{}) {
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
