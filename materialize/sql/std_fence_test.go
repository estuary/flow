package sql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	_ "github.com/mattn/go-sqlite3" // Import for register side-effects.
	"github.com/stretchr/testify/require"
)

func TestStdEndpointFencingCases(t *testing.T) {
	// runTest takes zero or more key range fixtures, followed by a final pair
	// which is the key range under test.
	var runTest = func(t *testing.T, ranges ...uint32) {
		var db, err = sql.Open("sqlite3", ":memory:")
		require.NoError(t, err)

		ctx := context.Background()

		// Leverage the Endpoint interface
		var endpoint Endpoint = NewStdEndpoint(nil, db, SQLiteSQLGenerator(), DefaultFlowTables(""))

		sql, err := endpoint.CreateTableStatement(endpoint.FlowTables().Checkpoints)
		require.NoError(t, err)
		_, err = db.Exec(sql)
		require.NoError(t, err)

		var fixtures = ranges[:len(ranges)-2]
		var testCase = ranges[len(ranges)-2:]

		for i := 0; i*2 < len(fixtures); i++ {
			_, err = db.Exec(`
			INSERT INTO `+endpoint.FlowTables().Checkpoints.Identifier+`
				(materialization, fence, key_begin, key_end, checkpoint)
				VALUES ("the/materialization", 5, ?, ?, ?)`,
				ranges[i*2],
				ranges[i*2+1],
				base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{byte(i + 1)}, 10)),
			)
			require.NoError(t, err)
		}

		// Add an extra fixture from a different materialization.
		_, err = db.Exec(`
			INSERT INTO ` + endpoint.FlowTables().Checkpoints.Identifier + `
				(materialization, fence, key_begin, key_end, checkpoint)
				VALUES ("other/one", 99, 0, 4294967295, "other-checkpoint")`)
		require.NoError(t, err)

		dump1, err := DumpTables(db, endpoint.FlowTables().Checkpoints)
		require.NoError(t, err)

		// Install a StdEndpoint fence.
		fence, err := endpoint.NewFence(ctx, "the/materialization", testCase[0], testCase[1])
		require.NoError(t, err)

		dump2, err := DumpTables(db, endpoint.FlowTables().Checkpoints)
		require.NoError(t, err)

		// Update it once.
		fence.SetCheckpoint(append(fence.Checkpoint(), []byte{0, 0, 0, 0, 0, 0, 0, 0}...))
		err = fence.(*StdFence).Update(ctx, func(ctx context.Context, sql string, arguments ...interface{}) (rowsAffected int64, _ error) {
			var result, err = db.ExecContext(ctx, sql, arguments...)
			if err == nil {
				rowsAffected, err = result.RowsAffected()
			}
			return rowsAffected, err
		})
		require.NoError(t, err)

		dump3, err := DumpTables(db, endpoint.FlowTables().Checkpoints)
		require.NoError(t, err)

		cupaloy.SnapshotT(t, dump1+"\n"+dump2+"\n"+dump3)
	}

	// If a fence exactly matches a checkpoint, we'll fence that checkpoint and its parent
	// but not siblings. The used checkpoint is that of the exact match.
	t.Run("exact match", func(t *testing.T) {
		runTest(t,
			0, 1000, // Old parent.
			0, 99, // Unrelated sibling.
			100, 199, // Exactly matched.
			200, 299, // Unrelated sibling.
			100, 199)
	})
	// If a fence sub-divides a parent, we'll fence the parent and grand parent
	// but not siblings of the parent. The checkpoint is the younger parent.
	t.Run("split from parent", func(t *testing.T) {
		runTest(t,
			0, 1000, // Grand parent.
			0, 499, // Younger uncle.
			500, 799, // Younger parent.
			800, 1000, // Other uncle.
			500, 599)
	})
	// If a new range straddles existing ranges (this shouldn't ever happen),
	// we'll fence the straddled ranges while taking the checkpoint of the parent.
	t.Run("straddle", func(t *testing.T) {
		runTest(t,
			0, 1000,
			0, 499,
			500, 1000,
			400, 599)
	})
	// If a new range covers another (this also shouldn't ever happen),
	// it takes the checkpoint of its parent while also fencing the covered sub-range.
	t.Run("covered child", func(t *testing.T) {
		runTest(t,
			0, 1000,
			100, 199,
			100, 800)
	})
}
