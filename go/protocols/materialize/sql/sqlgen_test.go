package sql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestSQLGenerator(t *testing.T) {
	var testTable = testTable()
	var flowCheckpoints = FlowCheckpointsTable(DefaultFlowCheckpoints)
	var flowMaterializations = FlowMaterializationsTable(DefaultFlowMaterializations)
	var allTables = []*Table{&testTable, flowCheckpoints, flowMaterializations}

	var endpoints = map[string]Endpoint{
		"postgres": NewStdEndpoint(nil, nil, PostgresSQLGenerator(), FlowTables{}),
		"sqlite":   NewStdEndpoint(nil, nil, SQLiteSQLGenerator(), FlowTables{}),
	}

	for dialect, ep := range endpoints {
		for _, table := range allTables {
			// Test all the generic sql generation functions for each table
			t.Run(fmt.Sprintf("%s_%s", dialect, table.Identifier), func(t *testing.T) {
				var createTable, err = ep.CreateTableStatement(table)
				require.NoError(t, err)

				// Store the Names of the key and value columns so we can reference them when
				// generating statements.
				var keyColumns []string
				var valueColumns []string
				for _, col := range table.Columns {
					if col.PrimaryKey {
						keyColumns = append(keyColumns, col.Name)
					} else {
						valueColumns = append(valueColumns, col.Name)
					}
				}
				query, _, err := ep.Generator().QueryOnPrimaryKey(table, valueColumns...)
				require.NoError(t, err)
				insertStatement, _, err := ep.Generator().InsertStatement(table)
				require.NoError(t, err)
				updateStatement, _, err := ep.Generator().UpdateStatement(table, valueColumns, keyColumns)
				require.NoError(t, err)

				var allSQL = strings.Join([]string{createTable, query, insertStatement, updateStatement}, "\n\n")
				cupaloy.SnapshotT(t, allSQL)
			})
		}
	}
}

func testTable() Table {
	return Table{
		Identifier:  "test_table",
		Comment:     "this is a test\nmultiline\ncomment",
		IfNotExists: false,
		Columns: []Column{
			{
				Name:       "key_a",
				Identifier: "\"key_a\"",
				Comment:    "key_a\nmultiline\ncomment",
				PrimaryKey: true,
				Type:       INTEGER,
				NotNull:    true,
			},
			{
				Name:       "key_b",
				Identifier: "key_b",
				PrimaryKey: true,
				Type:       STRING,
				NotNull:    true,
			},
			{
				Name:       "key_c",
				Identifier: "key_c",
				PrimaryKey: true,
				Type:       BOOLEAN,
				NotNull:    true,
			},
			{
				Name:       "val_x",
				Identifier: "val_x",
				Type:       BINARY,
			},
			{
				Name:       "val_y",
				Identifier: "val_y",
				Type:       NUMBER,
			},
			{
				Name:       "val_z",
				Identifier: "val_z",
				Type:       ARRAY,
			},
			{
				Name:       "flow_document",
				Identifier: "flow_document",
				Type:       OBJECT,
				NotNull:    true,
			},
		},
	}
}
