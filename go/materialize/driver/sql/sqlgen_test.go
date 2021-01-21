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
	var gazetteCheckpoints = GazetteCheckpointsTable()
	var flowMaterializations = FlowMaterializationsTable()
	var allTables = []*Table{&testTable, gazetteCheckpoints, flowMaterializations}

	var pgGen = PostgresSQLGenerator()
	var sqliteGen = SQLiteSQLGenerator()
	var generators = map[string]SQLGenerator{
		"postgres": &pgGen,
		"sqlite":   &sqliteGen,
	}

	for dialect, gen := range generators {
		for _, table := range allTables {
			// Test all the generic sql generation functions for each table
			t.Run(fmt.Sprintf("%s_%s", dialect, table.Name), func(t *testing.T) {
				var createTable, err = gen.CreateTable(table)
				require.NoError(t, err)

				var keyColumns []string
				var valueColumns []string
				for _, col := range table.Columns {
					if col.PrimaryKey {
						keyColumns = append(keyColumns, col.Name)
					} else {
						valueColumns = append(valueColumns, col.Name)
					}
				}
				query, _, err := gen.QueryOnPrimaryKey(table, valueColumns...)
				require.NoError(t, err)
				insertStatement, _, err := gen.InsertStatement(table)
				require.NoError(t, err)
				updateStatement, _, err := gen.UpdateStatement(table, valueColumns, keyColumns)
				require.NoError(t, err)

				var allSQL = strings.Join([]string{createTable, query, insertStatement, updateStatement}, "\n\n")
				cupaloy.SnapshotT(t, allSQL)
			})
		}
		// Test the DirectInsertStatement function, but only for the flow_materializations table
		// This doesn't need to be a valid MaterializationSpec for this test, but we do want to test
		// some json that contains single quotes and newlines.
		var materializationJSON = `{
            "collectionSpec": {
                "name": "foo",
                "schemaUri": "test://schema.test/mySchema.json",
                "key": ["/id"]
                "projections": [
                    {
                        "field": "wee'ee",
                        "ptr": "/wee",
                        "isPrimaryKey": false,
                        "userProvided": true
                    }
                ]
            },
            "fields": {
                "keys": ["id"],
                "values": ["wee'ee"],
                "document": "yes, please"
            }
        }`
		t.Run(fmt.Sprintf("%s_flow_materialization_insert", dialect), func(t *testing.T) {
			var insertStatement, err = gen.DirectInsertStatement(flowMaterializations, "test_table", materializationJSON)
			require.NoError(t, err)
			cupaloy.SnapshotT(t, insertStatement)
		})
	}
}

func TestDefaultQuoteStringValue(t *testing.T) {
	var testCases = map[string]string{
		"foo":            "'foo'",
		"he's 'bouta go": "'he''s ''bouta go'",
		"'moar quotes'":  "'''moar quotes'''",
		"":               "''",
	}
	for input, expected := range testCases {
		var actual = DefaultQuoteStringValue(input)
		require.Equal(t, expected, actual)
	}
}

func testTable() Table {
	return Table{
		Name:        "test_table",
		Comment:     "this is a test\nmultiline\ncomment",
		IfNotExists: false,
		Columns: []Column{
			{
				Name:       "key_a",
				Comment:    "key_a\nmultiline\ncomment",
				PrimaryKey: true,
				Type:       INTEGER,
				NotNull:    true,
			},
			{
				Name:       "key_b",
				PrimaryKey: true,
				Type:       STRING,
				NotNull:    true,
			},
			{
				Name:       "key_c",
				PrimaryKey: true,
				Type:       BOOLEAN,
				NotNull:    true,
			},
			{
				Name: "val_x",
				Type: BINARY,
			},
			{
				Name: "val_y",
				Type: NUMBER,
			},
			{
				Name: "val_z",
				Type: ARRAY,
			},
			{
				Name:    "flow_document",
				Type:    OBJECT,
				NotNull: true,
			},
		},
	}
}
