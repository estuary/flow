package sql

import (
	"database/sql"
	"fmt"
	"strings"
)

const (
	// DefaultFlowCheckpoints is the default table for checkpoints.
	DefaultFlowCheckpoints = "flow_checkpoints_v1"
	// DefaultFlowMaterializations is the default table for materialization specs.
	DefaultFlowMaterializations = "flow_materializations_v2"
)

// FlowTables is the table specifications for Flow.
type FlowTables struct {
	Checkpoints *Table // Table of Flow checkpoints.
	Specs       *Table // Table of MaterializationSpecs.
}

// DefaultFlowTables returns the default Flow *Table configurations and names with optional prefix.
// The prefix can be used to prepend pre-table identifiers such as schema names.
func DefaultFlowTables(prefix string) FlowTables {
	return FlowTables{
		Checkpoints: FlowCheckpointsTable(prefix + DefaultFlowCheckpoints),
		Specs:       FlowMaterializationsTable(prefix + DefaultFlowMaterializations),
	}
}

// FlowCheckpointsTable returns the Table description for the table that holds the checkpoint
// and nonce values for each materialization shard.
func FlowCheckpointsTable(name string) *Table {
	return &Table{
		Name:        name,
		Identifier:  name,
		IfNotExists: true,
		Comment:     "This table holds Flow processing checkpoints used for exactly-once processing of materializations",
		Columns: []Column{
			{
				Name:       "materialization",
				Identifier: "materialization",
				Comment:    "The name of the materialization.",
				PrimaryKey: true,
				Type:       STRING,
				NotNull:    true,
			},
			{
				Name:       "key_begin",
				Identifier: "key_begin",
				Comment:    "The inclusive lower-bound key hash covered by this checkpoint.",
				PrimaryKey: true,
				Type:       INTEGER,
				NotNull:    true,
			},
			{
				Name:       "key_end",
				Identifier: "key_end",
				Comment:    "The inclusive upper-bound key hash covered by this checkpoint.",
				PrimaryKey: true,
				Type:       INTEGER,
				NotNull:    true,
			},
			{
				Name:       "fence",
				Identifier: "fence",
				Comment:    "This nonce is used to uniquely identify unique process assignments of a shard and prevent them from conflicting.",
				Type:       INTEGER,
				NotNull:    true,
			},
			{
				Name:       "checkpoint",
				Identifier: "checkpoint",
				Comment:    "Checkpoint of the Flow consumer shard, encoded as base64 protobuf.",
				Type:       STRING,
			},
		},
	}
}

// FlowMaterializationsTable returns the Table description for the table that holds the
// MaterializationSpec that corresponds to each target table. This state is used both for sql
// generation and for validation.
func FlowMaterializationsTable(name string) *Table {
	return &Table{
		Name:        name,
		Identifier:  name,
		IfNotExists: true,
		Comment:     "This table is the source of truth for all materializations into this system.",
		Columns: []Column{
			{
				Name:       "materialization",
				Identifier: "materialization",
				Comment:    "The name of the materialization.",
				PrimaryKey: true,
				Type:       STRING,
				NotNull:    true,
			},
			{
				Name:       "version",
				Identifier: "version",
				Comment:    "Version of the materialization.",
				Type:       STRING,
				NotNull:    true,
			},
			{
				Name:       "spec",
				Identifier: "spec",
				Comment:    "Specification of the materialization, encoded as base64 protobuf.",
				Type:       STRING,
				NotNull:    true,
			},
		},
	}
}

// DumpTables is a convenience for testing which dumps the contents
// of the given tables into a debug string suitable for snapshotting.
func DumpTables(db *sql.DB, tables ...*Table) (string, error) {
	var b strings.Builder
	for tn, table := range tables {
		if tn > 0 {
			b.WriteString("\n\n") // make it more readable
		}
		var colNames strings.Builder
		for i, col := range table.Columns {
			if i > 0 {
				colNames.WriteString(", ")
			}
			colNames.WriteString(col.Identifier)
		}

		var sql = fmt.Sprintf("SELECT %s FROM %s;", colNames.String(), table.Identifier)
		rows, err := db.Query(sql)
		if err != nil {
			return "", err
		}
		defer rows.Close()

		fmt.Fprintf(&b, "%s:\n", table.Identifier)
		b.WriteString(colNames.String())

		for rows.Next() {
			var data = make([]anyColumn, len(table.Columns))
			var ptrs = make([]interface{}, len(table.Columns))
			for i := range data {
				ptrs[i] = &data[i]
			}
			if err = rows.Scan(ptrs...); err != nil {
				return "", err
			}
			b.WriteString("\n")
			for i, v := range ptrs {
				if i > 0 {
					b.WriteString(", ")
				}
				var val = v.(*anyColumn)
				b.WriteString(val.String())
			}
		}
	}
	return b.String(), nil
}

type anyColumn string

func (col *anyColumn) Scan(i interface{}) error {
	var sval string
	if b, ok := i.([]byte); ok {
		sval = string(b)
	} else {
		sval = fmt.Sprint(i)
	}
	*col = anyColumn(sval)
	return nil
}
func (col anyColumn) String() string {
	return string(col)
}
