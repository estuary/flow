package sql

const (
	// DefaultGazetteCheckpoints is the default table for checkpoints.
	DefaultGazetteCheckpoints = "gazette_checkpoints"
	// DefaultFlowMaterializations is the default table for materialization specs.
	DefaultFlowMaterializations = "flow_materializations"
)

// GazetteCheckpointsTable returns the Table description for the table that holds the checkpoint
// and nonce values for each materialization shard.
func GazetteCheckpointsTable(name string) *Table {
	return &Table{
		Identifier:  name,
		IfNotExists: true,
		Comment:     "This table holds journal checkpoints, which Flow manages in order to ensure exactly-once updates for materializations",
		Columns: []Column{
			{
				Name:       "shard_fqn",
				Identifier: "shard_fqn",
				Comment:    "The id of the consumer shard. Note that a single collection may have multiple consumer shards materializing it, and each will have a separate checkpoint.",
				PrimaryKey: true,
				Type:       STRING,
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
				Comment:    "Opaque checkpoint of the Flow consumer shard",
				Type:       BINARY,
			},
		},
	}
}

// FlowMaterializationsTable returns the Table description for the table that holds the
// MaterializationSpec that corresponds to each target table. This state is used both for sql
// generation and for validation.
func FlowMaterializationsTable(name string) *Table {
	return &Table{
		Identifier:  name,
		IfNotExists: true,
		Comment:     "This table is the source of truth for all materializations into this system.",
		Columns: []Column{
			{
				Name:       "table_name",
				Identifier: "table_name",
				Comment:    "The name of the target table of the materialization, which may or may not include a schema and catalog prefix",
				PrimaryKey: true,
				Type:       STRING,
				NotNull:    true,
			},
			{
				Name:       "spec",
				Identifier: "spec",
				Comment:    "Specification of the materialization, encoded as protobuf.",
				Type:       STRING,
				NotNull:    true,
			},
		},
	}
}
