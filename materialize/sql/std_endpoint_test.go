package sql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	pf "github.com/estuary/protocols/flow"
	_ "github.com/mattn/go-sqlite3" // Import for register side-effects.
	"github.com/stretchr/testify/require"
	pg "go.gazette.dev/core/broker/protocol"
)

func TestStdEndpointExecuteLoadSpec(t *testing.T) {

	// Simple test to get an example spec, persist it and load it to make sure it matches
	// Using sqlite as the implementation for the sql.DB database.
	var db, err = sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)

	ctx := context.Background()

	// Leverage the Endpoint interface
	var endpoint = NewStdEndpoint(nil, db, SQLiteSQLGenerator(), FlowTables{
		Checkpoints: FlowCheckpointsTable(DefaultFlowCheckpoints),
		Specs:       FlowMaterializationsTable(DefaultFlowMaterializations),
	})

	// Create the spec table.
	createSpecsSQL, err := endpoint.CreateTableStatement(endpoint.FlowTables().Specs)
	require.Nil(t, err)

	// Get an example spec, convert it to bytes.
	sourceSpec := exampleMaterializationSpec()
	specBytes, err := sourceSpec.Marshal()
	require.Nil(t, err)

	var insertSpecSQL = fmt.Sprintf("INSERT INTO %s (version, spec, materialization) VALUES (%s, %s, %s);",
		endpoint.FlowTables().Specs.Identifier,
		endpoint.Generator().ValueRenderer.Render("example_version"),
		endpoint.Generator().ValueRenderer.Render(base64.StdEncoding.EncodeToString(specBytes)),
		endpoint.Generator().ValueRenderer.Render(sourceSpec.Materialization.String()),
	)

	// Create the table and put the spec in it.
	err = endpoint.ExecuteStatements(ctx, []string{
		createSpecsSQL,
		insertSpecSQL,
	})
	require.Nil(t, err)

	// Load the spec back out of the database and validate it.
	version, destSpec, err := endpoint.LoadSpec(ctx, sourceSpec.Materialization)
	require.NoError(t, err)
	require.Equal(t, "example_version", version)
	require.Equal(t, sourceSpec, destSpec)

	require.Nil(t, db.Close())

}

func exampleMaterializationSpec() *pf.MaterializationSpec {
	return &pf.MaterializationSpec{
		Materialization:  "test_materialization",
		EndpointType:     pf.EndpointType_SQLITE,
		EndpointSpecJson: json.RawMessage(`{"path":"file:///hello-world.db"}`),
		Bindings: []*pf.MaterializationSpec_Binding{
			{
				ResourceSpecJson: json.RawMessage(`{"table":"trips1"}`),
				ResourcePath:     []string{"trips1"},
				Collection: pf.CollectionSpec{
					Collection: "acmeCo/tripdata",
					SchemaUri:  "file:///flow.yaml?ptr=/collections/acmeCo~1tripdata/schema",
					SchemaJson: json.RawMessage("{\"$id\":\"file:///data/git/est/junk/hf2/discover-source-s3.flow.yaml?ptr=/collections/acmeCo~1tripdata/schema\",\"properties\":{\"_meta\":{\"properties\":{\"file\":{\"type\":\"string\"},\"offset\":{\"minimum\":0,\"type\":\"integer\"}},\"required\":[\"file\",\"offset\"],\"type\":\"object\"}},\"required\":[\"_meta\"],\"type\":\"object\"}"),
					KeyPtrs:    []string{"/_meta/file", "/_meta/offset"},
					UuidPtr:    "/_meta/uuid",
					Projections: []pf.Projection{
						{
							Ptr:          "/_meta/file",
							Field:        "_meta/file",
							IsPrimaryKey: true,
							Inference: pf.Inference{
								Types:     []string{"string"},
								MustExist: true,
							},
						},
						{
							Ptr:          "/_meta/offset",
							Field:        "_meta/offset",
							IsPrimaryKey: true,
							Inference: pf.Inference{
								Types:     []string{"integer"},
								MustExist: true,
							},
						},
						{
							Field:        "flow_document",
							IsPrimaryKey: true,
							Inference: pf.Inference{
								Types:     []string{"object"},
								MustExist: true,
							},
						},
					},
					AckJsonTemplate: json.RawMessage("{\"_meta\":{\"ack\":true,\"uuid\":\"DocUUIDPlaceholder-329Bb50aa48EAa9ef\"}}"),
				},
				FieldSelection: pf.FieldSelection{
					Keys:     []string{"_meta/file", "_meta/offset"},
					Document: "flow_document",
				},
				Shuffle: pf.Shuffle{
					GroupName:        "materialize/acmeCo/postgres/trips1",
					SourceCollection: "acmeCo/tripdata",
					SourcePartitions: pg.LabelSelector{
						Include: pg.LabelSet{
							Labels: []pg.Label{
								{
									Name:  "estuary.dev/collection",
									Value: "acmeCo/tripdata",
								},
							},
						},
					},
					SourceUuidPtr:    "/_meta/uuid",
					ShuffleKeyPtr:    []string{"/_meta/file", "/_meta/offset"},
					UsesSourceKey:    true,
					SourceSchemaUri:  "file:///flow.yaml?ptr=/collections/acmeCo~1tripdata/schema",
					UsesSourceSchema: true,
				},
			},
		},
	}
}
