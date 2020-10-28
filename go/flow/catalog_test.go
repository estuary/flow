package flow

import (
	"testing"

	pf "github.com/estuary/flow/go/protocol"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

func TestLoadDerivedCollection(t *testing.T) {
	var catalog, err = NewCatalog("../../catalog.db", "")
	require.NoError(t, err)
	spec, err := catalog.LoadDerivedCollection("testing/int-strings")
	require.NoError(t, err)

	require.Equal(t, pf.CollectionSpec{
		Name:            "testing/int-strings",
		SchemaUri:       spec.SchemaUri,
		KeyPtrs:         []string{"/i"},
		PartitionFields: []string{},
		Projections: []*pf.Projection{
			{
				Field:        "eye",
				Ptr:          "/i",
				UserProvided: true,
				IsPrimaryKey: true,
				Inference: &pf.Inference{
					Title:       "",
					Description: "",
					Types:       []string{"integer"},
					MustExist:   true,
					String_:     new(pf.Inference_String),
				},
			},
			{
				Field:        "i",
				Ptr:          "/i",
				UserProvided: false,
				IsPrimaryKey: true,
				Inference: &pf.Inference{
					Title:       "",
					Description: "",
					Types:       []string{"integer"},
					MustExist:   true,
					String_:     new(pf.Inference_String),
				},
			},
		},
		JournalSpec:     spec.JournalSpec,
		UuidPtr:         spec.UuidPtr,
		AckJsonTemplate: spec.AckJsonTemplate,
	}, spec)
}

func TestLoadCapturedCollections(t *testing.T) {
	var catalog, err = NewCatalog("../../catalog.db", "")
	require.NoError(t, err)

	specs, err := catalog.LoadCapturedCollections()
	require.NoError(t, err)
	require.NotEmpty(t, specs)

	var spec = specs["testing/int-string"]

	require.Equal(t, &pf.CollectionSpec{
		Name:            "testing/int-string",
		SchemaUri:       spec.SchemaUri,
		KeyPtrs:         []string{"/i"},
		PartitionFields: []string{},
		Projections: []*pf.Projection{
			{
				Field:        "i",
				IsPrimaryKey: true,
				Ptr:          "/i",
				Inference: &pf.Inference{
					Title:       "",
					Description: "",
					Types:       []string{"integer"},
					MustExist:   true,
					String_:     new(pf.Inference_String),
				},
			},
			{
				Field: "s",
				Ptr:   "/s",
				Inference: &pf.Inference{
					Title:       "",
					Description: "",
					Types:       []string{"string"},
					MustExist:   true,
					String_:     &pf.Inference_String{MaxLength: 128},
				},
			},
		},
		JournalSpec:     spec.JournalSpec,
		UuidPtr:         spec.UuidPtr,
		AckJsonTemplate: spec.AckJsonTemplate,
	}, spec)
}

func TestLoadTransforms(t *testing.T) {
	var catalog, err = NewCatalog("../../catalog.db", "")
	require.NoError(t, err)

	specs, err := catalog.LoadTransforms("testing/int-strings")
	require.NoError(t, err)
	require.NotEmpty(t, specs)

	require.Equal(t, []pf.TransformSpec{
		{
			Name:        "appendStrings",
			CatalogDbId: specs[0].CatalogDbId,
			Derivation: pf.TransformSpec_Derivation{
				Name: "testing/int-strings",
			},
			Source: pf.TransformSpec_Source{
				Name: "testing/int-string",
				Partitions: pb.LabelSelector{
					Include: pb.MustLabelSet("estuary.dev/collection", "testing/int-string"),
				},
			},
			Shuffle: pf.Shuffle{
				ShuffleKeyPtr: []string{"/i"},
				UsesSourceKey: true,
				FilterRClocks: true,
			},
		},
	}, specs)
}
