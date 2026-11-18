package capture

import (
	"testing"

	pb "github.com/estuary/flow/go/gazette/broker/protocol"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

// A CollectionSpec which passes Validate().
func linkedTestCollection(name string) pf.CollectionSpec {
	return pf.CollectionSpec{
		Name:            pf.Collection(name),
		Key:             []string{"/id"},
		WriteSchemaJson: []byte(`true`),
		Projections: []pf.Projection{
			{Field: "id", Ptr: "/id", IsPrimaryKey: true},
		},
		PartitionTemplate: &pb.JournalSpec{
			Name:        pb.Journal(name + "/0000000000000000"),
			Replication: 1,
			Fragment: pb.JournalSpec_Fragment{
				Length:           1 << 24,
				CompressionCodec: pb.CompressionCodec_GZIP,
				RefreshInterval:  5 * 60 * 1000000000,
			},
		},
	}
}

func linkedTestValidate() Request_Validate {
	return Request_Validate{
		Name:          "acmeCo/task",
		ConnectorType: pf.CaptureSpec_IMAGE,
		ConfigJson:    []byte(`{}`),
	}
}

func TestRequestValidateLinkedCollections(t *testing.T) {
	var inline = linkedTestValidate()
	inline.Bindings = []*Request_Validate_Binding{{
		ResourceConfigJson: []byte(`{"table":"foo"}`),
		Collection:         linkedTestCollection("acmeCo/one"),
	}}
	require.NoError(t, inline.Validate())
	require.Equal(t, pf.Collection("acmeCo/one"),
		inline.BindingCollection(inline.Bindings[0]).Name)

	// Indirect form: many bindings over a small table.
	var indirect = linkedTestValidate()
	indirect.LinkedCollections = []pf.CollectionSpec{
		linkedTestCollection("acmeCo/one"),
		linkedTestCollection("acmeCo/two"),
	}
	for _, index := range []uint32{1, 1, 0} {
		indirect.Bindings = append(indirect.Bindings, &Request_Validate_Binding{
			ResourceConfigJson: []byte(`{"table":"foo"}`),
			CollectionIndex:    index,
		})
	}
	require.NoError(t, indirect.Validate())
	require.Equal(t, pf.Collection("acmeCo/two"),
		indirect.BindingCollection(indirect.Bindings[0]).Name)
	require.Equal(t, pf.Collection("acmeCo/one"),
		indirect.BindingCollection(indirect.Bindings[2]).Name)
	require.Same(t, indirect.BindingCollection(indirect.Bindings[0]),
		indirect.BindingCollection(indirect.Bindings[1]))

	var oob = linkedTestValidate()
	oob.LinkedCollections = []pf.CollectionSpec{linkedTestCollection("acmeCo/one")}
	oob.Bindings = []*Request_Validate_Binding{{
		ResourceConfigJson: []byte(`{"table":"foo"}`),
		CollectionIndex:    3,
	}}
	require.EqualError(t, oob.Validate(),
		"Bindings[0]: CollectionIndex 3 is out of range (1 LinkedCollections)")
	require.Nil(t, oob.BindingCollection(oob.Bindings[0]))

	var mixed = linkedTestValidate()
	mixed.LinkedCollections = []pf.CollectionSpec{linkedTestCollection("acmeCo/one")}
	mixed.Bindings = []*Request_Validate_Binding{{
		ResourceConfigJson: []byte(`{"table":"foo"}`),
		Collection:         linkedTestCollection("acmeCo/one"),
	}}
	require.EqualError(t, mixed.Validate(),
		"Bindings[0]: Collection is set but the spec is indirected (use CollectionIndex)")

	var strayIndex = linkedTestValidate()
	strayIndex.Bindings = []*Request_Validate_Binding{{
		ResourceConfigJson: []byte(`{"table":"foo"}`),
		Collection:         linkedTestCollection("acmeCo/one"),
		CollectionIndex:    1,
	}}
	require.EqualError(t, strayIndex.Validate(),
		"Bindings[0]: CollectionIndex is 1 but the spec has no LinkedCollections")

	var badEntry = linkedTestValidate()
	badEntry.LinkedCollections = []pf.CollectionSpec{{Name: "acmeCo/no-key"}}
	badEntry.Bindings = []*Request_Validate_Binding{{
		ResourceConfigJson: []byte(`{"table":"foo"}`),
	}}
	require.EqualError(t, badEntry.Validate(),
		"LinkedCollections[0]: key pointers are empty")
}

func TestResponseDiscoveredLinkedCollections(t *testing.T) {
	var inline = Response_Discovered{
		Bindings: []*Response_Discovered_Binding{{
			RecommendedName:    "one",
			ResourceConfigJson: []byte(`{"table":"foo"}`),
			DocumentSchemaJson: []byte(`true`),
			Key:                []string{"/id"},
		}},
	}
	require.NoError(t, inline.Validate())

	inline.Bindings[0].CollectionIndex = 1
	require.EqualError(t, inline.Validate(),
		"Bindings[0]: unexpected CollectionIndex (response has no LinkedCollections)")

	// Indirect form: many bindings over a small table.
	var indirect = Response_Discovered{
		LinkedCollections: []*Response_Discovered_Collection{
			{RecommendedName: "one", DocumentSchemaJson: []byte(`true`), Key: []string{"/id"}},
			{RecommendedName: "two", DocumentSchemaJson: []byte(`true`)},
		},
	}
	for _, index := range []uint32{1, 1, 0} {
		indirect.Bindings = append(indirect.Bindings, &Response_Discovered_Binding{
			ResourceConfigJson: []byte(`{"table":"foo"}`),
			CollectionIndex:    index,
		})
	}
	require.NoError(t, indirect.Validate())

	indirect.Bindings[2].CollectionIndex = 2
	require.EqualError(t, indirect.Validate(),
		"Bindings[2]: CollectionIndex 2 is out of bounds (2 LinkedCollections)")
	indirect.Bindings[2].CollectionIndex = 0

	// Mixed forms are invalid.
	indirect.Bindings[1].RecommendedName = "one"
	require.EqualError(t, indirect.Validate(),
		"Bindings[1]: unexpected inline collection fields (response has LinkedCollections)")
	indirect.Bindings[1].RecommendedName = ""

	indirect.LinkedCollections[1].DocumentSchemaJson = nil
	require.EqualError(t, indirect.Validate(),
		"LinkedCollections[1]: missing DocumentSchemaJson")
}
