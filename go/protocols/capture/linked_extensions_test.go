package capture

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
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
