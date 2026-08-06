package flow

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

// linkedTestCollection returns a CollectionSpec which passes Validate().
func linkedTestCollection(name string) CollectionSpec {
	return CollectionSpec{
		Name:            Collection(name),
		Key:             []string{"/id"},
		WriteSchemaJson: []byte(`true`),
		Projections: []Projection{
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

func linkedTestShardTemplate() *pc.ShardSpec {
	return &pc.ShardSpec{
		Id:                "capture/acmeCo/task/0000000000000000/00000000-00000000",
		RecoveryLogPrefix: "recovery",
		HintPrefix:        "/hints",
		MaxTxnDuration:    1000000000,
	}
}

func linkedTestRecoveryTemplate() *pb.JournalSpec {
	return &pb.JournalSpec{
		Name:        "recovery/capture/acmeCo/task/0000000000000000/00000000-00000000",
		Replication: 1,
		Fragment: pb.JournalSpec_Fragment{
			Length:           1 << 28,
			CompressionCodec: pb.CompressionCodec_SNAPPY,
			RefreshInterval:  5 * 60 * 1000000000,
		},
	}
}

// A capture binding which passes Validate() aside from its collection.
func linkedTestCaptureBinding() CaptureSpec_Binding {
	return CaptureSpec_Binding{
		ResourceConfigJson: []byte(`{"table":"foo"}`),
		ResourcePath:       []string{"foo"},
	}
}

func linkedTestCaptureSpec() CaptureSpec {
	return CaptureSpec{
		Name:                "acmeCo/task",
		ConnectorType:       CaptureSpec_IMAGE,
		ConfigJson:          []byte(`{}`),
		ShardTemplate:       linkedTestShardTemplate(),
		RecoveryLogTemplate: linkedTestRecoveryTemplate(),
	}
}

func TestCaptureSpecLinkedCollections(t *testing.T) {
	// Inline form: each binding carries its own collection.
	var inline = linkedTestCaptureSpec()
	for _, name := range []string{"acmeCo/one", "acmeCo/two"} {
		var b = linkedTestCaptureBinding()
		b.Collection = linkedTestCollection(name)
		inline.Bindings = append(inline.Bindings, &b)
	}
	require.NoError(t, inline.Validate())
	require.Equal(t, Collection("acmeCo/two"),
		inline.BindingCollection(inline.Bindings[1]).Name)

	// Indirect form: many bindings share a small table. Note that both bindings
	// resolve to the same entry, which is the whole point of the encoding.
	var indirect = linkedTestCaptureSpec()
	indirect.LinkedCollections = []CollectionSpec{
		linkedTestCollection("acmeCo/one"),
		linkedTestCollection("acmeCo/two"),
	}
	for _, index := range []uint32{1, 1, 0} {
		var b = linkedTestCaptureBinding()
		b.CollectionIndex = index
		indirect.Bindings = append(indirect.Bindings, &b)
	}
	// Inactive bindings index the same table.
	var inactive = linkedTestCaptureBinding()
	inactive.CollectionIndex = 1
	indirect.InactiveBindings = []*CaptureSpec_Binding{&inactive}

	require.NoError(t, indirect.Validate())
	require.Equal(t, Collection("acmeCo/two"), indirect.BindingCollection(indirect.Bindings[0]).Name)
	require.Equal(t, Collection("acmeCo/two"), indirect.BindingCollection(indirect.Bindings[1]).Name)
	require.Equal(t, Collection("acmeCo/one"), indirect.BindingCollection(indirect.Bindings[2]).Name)
	require.Equal(t, Collection("acmeCo/two"), indirect.BindingCollection(&inactive).Name)
	// Bindings sharing an index share one spec, not copies of it.
	require.Same(t, indirect.BindingCollection(indirect.Bindings[0]),
		indirect.BindingCollection(indirect.Bindings[1]))

	// An out-of-bounds index is rejected, and resolves to nil.
	var oob = indirect
	oob.Bindings = append([]*CaptureSpec_Binding{}, indirect.Bindings...)
	var bad = linkedTestCaptureBinding()
	bad.CollectionIndex = 2
	oob.Bindings = append(oob.Bindings, &bad)
	require.EqualError(t, oob.Validate(),
		"Bindings[3]: CollectionIndex 2 is out of range (2 LinkedCollections)")
	require.Nil(t, oob.BindingCollection(&bad))

	// Out-of-bounds is rejected for inactive bindings too.
	var oobInactive = indirect
	var badInactive = linkedTestCaptureBinding()
	badInactive.CollectionIndex = 7
	oobInactive.InactiveBindings = []*CaptureSpec_Binding{&badInactive}
	require.EqualError(t, oobInactive.Validate(),
		"InactiveBindings[0]: CollectionIndex 7 is out of range (2 LinkedCollections)")

	// Mixed form is rejected: an indirect-form binding may not also inline a spec.
	var mixed = linkedTestCaptureSpec()
	mixed.LinkedCollections = []CollectionSpec{linkedTestCollection("acmeCo/one")}
	var mixedBinding = linkedTestCaptureBinding()
	mixedBinding.Collection = linkedTestCollection("acmeCo/one")
	mixed.Bindings = []*CaptureSpec_Binding{&mixedBinding}
	require.EqualError(t, mixed.Validate(),
		"Bindings[0]: Collection is set but the spec is indirected (use CollectionIndex)")

	// Mixed form is rejected in the other direction, too: an inline-form
	// binding may not carry a CollectionIndex.
	var strayIndex = linkedTestCaptureSpec()
	var strayBinding = linkedTestCaptureBinding()
	strayBinding.Collection = linkedTestCollection("acmeCo/one")
	strayBinding.CollectionIndex = 1
	strayIndex.Bindings = []*CaptureSpec_Binding{&strayBinding}
	require.EqualError(t, strayIndex.Validate(),
		"Bindings[0]: CollectionIndex is 1 but the spec has no LinkedCollections")

	// Each table entry must itself be a valid CollectionSpec.
	var badEntry = linkedTestCaptureSpec()
	badEntry.LinkedCollections = []CollectionSpec{
		linkedTestCollection("acmeCo/one"),
		{Name: "acmeCo/no-key"},
	}
	var entryBinding = linkedTestCaptureBinding()
	badEntry.Bindings = []*CaptureSpec_Binding{&entryBinding}
	require.EqualError(t, badEntry.Validate(),
		"LinkedCollections[1]: key pointers are empty")
}

func linkedTestMaterializationSpec() MaterializationSpec {
	return MaterializationSpec{
		Name:                "acmeCo/task",
		ConnectorType:       MaterializationSpec_IMAGE,
		ConfigJson:          []byte(`{}`),
		ShardTemplate:       linkedTestShardTemplate(),
		RecoveryLogTemplate: linkedTestRecoveryTemplate(),
	}
}

func linkedTestMaterializationBinding() MaterializationSpec_Binding {
	return MaterializationSpec_Binding{
		ResourceConfigJson: []byte(`{"table":"foo"}`),
		ResourcePath:       []string{"foo"},
		// "id" is the sole projection of linkedTestCollection.
		FieldSelection: FieldSelection{Keys: []string{"id"}},
	}
}

func TestMaterializationSpecLinkedCollections(t *testing.T) {
	var inline = linkedTestMaterializationSpec()
	var inlineBinding = linkedTestMaterializationBinding()
	inlineBinding.Collection = linkedTestCollection("acmeCo/one")
	inline.Bindings = []*MaterializationSpec_Binding{&inlineBinding}
	require.NoError(t, inline.Validate())
	require.Equal(t, Collection("acmeCo/one"),
		inline.BindingCollection(&inlineBinding).Name)

	// Two bindings of one collection which differ in their `group_by` produce
	// two same-name table entries with differing keys. Duplicate names are
	// legal: only the index identifies an entry.
	var keyedOnOther = linkedTestCollection("acmeCo/one")
	keyedOnOther.Key = []string{"/other"}
	keyedOnOther.Projections = []Projection{
		{Field: "id", Ptr: "/id"},
		{Field: "other", Ptr: "/other", IsPrimaryKey: true},
	}

	var indirect = linkedTestMaterializationSpec()
	indirect.LinkedCollections = []CollectionSpec{
		linkedTestCollection("acmeCo/one"),
		keyedOnOther,
	}
	var byID = linkedTestMaterializationBinding()
	var byOther = linkedTestMaterializationBinding()
	byOther.CollectionIndex = 1
	byOther.FieldSelection = FieldSelection{Keys: []string{"other"}}
	indirect.Bindings = []*MaterializationSpec_Binding{&byID, &byOther}

	require.NoError(t, indirect.Validate())
	require.Equal(t, []string{"/id"}, indirect.BindingCollection(&byID).Key)
	require.Equal(t, []string{"/other"}, indirect.BindingCollection(&byOther).Key)

	// FieldSelection is validated against the *resolved* collection: "other"
	// has no projection in table entry zero.
	var wrongEntry = indirect
	var wrongBinding = linkedTestMaterializationBinding()
	wrongBinding.FieldSelection = FieldSelection{Keys: []string{"other"}}
	wrongEntry.Bindings = []*MaterializationSpec_Binding{&wrongBinding}
	require.EqualError(t, wrongEntry.Validate(),
		"Bindings[0]: the selected field 'other' has no corresponding projection")

	var oob = indirect
	var bad = linkedTestMaterializationBinding()
	bad.CollectionIndex = 9
	oob.Bindings = []*MaterializationSpec_Binding{&bad}
	require.EqualError(t, oob.Validate(),
		"Bindings[0]: CollectionIndex 9 is out of range (2 LinkedCollections)")
	require.Nil(t, oob.BindingCollection(&bad))
}

func linkedTestDerivation() CollectionSpec_Derivation {
	return CollectionSpec_Derivation{
		ConnectorType:       CollectionSpec_Derivation_SQLITE,
		ShardTemplate:       linkedTestShardTemplate(),
		RecoveryLogTemplate: linkedTestRecoveryTemplate(),
	}
}

func linkedTestTransform(name string) CollectionSpec_Derivation_Transform {
	return CollectionSpec_Derivation_Transform{
		Name:             Transform(name),
		LambdaConfigJson: []byte(`"SELECT 1;"`),
	}
}

func TestTransformValidateLambdaConfig(t *testing.T) {
	// An empty LambdaConfigJson is rejected, in both encoding forms.
	var inline = linkedTestDerivation()
	var transform = linkedTestTransform("no_lambda")
	transform.Collection = linkedTestCollection("acmeCo/one")
	transform.LambdaConfigJson = nil
	inline.Transforms = []CollectionSpec_Derivation_Transform{transform}
	require.EqualError(t, inline.Validate(), "Transform[0]: missing LambdaConfigJson")
	require.EqualError(t, transform.Validate(), "missing LambdaConfigJson")

	var indirect = linkedTestDerivation()
	indirect.LinkedCollections = []CollectionSpec{linkedTestCollection("acmeCo/one")}
	var indirectTransform = linkedTestTransform("no_lambda")
	indirectTransform.LambdaConfigJson = nil
	indirect.Transforms = []CollectionSpec_Derivation_Transform{indirectTransform}
	require.EqualError(t, indirect.Validate(), "Transform[0]: missing LambdaConfigJson")
}

func TestDerivationLinkedCollections(t *testing.T) {
	var inline = linkedTestDerivation()
	var inlineTransform = linkedTestTransform("from_one")
	inlineTransform.Collection = linkedTestCollection("acmeCo/one")
	inline.Transforms = []CollectionSpec_Derivation_Transform{inlineTransform}
	require.NoError(t, inline.Validate())
	require.Equal(t, Collection("acmeCo/one"),
		inline.TransformCollection(&inline.Transforms[0]).Name)

	// Indirect form: many transforms over one source collection.
	var indirect = linkedTestDerivation()
	indirect.LinkedCollections = []CollectionSpec{
		linkedTestCollection("acmeCo/one"),
		linkedTestCollection("acmeCo/two"),
	}
	var first = linkedTestTransform("from_two")
	first.CollectionIndex = 1
	var second = linkedTestTransform("also_from_two")
	second.CollectionIndex = 1
	indirect.Transforms = []CollectionSpec_Derivation_Transform{first, second}
	var inactive = linkedTestTransform("was_from_one")
	indirect.InactiveTransforms = []*CollectionSpec_Derivation_Transform{&inactive}

	require.NoError(t, indirect.Validate())
	require.Equal(t, Collection("acmeCo/two"),
		indirect.TransformCollection(&indirect.Transforms[0]).Name)
	require.Same(t, indirect.TransformCollection(&indirect.Transforms[0]),
		indirect.TransformCollection(&indirect.Transforms[1]))
	require.Equal(t, Collection("acmeCo/one"), indirect.TransformCollection(&inactive).Name)

	var oob = indirect
	var bad = linkedTestTransform("bad")
	bad.CollectionIndex = 2
	oob.Transforms = []CollectionSpec_Derivation_Transform{bad}
	require.EqualError(t, oob.Validate(),
		"Transform[0]: CollectionIndex 2 is out of range (2 LinkedCollections)")
	require.Nil(t, oob.TransformCollection(&bad))

	var mixed = linkedTestDerivation()
	mixed.LinkedCollections = []CollectionSpec{linkedTestCollection("acmeCo/one")}
	var mixedTransform = linkedTestTransform("mixed")
	mixedTransform.Collection = linkedTestCollection("acmeCo/one")
	mixed.Transforms = []CollectionSpec_Derivation_Transform{mixedTransform}
	require.EqualError(t, mixed.Validate(),
		"Transform[0]: Collection is set but the spec is indirected (use CollectionIndex)")
}
