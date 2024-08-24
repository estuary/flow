package flow

import (
	"github.com/estuary/flow/go/labels"
	flowLabels "github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/gogo/protobuf/proto"
	pb "go.gazette.dev/core/broker/protocol"
)

// BuildPartitionSpec builds a JournalSpec from the given |template| and |labels|.
// Labels must minimally provide the required runtime labels of the partition,
// such as partition values and the key range. Non-runtime labels are filtered
// as needed, and it's intended that the caller will simply pass all
// labels of an existing specification.
func BuildPartitionSpec(template *pf.JournalSpec, labels pf.LabelSet) (*pf.JournalSpec, error) {
	var spec = proto.Clone(template).(*pf.JournalSpec)

	for _, l := range labels.Labels {
		if flowLabels.IsRuntimeLabel(l.Name) {
			spec.LabelSet.AddValue(l.Name, l.Value)
		}
	}

	var suffix, err = flowLabels.PartitionSuffix(spec.LabelSet)
	if err != nil {
		return nil, err
	}
	spec.Name = pf.Journal(spec.Name.String() + "/" + suffix)

	return spec, nil
}

// BuildRecoverySpec builds a JournalSpec from the given |template|,
// for the given |shard|.
func BuildRecoverySpec(template *pf.JournalSpec, shard *pf.ShardSpec) *pf.JournalSpec {
	var spec = proto.Clone(template).(*pf.JournalSpec)
	spec.Name = shard.RecoveryLog()
	return spec
}

// BuildShardSpec builds a ShardSpec from the given |template| and |labels|.
// Labels must minimally provide the required runtime labels of the shard,
// such as its range specification. Non-runtime labels are filtered as needed,
// and it's intended that the caller will simply pass all labels of an existing
// specification.
func BuildShardSpec(template *pf.ShardSpec, labels pf.LabelSet) (*pf.ShardSpec, error) {
	var spec = proto.Clone(template).(*pf.ShardSpec)

	for _, l := range labels.Labels {
		if flowLabels.IsRuntimeLabel(l.Name) {
			spec.LabelSet.AddValue(l.Name, l.Value)
		}

		// A shard which is actively being split from another
		// parent (source) shard should not have hot standbys,
		// since we must complete the split workflow to even know
		// what hints they should begin recovery log replay from.
		if l.Name == flowLabels.SplitSource {
			spec.HotStandbys = 0
		}
	}

	var suffix, err = flowLabels.ShardSuffix(spec.LabelSet)
	if err != nil {
		return nil, err
	}
	spec.Id = pf.ShardID(spec.Id.String() + "/" + suffix)

	return spec, nil
}

// CollectionWatchRequest returns a ListRequest which watches all partitions of a collection.
func CollectionWatchRequest(spec *pf.CollectionSpec) pb.ListRequest {
	return pb.ListRequest{
		Selector: pb.LabelSelector{
			Include: pb.MustLabelSet(
				// "name:prefix" allows brokers to utilize their index over names.
				"name:prefix", spec.PartitionTemplate.Name.String()+"/",
				labels.Collection, spec.Name.String(),
			),
		},
		Watch: true,
	}
}
