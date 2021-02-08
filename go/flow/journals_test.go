package flow

import (
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

func TestBuildingPartition(t *testing.T) {
	var rules = []pf.JournalRules_Rule{
		{
			Template: pb.JournalSpec{Replication: 42},
		},
		{
			Selector: pb.LabelSelector{Include: pb.MustLabelSet("not/matched", "value")},
			Template: pb.JournalSpec{Replication: 999},
		},
	}
	var collection = &pf.CollectionSpec{
		Collection:      "a/collection",
		PartitionFields: []string{"bar", "foo"},
	}
	var shard = &pc.ShardSpec{
		Id:                "a/shard/id",
		RecoveryLogPrefix: "recovery",
	}

	var partition = BuildPartitionSpec(collection, tuple.Tuple{"value", 123}, rules)
	var log = BuildRecoveryLogSpec(shard, rules)

	cupaloy.SnapshotT(t, []pb.JournalSpec{partition, log})
}
