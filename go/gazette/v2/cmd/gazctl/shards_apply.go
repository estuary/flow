package main

import (
	"context"
	"os"

	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/consumer/shardspace"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type cmdShardsApply struct {
	ApplyConfig
}

func init() {
	_ = mustAddCmd(cmdShards, "apply", "Apply shard specifications", `
Apply a collection of ShardSpec creations, updates, or deletions.

ShardSpecs should be provided as a YAML list, the same format produced by
"gazctl shards list". Consumers verify that the etcd "revision" field of each
ShardSpec is correct, and will fail the entire apply operation if any have since
been updated. A common operational pattern is to list, edit, and re-apply a
collection of ShardSpecs; this check ensures concurrent modifications are caught.

ShardSpecs may be created by setting "revision" to zero or omitting it altogether.

ShardSpecs may be deleted by setting their field "delete" to true.
`+maxTxnSizeWarning, &cmdShardsApply{})
}

func (cmd *cmdShardsApply) Execute([]string) error {
	startup()

	var set shardspace.Set
	mbp.Must(cmd.decode(&set), "failed to decode shardspace from YAML")

	var req = newShardSpecApplyRequest(set)
	mbp.Must(req.Validate(), "failed to validate ApplyRequest")

	if cmd.DryRun {
		_ = proto.MarshalText(os.Stdout, req)
		return nil
	}

	var ctx = context.Background()
	var resp, err = consumer.ApplyShardsInBatches(ctx, shardsCfg.Consumer.MustShardClient(ctx), req, cmd.MaxTxnSize)
	mbp.Must(err, "failed to apply shards")
	log.WithField("rev", resp.Header.Etcd.Revision).Info("successfully applied")

	return nil
}

// newShardSpecApplyRequest builds the ApplyRequest.
func newShardSpecApplyRequest(set shardspace.Set) *consumer.ApplyRequest {
	set.PushDown()

	var req = new(consumer.ApplyRequest)
	for i := range set.Shards {
		var change = consumer.ApplyRequest_Change{ExpectModRevision: set.Shards[i].Revision}

		if set.Shards[i].Delete != nil && *set.Shards[i].Delete == true {
			change.Delete = set.Shards[i].Spec.Id
		} else {
			change.Upsert = &set.Shards[i].Spec
		}
		req.Changes = append(req.Changes, change)
	}

	return req
}
