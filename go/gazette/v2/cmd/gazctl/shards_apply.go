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
	resp, err := consumer.ApplyShards(ctx, shardsCfg.Consumer.ShardClient(ctx), req)
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
