package main

import (
	"context"
	"os"

	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type cmdShardsApply struct {
	ApplyConfig
}

func (cmd *cmdShardsApply) Execute([]string) error {
	startup()

	var shards []yamlShard
	mbp.Must(cmd.decode(&shards), "failed to decode shards from YAML")

	// Build the complete ApplyRequest
	var req = new(consumer.ApplyRequest)
	for i := range shards {
		var change = consumer.ApplyRequest_Change{ExpectModRevision: shards[i].Revision}

		if shards[i].Delete {
			change.Delete = shards[i].ShardSpec.Id
		} else {
			change.Upsert = &shards[i].ShardSpec
		}
		req.Changes = append(req.Changes, change)
	}
	mbp.Must(req.Validate(), "failed to validate ApplyRequest")

	if cmd.DryRun {
		proto.MarshalText(os.Stdout, req)
		return nil
	}

	var ctx = context.Background()
	resp, err := consumer.ApplyShards(ctx, shardsCfg.Consumer.ShardClient(ctx), req)
	mbp.Must(err, "failed to apply shards")

	log.WithField("rev", resp.Header.Etcd.Revision).Info("successfully applied")
	return nil
}
