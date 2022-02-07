package gazctlcmd

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

type cmdShardsUnassign struct {
	Failed   bool   `long:"failed" description:"Only remove assignments from failed shards"`
	DryRun   bool   `long:"dry-run" description:"Perform a dry-run, printing matching shards"`
	Selector string `long:"selector" short:"l" required:"true" description:"Label Selector query to filter on"`
}

func init() {
	CommandRegistry.AddCommand("shards", "unassign", "Remove a shard assignment", `
Removes the assignment of a shard from its primary consumer process, causing it
to be shortly rescheduled by the allocator. This can be used to restart a failed
shard, or to move a shard off an overloaded node.

Use --selector to supply a LabelSelector which constrains the set of returned
shards. Shard selectors support an additional meta-label "id". See the 'shards
list' command for more details about label selectors.
`, &cmdShardsUnassign{})
}

func (cmd *cmdShardsUnassign) Execute([]string) (err error) {
	startup(ShardsCfg.BaseConfig)

	var listResp = listShards(cmd.Selector)
	if listResp.Status != pc.Status_OK {
		return fmt.Errorf("unexpected listShard status: %v", listResp.Status.String())
	}
	var ctx = context.Background()
	var client = pc.NewShardClient(ShardsCfg.Consumer.MustDial(ctx))

	unassignResp, err := client.Unassign(pb.WithDispatchDefault(ctx), &pc.UnassignRequest{
		Shards:     shardIds(listResp.Shards),
		OnlyFailed: cmd.Failed,
		DryRun:     cmd.DryRun,
	})
	if err != nil {
		return fmt.Errorf("unassigning shard: %w", err)
	} else if err := unassignResp.Validate(); err != nil {
		return fmt.Errorf("invalid response: %w", err)
	}

	if len(unassignResp.Shards) == 0 {
		log.Warn("No shards assignments were modified. Use `shards list -l SELECTOR` to test your selector.")
	} else {
		for _, shardId := range unassignResp.Shards {
			for _, origShard := range listResp.Shards {
				if shardId == origShard.Spec.Id {
					log.Infof("Successfully unassigned shard: id=%v. Previous status=%v, previous route members=%v", origShard.Spec.Id.String(), origShard.Status, origShard.Route.Members)
				}
			}
		}
	}

	return nil
}

func shardIds(shards []pc.ListResponse_Shard) []pc.ShardID {
	var ids []pc.ShardID

	for _, shard := range shards {
		ids = append(ids, shard.Spec.Id)
	}

	return ids
}
