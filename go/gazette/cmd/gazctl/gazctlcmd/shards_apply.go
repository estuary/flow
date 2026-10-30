package gazctlcmd

import (
	"context"
	"os"

	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/shardspace"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdShardsApply struct {
	ApplyConfig
}

func init() {
	CommandRegistry.AddCommand("shards", "apply", "Apply shard specifications", `
Apply a collection of ShardSpec creations, updates, or deletions.

ShardSpecs should be provided as a YAML list, the same format produced by
"gazctl shards list". Consumers verify that the Etcd "revision" field of each
ShardSpec is correct, and will fail the entire apply operation if any have since
been updated. A common operational pattern is to list, edit, and re-apply a
collection of ShardSpecs; this check ensures concurrent modifications are caught.

You may explicitly inform the broker to apply your ShardSpecs regardless of the
current state of specifications in Etcd by passing in a revision value of -1.
This commonly done when operators keep ShardSpecs in version control as their
source of truth.

ShardSpecs may be created by setting "revision" to zero or omitting it altogether.

ShardSpecs may be deleted by setting their field "delete" to true.
`+maxTxnSizeWarning, &cmdShardsApply{})
}

func (cmd *cmdShardsApply) Execute([]string) error {
	startup(ShardsCfg.BaseConfig)

	var set shardspace.Set
	mbp.Must(cmd.decode(&set), "failed to decode shardspace from YAML")

	var ctx = context.Background()
	var req = newShardSpecApplyRequest(set)

	mbp.Must(req.Validate(), "failed to validate ApplyRequest")
	mbp.Must(consumer.VerifyReferencedJournals(ctx, ShardsCfg.Broker.MustJournalClient(ctx), req),
		"failed to validate journals of the ApplyRequest")

	if cmd.DryRun {
		_ = proto.MarshalText(os.Stdout, req)
		return nil
	}

	var resp, err = consumer.ApplyShardsInBatches(ctx, ShardsCfg.Consumer.MustShardClient(ctx), req, cmd.MaxTxnSize)
	mbp.Must(err, "failed to apply shards")
	log.WithField("rev", resp.Header.Etcd.Revision).Info("successfully applied")

	return nil
}

// newShardSpecApplyRequest builds the ApplyRequest.
func newShardSpecApplyRequest(set shardspace.Set) *pc.ApplyRequest {
	set.PushDown()

	var req = new(pc.ApplyRequest)
	for i := range set.Shards {
		var change = pc.ApplyRequest_Change{ExpectModRevision: set.Shards[i].Revision}

		if set.Shards[i].Delete != nil && *set.Shards[i].Delete == true {
			change.Delete = set.Shards[i].Spec.Id
		} else {
			change.Upsert = &set.Shards[i].Spec
		}
		req.Changes = append(req.Changes, change)
	}
	return req
}
