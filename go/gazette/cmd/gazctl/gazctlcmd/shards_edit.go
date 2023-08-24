package gazctlcmd

import (
	"bytes"
	"context"
	"io"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/cmd/gazctl/gazctlcmd/editor"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/shardspace"
	mbp "go.gazette.dev/core/mainboilerplate"
	"gopkg.in/yaml.v2"
)

type cmdShardsEdit struct {
	EditConfig
}

func init() {
	CommandRegistry.AddCommand("shards", "edit", "Edit shard specifications", shardsEditLongDesc, &cmdShardsEdit{})
}

func (cmd *cmdShardsEdit) Execute([]string) error {
	startup(ShardsCfg.BaseConfig)

	var ctx = context.Background()
	var rsc = ShardsCfg.Consumer.MustRoutedShardClient(ctx)
	var rjc = ShardsCfg.Broker.MustRoutedJournalClient(ctx)

	return editor.EditRetryLoop(editor.RetryLoopArgs{
		ShardClient:      rsc,
		JournalClient:    rjc,
		FilePrefix:       "gazctl-shards-edit-",
		SelectFn:         cmd.selectSpecs,
		ApplyFn:          cmd.applyShardSpecYAML,
		AbortIfUnchanged: true,
	})
}

func (cmd *cmdShardsEdit) selectSpecs(client pc.ShardClient, _ pb.JournalClient) io.Reader {
	var resp = listShards(client, cmd.Selector)

	var buf = &bytes.Buffer{}
	if len(resp.Shards) == 0 {
		log.WithField("selector", cmd.Selector).Panic("no shards match selector")
	}
	writeHoistedYAMLShardSpace(buf, resp)

	return buf
}

func (cmd *cmdShardsEdit) applyShardSpecYAML(b []byte, shardClient pc.ShardClient, journalClient pb.JournalClient) error {
	var set shardspace.Set
	if err := yaml.UnmarshalStrict(b, &set); err != nil {
		return err
	}

	var ctx = context.Background()
	var req = newShardSpecApplyRequest(set)

	if err := req.Validate(); err != nil {
		return err
	} else if err = consumer.VerifyReferencedJournals(ctx, journalClient, req); err != nil {
		return errors.WithMessage(err, "verifying referenced journals")
	}

	var resp, err = consumer.ApplyShardsInBatches(ctx, shardClient, req, cmd.MaxTxnSize)
	mbp.Must(err, "failed to apply shards")
	log.WithField("rev", resp.Header.Etcd.Revision).Info("successfully applied")
	return nil
}
