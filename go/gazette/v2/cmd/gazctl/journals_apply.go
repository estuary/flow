package main

import (
	"context"
	"os"

	"github.com/LiveRamp/gazette/v2/pkg/client"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/protocol/journalspace"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type cmdJournalsApply struct {
	ApplyConfig
}

func (cmd *cmdJournalsApply) Execute([]string) error {
	startup()

	// Decode journal specification tree from YAML.
	var tree journalspace.Node
	mbp.Must(cmd.decode(&tree), "failed to decode journal tree")
	mbp.Must(tree.Validate(), "journal tree failed to validate")

	var req = newJournalSpecApplyRequest(&tree)
	mbp.Must(req.Validate(), "failed to validate ApplyRequest")

	if cmd.DryRun {
		_ = proto.MarshalText(os.Stdout, req)
		return nil
	}

	var ctx = context.Background()
	var resp, err = client.ApplyJournalsInBatches(ctx, journalsCfg.Broker.JournalClient(ctx), req, cmd.MaxTxnSize)
	mbp.Must(err, "failed to apply journals")
	log.WithField("rev", resp.Header.Etcd.Revision).Info("successfully applied")

	return nil
}

// newJournalSpecApplyRequest flattens a journal specification tree into
// concrete JournalSpecs and builds the request.
func newJournalSpecApplyRequest(tree *journalspace.Node) *pb.ApplyRequest {
	var req = new(pb.ApplyRequest)

	tree.PushDown()
	_ = tree.WalkTerminalNodes(func(node *journalspace.Node) error {
		var change = pb.ApplyRequest_Change{ExpectModRevision: node.Revision}

		if node.Delete != nil && *node.Delete {
			change.Delete = node.Spec.Name
		} else {
			change.Upsert = &node.Spec
		}
		req.Changes = append(req.Changes, change)
		return nil
	})
	return req
}
