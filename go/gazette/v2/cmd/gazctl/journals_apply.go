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

	// Flatten into concrete JournalSpecs & build the request.
	var nodes = tree.Flatten()
	var req = new(pb.ApplyRequest)

	for i := range nodes {
		var change = pb.ApplyRequest_Change{ExpectModRevision: nodes[i].Revision}

		if nodes[i].Delete {
			change.Delete = nodes[i].JournalSpec.Name
		} else {
			change.Upsert = &nodes[i].JournalSpec
		}
		req.Changes = append(req.Changes, change)
	}
	mbp.Must(req.Validate(), "failed to validate ApplyRequest")

	if cmd.DryRun {
		proto.MarshalText(os.Stdout, req)
		return nil
	}

	var ctx = context.Background()
	resp, err := client.ApplyJournals(ctx, journalsCfg.Broker.JournalClient(ctx), req)
	mbp.Must(err, "failed to apply journals")

	log.WithField("rev", resp.Header.Etcd.Revision).Info("successfully applied")
	return nil
}
