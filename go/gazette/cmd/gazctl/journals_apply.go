package main

import (
	"context"
	"os"

	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/broker/journalspace"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdJournalsApply struct {
	ApplyConfig
}

func init() {
	_ = mustAddCmd(cmdJournals, "apply", "Apply journal specifications", `
Apply a collection of JournalSpec creations, updates, or deletions.

JournalSpecs should be provided as a YAML journal hierarchy, the format
produced by "gazctl journals list". This YAML hierarchy format is sugar for
succinctly representing a collection of JournalSpecs, which typically exhibit
common prefixes and configuration. gazctl will flatten the YAML hierarchy
into the implicated collection of JournalSpec changes, and send each to the
brokers for application.

Brokers verify that the etcd "revision" field of each JournalSpec is correct,
and will fail the entire apply operation if any have since been updated. A
common operational pattern is to list, edit, and re-apply a collection of
JournalSpecs; this check ensures concurrent modifications are caught.

You may explicitly inform the broker to apply your JournalSpecs regardless of the
current state of specifications in Etcd by passing in a revision value of -1.
This commonly done when operators keep JournalSpecs in version control as their
source of truth.

JournalSpecs may be created by setting "revision" to zero or omitting altogether.

JournalSpecs may be deleted by setting field "delete" to true on individual
journals or parents thereof in the hierarchy. Note that deleted parent prefixes
will cascade only to JournalSpecs *explicitly listed* as children of the prefix
in the YAML, and not to other JournalSpecs which may exist with the prefix but
are not enumerated.
`+maxTxnSizeWarning, &cmdJournalsApply{})
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
	var resp, err = client.ApplyJournalsInBatches(ctx, journalsCfg.Broker.MustJournalClient(ctx), req, cmd.MaxTxnSize)
	mbp.Must(err, "failed to apply journals")
	log.WithField("revision", resp.Header.Etcd.Revision).Info("successfully applied")

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
