package main

import (
	"context"

	"github.com/LiveRamp/gazette/v2/pkg/client"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
)

type cmdJournalResetHead struct {
	Selector string `long:"selector" short:"l" required:"true" description:"Label Selector query to filter on"`
}

func (cmd *cmdJournalResetHead) Execute([]string) error {
	startup()

	var err error
	var ctx = context.Background()
	var rjc = journalsCfg.Broker.RoutedJournalClient(ctx)

	// Get the list of journals which match this selector.
	var listRequest pb.ListRequest
	listRequest.Selector, err = pb.ParseLabelSelector(cmd.Selector)
	mbp.Must(err, "failed to parse label selector", "selector", cmd.Selector)

	var listResp *pb.ListResponse
	listResp, err = client.ListAllJournals(ctx, rjc, listRequest)
	mbp.Must(err, "failed to resolved journals from selector", cmd.Selector)

	// Query the largest indexed fragment offset of each journal, and reset the append offset to that head.
	for _, journal := range listResp.Journals {
		var r = client.NewReader(ctx, rjc, pb.ReadRequest{
			Journal:      journal.Spec.Name,
			Offset:       -1,
			Block:        false,
			MetadataOnly: true,
		})
		if _, err = r.Read(nil); err != client.ErrOffsetNotYetAvailable {
			mbp.Must(err, "failed to read head of journal", "journal", journal.Spec.Name)
		}
		// Issue a zero-byte write at the indexed head.
		var a = client.NewAppender(ctx, rjc, pb.AppendRequest{
			Journal: journal.Spec.Name,
			Offset:  r.Response.Offset,
		})
		mbp.Must(a.Close(), "failed to reset journal offset", "journal", journal.Spec.Name)
	}
	return nil
}
