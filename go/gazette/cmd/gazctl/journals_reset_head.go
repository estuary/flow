package main

import (
	"context"

	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/client"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdJournalResetHead struct {
	Selector string `long:"selector" short:"l" required:"true" description:"Label Selector query to filter on"`
}

func init() {
	_ = mustAddCmd(cmdJournals, "reset-head", "Reset journal append offset (disaster recovery)", `
Reset the append offset of journals.

Gazette appends are transactional: all brokers must agree on the exact offsets
at which an append operation will be written into a journal. The offset is an
explicit participate in the broker's transaction protocol. New participants are
"caught up" on the current offset by participating in broker transactions, and
brokers will delay releasing responsibility for a journal until all peers have
participated in a synchronizing transaction. This makes Gazette tolerant to up
to R-1 independent broker process failures, where R is the replication factor
of the journal.

However, disasters and human errors do happen, and if R or more independent
failures occur, Gazette employs a fail-safe to minimize the potential for a
journal offset to be written more than once: brokers require that the remote
fragment index not include a fragment offset larger than the append offset known
to replicating broker peers, and will refuse the append if this constraint is
violated.

Eg, If N >= R failures occur, then the set of broker peers of a journal will not
have participated in an append transaction; their append offset will be zero,
which is less than the maximum offset contained in the fragment store. The
brokers will refuse all appends to preclude double-writing of an offset.

This condition must be explicitly cleared by the Gazette operator using the
reset-head command. The operator should delay running reset-head until absolutely
confident that all journal fragments have been persisted to cloud storage (eg,
because all previous broker processes have exited).

Then, the effect of reset-head is to jump the append offset forward to the
maximum indexed offset, allowing new append operations to proceed.

reset-head is safe to run against journals which are in a fully consistent state,
though it is likely to fail harmlessly if the journal is being actively written.
`, &cmdJournalResetHead{})
}

func (cmd *cmdJournalResetHead) Execute([]string) error {
	startup()

	var err error
	var ctx = context.Background()
	var rjc = journalsCfg.Broker.MustRoutedJournalClient(ctx)

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
