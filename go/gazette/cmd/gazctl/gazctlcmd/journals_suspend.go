package gazctlcmd

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	"golang.org/x/sync/errgroup"
)

type cmdJournalSuspend struct {
	Selector string `long:"selector" short:"l" required:"true" description:"Label Selector query to filter on"`
	Force    bool   `long:"force" short:"f" description:"Suspend journals even if they have local fragments"`
}

func init() {
	CommandRegistry.AddCommand("journals", "suspend", "Suspend idle journals", `
Suspend idle journals to use fewer or zero broker replicas.

Suspension updates the 'suspend' field of the JournalSpec with its suspended
status and its resumption offset. When applying other updates to JournalSpecs,
operators utilizing journal suspension must take care to not overwrite the
journal's suspension configuration.

Typically this means reading the current JournalSpec and its ModRevision,
copying the current 'suspend' field alongside other changes being made,
and then applying the updated JournalSpec with ExpectModRevision.

The 'journals edit' subcommand uses this workflow and is safe to use with suspension.
`, &cmdJournalSuspend{})
}

func (cmd *cmdJournalSuspend) Execute([]string) error {
	startup(JournalsCfg.BaseConfig)

	var err error
	var ctx = context.Background()
	var rjc = JournalsCfg.Broker.MustRoutedJournalClient(ctx)

	// Get the list of journals which match this selector.
	var listRequest pb.ListRequest
	listRequest.Selector, err = pb.ParseLabelSelector(cmd.Selector)
	mbp.Must(err, "failed to parse label selector", "selector", cmd.Selector)

	var listResp *pb.ListResponse
	listResp, err = client.ListAllJournals(ctx, rjc, listRequest)
	mbp.Must(err, "failed to resolve journals from selector", cmd.Selector)

	group, ctx := errgroup.WithContext(ctx)

	for _, journal := range listResp.Journals {
		if journal.Spec.Suspend.GetLevel() == pb.JournalSpec_Suspend_FULL {
			continue
		}
		group.Go(func() error {
			return suspendJournal(ctx, rjc, journal.Spec.Name, cmd.Force)
		})
	}
	return group.Wait()
}

func suspendJournal(
	ctx context.Context,
	rjc pb.RoutedJournalClient,
	journal pb.Journal,
	force bool,
) error {

	var mode = pb.AppendRequest_SUSPEND_IF_FLUSHED
	if force {
		mode = pb.AppendRequest_SUSPEND_NOW
	}

	var a = client.NewAppender(ctx, rjc, pb.AppendRequest{
		Journal: journal,
		Suspend: mode,
	})
	var err = a.Close()

	if err == nil || a.Response.Status == pb.Status_SUSPENDED {
		log.WithFields(log.Fields{
			"journal": journal,
			"status":  a.Response.Status,
		}).Info("journal suspension status")
		return nil
	} else {
		return fmt.Errorf("failed to suspend journal %s: %w", journal, err)
	}
}
