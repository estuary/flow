package gazctlcmd

import (
	"bytes"
	"context"
	"io"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/broker/journalspace"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/cmd/gazctl/gazctlcmd/editor"
	pc "go.gazette.dev/core/consumer/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	"gopkg.in/yaml.v2"
)

type cmdJournalsEdit struct {
	EditConfig
}

func init() {
	CommandRegistry.AddCommand("journals", "edit", "Edit journal specifications", journalsEditLongDesc, &cmdJournalsEdit{})
}

func (cmd *cmdJournalsEdit) Execute([]string) error {
	startup(JournalsCfg.BaseConfig)
	var ctx = context.Background()
	var rjc = JournalsCfg.Broker.MustRoutedJournalClient(ctx)

	return editor.EditRetryLoop(editor.RetryLoopArgs{
		FilePrefix:       "gazctl-journals-edit-",
		JournalClient:    rjc,
		SelectFn:         cmd.selectSpecs,
		ApplyFn:          cmd.applySpecs,
		AbortIfUnchanged: true,
	})
}

// selectSpecs returns the hoisted YAML specs of journals matching the selector.
func (cmd *cmdJournalsEdit) selectSpecs(_ pc.ShardClient, journalClient pb.JournalClient) io.Reader {
	var resp = listJournals(journalClient, cmd.Selector)

	if len(resp.Journals) == 0 {
		log.WithField("selector", cmd.Selector).Panic("no journals match selector")
	}
	var buf = &bytes.Buffer{}
	writeHoistedJournalSpecTree(buf, resp)

	return buf
}

func (cmd *cmdJournalsEdit) applySpecs(b []byte, _ pc.ShardClient, journalClient pb.JournalClient) error {
	var tree journalspace.Node
	if err := yaml.UnmarshalStrict(b, &tree); err != nil {
		return err
	}
	if err := tree.Validate(); err != nil {
		return err
	}

	var req = newJournalSpecApplyRequest(&tree)
	if err := req.Validate(); err != nil {
		return err
	}

	var ctx = context.Background()
	var resp, err = client.ApplyJournalsInBatches(ctx, journalClient, req, cmd.MaxTxnSize)
	mbp.Must(err, "failed to apply journals")
	log.WithField("revision", resp.Header.Etcd.Revision).Info("successfully applied")

	return nil
}
