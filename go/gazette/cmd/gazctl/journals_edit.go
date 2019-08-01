package main

import (
	"bytes"
	"context"
	"io"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/protocol/journalspace"
	"go.gazette.dev/core/client"
	"go.gazette.dev/core/cmd/gazctl/editor"
	mbp "go.gazette.dev/core/mainboilerplate"
	"gopkg.in/yaml.v2"
)

type cmdJournalsEdit struct {
	EditConfig
}

func init() {
	_ = mustAddCmd(cmdJournals, "edit", "Edit journal specifications", journalsEditLongDesc, &cmdJournalsEdit{})
}

func (cmd *cmdJournalsEdit) Execute([]string) error {
	startup()
	return editor.EditRetryLoop(editor.RetryLoopArgs{
		FilePrefix:       "gazctl-journals-edit-",
		SelectFn:         cmd.selectSpecs,
		ApplyFn:          cmd.applySpecs,
		AbortIfUnchanged: true,
	})
}

// selectSpecs returns the hoisted YAML specs of journals matching the selector.
func (cmd *cmdJournalsEdit) selectSpecs() io.Reader {
	var resp = listJournals(cmd.Selector)

	if len(resp.Journals) == 0 {
		log.WithField("selector", cmd.Selector).Panic("no journals match selector")
	}
	var buf = &bytes.Buffer{}
	writeHoistedJournalSpecTree(buf, resp)

	return buf
}

func (cmd *cmdJournalsEdit) applySpecs(b []byte) error {
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
	var resp, err = client.ApplyJournalsInBatches(ctx, journalsCfg.Broker.MustJournalClient(ctx), req, cmd.MaxTxnSize)
	mbp.Must(err, "failed to apply journals")
	log.WithField("rev", resp.Header.Etcd.Revision).Info("successfully applied")

	return nil
}
