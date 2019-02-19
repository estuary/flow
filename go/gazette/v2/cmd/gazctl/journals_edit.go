package main

import (
	"bytes"
	"context"
	"io"

	"github.com/LiveRamp/gazette/v2/cmd/gazctl/editor"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	"github.com/LiveRamp/gazette/v2/pkg/protocol/journalspace"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type cmdJournalsEdit struct {
	EditConfig
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
	var resp, err = client.ApplyJournalsInBatches(ctx, journalsCfg.Broker.JournalClient(ctx), req, cmd.MaxTxnSize)
	mbp.Must(err, "failed to apply journals")
	log.WithField("rev", resp.Header.Etcd.Revision).Info("successfully applied")

	return nil
}
