package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/LiveRamp/gazette/v2/pkg/client"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/protocol/journalspace"
	"github.com/gogo/protobuf/proto"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/yaml.v2"
)

type cmdJournalsList struct {
	ListConfig
	Stores bool `long:"stores" description:"Show fragment store column"`
}

func (cmd *cmdJournalsList) Execute([]string) error {
	startup()

	var resp = listJournals(cmd.Selector)

	switch cmd.Format {
	case "table":
		cmd.outputTable(resp)
	case "yaml":
		cmd.outputYAML(resp)
	case "json":
		mbp.Must(json.NewEncoder(os.Stdout).Encode(resp), "failed to encode to json")
	case "proto":
		mbp.Must(proto.MarshalText(os.Stdout, resp), "failed to write output")
	}
	return nil
}

func (cmd *cmdJournalsList) outputTable(resp *pb.ListResponse) {
	var table = tablewriter.NewWriter(os.Stdout)

	var headers = []string{"Name"}
	if cmd.RF {
		headers = append(headers, "RF")
	}
	if cmd.Primary {
		headers = append(headers, "Primary")
	}
	if cmd.Replicas {
		headers = append(headers, "Replicas")
	}
	if cmd.Stores {
		headers = append(headers, "Stores")
	}
	for _, l := range cmd.Labels {
		headers = append(headers, l)
	}
	table.SetHeader(headers)

	for _, j := range resp.Journals {
		var primary = "<none>"
		var replicas []string

		for i, m := range j.Route.Members {
			if int32(i) == j.Route.Primary {
				primary = m.Suffix
			} else {
				replicas = append(replicas, m.Suffix)
			}
		}

		var row = []string{
			j.Spec.Name.String(),
		}
		if cmd.RF {
			row = append(row, fmt.Sprintf("%d", j.Spec.Replication))
		}
		if cmd.Primary {
			row = append(row, primary)
		}
		if cmd.Replicas {
			row = append(row, strings.Join(replicas, ","))
		}
		if cmd.Stores {
			if len(j.Spec.Fragment.Stores) != 0 {
				row = append(row, string(j.Spec.Fragment.Stores[0]))
			} else {
				row = append(row, "<none>")
			}
		}
		for _, l := range cmd.Labels {
			if v := j.Spec.LabelSet.ValuesOf(l); v == nil {
				row = append(row, "<none>")
			} else {
				row = append(row, strings.Join(v, ","))
			}
		}
		table.Append(row)
	}
	table.Render()
}

func (cmd *cmdJournalsList) outputYAML(resp *pb.ListResponse) {
	writeHoistedJournalSpecTree(os.Stdout, resp)
}

func listJournals(s string) *pb.ListResponse {
	var err error
	var req pb.ListRequest
	var ctx = context.Background()

	req.Selector, err = pb.ParseLabelSelector(s)
	mbp.Must(err, "failed to parse label selector", "selector", s)

	resp, err := client.ListAllJournals(ctx, pb.NewJournalClient(journalsCfg.Broker.Dial(ctx)), req)
	mbp.Must(err, "failed to list journals")

	return resp
}

func writeHoistedJournalSpecTree(w io.Writer, resp *pb.ListResponse) {
	// Initialize Nodes for each journal. Extract a journal specification tree,
	// and hoist common configuration to parent nodes to minimize config DRY.
	var nodes []journalspace.Node

	for i := range resp.Journals {
		nodes = append(nodes, journalspace.Node{
			JournalSpec: resp.Journals[i].Spec,
			Revision:    resp.Journals[i].ModRevision,
		})
	}

	var tree = journalspace.ExtractTree(nodes)
	tree.HoistSpecs()

	// Render journal specification tree.
	b, err := yaml.Marshal(tree)
	mbp.Must(err, "failed to encode journals")
	w.Write(b)
}
