package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/olekukonko/tablewriter"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/broker/journalspace"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	"gopkg.in/yaml.v2"
)

type cmdJournalsList struct {
	ListConfig
	Stores bool `long:"stores" description:"Show fragment store column"`
}

func init() {
	_ = mustAddCmd(cmdJournals, "list", "List journals", `
List journal specifications and status.

Use --selector to supply a LabelSelector which constrains the set of returned
journals. Journal selectors support additional meta-labels "name" and "prefix".

Match JournalSpecs having an exact name:
>    --selector "name in (foo/bar, baz/bing)"

Match JournalSpecs having a name prefix (must end in '/'):
>    --selector "prefix = my/prefix/"

Results can be output in a variety of --format options:
yaml:  Prints a YAML journal hierarchy, compatible with "journals apply"
json:  Prints JournalSpecs encoded as JSON, one per line.
proto: Prints JournalSpecs encoded in protobuf text format
table: Prints as a table (see other flags for column choices)

When output as a journal hierarchy, gazctl will "hoist" the returned collection
of JournalSpecs into a hierarchy of journals having common prefixes and,
typically, common configuration. This hierarchy is simply sugar for and is
exactly equivalent to the original JournalSpecs.
`, &cmdJournalsList{})
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
		var m = jsonpb.Marshaler{OrigName: true, EmitDefaults: true}
		mbp.Must(m.Marshal(os.Stdout, resp), "failed to encode to json")
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

	resp, err := client.ListAllJournals(ctx, journalsCfg.Broker.MustJournalClient(ctx), req)
	mbp.Must(err, "failed to list journals")

	return resp
}

func writeHoistedJournalSpecTree(w io.Writer, resp *pb.ListResponse) {
	b, err := yaml.Marshal(journalspace.FromListResponse(resp))
	_, _ = w.Write(b)
	mbp.Must(err, "failed to encode journals")
}
