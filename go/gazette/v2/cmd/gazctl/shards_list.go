package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/consumer/shardspace"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/gogo/protobuf/proto"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/yaml.v2"
)

type cmdShardsList struct {
	ListConfig
}

func init() {
	_ = mustAddCmd(cmdShards, "list", "List shards", `
List shard specifications and status.

Use --selector to supply a LabelSelector which constrains the set of returned
shards. Shard selectors support an additional meta-label "id".

Match ShardSpecs having a specific ID:
>    --selector "id in (shard-12, shard-34)"

Results can be output in a variety of --format options:
yaml:  Prints shards in YAML form, compatible with "shards apply"
json:  Prints ShardSpecs encoded as JSON
proto: Prints ShardSpecs encoded in protobuf text format
table: Prints as a table (see other flags for column choices)
`, &cmdShardsList{})
}

func (cmd *cmdShardsList) Execute([]string) error {
	startup()

	var resp = listShards(cmd.Selector)

	switch cmd.Format {
	case "table":
		cmd.outputTable(resp)
	case "yaml":
		writeHoistedYAMLShardSpace(os.Stdout, resp)
	case "json":
		mbp.Must(json.NewEncoder(os.Stdout).Encode(resp), "failed to encode to json")
	case "proto":
		mbp.Must(proto.MarshalText(os.Stdout, resp), "failed to write output")
	}
	return nil
}

func (cmd *cmdShardsList) outputTable(resp *consumer.ListResponse) {
	var table = tablewriter.NewWriter(os.Stdout)

	var headers = []string{"ID", "Status"}
	if cmd.RF {
		headers = append(headers, "RF")
	}
	if cmd.Primary {
		headers = append(headers, "Primary")
	}
	if cmd.Replicas {
		headers = append(headers, "Replicas")
	}
	for _, l := range cmd.Labels {
		headers = append(headers, l)
	}
	table.SetHeader(headers)

	for _, j := range resp.Shards {
		var primary = "<none>"
		var replicas []string
		var status consumer.ReplicaStatus

		for i, m := range j.Route.Members {
			var s = fmt.Sprintf("%s:%s", m.Suffix, j.Status[i].Code)
			status.Reduce(&j.Status[i])

			if int32(i) == j.Route.Primary {
				primary = s
			} else {
				replicas = append(replicas, s)
			}
		}

		var row = []string{
			j.Spec.Id.String(),
			status.Code.String(),
		}
		if cmd.RF {
			var rf int
			if !j.Spec.Disable {
				rf = int(j.Spec.HotStandbys) + 1
			}
			row = append(row, fmt.Sprintf("%d", rf))
		}
		if cmd.Primary {
			row = append(row, primary)
		}
		if cmd.Replicas {
			row = append(row, strings.Join(replicas, ","))
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

func listShards(s string) *consumer.ListResponse {
	var err error
	var req = new(consumer.ListRequest)
	var ctx = context.Background()

	req.Selector, err = pb.ParseLabelSelector(s)
	mbp.Must(err, "failed to parse label selector", "selector", s)

	resp, err := consumer.ListShards(ctx, consumer.NewShardClient(shardsCfg.Consumer.Dial(ctx)), req)
	mbp.Must(err, "failed to list shards")

	return resp
}

func writeHoistedYAMLShardSpace(w io.Writer, resp *consumer.ListResponse) {
	var b, err = yaml.Marshal(shardspace.FromListResponse(resp))
	_, _ = w.Write(b)
	mbp.Must(err, "failed to encode shardspace Set")
}
