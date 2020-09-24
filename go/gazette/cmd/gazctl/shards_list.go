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
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/shardspace"
	mbp "go.gazette.dev/core/mainboilerplate"
	"gopkg.in/yaml.v2"
)

type cmdShardsList struct {
	ListConfig
	Lag bool `long:"lag" description:"Show the amount of unread data for each shard"`
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

It's recommended that --lag be used with a relatively focused --selector,
as fetching consumption lag for a large number of shards may take a while.
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
		var m = jsonpb.Marshaler{OrigName: true, EmitDefaults: true}
		mbp.Must(m.Marshal(os.Stdout, resp), "failed to encode to json")
	case "proto":
		mbp.Must(proto.MarshalText(os.Stdout, resp), "failed to write output")
	}
	return nil
}

func (cmd *cmdShardsList) outputTable(resp *pc.ListResponse) {
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

	var rsc pc.RoutedShardClient
	var rjc pb.RoutedJournalClient
	if cmd.Lag {
		headers = append(headers, "Lag")
		var ctx = context.Background()
		rsc = shardsCfg.Consumer.MustRoutedShardClient(ctx)
		rjc = shardsCfg.Broker.MustRoutedJournalClient(ctx)
	}

	table.SetHeader(headers)

	for _, j := range resp.Shards {
		var primary = "<none>"
		var replicas []string
		var status pc.ReplicaStatus

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
		if cmd.Lag {
			row = append(row, getLag(j.Spec, rsc, rjc))
		}
		table.Append(row)
	}
	table.Render()
}

func listShards(s string) *pc.ListResponse {
	var err error
	var req = new(pc.ListRequest)
	var ctx = context.Background()

	req.Selector, err = pb.ParseLabelSelector(s)
	mbp.Must(err, "failed to parse label selector", "selector", s)

	resp, err := consumer.ListShards(ctx, pc.NewShardClient(shardsCfg.Consumer.MustDial(ctx)), req)
	mbp.Must(err, "failed to list shards")

	return resp
}

func writeHoistedYAMLShardSpace(w io.Writer, resp *pc.ListResponse) {
	var b, err = yaml.Marshal(shardspace.FromListResponse(resp))
	_, _ = w.Write(b)
	mbp.Must(err, "failed to encode shardspace Set")
}

func getLag(spec pc.ShardSpec, rsc pc.RoutedShardClient, rjc pb.RoutedJournalClient) string {
	var ctx = context.Background()
	var statReq = pc.StatRequest{
		Shard: spec.Id,
	}
	var statResp, err = consumer.StatShard(ctx, rsc, &statReq)
	mbp.Must(err, "failed to stat shard")

	var out = make([]string, 0, len(statResp.ReadThrough))
	for journal, offset := range statResp.ReadThrough {
		var readReq = pb.ReadRequest{
			Journal: pb.Journal(journal),
			Offset:  -1,
		}
		var reader = client.NewReader(ctx, rjc, readReq)
		_, err = reader.Read(nil)
		if err != nil && err != client.ErrOffsetNotYetAvailable {
			mbp.Must(err, "failed to read journal", journal)
		}
		out = append(out, fmt.Sprintf("%s:%d", journal, reader.Response.WriteHead-offset))
	}

	return strings.Join(out, ", ")
}
