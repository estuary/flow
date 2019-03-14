package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/LiveRamp/gazette/v2/pkg/client"
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
	Lag bool `long:"lag" description:"Show consumer lag. It is recomended that this flag be used along side the --selector flag as fetching lag for all shards may take a long time."`
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

	var rsc consumer.RoutedShardClient
	var rjc pb.RoutedJournalClient
	if cmd.Lag {
		headers = append(headers, "Lag")
		var ctx = context.Background()
		rsc = shardsCfg.Consumer.RoutedShardClient(ctx)
		rjc = shardsCfg.Broker.RoutedJournalClient(ctx)
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
		if cmd.Lag {
			row = append(row, getLag(j.Spec, rsc, rjc))
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

func getLag(spec consumer.ShardSpec, rsc consumer.RoutedShardClient, rjc pb.RoutedJournalClient) string {
	var ctx = context.Background()
	var statReq = consumer.StatRequest{
		Shard: spec.Id,
	}
	var statResp, err = consumer.StatShard(ctx, rsc, &statReq)
	mbp.Must(err, "failed to stat shard")

	var out = make([]string, 0, len(statResp.Offsets))
	for journal, offset := range statResp.Offsets {
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
