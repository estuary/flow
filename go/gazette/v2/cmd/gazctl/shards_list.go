package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/gogo/protobuf/proto"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/yaml.v2"
)

type cmdShardsList struct {
	ListConfig
}

func (cmd *cmdShardsList) Execute([]string) error {
	startup()

	var err error
	var req = new(consumer.ListRequest)
	var ctx = context.Background()

	req.Selector, err = pb.ParseLabelSelector(cmd.Selector)
	mbp.Must(err, "failed to parse label selector", "selector", cmd.Selector)

	resp, err := consumer.ListShards(ctx, consumer.NewShardClient(shardsCfg.Consumer.Dial(ctx)), req)
	mbp.Must(err, "failed to list shards")

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

func (cmd *cmdShardsList) outputYAML(resp *consumer.ListResponse) {
	var shards []yamlShard
	for i := range resp.Shards {
		shards = append(shards, yamlShard{
			ShardSpec: resp.Shards[i].Spec,
			Revision:  resp.Shards[i].ModRevision,
		})
	}
	var b, err = yaml.Marshal(shards)
	os.Stdout.Write(b)
	mbp.Must(err, "failed to encode shards")
}

type yamlShard struct {
	consumer.ShardSpec `yaml:",omitempty,inline"`
	// Delete marks that a Shard should be deleted.
	Delete bool `yaml:",omitempty"`
	// Revision of the Shard within Etcd.
	Revision int64 `yaml:",omitempty"`
}
