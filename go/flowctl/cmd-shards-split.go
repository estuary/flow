package main

import (
	"context"
	"fmt"

	pf "github.com/estuary/protocols/flow"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/cmd/gazctl/gazctlcmd"
	pc "go.gazette.dev/core/consumer/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

// This command will be under the shards command which leverages the gazctlcmd.ShardsCfg config.
type cmdSplit struct {
	Shard         string                `long:"shard" required:"true" description:"Shard to split"`
	SplitOnRClock bool                  `long:"split-rclock" description:"Split on rotated clock (instead of on key)"`
	Diagnostics   mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func init() {
	// Automatically register this command under the shards command
	gazctlcmd.CommandRegistry.AddCommand("shards", "split", "Split a Flow processing shard", `
Split a Flow processing shard into two, either on shuffled key or rotated clock.
`, &cmdSplit{})
}

func (cmd cmdSplit) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(gazctlcmd.ShardsCfg.Log)

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")
	pb.RegisterGRPCDispatcher(gazctlcmd.ShardsCfg.Zone)

	var ctx = context.Background()
	ctx = pb.WithDispatchDefault(ctx)
	var conn = gazctlcmd.ShardsCfg.Consumer.MustDial(ctx)
	var splitter = pf.NewSplitterClient(conn)

	var resp, err = splitter.Split(ctx, &pf.SplitRequest{
		Shard:         pc.ShardID(cmd.Shard),
		SplitOnKey:    !cmd.SplitOnRClock,
		SplitOnRclock: cmd.SplitOnRClock,
	})

	if err != nil {
		return fmt.Errorf("splitting shard: %w", err)
	} else if resp.Status != pc.Status_OK {
		return fmt.Errorf("splitting shard status: %s", resp.Status.String())
	}

	log.WithFields(log.Fields{
		"parent": resp.ParentRange.String(),
		"lhs":    resp.LhsRange.String(),
		"rhs":    resp.RhsRange.String(),
	}).Info("split result")

	fmt.Println("Started split.")
	return nil
}
