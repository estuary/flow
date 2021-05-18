package main

import (
	"context"
	"fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdSplit struct {
	Consumer      mbp.AddressConfig     `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	Shard         string                `long:"shard" required:"true" description:"Shard to split"`
	SplitOnRClock bool                  `long:"split-rclock" description:"Split on rotated clock (instead of on key)"`
	Log           mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics   mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd cmdSplit) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")
	pb.RegisterGRPCDispatcher("local")

	var ctx = context.Background()
	ctx = pb.WithDispatchDefault(ctx)
	var conn = cmd.Consumer.MustDial(ctx)
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
