package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/estuary/flow/go/connector"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/ops"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	"gopkg.in/yaml.v3"
)

type apiDiscover struct {
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
	Image       string                `long:"image" required:"true" description:"Docker image of the connector to use"`
	Network     string                `long:"network" description:"The Docker network that connector containers are given access to."`
	Name        string                `long:"name" description:"The Docker container name."`
	Config      string                `long:"config" description:"Path to the connector endpoint configuration"`
	Output      string                `long:"output" choice:"json" choice:"proto" default:"json"`
}

func (cmd apiDiscover) execute(ctx context.Context) (*pc.Response_Discovered, error) {
	var config, err = readConfig(cmd.Config)
	if err != nil {
		return nil, err
	}

	spec, err := json.Marshal(struct {
		Image  string          `json:"image"`
		Config json.RawMessage `json:"config"`
	}{
		Image:  cmd.Image,
		Config: config,
	})
	if err != nil {
		return nil, err
	}
	var publisher = ops.NewLocalPublisher(ops.ShardLabeling{
		TaskName: cmd.Name,
	})

	var request = &pc.Request{
		Discover: &pc.Request_Discover{
			ConnectorType: pf.CaptureSpec_IMAGE,
			ConfigJson:    spec,
		},
	}
	response, err := connector.Invoke[pc.Response](
		ctx,
		request,
		cmd.Network,
		publisher,
		func(driver *connector.Driver) (pc.Connector_CaptureClient, error) {
			return driver.CaptureClient().Capture(ctx)
		},
	)
	if err != nil {
		return nil, err
	}
	return response.Discovered, nil
}

func (cmd apiDiscover) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)
	var ctx, cancelFn = context.WithTimeout(context.Background(), time.Second*30)
	defer cancelFn()

	logrus.WithFields(logrus.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")
	pb.RegisterGRPCDispatcher("local")

	var resp, err = cmd.execute(ctx)
	if err != nil {
		return err
	}

	if cmd.Output == "json" {
		return (&jsonpb.Marshaler{}).Marshal(os.Stdout, resp)
	} else if cmd.Output == "proto" {
		var b, err = resp.Marshal()
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(b)
		return err
	} else {
		panic(cmd.Output)
	}
}

func readConfig(path string) (raw json.RawMessage, err error) {
	var iface interface{}

	if r, err := os.Open(path); err != nil {
		return nil, fmt.Errorf("opening config: %w", err)
	} else if err = yaml.NewDecoder(r).Decode(&iface); err != nil {
		return nil, fmt.Errorf("decoding config: %w", err)
	} else if raw, err = json.Marshal(iface); err != nil {
		return nil, fmt.Errorf("encoding JSON config: %w", err)
	}

	return raw, nil
}
