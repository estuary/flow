package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gogo/protobuf/jsonpb"

	"github.com/estuary/flow/go/capture"
	"github.com/estuary/flow/go/connector"
	"github.com/estuary/flow/go/flow/ops"
	"github.com/estuary/flow/go/materialize"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type specResponse struct {
	Type               string          `json:"type"`
	DocumentationURL   string          `json:"documentationURL"`
	EndpointSpecSchema json.RawMessage `json:"endpointSpecSchema"`
	ResourceSpecSchema json.RawMessage `json:"resourceSpecSchema"`
	Oauth2Spec         json.RawMessage `json:"oauth2Spec"`
}

type apiSpec struct {
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
	Image       string                `long:"image" required:"true" description:"Docker image of the connector to use"`
	Network     string                `long:"network" description:"The Docker network that connector containers are given access to."`
}

const FLOW_RUNTIME_PROTOCOL_KEY = "FLOW_RUNTIME_PROTOCOL"

func (cmd apiSpec) execute(ctx context.Context) (specResponse, error) {
	var endpointSpec, err = json.Marshal(struct {
		Image  string   `json:"image"`
		Config struct{} `json:"config"`
	}{Image: cmd.Image})

	if err != nil {
		return specResponse{}, err
	}

	var imageMeta []struct {
		Config *struct {
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
	}

	if err = connector.PullImage(ctx, cmd.Image); err != nil {
		return specResponse{}, err
	} else if o, err := connector.DockerInspect(ctx, cmd.Image); err != nil {
		return specResponse{}, err
	} else if err = json.Unmarshal(o, &imageMeta); err != nil {
		return specResponse{}, fmt.Errorf("parsing inspect image %w", err)
	} else if len(imageMeta) == 0 || imageMeta[0].Config == nil {
		return specResponse{}, fmt.Errorf("inspected image metadata is malformed: %s", string(o))
	}

	if protocol, ok := imageMeta[0].Config.Labels[FLOW_RUNTIME_PROTOCOL_KEY]; protocol == "materialize" {
		return cmd.specMaterialization(ctx, endpointSpec)
	} else if protocol == "capture" {
		return cmd.specCapture(ctx, endpointSpec)
	} else if ok {
		return specResponse{}, fmt.Errorf("image labels specify unknown protocol %s=%s", FLOW_RUNTIME_PROTOCOL_KEY, protocol)
	} else if strings.HasPrefix(cmd.Image, "ghcr.io/estuary/materialize-") {
		// For backward compatibility with old images that do not have the labels
		return cmd.specMaterialization(ctx, endpointSpec)
	} else {
		return cmd.specCapture(ctx, endpointSpec)
	}
}

func (cmd apiSpec) specCapture(ctx context.Context, spec json.RawMessage) (specResponse, error) {
	var driver, err = capture.NewDriver(ctx,
		flow.EndpointType_AIRBYTE_SOURCE, spec, cmd.Network, ops.StdLogger())
	if err != nil {
		return specResponse{}, err
	}

	resp, err := driver.Spec(ctx, &pc.SpecRequest{
		EndpointType:     flow.EndpointType_AIRBYTE_SOURCE,
		EndpointSpecJson: spec,
	})
	if err != nil {
		return specResponse{}, err
	}

	var oauth2Spec bytes.Buffer
	if resp.Oauth2Spec != nil {
		// Serialize OAuth2Spec using canonical proto JSON
		err = (&jsonpb.Marshaler{}).Marshal(&oauth2Spec, resp.Oauth2Spec)
		if err != nil {
			return specResponse{}, err
		}
	}

	return specResponse{
		Type:               "capture",
		DocumentationURL:   resp.DocumentationUrl,
		EndpointSpecSchema: resp.EndpointSpecSchemaJson,
		ResourceSpecSchema: resp.ResourceSpecSchemaJson,
		Oauth2Spec:         oauth2Spec.Bytes(),
	}, nil
}

func (cmd apiSpec) specMaterialization(ctx context.Context, spec json.RawMessage) (specResponse, error) {
	var driver, err = materialize.NewDriver(ctx,
		flow.EndpointType_FLOW_SINK, spec, cmd.Network, ops.StdLogger())
	if err != nil {
		return specResponse{}, err
	}

	resp, err := driver.Spec(ctx, &pm.SpecRequest{
		EndpointType:     flow.EndpointType_FLOW_SINK,
		EndpointSpecJson: spec,
	})
	if err != nil {
		return specResponse{}, err
	}

	var oauth2Spec bytes.Buffer
	if resp.Oauth2Spec != nil {
		// Serialize OAuth2Spec using canonical proto JSON
		err = (&jsonpb.Marshaler{}).Marshal(&oauth2Spec, resp.Oauth2Spec)
		if err != nil {
			return specResponse{}, err
		}
	}

	return specResponse{
		Type:               "materialization",
		DocumentationURL:   resp.DocumentationUrl,
		EndpointSpecSchema: resp.EndpointSpecSchemaJson,
		ResourceSpecSchema: resp.ResourceSpecSchemaJson,
		Oauth2Spec:         oauth2Spec.Bytes(),
	}, nil
}

func (cmd apiSpec) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)
	var ctx, cancelFn = context.WithTimeout(context.Background(), time.Minute)
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
	return json.NewEncoder(os.Stdout).Encode(resp)
}
