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

	"github.com/estuary/flow/go/connector"
	"github.com/estuary/flow/go/labels"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/estuary/flow/go/protocols/ops"
	"github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type specResponse struct {
	Protocol             string          `json:"protocol"`
	DocumentationURL     string          `json:"documentationUrl"`
	ConfigSchema         json.RawMessage `json:"configSchema"`
	ResourceConfigSchema json.RawMessage `json:"resourceConfigSchema"`
	Oauth2               json.RawMessage `json:"oauth2"`
}

type apiSpec struct {
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
	Image       string                `long:"image" required:"true" description:"Docker image of the connector to use"`
	Network     string                `long:"network" description:"The Docker network that connector containers are given access to."`
	Name        string                `long:"name" description:"The Docker container name."`
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
	} else if o, err := connector.InspectImage(ctx, cmd.Image); err != nil {
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
	var publisher = ops.NewLocalPublisher(labels.ShardLabeling{
		TaskName: cmd.Name,
	})
	var request = &pc.Request{
		Spec: &pc.Request_Spec{
			ConnectorType: pf.CaptureSpec_IMAGE,
			ConfigJson:    spec,
		},
	}
	var response, err = connector.Invoke[pc.Response](
		ctx,
		request,
		cmd.Network,
		publisher,
		func(driver *connector.Driver) (pc.Connector_CaptureClient, error) {
			return driver.CaptureClient().Capture(ctx)
		},
	)
	if err != nil {
		return specResponse{}, err
	} else if response.Spec == nil {
		return specResponse{}, fmt.Errorf("missing Spec response")
	}

	var oauth2Spec bytes.Buffer
	if response.Spec.Oauth2 != nil {
		// Serialize OAuth2Spec using canonical proto JSON
		err = (&jsonpb.Marshaler{}).Marshal(&oauth2Spec, response.Spec.Oauth2)
		if err != nil {
			return specResponse{}, err
		}
	}

	return specResponse{
		Protocol:             "capture",
		DocumentationURL:     response.Spec.DocumentationUrl,
		ConfigSchema:         response.Spec.ConfigSchemaJson,
		ResourceConfigSchema: response.Spec.ResourceConfigSchemaJson,
		Oauth2:               oauth2Spec.Bytes(),
	}, nil
}

func (cmd apiSpec) specMaterialization(ctx context.Context, spec json.RawMessage) (specResponse, error) {
	var publisher = ops.NewLocalPublisher(labels.ShardLabeling{
		TaskName: cmd.Name,
	})
	var request = &pm.Request{
		Spec: &pm.Request_Spec{
			ConnectorType: pf.MaterializationSpec_IMAGE,
			ConfigJson:    spec,
		},
	}
	var response, err = connector.Invoke[pm.Response](
		ctx,
		request,
		cmd.Network,
		publisher,
		func(driver *connector.Driver) (pm.Connector_MaterializeClient, error) {
			return driver.MaterializeClient().Materialize(ctx)
		},
	)
	if err != nil {
		return specResponse{}, err
	} else if response.Spec == nil {
		return specResponse{}, fmt.Errorf("missing Spec response")
	}

	var oauth2Spec bytes.Buffer
	if response.Spec.Oauth2 != nil {
		// Serialize OAuth2Spec using canonical proto JSON
		err = (&jsonpb.Marshaler{}).Marshal(&oauth2Spec, response.Spec.Oauth2)
		if err != nil {
			return specResponse{}, err
		}
	}

	return specResponse{
		Protocol:             "materialization",
		DocumentationURL:     response.Spec.DocumentationUrl,
		ConfigSchema:         response.Spec.ConfigSchemaJson,
		ResourceConfigSchema: response.Spec.ResourceConfigSchemaJson,
		Oauth2:               oauth2Spec.Bytes(),
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
