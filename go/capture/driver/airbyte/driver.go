package airbyte

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/estuary/connectors/go-types/airbyte"
	"github.com/estuary/connectors/go-types/shardrange"
	"github.com/estuary/flow/go/capture/lifecycle"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/go-openapi/jsonpointer"
	log "github.com/sirupsen/logrus"
)

// EndpointSpec is the configuration for Airbyte source connectors.
// It must match the one defined for the source specs (flow.yaml) in Rust.
type EndpointSpec struct {
	Image  string
	Config json.RawMessage
}

// Validate the configuration.
func (c EndpointSpec) Validate() error {
	if c.Image == "" {
		return fmt.Errorf("expected `image`")
	}
	return nil
}

// ResourceSpec is the configuration for Airbyte source streams.
type ResourceSpec struct {
	Stream    string           `json:"stream"`
	Namespace string           `json:"namespace,omitempty"`
	SyncMode  airbyte.SyncMode `json:"syncMode"`
}

// Validate the configuration.
func (c ResourceSpec) Validate() error {
	if c.Stream == "" {
		return fmt.Errorf("expected `stream`")
	}

	switch c.SyncMode {
	case airbyte.SyncModeFullRefresh, airbyte.SyncModeIncremental: // Pass.
	default:
		return fmt.Errorf("invalid sync mode %q (expected %s or %s)",
			c.SyncMode, airbyte.SyncModeFullRefresh, airbyte.SyncModeIncremental)
	}

	// Namespace is optional.

	return nil
}

// driver implements the pm.DriverServer interface.
type driver struct{}

// NewDriver returns a new JSON docker image driver.
func NewDriver() pc.DriverServer { return driver{} }

func (driver) Spec(ctx context.Context, req *pc.SpecRequest) (*pc.SpecResponse, error) {
	var source = new(EndpointSpec)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}

	var spec *airbyte.Spec
	var err = RunConnector(ctx, source.Image,
		[]string{"spec"},
		// No configuration is passed to the connector.
		nil,
		// No stdin is sent to the connector.
		func(w io.Writer) error { return nil },
		// Expect to decode Airbyte messages, and a ConnectorSpecification specifically.
		func() interface{} { return new(airbyte.Message) },
		func(i interface{}) error {
			if rec := i.(*airbyte.Message); rec.Log != nil {
				log.StandardLogger().WithFields(log.Fields{
					"image": source.Image,
				}).Log(airbyteToLogrusLevel(rec.Log.Level), rec.Log.Message)
			} else if rec.Spec != nil {
				spec = rec.Spec
			} else {
				return fmt.Errorf("unexpected connector message: %v", rec)
			}
			return nil
		},
	)

	// Expect connector spit out a successful ConnectorSpecification.
	if err == nil && spec == nil {
		err = fmt.Errorf("connector didn't produce a Specification")
	}
	if err != nil {
		return nil, err
	}

	return &pc.SpecResponse{
		DocumentationUrl: spec.DocumentationURL,
		SpecSchemaJson:   spec.ConnectionSpecification,
	}, nil
}

func (driver) Discover(ctx context.Context, req *pc.DiscoverRequest) (*pc.DiscoverResponse, error) {
	var source = new(EndpointSpec)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}

	var catalog *airbyte.Catalog
	var err = RunConnector(ctx, source.Image,
		[]string{
			"discover",
			"--config",
			"/tmp/config.json",
		},
		// Write configuration JSON to connector input.
		map[string]interface{}{"config.json": source.Config},
		// No stdin is sent to the connector.
		func(w io.Writer) error { return nil },
		// Expect to decode Airbyte messages, and a ConnectionStatus specifically.
		func() interface{} { return new(airbyte.Message) },
		func(i interface{}) error {
			if rec := i.(*airbyte.Message); rec.Log != nil {
				log.StandardLogger().WithFields(log.Fields{
					"image": source.Image,
				}).Log(airbyteToLogrusLevel(rec.Log.Level), rec.Log.Message)
			} else if rec.Catalog != nil {
				catalog = rec.Catalog
			} else {
				return fmt.Errorf("unexpected connector message: %v", rec)
			}
			return nil
		},
	)

	// Expect connector spit out a successful ConnectionStatus.
	if err == nil && catalog == nil {
		err = fmt.Errorf("connector didn't produce a Catalog")
	}
	if err != nil {
		return nil, err
	}

	var resp = new(pc.DiscoverResponse)
	for _, stream := range catalog.Streams {
		// Use incremental mode if available.
		var mode = airbyte.SyncModeFullRefresh
		for _, m := range stream.SupportedSyncModes {
			if m == airbyte.SyncModeIncremental {
				mode = m
			}
		}

		var resourceSpec, err = json.Marshal(ResourceSpec{
			Stream:    stream.Name,
			Namespace: stream.Namespace,
			SyncMode:  mode,
		})
		if err != nil {
			return nil, fmt.Errorf("encoding resource spec: %w", err)
		}

		// Encode array of hierarchical properties as a JSON-pointer.
		var keyPtrs []string
		for _, tokens := range stream.SourceDefinedPrimaryKey {
			for i := range tokens {
				tokens[i] = jsonpointer.Escape(tokens[i])
			}
			keyPtrs = append(keyPtrs, "/"+strings.Join(tokens, "/"))
		}

		resp.Bindings = append(resp.Bindings, &pc.DiscoverResponse_Binding{
			RecommendedName:    stream.Name,
			ResourceSpecJson:   json.RawMessage(resourceSpec),
			DocumentSchemaJson: stream.JSONSchema,
			KeyPtrs:            keyPtrs,
		})
	}

	return resp, nil
}

func (driver) Validate(ctx context.Context, req *pc.ValidateRequest) (*pc.ValidateResponse, error) {
	var source = new(EndpointSpec)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}

	var status *airbyte.ConnectionStatus
	var err = RunConnector(ctx, source.Image,
		[]string{
			"check",
			"--config",
			"/tmp/config.json",
		},
		// Write configuration JSON to connector input.
		map[string]interface{}{"config.json": source.Config},
		// No stdin is sent to the connector.
		func(w io.Writer) error { return nil },
		// Expect to decode Airbyte messages, and a ConnectionStatus specifically.
		func() interface{} { return new(airbyte.Message) },
		func(i interface{}) error {
			if rec := i.(*airbyte.Message); rec.Log != nil {
				// TODO - send these back through the Flow capture protocol ?
				log.StandardLogger().WithFields(log.Fields{
					"image":   source.Image,
					"capture": req.Capture,
				}).Log(airbyteToLogrusLevel(rec.Log.Level), rec.Log.Message)
			} else if rec.ConnectionStatus != nil {
				status = rec.ConnectionStatus
			} else {
				return fmt.Errorf("unexpected connector message: %v", rec)
			}
			return nil
		},
	)

	// Expect connector spit out a successful ConnectionStatus.
	if err == nil && status == nil {
		err = fmt.Errorf("connector didn't produce a ConnectionStatus")
	} else if err == nil && status.Status != airbyte.StatusSucceeded {
		err = fmt.Errorf("%s: %s", status.Status, status.Message)
	}
	if err != nil {
		return nil, err
	}

	// Parse stream bindings and send back their resource paths.
	var resp = new(pc.ValidateResponse)
	for _, binding := range req.Bindings {
		var stream = new(ResourceSpec)
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, stream); err != nil {
			return nil, fmt.Errorf("parsing stream configuration: %w", err)
		}
		resp.Bindings = append(resp.Bindings, &pc.ValidateResponse_Binding{
			ResourcePath: []string{stream.Stream},
		})
	}
	return resp, nil
}

func (driver) Capture(req *pc.CaptureRequest, stream pc.Driver_CaptureServer) error {
	var source = new(EndpointSpec)
	if err := req.Validate(); err != nil {
		return fmt.Errorf("validating request: %w", err)
	} else if err := pf.UnmarshalStrict(req.Capture.EndpointSpecJson, source); err != nil {
		return fmt.Errorf("parsing connector configuration: %w", err)
	}

	var streamToBinding = make(map[string]int)

	// Build configured Airbyte catalog.
	var catalog = airbyte.ConfiguredCatalog{
		Streams: nil,
		Tail:    req.Tail,
		Range: shardrange.Range{
			Begin: req.KeyBegin,
			End:   req.KeyEnd,
		},
	}
	for i, binding := range req.Capture.Bindings {
		var resource = new(ResourceSpec)
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, resource); err != nil {
			return fmt.Errorf("parsing stream configuration: %w", err)
		}

		var projections = make(map[string]string)
		for _, p := range binding.Collection.Projections {
			projections[p.Field] = p.Ptr
		}

		catalog.Streams = append(catalog.Streams,
			airbyte.ConfiguredStream{
				SyncMode:            resource.SyncMode,
				DestinationSyncMode: airbyte.DestinationSyncModeAppend,
				Stream: airbyte.Stream{
					Name:               resource.Stream,
					Namespace:          resource.Namespace,
					JSONSchema:         binding.Collection.SchemaJson,
					SupportedSyncModes: []airbyte.SyncMode{resource.SyncMode},
				},
				Projections: projections,
			})
		streamToBinding[resource.Stream] = i
	}

	if log.GetLevel() >= log.DebugLevel {
		var catalogJSON, err = json.Marshal(&catalog)
		if err != nil {
			return fmt.Errorf("encoding catalog: %w", err)
		}

		log.WithFields(log.Fields{
			"capture": req.Capture.Capture,
			"catalog": string(catalogJSON),
		}).Debug("using configured catalog")
	}

	var invokeArgs = []string{
		"read",
		"--config",
		"/tmp/config.json",
		"--catalog",
		"/tmp/catalog.json",
	}
	var invokeFiles = map[string]interface{}{
		"config.json":  source.Config,
		"catalog.json": catalog,
	}

	if len(req.DriverCheckpointJson) != 0 {
		invokeArgs = append(invokeArgs, "--state", "/tmp/state.json")
		invokeFiles["state.json"] = req.DriverCheckpointJson
	}

	if err := stream.Send(&pc.CaptureResponse{Opened: &pc.CaptureResponse_Opened{}}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	var resp *pc.CaptureResponse

	// Invoke the connector for reading.
	if err := RunConnector(stream.Context(), source.Image,
		invokeArgs,
		invokeFiles,
		// No stdin is sent to the connector.
		func(w io.Writer) error { return nil },
		// Expect to decode Airbyte messages.
		func() interface{} { return new(airbyte.Message) },
		func(i interface{}) error {
			if rec := i.(*airbyte.Message); rec.Log != nil {
				log.StandardLogger().WithFields(log.Fields{
					"image":   source.Image,
					"capture": req.Capture.Capture,
				}).Log(airbyteToLogrusLevel(rec.Log.Level), rec.Log.Message)
			} else if rec.State != nil {
				return lifecycle.WriteCommit(stream, &resp,
					&pc.CaptureResponse_Commit{DriverCheckpointJson: rec.State.Data})
			} else if rec.Record != nil {
				if b, ok := streamToBinding[rec.Record.Stream]; ok {
					return lifecycle.StageCaptured(stream, &resp, b, rec.Record.Data)
				}
				return fmt.Errorf("connector record with unknown stream %q", rec.Record.Stream)
			} else {
				return fmt.Errorf("unexpected connector message: %v", rec)
			}
			return nil
		},
	); err != nil {
		return err
	}

	if resp == nil {
		return nil // Connector flushed prior to exiting. All done.
	}

	// Write a final commit, followed by EOF.
	// This happens only when a connector writes output and exits _without_
	// writing a final state checkpoint. We generate a synthetic commit now,
	// and the nil checkpoint means the assumed behavior of the next invocation
	// will be "full refresh".
	return lifecycle.WriteCommit(stream, &resp,
		&pc.CaptureResponse_Commit{DriverCheckpointJson: nil})
}

// LogrusLevel returns an appropriate logrus.Level for the connector LogLevel.
func airbyteToLogrusLevel(l airbyte.LogLevel) log.Level {
	switch l {
	case airbyte.LogLevelTrace:
		return log.TraceLevel
	case airbyte.LogLevelDebug:
		return log.DebugLevel
	case airbyte.LogLevelInfo:
		return log.InfoLevel
	case airbyte.LogLevelWarn:
		return log.WarnLevel
	default: // Includes LogLevelError, LogLevelFatal.
		return log.ErrorLevel
	}
}
