package airbyte

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/alecthomas/jsonschema"
	"github.com/estuary/flow/go/flow/ops"
	"github.com/estuary/protocols/airbyte"
	pc "github.com/estuary/protocols/capture"
	pf "github.com/estuary/protocols/flow"
	"github.com/go-openapi/jsonpointer"
	log "github.com/sirupsen/logrus"
)

// EndpointSpec is the configuration for Airbyte source connectors.
// It must match the one defined for the source specs (flow.yaml) in Rust.
type EndpointSpec struct {
	Image  string          `json:"image"`
	Config json.RawMessage `json:"config"`
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
// Though driver is a gRPC service stub, it's called in synchronous and
// in-process contexts to minimize ser/de & memory copies. As such it
// doesn't get to assume deep ownership of its requests, and must
// proto.Clone() shared state before mutating it.
type driver struct {
	networkName string
	logger      ops.Logger
}

// NewDriver returns a new JSON docker image driver.
func NewDriver(networkName string, logger ops.Logger) pc.DriverServer {
	return driver{
		networkName: networkName,
		logger:      logger,
	}
}

// Spec delegates to the `spec` command of the identified Airbyte image.
func (d driver) Spec(ctx context.Context, req *pc.SpecRequest) (*pc.SpecResponse, error) {
	var source = new(EndpointSpec)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}

	var spec *airbyte.Spec
	var err = RunConnector(ctx, source.Image, d.networkName,
		[]string{"spec"},
		// No configuration is passed to the connector.
		nil,
		// No stdin is sent to the connector.
		func(w io.Writer) error { return nil },
		// Expect to decode Airbyte messages, and a ConnectorSpecification specifically.
		NewConnectorJSONOutput(
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
			d.onStdoutDecodeError,
		),
		d.logger,
	)

	// Expect connector spit out a successful ConnectorSpecification.
	if err == nil && spec == nil {
		err = fmt.Errorf("connector didn't produce a Specification")
	}
	if err != nil {
		return nil, err
	}

	var reflector = jsonschema.Reflector{ExpandedStruct: true}
	resourceSchema, err := reflector.Reflect(new(ResourceSpec)).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.SpecResponse{
		EndpointSpecSchemaJson: spec.ConnectionSpecification,
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       spec.DocumentationURL,
	}, nil
}

// Discover delegates to the `discover` command of the identified Airbyte image.
func (d driver) Discover(ctx context.Context, req *pc.DiscoverRequest) (*pc.DiscoverResponse, error) {
	var source = new(EndpointSpec)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}

	var catalog *airbyte.Catalog
	var err = RunConnector(ctx, source.Image, d.networkName,
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
		NewConnectorJSONOutput(
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
			d.onStdoutDecodeError,
		),
		d.logger,
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

// Validate delegates to the `check` command of the identified Airbyte image.
// It does no actual validation unfortunately.
func (d driver) Validate(ctx context.Context, req *pc.ValidateRequest) (*pc.ValidateResponse, error) {
	var source = new(EndpointSpec)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}

	var status *airbyte.ConnectionStatus
	var err = RunConnector(ctx, source.Image, d.networkName,
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
		NewConnectorJSONOutput(
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
			d.onStdoutDecodeError,
		),
		d.logger,
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

// ApplyUpsert is a no-op (not supported by Airbyte connectors).
func (d driver) ApplyUpsert(context.Context, *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return new(pc.ApplyResponse), nil
}

// ApplyDelete is a no-op (not supported by Airbyte connectors).
func (d driver) ApplyDelete(context.Context, *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return new(pc.ApplyResponse), nil
}

// Pull delegates to the `read` command of the identified Airbyte image.
func (d driver) Pull(stream pc.Driver_PullServer) error {
	var source = new(EndpointSpec)

	// Read Open request.
	var req, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("reading open: %w", err)
	} else if err = req.Validate(); err != nil {
		return fmt.Errorf("open request: %w", err)
	} else if req.Open == nil {
		return fmt.Errorf("Open was expected but is empty")
	} else if err := pf.UnmarshalStrict(req.Open.Capture.EndpointSpecJson, source); err != nil {
		return fmt.Errorf("parsing connector configuration: %w", err)
	}

	var open = req.Open
	var streamToBinding = make(map[string]int)

	// Build configured Airbyte catalog.
	var catalog = airbyte.ConfiguredCatalog{
		Streams: nil,
		Tail:    open.Tail,
		Range: airbyte.Range{
			Begin: open.KeyBegin,
			End:   open.KeyEnd,
		},
	}
	for i, binding := range open.Capture.Bindings {
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
			"capture": open.Capture.Capture,
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

	if len(open.DriverCheckpointJson) != 0 {
		invokeArgs = append(invokeArgs, "--state", "/tmp/state.json")
		invokeFiles["state.json"] = open.DriverCheckpointJson
	}

	if err := stream.Send(&pc.PullResponse{Opened: &pc.PullResponse_Opened{}}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	var resp *pc.PullResponse

	// We'll re-use this fields map whenever we log connector output.
	var logFields = log.Fields{
		ops.LogSourceField: source.Image,
	}

	// Invoke the connector for reading.
	if err := RunConnector(stream.Context(), source.Image, d.networkName,
		invokeArgs,
		invokeFiles,
		func(w io.Writer) error {
			for {
				var req, err = stream.Recv()
				if err == io.EOF {
					return nil
				} else if err != nil {
					return err
				} else if err = req.Validate(); err != nil {
					return err
				}

				if req.Acknowledge != nil {
					// TODO(johnny): Pass as stdin to the connector.
				}
			}
		},
		// Expect to decode Airbyte messages.
		NewConnectorJSONOutput(
			func() interface{} { return new(airbyte.Message) },
			func(i interface{}) error {
				if rec := i.(*airbyte.Message); rec.Log != nil {
					d.logger.Log(airbyteToLogrusLevel(rec.Log.Level), logFields, rec.Log.Message)
				} else if rec.State != nil {
					return pc.WritePullCheckpoint(stream, &resp,
						&pf.DriverCheckpoint{
							DriverCheckpointJson: rec.State.Data,
							Rfc7396MergePatch:    rec.State.Merge,
						})
				} else if rec.Record != nil {
					if b, ok := streamToBinding[rec.Record.Stream]; ok {
						return pc.StagePullDocuments(stream, &resp, b, rec.Record.Data)
					}
					return fmt.Errorf("connector record with unknown stream %q", rec.Record.Stream)
				} else {
					return fmt.Errorf("unexpected connector message: %v", rec)
				}
				return nil
			},
			d.onStdoutDecodeError,
		),
		d.logger,
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
	return pc.WritePullCheckpoint(stream, &resp,
		&pf.DriverCheckpoint{
			DriverCheckpointJson: nil,
			Rfc7396MergePatch:    false,
		})
}

// onStdoutDecodeError is invoked whenever there's an error parsing a line into an airbyte JSON message.
// If the line can be parsed as a JSON object, then we'll treat it as an error since it could
// represent an airbyte message with an unknown or incompatible field.
// If the line cannot be parsed into a JSON object, then the line will be logged and the error
// ignored. This is because such a line most likely represents some non-JSON output from a println
// in the connector code, which is, unfortunately, common among airbyte connectors.
func (d driver) onStdoutDecodeError(naughtyLine []byte, decodeError error) error {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(naughtyLine, &obj); err == nil {
		// This was a naughty JSON object
		return decodeError
	} else {
		// We can't parse this as an object, so we'll just log it as plain text
		d.logger.Log(log.InfoLevel, log.Fields{
			ops.LogSourceField: "ignored invalid output from connector stdout",
		}, strings.TrimSpace(string(naughtyLine))) // naughtyLine ends with a newline, so trim
		return nil
	}
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
