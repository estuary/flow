package airbyte

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/estuary/flow/go/connector"
	"github.com/estuary/flow/go/flow/ops"
	"github.com/estuary/flow/go/protocols/airbyte"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	protoio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/proto"
	"github.com/sirupsen/logrus"
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

func (c EndpointSpec) fields() logrus.Fields {
	return logrus.Fields{ops.LogSourceField: c.Image}
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
	var logger = ops.NewLoggerWithFields(d.logger, logrus.Fields{
		ops.LogSourceField: source.Image,
		"operation":        "spec",
	})

	var decrypted, err = connector.DecryptConfig(ctx, source.Config)
	if err != nil {
		return nil, err
	}
	defer connector.ZeroBytes(decrypted) // connector.Run will also ZeroBytes().
	req.EndpointSpecJson = decrypted

	var resp *pc.SpecResponse
	err = connector.Run(ctx, source.Image, connector.Capture, d.networkName,
		[]string{"spec"},
		// No configuration is passed to the connector.
		nil,
		// No stdin is sent to the connector.
		func(w io.Writer) error {
			defer connector.ZeroBytes(decrypted)
			return protoio.NewUint32DelimitedWriter(w, binary.LittleEndian).
				WriteMsg(req)
		},
		// Expect to decode Airbyte messages, and a ConnectorSpecification specifically.
		connector.NewProtoOutput(
			func() proto.Message { return new(pc.SpecResponse) },
			func(m proto.Message) error {
				if resp != nil {
					return fmt.Errorf("read more than one SpecResponse")
				}
				resp = m.(*pc.SpecResponse)
				return nil
			},
		),
		logger,
	)
	return resp, err

}

// Discover delegates to the `discover` command of the identified Airbyte image.
func (d driver) Discover(ctx context.Context, req *pc.DiscoverRequest) (*pc.DiscoverResponse, error) {
	var source = new(EndpointSpec)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}
	var logger = ops.NewLoggerWithFields(d.logger, logrus.Fields{
		ops.LogSourceField: source.Image,
		"operation":        "discover",
	})

	var decrypted, err = connector.DecryptConfig(ctx, source.Config)
	if err != nil {
		return nil, err
	}
	defer connector.ZeroBytes(decrypted) // connector.Run will also ZeroBytes().
	req.EndpointSpecJson = decrypted

	var resp *pc.DiscoverResponse
	err = connector.Run(ctx, source.Image, connector.Capture, d.networkName,
		[]string{
			"discover",
		},
		nil,
		func(w io.Writer) error {
			defer connector.ZeroBytes(decrypted)
			return protoio.NewUint32DelimitedWriter(w, binary.LittleEndian).
				WriteMsg(req)
		},
		connector.NewProtoOutput(
			func() proto.Message { return new(pc.DiscoverResponse) },
			func(m proto.Message) error {
				if resp != nil {
					return fmt.Errorf("read more than one DiscoverResponse")
				}
				resp = m.(*pc.DiscoverResponse)
				return nil
			},
		),
		logger,
	)

	// Expect connector spit out a successful ConnectionStatus.
	if err == nil && resp == nil {
		err = fmt.Errorf("connector didn't produce a Catalog")
	} else if err != nil {
		return nil, err
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

	var decrypted, err = connector.DecryptConfig(ctx, source.Config)
	if err != nil {
		return nil, err
	}
	defer connector.ZeroBytes(decrypted) // RunConnector will also ZeroBytes().
	var logger = ops.NewLoggerWithFields(d.logger, logrus.Fields{
		ops.LogSourceField: source.Image,
		"operation":        "validate",
	})
	req.EndpointSpecJson = decrypted

	var resp *pc.ValidateResponse
	err = connector.Run(ctx, source.Image, connector.Capture, d.networkName,
		[]string{
			"validate",
		},
		nil,
		func(w io.Writer) error {
			defer connector.ZeroBytes(decrypted)
			return protoio.NewUint32DelimitedWriter(w, binary.LittleEndian).
				WriteMsg(req)
		},
		connector.NewProtoOutput(
			func() proto.Message { return new(pc.ValidateResponse) },
			func(m proto.Message) error {
				if resp != nil {
					return fmt.Errorf("read more than one ValidateResponse")
				}
				resp = m.(*pc.ValidateResponse)
				return nil
			},
		),
		logger,
	)

	if err == nil && resp == nil {
		err = fmt.Errorf("connector didn't produce a response")
	}
	if err != nil {
		return nil, err
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

	var logger = ops.NewLoggerWithFields(d.logger, logrus.Fields{
		ops.LogSourceField: source.Image,
		"operation":        "read",
	})

	decrypted, err := connector.DecryptConfig(stream.Context(), source.Config)
	if err != nil {
		return err
	}
	defer connector.ZeroBytes(decrypted) // RunConnector will also ZeroBytes().

	req.Open.Capture.EndpointSpecJson = decrypted

	if err := stream.Send(&pc.PullResponse{Opened: &pc.PullResponse_Opened{}}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	// Invoke the connector for reading.
	return connector.Run(stream.Context(), source.Image, connector.Capture, d.networkName,
		[]string{"pull"},
		nil,
		func(w io.Writer) error {
			defer connector.ZeroBytes(decrypted)
			var enc = protoio.NewUint32DelimitedWriter(w, binary.LittleEndian)
			var err = enc.WriteMsg(req)

			if err != nil {
				return fmt.Errorf("proxying Open: %w", err)
			}

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
		connector.NewProtoOutput(
			func() proto.Message { return new(pc.PullResponse) },
			func(m proto.Message) error {
				return stream.Send(m.(*pc.PullResponse))
			},
		),
		logger,
	)
}

// onStdoutDecodeError returns a function that is invoked whenever there's an error parsing a line
// into an airbyte JSON message. If the line can be parsed as a JSON object, then we'll treat it as
// an error since it could represent an airbyte message with an unknown or incompatible field. If
// the line cannot be parsed into a JSON object, then the line will be logged and the error ignored.
// This is because such a line most likely represents some non-JSON output from a println in the
// connector code, which is, unfortunately, common among airbyte connectors.
func onStdoutDecodeError(logger ops.Logger) func([]byte, error) error {
	return func(naughtyLine []byte, decodeError error) error {
		var obj json.RawMessage
		if err := json.Unmarshal(naughtyLine, &obj); err == nil {
			// This was a naughty JSON object
			return decodeError
		} else {
			// We can't parse this as an object, so we'll just log it as plain text
			logger.Log(logrus.InfoLevel, logrus.Fields{
				// The `logSource` will already be set to the image name, so we use "sourceDesc"
				// here so that the log will include both fields.
				"sourceDesc": "ignored non-json output from connector stdout",
			}, strings.TrimSpace(string(naughtyLine))) // naughtyLine ends with a newline, so trim
			return nil
		}
	}
}

// LogrusLevel returns an appropriate logrus.Level for the connector LogLevel.
func airbyteToLogrusLevel(l airbyte.LogLevel) logrus.Level {
	switch l {
	case airbyte.LogLevelTrace:
		return logrus.TraceLevel
	case airbyte.LogLevelDebug:
		return logrus.DebugLevel
	case airbyte.LogLevelInfo:
		return logrus.InfoLevel
	case airbyte.LogLevelWarn:
		return logrus.WarnLevel
	default: // Includes LogLevelError, LogLevelFatal.
		return logrus.ErrorLevel
	}
}
