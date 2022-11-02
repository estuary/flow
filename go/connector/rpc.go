package connector

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/estuary/flow/go/flow/ops"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	protoio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/proto"
)

// EndpointSpec is the common configuration shape of image connectors.
// It must match the one defined for the source specs (flow.yaml) in Rust.
type EndpointSpec struct {
	Image  string          `json:"image"`
	Config json.RawMessage `json:"config"`
}

// Validate returns an error if EndpointSpec is invalid.
func (c EndpointSpec) Validate() error {
	if c.Image == "" {
		return fmt.Errorf("expected `image`")
	}
	return nil
}

// UnaryRPC invokes a connector with `command` and the given `request` and `response`.
// The connectors response is decoded into `response`.
func UnaryRPC(
	ctx context.Context,
	command string,
	protocol Protocol,
	request interface {
		proto.Message
		Validate() error
	},
	response interface {
		proto.Message
	},
	logger ops.Logger,
	network string,
) error {
	var source = new(EndpointSpec)

	if err := request.Validate(); err != nil {
		return fmt.Errorf("go.estuary.dev/E118: validating request: %w", err)
	} else if err = pf.UnmarshalStrict(*pluckEndpointSpec(request), source); err != nil {
		return fmt.Errorf("go.estuary.dev/E119: parsing connector configuration: %w", err)
	}

	var decrypted, err = DecryptConfig(ctx, source.Config)
	if err != nil {
		return err
	}
	defer ZeroBytes(decrypted)             // connector.Run will also ZeroBytes().
	var cloned = proto.Clone(request)      // Deep copy to not mutate original request.
	*pluckEndpointSpec(cloned) = decrypted // Pass along decrypted config.
	request = nil                          // Ensure we don't use original again.

	var first = true
	err = Run(ctx, source.Image, protocol, network,
		[]string{command},
		func(w io.Writer) error {
			defer ZeroBytes(decrypted)
			return protoio.NewUint32DelimitedWriter(w, binary.LittleEndian).WriteMsg(cloned)
		},
		NewProtoOutput(
			func() proto.Message { return response },
			func(m proto.Message) error {
				if !first {
					return fmt.Errorf("go.estuary.dev/E120: read more than one %s response", command)
				}
				first = false
				return nil
			},
		),
		logger,
	)

	if err == nil && first {
		err = fmt.Errorf("go.estuary.dev/E121: connector didn't produce a %s response", command)
	}
	return err
}

func StreamRPC(
	stream interface {
		Context() context.Context
		SendMsg(interface{}) error
		RecvMsg(interface{}) error
	},
	command string,
	protocol Protocol,
	newReqFn func() proto.Message,
	newRespFn func() proto.Message,
	logger ops.Logger,
	network string,
) error {
	var (
		ctx    = stream.Context()
		source = new(EndpointSpec)
		open   = newReqFn()
	)

	// Read `open` request.
	if err := stream.RecvMsg(open); err != nil {
		return fmt.Errorf("go.estuary.dev/E122: reading Open request: %w", err)
	} else if err := pf.UnmarshalStrict(*pluckEndpointSpec(open), source); err != nil {
		return fmt.Errorf("go.estuary.dev/E123: parsing connector configuration: %w", err)
	}

	var decrypted, err = DecryptConfig(ctx, source.Config)
	if err != nil {
		return err
	}
	defer ZeroBytes(decrypted)             // connector.Run will also ZeroBytes().
	var cloned = proto.Clone(open)         // Deep copy to not mutate original request.
	*pluckEndpointSpec(cloned) = decrypted // Pass along decrypted config.
	open = nil                             // Ensure we don't use original again.

	return Run(ctx, source.Image, protocol, network,
		[]string{command},
		func(w io.Writer) error { return protoWriteLoop(stream, cloned, newReqFn, w) },
		NewProtoOutput(
			func() proto.Message { return newRespFn() },
			func(m proto.Message) error { return stream.SendMsg(m) },
		),
		logger,
	)
}

// ProtoWriteLoop reads |stream| and proxies messages to the container Writer.
func protoWriteLoop(
	stream interface {
		RecvMsg(interface{}) error
	},
	open proto.Message,
	newFn func() proto.Message,
	w io.Writer,
) error {
	var enc = protoio.NewUint32DelimitedWriter(w, binary.LittleEndian)
	var err = enc.WriteMsg(open)
	ZeroBytes(*pluckEndpointSpec(open)) // No longer needed.

	if err != nil {
		return fmt.Errorf("go.estuary.dev/E124: writing initial request to connector: %w", err)
	}

	for {
		var req = newFn()
		if err = stream.RecvMsg(req); err == io.EOF {
			return nil // Clean shutdown.
		} else if err != nil {
			return fmt.Errorf("go.estuary.dev/E125: reading from runtime: %w", err)
		} else if err = enc.WriteMsg(req); err != nil {
			return fmt.Errorf("go.estuary.dev/E126: writing to connector: %w", err)
		}
	}
}

// pluckEndpointSpec returns a reference to the serialized EndpointSpec of the message.
func pluckEndpointSpec(m proto.Message) *json.RawMessage {
	switch mm := m.(type) {
	case *pm.SpecRequest:
		return &mm.EndpointSpecJson
	case *pm.ValidateRequest:
		return &mm.EndpointSpecJson
	case *pm.ApplyRequest:
		return &mm.Materialization.EndpointSpecJson
	case *pm.TransactionRequest:
		return &mm.Open.Materialization.EndpointSpecJson

	case *pc.SpecRequest:
		return &mm.EndpointSpecJson
	case *pc.DiscoverRequest:
		return &mm.EndpointSpecJson
	case *pc.ValidateRequest:
		return &mm.EndpointSpecJson
	case *pc.ApplyRequest:
		return &mm.Capture.EndpointSpecJson
	case *pc.PullRequest:
		return &mm.Open.Capture.EndpointSpecJson
	default:
		panic(fmt.Sprintf("go.estuary.dev/E126: unexpected message type %#T", m))
	}
}
