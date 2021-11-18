package ingest

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/alecthomas/jsonschema"
	"github.com/estuary/flow/go/flow/ops"
	pc "github.com/estuary/protocols/capture"
	pf "github.com/estuary/protocols/flow"
)

// EndpointSpec is the configuration for Ingestions.
// It must match the one defined for the source specs (flow.yaml) in Rust.
type EndpointSpec struct{}

// Validate the configuration.
func (c EndpointSpec) Validate() error {
	return nil
}

// ResourceSpec is the configuration for bound ingestion resources.
type ResourceSpec struct {
	// TODO(johnny): I'm not at all sure that "name" is what we want,
	// but we require *something* to produce distinct resource paths,
	// and ingest captures *should* have some means of naming bindings
	// that's decoupled from the bound collection name.
	Name string `json:"name"`
}

// Validate the configuration.
func (c ResourceSpec) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("missing name")
	}
	return nil
}

// driver implements the pc.DriverServer interface.
// Though driver is a gRPC service stub, it's called in synchronous and
// in-process contexts to minimize ser/de & memory copies. As such it
// doesn't get to assume deep ownership of its requests, and must
// proto.Clone() shared state before mutating it.
type driver struct {
	logger ops.Logger
}

func NewDriver(logger ops.Logger) pc.DriverServer {
	return driver{
		logger: logger,
	}
}

// Spec returns the specification of the ingest driver.
func (d driver) Spec(ctx context.Context, req *pc.SpecRequest) (*pc.SpecResponse, error) {
	var source = new(EndpointSpec)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}

	var reflector = jsonschema.Reflector{ExpandedStruct: true}

	endpointSchema, err := reflector.Reflect(new(EndpointSpec)).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}
	resourceSchema, err := reflector.Reflect(new(ResourceSpec)).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       "https://docs.estuary.dev",
	}, nil
}

// Discover is a no-op.
func (d driver) Discover(ctx context.Context, req *pc.DiscoverRequest) (*pc.DiscoverResponse, error) {
	var source = new(EndpointSpec)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}

	var resp = new(pc.DiscoverResponse)
	return resp, nil
}

// Validate is a no-op.
func (d driver) Validate(ctx context.Context, req *pc.ValidateRequest) (*pc.ValidateResponse, error) {
	var source = new(EndpointSpec)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}

	// Parse stream bindings and send back their resource paths.
	var resp = new(pc.ValidateResponse)
	for _, binding := range req.Bindings {
		var resource = new(ResourceSpec)
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, resource); err != nil {
			return nil, fmt.Errorf("parsing resource configuration: %w", err)
		}
		resp.Bindings = append(resp.Bindings, &pc.ValidateResponse_Binding{
			ResourcePath: []string{resource.Name},
		})
	}
	return resp, nil
}

// ApplyUpsert is a no-op.
func (d driver) ApplyUpsert(context.Context, *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return new(pc.ApplyResponse), nil
}

// ApplyDelete is a no-op.
func (d driver) ApplyDelete(context.Context, *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return new(pc.ApplyResponse), nil
}

// Pull is not implemented.
func (d driver) Pull(stream pc.Driver_PullServer) error {
	return fmt.Errorf("Ingest driver doesn't support Pull")
}
