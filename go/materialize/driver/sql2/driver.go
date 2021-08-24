package sql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/alecthomas/jsonschema"
	"github.com/estuary/flow/go/materialize/lifecycle"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// Resource is a driver-provided type which represents the SQL resource
// (for example, a table) bound to by a binding.
type Resource interface {
	// Validate returns an error if the Resource is malformed.
	Validate() error
	// Path returns the fully qualified name of the resource, as '.'-separated components.
	Path() ResourcePath
	// DeltaUpdates is true if the resource should be materialized using delta updates.
	DeltaUpdates() bool
}

// ResourcePath is '.'-separated path components of a fully qualified database resource.
type ResourcePath []string

// Join the ResourcePath into a '.'-separated string.
func (p ResourcePath) Join() string {
	return strings.Join(p, ".")
}

// Driver implements the pm.DriverServer interface.
type Driver struct {
	// URL at which documentation for the driver may be found.
	DocumentationURL string
	// Instance of the type into which endpoint specifications are parsed.
	EndpointSpecType interface{}
	// Instance of the type into which resource specifications are parsed.
	ResourceSpecType Resource
	// NewEndpoint returns an Endpoint, which will be used to handle interactions with the database.
	NewEndpoint func(context.Context, json.RawMessage) (*Endpoint, error)
	// NewResource returns an uninitialized Resource which may be parsed into.
	NewResource func(ep *Endpoint) Resource
	// NewTransactor returns a Transactor ready for lifecycle.RunTransactions.
	NewTransactor func(*Endpoint, *pf.MaterializationSpec, *Fence, []Resource) (lifecycle.Transactor, error)
}

var _ pm.DriverServer = &Driver{}

// Spec implements the DriverServer interface.
func (d *Driver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	// Use reflection to build JSON Schemas from endpoint and resource configuration types.
	var reflector = jsonschema.Reflector{
		ExpandedStruct: true,
	}
	endpointSchema, err := reflector.Reflect(d.EndpointSpecType).MarshalJSON()
	if err != nil {
		return nil, err
	}
	resourceSchema, err := reflector.Reflect(d.ResourceSpecType).MarshalJSON()
	if err != nil {
		return nil, err
	}

	return &pm.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       d.DocumentationURL,
	}, nil
}

// Validate implements the DriverServer interface.
func (d *Driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	var endpoint, err = d.NewEndpoint(ctx, req.EndpointSpecJson)
	if err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	}
	// Load existing bindings indexed under their target table.
	_, existing, err := indexBindings(d, endpoint, req.Materialization)
	if err != nil {
		return nil, err
	}

	// Produce constraints for each specification binding, in turn.
	var resp = new(pm.ValidateResponse)
	for _, spec := range req.Bindings {
		var resource, err = parseResource(
			d.NewResource(endpoint), spec.ResourceSpecJson, &spec.Collection)
		if err != nil {
			return nil, err
		}

		var target = resource.Path().Join()
		current, constraints, err := loadConstraints(
			target,
			&spec.Collection,
			existing,
		)
		if err != nil {
			return nil, err
		}

		if current != nil && current.DeltaUpdates != resource.DeltaUpdates() {
			return nil, fmt.Errorf(
				"cannot alter delta-updates mode of existing target %s", target)
		}

		resp.Bindings = append(resp.Bindings,
			&pm.ValidateResponse_Binding{
				Constraints:  constraints,
				DeltaUpdates: resource.DeltaUpdates(),
				ResourcePath: resource.Path(),
			})
	}
	return resp, nil
}

// Apply implements the DriverServer interface.
func (d *Driver) Apply(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	var endpoint, err = d.NewEndpoint(ctx, req.Materialization.EndpointSpecJson)
	if err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	}
	// Load existing bindings indexed under their target table.
	loaded, existing, err := indexBindings(d, endpoint, req.Materialization.Materialization)
	if err != nil {
		return nil, err
	}

	// Create the materializations & checkpoints tables, if they don't exist.
	createCheckpointsSQL, err := endpoint.Generator.CreateTable(endpoint.Tables.Checkpoints)
	if err != nil {
		return nil, fmt.Errorf("generating checkpoints schema: %w", err)
	}
	createSpecsSQL, err := endpoint.Generator.CreateTable(endpoint.Tables.Specs)
	if err != nil {
		return nil, fmt.Errorf("generating specs schema: %w", err)
	}

	// Insert or update the materialization specification.
	var upsertSpecSQL string
	if loaded == nil {
		upsertSpecSQL = "INSERT INTO %s (version, spec, materialization) VALUES (%s, %s, %s);"
	} else {
		upsertSpecSQL = "UPDATE %s SET version = %s, spec = %s WHERE materialization = %s;"
	}
	specBytes, err := req.Materialization.Marshal()
	if err != nil {
		panic(err) // Cannot fail.
	}
	upsertSpecSQL = fmt.Sprintf(upsertSpecSQL,
		endpoint.Tables.Specs.Identifier,
		// Note that each version of upsertSpecSQL takes parameters in the same order.
		endpoint.Generator.QuoteStringValue(req.Version),
		endpoint.Generator.QuoteStringValue(base64.StdEncoding.EncodeToString(specBytes)),
		endpoint.Generator.QuoteStringValue(req.Materialization.Materialization.String()),
	)

	var statements = []string{
		createCheckpointsSQL,
		createSpecsSQL,
		upsertSpecSQL,
	}

	// Validate and build SQL statements to apply each binding.
	for _, spec := range req.Materialization.Bindings {
		if built, err := generateApplyStatements(endpoint, existing, spec); err != nil {
			return nil, err
		} else {
			statements = append(statements, built...)
		}
	}

	// Apply the statements if not in DryRun.
	if !req.DryRun {
		if err = endpoint.ApplyStatements(statements); err != nil {
			return nil, fmt.Errorf("applying schema updates: %w", err)
		}
	}

	// Build and return a description of what happened (or would have happened).
	return &pm.ApplyResponse{
		ActionDescription: fmt.Sprintf(
			"BEGIN;\n%s\nCOMMIT;\n",
			strings.Join(statements, "\n\n"),
		),
	}, nil
}

// Transactions implements the DriverServer interface.
func (d *Driver) Transactions(stream pm.Driver_TransactionsServer) error {
	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("read Open: %w", err)
	} else if open.Open == nil {
		return fmt.Errorf("expected Open, got %#v", open)
	}

	endpoint, err := d.NewEndpoint(
		stream.Context(),
		open.Open.Materialization.EndpointSpecJson,
	)
	if err != nil {
		return fmt.Errorf("building endpoint: %w", err)
	}

	// Verify the opened materialization has been applied to the database,
	// and that the versions match.
	if version, spec, err := endpoint.LoadSpec(open.Open.Materialization.Materialization); err != nil {
		return fmt.Errorf("loading materialization spec: %w", err)
	} else if spec == nil {
		return fmt.Errorf("materialization has not been applied")
	} else if version != open.Open.Version {
		return fmt.Errorf(
			"applied and current materializations are different versions (applied: %s vs current: %s)",
			version, open.Open.Version)
	}

	fence, err := endpoint.NewFence(
		open.Open.Materialization.Materialization,
		open.Open.KeyBegin,
		open.Open.KeyEnd,
	)
	if err != nil {
		return fmt.Errorf("installing fence: %w", err)
	}

	// Parse resource specifications.
	var resources []Resource
	for _, spec := range open.Open.Materialization.Bindings {
		if resource, err := parseResource(
			d.NewResource(endpoint),
			spec.ResourceSpecJson,
			&spec.Collection,
		); err != nil {
			return err
		} else {
			resources = append(resources, resource)
		}
	}

	transactor, err := d.NewTransactor(
		endpoint, open.Open.Materialization, fence, resources)
	if err != nil {
		return err
	}

	if err = stream.Send(&pm.TransactionResponse{
		Opened: &pm.TransactionResponse_Opened{FlowCheckpoint: fence.Checkpoint}}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	return lifecycle.RunTransactions(stream, transactor, fence.LogEntry())
}

// loadConstraints retrieves an existing binding spec under the given
// target, if any, and then builds & returns constraints for the current
// collection given the (possible) existing binding.
func loadConstraints(
	target string,
	collection *pf.CollectionSpec,
	existing map[string]*pf.MaterializationSpec_Binding,
) (
	*pf.MaterializationSpec_Binding,
	map[string]*pm.Constraint,
	error,
) {
	var current, ok = existing[target]
	if ok && current == nil { // Already visited.
		return nil, nil, fmt.Errorf("duplicate binding for %s", target)
	}
	existing[target] = nil // Mark as visited.

	var constraints map[string]*pm.Constraint
	if current == nil {
		constraints = ValidateNewSQLProjections(collection)
	} else {
		constraints = ValidateMatchesExisting(current, collection)
	}

	return current, constraints, nil
}

// Index the binding specifications of the persisted materialization |name|,
// keyed on the Resource.TargetName() of each binding.
// If |name| isn't persisted, an empty map is returned.
func indexBindings(d *Driver, ep *Endpoint, name pf.Materialization) (
	*pf.MaterializationSpec,
	map[string]*pf.MaterializationSpec_Binding,
	error,
) {
	var index = make(map[string]*pf.MaterializationSpec_Binding)

	var _, loaded, err = ep.LoadSpec(name)
	if err != nil {
		return nil, nil, fmt.Errorf("loading previously-stored spec: %w", err)
	} else if loaded == nil {
		return loaded, index, nil
	}

	for _, spec := range loaded.Bindings {
		var r, err = parseResource(d.NewResource(ep), spec.ResourceSpecJson, &spec.Collection)
		if err != nil {
			return nil, nil, err
		}
		var target = r.Path().Join()

		if _, ok := index[target]; ok {
			return nil, nil, fmt.Errorf("duplicate binding for %s", target)
		}
		index[target] = spec
	}

	return loaded, index, nil
}

func parseResource(r Resource, config json.RawMessage, c *pf.CollectionSpec) (Resource, error) {
	if err := pf.UnmarshalStrict(config, r); err != nil {
		return nil, fmt.Errorf("parsing resource configuration for binding %s: %w", c.Collection, err)
	}
	return r, nil
}
