package sql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	schemagen "github.com/estuary/flow/go/protocols/materialize/go-schema-gen"
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
	NewEndpoint func(context.Context, json.RawMessage) (Endpoint, error)
	// NewResource returns an uninitialized Resource which may be parsed into.
	NewResource func(ep Endpoint) Resource
	// NewTransactor returns a Transactor ready for pm.RunTransactions.
	NewTransactor func(context.Context, Endpoint, *pf.MaterializationSpec, Fence, []Resource) (pm.Transactor, error)
}

var _ pm.ConnectorServer = &Driver{}

func (d *Driver) doSpec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	var endpointSchema, err = schemagen.GenerateSchema("SQL Connection", d.EndpointSpecType).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("SQL Table", d.ResourceSpecType).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         d.DocumentationURL,
	}, nil
}

// Validate implements the DriverServer interface.
func (d *Driver) doValidate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	var endpoint, err = d.NewEndpoint(ctx, req.ConfigJson)
	if err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	}
	// Load existing bindings indexed under their target table.
	_, _, existing, err := indexBindings(ctx, d, endpoint, req.Name)
	if err != nil {
		return nil, err
	}

	// Produce constraints for each specification binding, in turn.
	var resp = new(pm.Response_Validated)
	for _, spec := range req.Bindings {
		var resource, err = parseResource(
			d.NewResource(endpoint), spec.ResourceConfigJson, &spec.Collection)
		if err != nil {
			return nil, err
		}

		var target = resource.Path().Join()
		current, constraints, err := loadConstraints(
			target,
			resource.DeltaUpdates(),
			&spec.Collection,
			existing,
		)
		if err != nil {
			return nil, err
		}

		// There's no particular reason why we _need_ to constrain this, but it seems smart to only
		// relax it if we need to. We previously disallowed all changes to the delta_updates
		// configuration, and relaxed it because we wanted to enable delta_updates for an existing
		// binding, and couldn't think of why it would hurt anything. But disabling delta_updates
		// for an existing binding might not be as simple, since Load implementations may not be
		// prepared to deal with the potential for duplicate primary keys. So I'm leaving this
		// validation in place for now, since there's no need to relax it further at the moment.
		if current != nil && current.DeltaUpdates && !resource.DeltaUpdates() {
			return nil, fmt.Errorf(
				"cannot disable delta-updates mode of existing target %s", target)
		}

		resp.Bindings = append(resp.Bindings,
			&pm.Response_Validated_Binding{
				Constraints:  constraints,
				DeltaUpdates: resource.DeltaUpdates(),
				ResourcePath: resource.Path(),
			})
	}
	return resp, nil
}

func (d *Driver) doApply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}
	var endpoint, err = d.NewEndpoint(ctx, req.Materialization.ConfigJson)
	if err != nil {
		return nil, fmt.Errorf("building endpoint: %w", err)
	}

	// Load existing bindings indexed under their target table.
	loaded, version, existing, err := indexBindings(ctx, d, endpoint, req.Materialization.Name)
	if err != nil {
		return nil, err
	}
	// A reapplication of the current version is a no-op.
	if version == req.Version {
		return new(pm.Response_Applied), nil
	}

	// Create the materializations & checkpoints tables, if they don't exist.
	createCheckpointsSQL, err := endpoint.CreateTableStatement(endpoint.FlowTables().Checkpoints)
	if err != nil {
		return nil, fmt.Errorf("generating checkpoints schema: %w", err)
	}
	createSpecsSQL, err := endpoint.CreateTableStatement(endpoint.FlowTables().Specs)
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

	var generator = endpoint.Generator()
	upsertSpecSQL = fmt.Sprintf(upsertSpecSQL,
		endpoint.FlowTables().Specs.Identifier,
		// Note that each version of upsertSpecSQL takes parameters in the same order.
		generator.ValueRenderer.Render(req.Version),
		generator.ValueRenderer.Render(base64.StdEncoding.EncodeToString(specBytes)),
		generator.ValueRenderer.Render(req.Materialization.Name.String()),
	)

	var statements = []string{
		createCheckpointsSQL,
		createSpecsSQL,
		upsertSpecSQL,
	}

	// Validate and build SQL statements to apply each binding.
	for _, spec := range req.Materialization.Bindings {
		if applyStatements, err := generateApplyStatements(endpoint, existing, spec); err != nil {
			return nil, fmt.Errorf("building statement for binding %s: %w", ResourcePath(spec.ResourcePath).Join(), err)
		} else {
			statements = append(statements, applyStatements...)
		}
	}

	// Execute the statements if not in DryRun.
	if !req.DryRun {
		if err = endpoint.ExecuteStatements(ctx, statements); err != nil {
			return nil, fmt.Errorf("applying schema updates: %w", err)
		}
	}

	// Build and return a description of what happened (or would have happened).
	return &pm.Response_Applied{
		ActionDescription: fmt.Sprintf(
			"BEGIN;\n%s\nCOMMIT;\n",
			strings.Join(statements, "\n\n"),
		),
	}, nil
}

func (d *Driver) newTransactor(ctx context.Context, open pm.Request_Open) (pm.Transactor, *pm.Response_Opened, error) {
	var endpoint, err = d.NewEndpoint(ctx, open.Materialization.ConfigJson)
	if err != nil {
		return nil, nil, fmt.Errorf("building endpoint: %w", err)
	}

	// Verify the opened materialization has been applied to the database,
	// and that the versions match.
	if version, spec, err := endpoint.LoadSpec(ctx, open.Materialization.Name); err != nil {
		return nil, nil, fmt.Errorf("loading materialization spec: %w", err)
	} else if spec == nil {
		return nil, nil, fmt.Errorf("materialization has not been applied")
	} else if version != open.Version {
		return nil, nil, fmt.Errorf(
			"applied and current materializations are different versions (applied: %s vs current: %s)",
			version, open.Version)
	}

	fence, err := endpoint.NewFence(
		ctx,
		open.Materialization.Name,
		open.Range.KeyBegin,
		open.Range.KeyEnd,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("installing fence: %w", err)
	}

	// Parse resource specifications.
	var resources []Resource
	for _, spec := range open.Materialization.Bindings {
		if resource, err := parseResource(
			d.NewResource(endpoint),
			spec.ResourceConfigJson,
			&spec.Collection,
		); err != nil {
			return nil, nil, err
		} else {
			resources = append(resources, resource)
		}
	}

	transactor, err := d.NewTransactor(ctx, endpoint, open.Materialization, fence, resources)
	if err != nil {
		return nil, nil, err
	}

	return transactor, &pm.Response_Opened{RuntimeCheckpoint: fence.Checkpoint()}, nil
}

// Transactions implements the DriverServer interface.
func (d *Driver) Materialize(stream pm.Connector_MaterializeServer) error {
	var ctx = stream.Context()

	for {
		var request, err = stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		} else if err := request.Validate_(); err != nil {
			return err
		}

		switch {
		case request.Spec != nil:
			if response, err := d.doSpec(ctx, request.Spec); err != nil {
				return err
			} else {
				stream.Send(&pm.Response{Spec: response})
			}
		case request.Validate != nil:
			if response, err := d.doValidate(ctx, request.Validate); err != nil {
				return err
			} else {
				stream.Send(&pm.Response{Validated: response})
			}
		case request.Apply != nil:
			if response, err := d.doApply(ctx, request.Apply); err != nil {
				return err
			} else {
				stream.Send(&pm.Response{Applied: response})
			}
		case request.Open != nil:
			var transactor, opened, err = d.newTransactor(ctx, *request.Open)
			if err != nil {
				return err
			}
			return pm.RunTransactions(stream, *request.Open, *opened, transactor)
		default:
			return fmt.Errorf("protocol error: request %#v was not expected", request)
		}
	}
}

// loadConstraints retrieves an existing binding spec under the given
// target, if any, and then builds & returns constraints for the current
// collection given the (possible) existing binding.
func loadConstraints(
	target string,
	deltaUpdates bool,
	collection *pf.CollectionSpec,
	existing map[string]*pf.MaterializationSpec_Binding,
) (
	*pf.MaterializationSpec_Binding,
	map[string]*pm.Response_Validated_Constraint,
	error,
) {
	var current, ok = existing[target]
	if ok && current == nil { // Already visited.
		return nil, nil, fmt.Errorf("duplicate binding for %s", target)
	}
	existing[target] = nil // Mark as visited.

	var constraints map[string]*pm.Response_Validated_Constraint
	if current == nil {
		constraints = ValidateNewSQLProjections(collection, deltaUpdates)
	} else {
		constraints = ValidateMatchesExisting(current, collection)
	}

	return current, constraints, nil
}

// Index the binding specifications of the persisted materialization |name|,
// keyed on the Resource.TargetName() of each binding.
// If |name| isn't persisted, an empty map is returned.
func indexBindings(ctx context.Context, d *Driver, ep Endpoint, name pf.Materialization) (
	_ *pf.MaterializationSpec,
	version string,
	_ map[string]*pf.MaterializationSpec_Binding,
	_ error,
) {
	var index = make(map[string]*pf.MaterializationSpec_Binding)

	version, loaded, err := ep.LoadSpec(ctx, name)
	if err != nil {
		return nil, "", nil, fmt.Errorf("loading previously-stored spec: %w", err)
	} else if loaded == nil {
		return nil, "", index, nil
	}

	for _, spec := range loaded.Bindings {
		var r, err = parseResource(d.NewResource(ep), spec.ResourceConfigJson, &spec.Collection)
		if err != nil {
			return nil, "", nil, err
		}
		var target = r.Path().Join()

		if _, ok := index[target]; ok {
			return nil, "", nil, fmt.Errorf("duplicate binding for %s", target)
		}
		index[target] = spec
	}

	return loaded, version, index, nil
}

func parseResource(r Resource, config json.RawMessage, c *pf.CollectionSpec) (Resource, error) {
	if err := pf.UnmarshalStrict(config, r); err != nil {
		return nil, fmt.Errorf("parsing resource configuration for binding %s: %w", c.Name, err)
	}
	return r, nil
}
