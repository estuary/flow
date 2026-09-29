//! The lab's reference materialization connector: a pure sink.
//!
//! It follows the materialize protocol faithfully and cheaply: every Load
//! misses (no `Loaded` is ever sent), Stores are discarded, and every commit
//! and acknowledgement is immediate. It's the baseline which experiments fork
//! to replicate a particular connector's effect. Natural points to change:
//!
//! - `Kind::Load`: respond `Loaded` documents, to exercise the load path. What
//!   a Loaded document must look like is binding-specific (it's a prior Store
//!   of the same key), which is why the baseline loads nothing.
//! - `Kind::StartCommit` / `Kind::Acknowledge`: delay, or do work, to model a
//!   connector whose commit or post-commit apply is slow.
//! - `Kind::Opened`: return `disable_load_optimization`, or a runtime
//!   checkpoint (making the connector remote-authoritative).
//!
//! It's served by `runtime_lab::connector::serve`, which a fork keeps: it
//! speaks the protobuf codec only (its `local:` endpoint must set
//! `protobuf: true`), and joins the host's connectors cgroup.

use anyhow::Context;
use proto_flow::materialize::{Request, Response, request, response};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EndpointConfig {}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceConfig {
    table: String,
}

fn main() -> std::process::ExitCode {
    runtime_lab::connector::serve("materialize-sink", handle)
}

fn handle(request: Request) -> anyhow::Result<Option<Response>> {
    use request::Kind;

    let kind = match request.kind.context("request sets no sub-message")? {
        Kind::Spec(_) => response::Kind::Spec(Box::new(spec())),
        Kind::Validate(validate) => response::Kind::Validated(validated(*validate)?),
        Kind::Apply(_) => response::Kind::Applied(response::Applied::default()),
        Kind::Open(open) => {
            let spec = open
                .materialization
                .context("Open is missing its materialization")?;
            let _: EndpointConfig = serde_json::from_slice(&spec.config_json)
                .context("parsing endpoint configuration")?;
            response::Kind::Opened(response::Opened::default())
        }
        Kind::Load(_) => return Ok(None), // Every key misses.
        Kind::Flush(_) => response::Kind::Flushed(response::Flushed::default()),
        Kind::Store(_) => return Ok(None),
        Kind::StartCommit(_) => response::Kind::StartedCommit(response::StartedCommit::default()),
        Kind::Acknowledge(_) => response::Kind::Acknowledged(response::Acknowledged::default()),
    };
    Ok(Some(Response {
        kind: Some(kind),
        ..Default::default()
    }))
}

fn spec() -> response::Spec {
    let config_schema = serde_json::json!({
        "type": "object",
        "title": "Lab materialization sink",
        "properties": {},
    });
    let resource_schema = serde_json::json!({
        "type": "object",
        "required": ["table"],
        "properties": {
            "table": {"type": "string", "x-collection-name": true},
        },
    });

    response::Spec {
        protocol: 3032023,
        config_schema_json: config_schema.to_string().into(),
        resource_config_schema_json: resource_schema.to_string().into(),
        documentation_url: "https://github.com/estuary/flow/tree/master/crates/runtime-lab"
            .to_string(),
        ..Default::default()
    }
}

fn validated(validate: request::Validate) -> anyhow::Result<response::Validated> {
    use response::validated::constraint::Type;
    use response::validated::{Binding, Constraint, ProjectionConstraint};

    let _: EndpointConfig =
        serde_json::from_slice(&validate.config_json).context("parsing endpoint configuration")?;

    let mut bindings = Vec::new();
    for binding in &validate.bindings {
        let resource: ResourceConfig = serde_json::from_slice(&binding.resource_config_json)
            .context("parsing resource configuration")?;
        let collection = binding
            .collection
            .as_ref()
            .context("binding is missing its collection")?;

        // Like a typical warehouse: the key and root document are required,
        // and everything else is optional. Field selection shapes fields of
        // every Store the runtime sends.
        let projection_constraints = collection
            .projections
            .iter()
            .map(|projection| {
                let r#type =
                    if projection.ptr.is_empty() || collection.key.contains(&projection.ptr) {
                        Type::LocationRequired
                    } else {
                        Type::FieldOptional
                    };
                ProjectionConstraint {
                    field: projection.field.clone(),
                    constraint: Some(Constraint {
                        r#type: r#type as i32,
                        reason: String::new(),
                        folded_field: String::new(),
                    }),
                }
            })
            .collect();

        bindings.push(Binding {
            resource_path: vec![resource.table],
            delta_updates: false,
            projection_constraints,
            ..Default::default()
        });
    }
    Ok(response::Validated { bindings })
}
