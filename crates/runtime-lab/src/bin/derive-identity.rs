//! The lab's reference derivation connector: an identity transform.
//!
//! Every read source document is published unchanged, and the connector holds
//! no state, so Flush is immediate and the runtime owns the checkpoint. It's
//! the baseline which experiments fork. Natural points to change:
//!
//! - `Kind::Read`: publish a fraction, a transformation, or a fan-out of each
//!   document, or none at all.
//! - `Kind::Flush`: return connector state, or `more: true` to exercise the
//!   multi-round Flush scatter / gather between shards.
//!
//! Transforms are reported read-only by default, as a stateless transform (like
//! a SQLite `SELECT`) is, which lets the shuffle distribute its reads across
//! r-clock splits. Endpoint config `{"readOnly": false}` reports them as
//! stateful instead, routing reads by key alone.
//!
//! It's served by `runtime_lab::connector::serve`, which a fork keeps: it
//! speaks the protobuf codec only (its `local:` endpoint must set
//! `protobuf: true`), and joins the host's connectors cgroup.

use anyhow::Context;
use proto_flow::derive::{Request, Response, request, response};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct EndpointConfig {
    #[serde(default = "read_only_default")]
    read_only: bool,
}

fn read_only_default() -> bool {
    true
}

fn main() -> std::process::ExitCode {
    runtime_lab::connector::serve("derive-identity", handle)
}

fn handle(request: Request) -> anyhow::Result<Option<Response>> {
    use request::Kind;

    let kind = match request.kind.context("request sets no sub-message")? {
        Kind::Spec(_) => response::Kind::Spec(Box::new(spec())),
        Kind::Validate(validate) => {
            let config: EndpointConfig = serde_json::from_slice(&validate.config_json)
                .context("parsing endpoint configuration")?;
            response::Kind::Validated(response::Validated {
                transforms: validate
                    .transforms
                    .iter()
                    .map(|_| response::validated::Transform {
                        read_only: config.read_only,
                    })
                    .collect(),
                generated_files: Default::default(),
            })
        }
        Kind::Open(_) => response::Kind::Opened(response::Opened::default()),
        Kind::Read(read) => response::Kind::Published(response::Published {
            doc_json: read.doc_json,
        }),
        Kind::Flush(_) => response::Kind::Flushed(response::Flushed::default()),
        Kind::StartCommit(_) => response::Kind::StartedCommit(response::StartedCommit::default()),
        Kind::Reset(_) => return Ok(None),
    };
    Ok(Some(Response {
        kind: Some(kind),
        ..Default::default()
    }))
}

fn spec() -> response::Spec {
    let config_schema = serde_json::json!({
        "type": "object",
        "title": "Lab identity derivation",
        "properties": {
            "readOnly": {"type": "boolean", "default": true},
        },
    });
    let lambda_schema = serde_json::json!({"type": "null"});

    response::Spec {
        protocol: 3032023,
        config_schema_json: config_schema.to_string().into(),
        resource_config_schema_json: lambda_schema.to_string().into(),
        documentation_url: "https://github.com/estuary/flow/tree/master/crates/runtime-lab"
            .to_string(),
        ..Default::default()
    }
}
