use proto_flow::{capture, connector, derive, materialize};

/// Sentinel `estuary.dev/task-name` of a unary Spec request, which has no task.
pub const SPEC_TASK_NAME: &str = "<spec>";

/// The `ops::TaskType` a stream's first request implies.
pub fn task_type(request: &connector::request::Kind) -> ops::TaskType {
    match request {
        connector::request::Kind::Capture(_) => ops::TaskType::Capture,
        connector::request::Kind::Derive(_) => ops::TaskType::Derivation,
        connector::request::Kind::Materialize(_) => ops::TaskType::Materialization,
    }
}

/// Task type and catalog name a stream's first request is authorized against.
pub fn task_identity(request: &connector::request::Kind) -> anyhow::Result<(ops::TaskType, &str)> {
    let task_name = match request {
        connector::request::Kind::Capture(request) => capture_task_name(request)?,
        connector::request::Kind::Derive(request) => derive_task_name(request)?,
        connector::request::Kind::Materialize(request) => materialize_task_name(request)?,
    };
    Ok((task_type(request), task_name))
}

/// LabelSet a Connector RPC is authorized against.
pub fn task_label_set(
    task_type: ops::TaskType,
    task_name: &str,
) -> proto_gazette::broker::LabelSet {
    labels::build_set([
        (labels::TASK_NAME, task_name),
        (labels::TASK_TYPE, task_type.as_str_name()),
    ])
}

fn invalid(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(tonic::Status::invalid_argument(message.into()))
}

fn capture_task_name(request: &capture::Request) -> anyhow::Result<&str> {
    Ok(match &request.kind {
        Some(capture::request::Kind::Spec(_)) => SPEC_TASK_NAME,
        Some(capture::request::Kind::Discover(request)) => &request.name,
        Some(capture::request::Kind::Validate(request)) => &request.name,
        Some(capture::request::Kind::Apply(request)) => {
            &request
                .capture
                .as_ref()
                .ok_or_else(|| invalid("`apply` missing required `capture`"))?
                .name
        }
        Some(capture::request::Kind::Open(request)) => {
            &request
                .capture
                .as_ref()
                .ok_or_else(|| invalid("`open` missing required `capture`"))?
                .name
        }
        _ => {
            return Err(invalid(format!(
                "Capture protocol error (expected a valid first request) from client: {}",
                serde_json::to_string(request).unwrap_or_default()
            )));
        }
    })
}

fn derive_task_name(request: &derive::Request) -> anyhow::Result<&str> {
    Ok(match &request.kind {
        Some(derive::request::Kind::Spec(_)) => SPEC_TASK_NAME,
        Some(derive::request::Kind::Validate(request)) => {
            &request
                .collection
                .as_ref()
                .ok_or_else(|| invalid("`validate` missing required `collection`"))?
                .name
        }
        Some(derive::request::Kind::Open(request)) => {
            &request
                .collection
                .as_ref()
                .ok_or_else(|| invalid("`open` missing required `collection`"))?
                .name
        }
        _ => {
            return Err(invalid(format!(
                "Derive protocol error (expected a valid first request) from client: {}",
                serde_json::to_string(request).unwrap_or_default()
            )));
        }
    })
}

fn materialize_task_name(request: &materialize::Request) -> anyhow::Result<&str> {
    Ok(match &request.kind {
        Some(materialize::request::Kind::Spec(_)) => SPEC_TASK_NAME,
        Some(materialize::request::Kind::Validate(request)) => &request.name,
        Some(materialize::request::Kind::Apply(request)) => {
            &request
                .materialization
                .as_ref()
                .ok_or_else(|| invalid("`apply` missing required `materialization`"))?
                .name
        }
        Some(materialize::request::Kind::Open(request)) => {
            &request
                .materialization
                .as_ref()
                .ok_or_else(|| invalid("`open` missing required `materialization`"))?
                .name
        }
        _ => {
            return Err(invalid(format!(
                "Materialize protocol error (expected a valid first request) from client: {}",
                serde_json::to_string(request).unwrap_or_default()
            )));
        }
    })
}
