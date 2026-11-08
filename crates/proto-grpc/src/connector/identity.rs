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
    let task_name = match task_name {
        None => SPEC_TASK_NAME,
        // Reject a non-Spec request that carries the SPEC_TASK_NAME sentinel.
        Some(SPEC_TASK_NAME) => {
            return Err(invalid(format!(
                "`{SPEC_TASK_NAME}` is reserved for Spec requests and cannot name a task"
            )));
        }
        Some(task_name) => task_name,
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
    anyhow::Error::new(crate::StatusError(tonic::Status::invalid_argument(
        message.into(),
    )))
}

/// `None` is a task-less Spec, which alone may carry [`SPEC_TASK_NAME`].
fn capture_task_name(request: &capture::Request) -> anyhow::Result<Option<&str>> {
    Ok(match &request.kind {
        Some(capture::request::Kind::Spec(_)) => None,
        Some(capture::request::Kind::Discover(request)) => Some(&request.name),
        Some(capture::request::Kind::Validate(request)) => Some(&request.name),
        Some(capture::request::Kind::Apply(request)) => Some(
            &request
                .capture
                .as_ref()
                .ok_or_else(|| invalid("`apply` missing required `capture`"))?
                .name,
        ),
        Some(capture::request::Kind::Open(request)) => Some(
            &request
                .capture
                .as_ref()
                .ok_or_else(|| invalid("`open` missing required `capture`"))?
                .name,
        ),
        _ => {
            return Err(invalid(format!(
                "Capture protocol error (expected a valid first request) from client: {}",
                serde_json::to_string(request).unwrap_or_default()
            )));
        }
    })
}

fn derive_task_name(request: &derive::Request) -> anyhow::Result<Option<&str>> {
    Ok(match &request.kind {
        Some(derive::request::Kind::Spec(_)) => None,
        Some(derive::request::Kind::Validate(request)) => Some(
            &request
                .collection
                .as_ref()
                .ok_or_else(|| invalid("`validate` missing required `collection`"))?
                .name,
        ),
        Some(derive::request::Kind::Open(request)) => Some(
            &request
                .collection
                .as_ref()
                .ok_or_else(|| invalid("`open` missing required `collection`"))?
                .name,
        ),
        _ => {
            return Err(invalid(format!(
                "Derive protocol error (expected a valid first request) from client: {}",
                serde_json::to_string(request).unwrap_or_default()
            )));
        }
    })
}

fn materialize_task_name(request: &materialize::Request) -> anyhow::Result<Option<&str>> {
    Ok(match &request.kind {
        Some(materialize::request::Kind::Spec(_)) => None,
        Some(materialize::request::Kind::Validate(request)) => Some(&request.name),
        Some(materialize::request::Kind::Apply(request)) => Some(
            &request
                .materialization
                .as_ref()
                .ok_or_else(|| invalid("`apply` missing required `materialization`"))?
                .name,
        ),
        Some(materialize::request::Kind::Open(request)) => Some(
            &request
                .materialization
                .as_ref()
                .ok_or_else(|| invalid("`open` missing required `materialization`"))?
                .name,
        ),
        _ => {
            return Err(invalid(format!(
                "Materialize protocol error (expected a valid first request) from client: {}",
                serde_json::to_string(request).unwrap_or_default()
            )));
        }
    })
}

#[cfg(test)]
mod test {
    use serde_json::json;

    #[test]
    fn task_identity_of_every_request_shape() {
        let outcomes: Vec<String> = [
            json!({"capture": {"spec": {}}}),
            json!({"capture": {"discover": {"name": "acmeCo/capture"}}}),
            json!({"capture": {"validate": {"name": "acmeCo/capture"}}}),
            json!({"capture": {"apply": {"capture": {"name": "acmeCo/capture"}}}}),
            json!({"capture": {"open": {"capture": {"name": "acmeCo/capture"}}}}),
            json!({"derive": {"spec": {}}}),
            json!({"derive": {"validate": {"collection": {"name": "acmeCo/derivation"}}}}),
            json!({"derive": {"open": {"collection": {"name": "acmeCo/derivation"}}}}),
            json!({"materialize": {"spec": {}}}),
            json!({"materialize": {"validate": {"name": "acmeCo/materialization"}}}),
            json!({"materialize": {"apply": {"materialization": {"name": "acmeCo/materialization"}}}}),
            json!({"materialize": {"open": {"materialization": {"name": "acmeCo/materialization"}}}}),
            // No named operation at all.
            json!({"capture": {}}),
            // A named operation missing the spec which names its task.
            json!({"materialize": {"apply": {}}}),
            // The reserved sentinel, named by a request which is not a Spec.
            json!({"derive": {"open": {"collection": {"name": super::SPEC_TASK_NAME}}}}),
        ]
        .into_iter()
        .map(|request| {
            let request: proto_flow::connector::Request = serde_json::from_value(request).unwrap();

            match super::task_identity(request.kind.as_ref().expect("a request kind is set")) {
                Ok((task_type, task_name)) => format!("{} {task_name}", task_type.as_str_name()),
                Err(err) => {
                    let status = err.downcast_ref::<crate::StatusError>().unwrap();
                    format!("{:?}: {}", status.code(), status.message())
                }
            }
        })
        .collect();

        insta::assert_debug_snapshot!(outcomes);
    }
}
