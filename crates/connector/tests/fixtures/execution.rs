use proto_flow::{capture, connector as proto, derive, flow, materialize};

pub(crate) const PYTHON_IMAGE: &str = "ghcr.io/estuary/derive-python:stable";

fn shard_template() -> serde_json::Value {
    serde_json::json!({"labels": {"labels": [{"name": labels::BUILD, "value": "1122334455667788"}]}})
}

pub(crate) fn vmm_execution() -> Option<flow::ConnectorExecution> {
    Some(flow::ConnectorExecution {
        vmm: true,
        egress: None,
    })
}

pub(crate) fn egress_execution(vmm: bool, hosts: &[&str]) -> Option<flow::ConnectorExecution> {
    Some(flow::ConnectorExecution {
        vmm,
        egress: Some(flow::connector_execution::Egress {
            hosts: hosts.iter().map(ToString::to_string).collect(),
        }),
    })
}

/// Every first-request shape of every protocol, as a connector whose endpoint
/// is `connector_type` and `config_json`. Requests which embed the task's
/// built spec (Apply and Open) carry `spec_execution` within it.
pub(crate) fn every_first_request(
    connector_type: fn(ops::TaskType) -> i32,
    config_json: &bytes::Bytes,
    spec_execution: Option<flow::ConnectorExecution>,
) -> Vec<(
    &'static str,
    ops::TaskType,
    &'static str,
    proto::request::Kind,
)> {
    use ops::TaskType::{Capture, Derivation, Materialization};
    let (task, spec_task) = ("acmeCo/task", proto_grpc::connector::SPEC_TASK_NAME);
    let config_json = config_json.clone();

    let capture_spec = flow::CaptureSpec {
        name: task.to_string(),
        connector_type: connector_type(Capture),
        config_json: config_json.clone(),
        execution: spec_execution.clone(),
        shard_template: serde_json::from_value(shard_template()).unwrap(),
        ..Default::default()
    };
    let collection_spec = flow::CollectionSpec {
        name: task.to_string(),
        derivation: Some(Box::new(flow::collection_spec::Derivation {
            connector_type: connector_type(Derivation),
            config_json: config_json.clone(),
            execution: spec_execution.clone(),
            shard_template: serde_json::from_value(shard_template()).unwrap(),
            ..Default::default()
        })),
        ..Default::default()
    };
    let materialization_spec = flow::MaterializationSpec {
        name: task.to_string(),
        connector_type: connector_type(Materialization),
        config_json: config_json.clone(),
        execution: spec_execution.clone(),
        shard_template: serde_json::from_value(shard_template()).unwrap(),
        ..Default::default()
    };

    let capture = |kind| {
        proto::request::Kind::Capture(capture::Request {
            kind: Some(kind),
            ..Default::default()
        })
    };
    let derive = |kind| {
        proto::request::Kind::Derive(derive::Request {
            kind: Some(kind),
            ..Default::default()
        })
    };
    let materialize = |kind| {
        proto::request::Kind::Materialize(materialize::Request {
            kind: Some(kind),
            ..Default::default()
        })
    };

    vec![
        (
            "capture Spec",
            Capture,
            spec_task,
            capture(capture::request::Kind::Spec(capture::request::Spec {
                connector_type: connector_type(Capture),
                config_json: config_json.clone(),
            })),
        ),
        (
            "capture Discover",
            Capture,
            task,
            capture(capture::request::Kind::Discover(Box::new(
                capture::request::Discover {
                    name: task.to_string(),
                    connector_type: connector_type(Capture),
                    config_json: config_json.clone(),
                    ..Default::default()
                },
            ))),
        ),
        (
            "capture Validate",
            Capture,
            task,
            capture(capture::request::Kind::Validate(Box::new(
                capture::request::Validate {
                    name: task.to_string(),
                    connector_type: connector_type(Capture),
                    config_json: config_json.clone(),
                    ..Default::default()
                },
            ))),
        ),
        (
            "capture Apply",
            Capture,
            task,
            capture(capture::request::Kind::Apply(Box::new(
                capture::request::Apply {
                    capture: Some(capture_spec.clone()),
                    ..Default::default()
                },
            ))),
        ),
        (
            "capture Open",
            Capture,
            task,
            capture(capture::request::Kind::Open(Box::new(
                capture::request::Open {
                    capture: Some(capture_spec),
                    ..Default::default()
                },
            ))),
        ),
        (
            "derive Spec",
            Derivation,
            spec_task,
            derive(derive::request::Kind::Spec(derive::request::Spec {
                connector_type: connector_type(Derivation),
                config_json: config_json.clone(),
            })),
        ),
        (
            "derive Validate",
            Derivation,
            task,
            derive(derive::request::Kind::Validate(Box::new(
                derive::request::Validate {
                    connector_type: connector_type(Derivation),
                    config_json: config_json.clone(),
                    collection: Some(flow::CollectionSpec {
                        name: task.to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ))),
        ),
        (
            "derive Open",
            Derivation,
            task,
            derive(derive::request::Kind::Open(Box::new(
                derive::request::Open {
                    collection: Some(collection_spec),
                    ..Default::default()
                },
            ))),
        ),
        (
            "materialize Spec",
            Materialization,
            spec_task,
            materialize(materialize::request::Kind::Spec(
                materialize::request::Spec {
                    connector_type: connector_type(Materialization),
                    config_json: config_json.clone(),
                },
            )),
        ),
        (
            "materialize Validate",
            Materialization,
            task,
            materialize(materialize::request::Kind::Validate(Box::new(
                materialize::request::Validate {
                    name: task.to_string(),
                    connector_type: connector_type(Materialization),
                    config_json: config_json.clone(),
                    ..Default::default()
                },
            ))),
        ),
        (
            "materialize Apply",
            Materialization,
            task,
            materialize(materialize::request::Kind::Apply(Box::new(
                materialize::request::Apply {
                    materialization: Some(materialization_spec.clone()),
                    ..Default::default()
                },
            ))),
        ),
        (
            "materialize Open",
            Materialization,
            task,
            materialize(materialize::request::Kind::Open(Box::new(
                materialize::request::Open {
                    materialization: Some(materialization_spec),
                    ..Default::default()
                },
            ))),
        ),
    ]
}

pub(crate) fn image_connector_type(task_type: ops::TaskType) -> i32 {
    match task_type {
        ops::TaskType::Capture => flow::capture_spec::ConnectorType::Image as i32,
        ops::TaskType::Derivation => flow::collection_spec::derivation::ConnectorType::Image as i32,
        ops::TaskType::Materialization => flow::materialization_spec::ConnectorType::Image as i32,
        _ => unreachable!(),
    }
}
