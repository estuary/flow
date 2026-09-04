use super::Connectors;
use futures::StreamExt;
use proto_flow::{capture, derive, materialize};

/// NoOpConnectors are permissive placeholders for interactions with connectors,
/// that never fail and return the right shape of response.
#[derive(Clone, Debug)]
pub struct NoOpConnectors;

impl Connectors for NoOpConnectors {
    fn capture<'a, R>(
        &'a self,
        _data_plane: &'a tables::DataPlane,
        _task: &'a models::Capture,
        request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<capture::Response>> + Send + 'a
    where
        R: futures::Stream<Item = capture::Request> + Send + Unpin + 'static,
    {
        request_rx.map(|request| {
            let response = match request.kind {
                Some(capture::request::Kind::Spec(_spec)) => capture::Response {
                    kind: Some(capture::response::Kind::Spec(Box::new(
                        capture::response::Spec {
                            resource_path_pointers: Vec::new(),
                            config_schema_json: "true".into(),
                            resource_config_schema_json: "true".into(),
                            ..Default::default()
                        },
                    ))),
                    ..Default::default()
                },
                Some(capture::request::Kind::Validate(validate)) => capture::Response {
                    kind: Some(capture::response::Kind::Validated(
                        capture::response::Validated {
                            bindings: validate
                                .bindings
                                .iter()
                                .enumerate()
                                .map(|(i, _)| capture::response::validated::Binding {
                                    resource_path: vec![format!("binding-{i}")],
                                })
                                .collect(),
                        },
                    )),
                    ..Default::default()
                },
                _ => anyhow::bail!("expected Spec or Validate"),
            };
            Ok(response)
        })
    }

    fn derive<'a, R>(
        &'a self,
        _data_plane: &'a tables::DataPlane,
        _task: &'a models::Collection,
        request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<derive::Response>> + Send + 'a
    where
        R: futures::Stream<Item = derive::Request> + Send + Unpin + 'static,
    {
        request_rx.map(|request| {
            let response = match request.kind {
                Some(derive::request::Kind::Spec(_spec)) => derive::Response {
                    kind: Some(derive::response::Kind::Spec(Box::new(
                        derive::response::Spec {
                            config_schema_json: "true".into(),
                            resource_config_schema_json: "true".into(),
                            ..Default::default()
                        },
                    ))),
                    ..Default::default()
                },
                Some(derive::request::Kind::Validate(_validate)) => derive::Response {
                    kind: Some(derive::response::Kind::Validated(
                        derive::response::Validated::default(),
                    )),
                    ..Default::default()
                },
                _ => anyhow::bail!("expected Spec or Validate"),
            };
            Ok(response)
        })
    }

    fn materialize<'a, R>(
        &'a self,
        _data_plane: &'a tables::DataPlane,
        _task: &'a models::Materialization,
        request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<materialize::Response>> + Send + 'a
    where
        R: futures::Stream<Item = materialize::Request> + Send + Unpin + 'static,
    {
        request_rx.map(|request| {
            let response = match request.kind {
                Some(materialize::request::Kind::Spec(_spec)) => materialize::Response {
                    kind: Some(materialize::response::Kind::Spec(Box::new(
                        materialize::response::Spec {
                            config_schema_json: "true".into(),
                            resource_config_schema_json: "true".into(),
                            ..Default::default()
                        },
                    ))),
                    ..Default::default()
                },
                Some(materialize::request::Kind::Validate(validate)) => materialize::Response {
                    kind: Some(materialize::response::Kind::Validated(materialize::response::Validated {
                        bindings: validate
                            .resolved_bindings()
                            .enumerate()
                            .map(|(i, (_binding, resolved))| {
                                // Return FIELD_OPTIONAL for every collection projection
                                // so that field selection validation succeeds.
                                let projection_constraints = resolved
                                    .map(|(collection, _identity)| &collection.projections)
                                    .into_iter()
                                    .flatten()
                                    .map(|p| {
                                        materialize::response::validated::ProjectionConstraint {
                                            field: p.field.clone(),
                                            constraint: Some(materialize::response::validated::Constraint {
                                                r#type: materialize::response::validated::constraint::Type::FieldOptional as i32,
                                                reason: String::new(),
                                                folded_field: String::new(),
                                            }),
                                        }
                                    })
                                    .collect();

                                materialize::response::validated::Binding {
                                    resource_path: vec![format!("binding-{i}")],
                                    projection_constraints,
                                    ..Default::default()
                                }
                            })
                            .collect(),
                    })),
                    ..Default::default()
                },
                _ => anyhow::bail!("expected Spec or Validate"),
            };
            Ok(response)
        })
    }
}
