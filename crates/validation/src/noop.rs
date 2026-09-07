use proto_flow::{capture, connector, derive, materialize};

/// A permissive unary connector used when connector validation is disabled.
pub(crate) async fn no_op_connector(
    request: connector::Request,
) -> anyhow::Result<(connector::response::Started, connector::response::Kind)> {
    let (spec, response) = match request.kind {
        Some(connector::request::Kind::Capture(capture::Request {
            kind: Some(capture::request::Kind::Validate(validate)),
            ..
        })) => {
            let spec = capture::response::Spec {
                resource_path_pointers: Vec::new(),
                config_schema_json: "true".into(),
                resource_config_schema_json: "true".into(),
                ..Default::default()
            };
            let response = capture::Response {
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
            };
            (
                connector::response::started::Spec::Capture(Box::new(spec)),
                connector::response::Kind::Capture(response),
            )
        }
        Some(connector::request::Kind::Derive(derive::Request {
            kind: Some(derive::request::Kind::Validate(_)),
            ..
        })) => {
            let spec = derive::response::Spec {
                config_schema_json: "true".into(),
                resource_config_schema_json: "true".into(),
                ..Default::default()
            };
            let response = derive::Response {
                kind: Some(derive::response::Kind::Validated(
                    derive::response::Validated::default(),
                )),
                ..Default::default()
            };
            (
                connector::response::started::Spec::Derive(Box::new(spec)),
                connector::response::Kind::Derive(response),
            )
        }
        Some(connector::request::Kind::Materialize(materialize::Request {
            kind: Some(materialize::request::Kind::Validate(validate)),
            ..
        })) => {
            let spec = materialize::response::Spec {
                config_schema_json: "true".into(),
                resource_config_schema_json: "true".into(),
                ..Default::default()
            };
            let response = materialize::Response {
                kind: Some(materialize::response::Kind::Validated(
                    materialize::response::Validated {
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
                                    .map(|p| materialize::response::validated::ProjectionConstraint {
                                        field: p.field.clone(),
                                        constraint: Some(
                                            materialize::response::validated::Constraint {
                                                r#type: materialize::response::validated::constraint::Type::FieldOptional as i32,
                                                reason: String::new(),
                                                folded_field: String::new(),
                                            },
                                        ),
                                    })
                                    .collect();

                                materialize::response::validated::Binding {
                                    resource_path: vec![format!("binding-{i}")],
                                    projection_constraints,
                                    ..Default::default()
                                }
                            })
                            .collect(),
                    },
                )),
                ..Default::default()
            };
            (
                connector::response::started::Spec::Materialize(Box::new(spec)),
                connector::response::Kind::Materialize(response),
            )
        }
        _ => anyhow::bail!("expected a unary Validate connector request"),
    };

    Ok((
        connector::response::Started {
            spec: Some(spec),
            ..Default::default()
        },
        response,
    ))
}
