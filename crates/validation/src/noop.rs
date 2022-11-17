use super::Drivers;
use futures::future::LocalBoxFuture;
use proto_flow::{capture, materialize};

/// NoOpDrivers are permissive placeholders for interaction with connectors,
/// that do not fail and return the right shape of response.
pub struct NoOpDrivers;

impl Drivers for NoOpDrivers {
    fn validate_materialization<'a>(
        &'a self,
        request: materialize::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<materialize::ValidateResponse, anyhow::Error>> {
        use materialize::{
            constraint::Type, validate_response::Binding, Constraint, ValidateResponse,
        };
        use std::collections::HashMap;

        Box::pin(async move {
            let response_bindings = request
                .bindings
                .into_iter()
                .enumerate()
                .map(|(i, b)| {
                    let resource_path = vec![format!("binding-{}", i)];
                    let constraints = b
                        .collection
                        .expect("collection must exist")
                        .projections
                        .into_iter()
                        .map(|proj| {
                            (
                                proj.field,
                                Constraint {
                                    r#type: Type::FieldOptional as i32,
                                    reason: "no-op validator allows everything".to_string(),
                                },
                            )
                        })
                        .collect::<HashMap<_, _>>();
                    Binding {
                        constraints,
                        resource_path,
                        delta_updates: true,
                    }
                })
                .collect::<Vec<_>>();
            Ok(ValidateResponse {
                bindings: response_bindings,
            })
        })
    }

    fn validate_capture<'a>(
        &'a self,
        request: capture::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<capture::ValidateResponse, anyhow::Error>> {
        use capture::{validate_response::Binding, ValidateResponse};
        Box::pin(async move {
            let bindings = request
                .bindings
                .into_iter()
                .enumerate()
                .map(|(i, _)| Binding {
                    resource_path: vec![format!("binding-{}", i)],
                })
                .collect::<Vec<_>>();
            Ok(ValidateResponse { bindings })
        })
    }
}
