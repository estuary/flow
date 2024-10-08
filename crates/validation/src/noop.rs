use super::Connectors;
use futures::future::BoxFuture;
use proto_flow::{capture, derive, materialize};
use std::collections::BTreeMap;

/// NoOpConnectors are permissive placeholders for interactions with connectors,
/// that never fail and return the right shape of response.
pub struct NoOpConnectors;

impl Connectors for NoOpConnectors {
    fn validate_capture<'a>(
        &'a self,
        request: capture::Request,
        _data_plane: &tables::DataPlane,
    ) -> BoxFuture<'a, anyhow::Result<capture::Response>> {
        let capture::Request {
            validate: Some(mut request),
            ..
        } = request
        else {
            unreachable!()
        };
        use capture::response::{validated::Binding, Validated};

        Box::pin(async move {
            let bindings = std::mem::take(&mut request.bindings)
                .into_iter()
                .enumerate()
                .map(|(i, _)| Binding {
                    resource_path: vec![format!("binding-{}", i)],
                })
                .collect::<Vec<_>>();
            Ok(capture::Response {
                validated: Some(Validated { bindings }),
                ..Default::default()
            })
        })
    }

    fn validate_derivation<'a>(
        &'a self,
        request: derive::Request,
        _data_plane: &tables::DataPlane,
    ) -> BoxFuture<'a, anyhow::Result<derive::Response>> {
        let derive::Request {
            validate: Some(mut request),
            ..
        } = request
        else {
            unreachable!()
        };
        use derive::response::{validated::Transform, Validated};

        Box::pin(async move {
            let transforms = std::mem::take(&mut request.transforms)
                .into_iter()
                .map(|_| Transform { read_only: false })
                .collect::<Vec<_>>();
            Ok(derive::Response {
                validated: Some(Validated {
                    transforms,
                    generated_files: BTreeMap::new(),
                }),
                ..Default::default()
            })
        })
    }

    fn validate_materialization<'a>(
        &'a self,
        request: materialize::Request,
        _data_plane: &tables::DataPlane,
    ) -> BoxFuture<'a, anyhow::Result<materialize::Response>> {
        let materialize::Request {
            validate: Some(mut request),
            ..
        } = request
        else {
            unreachable!()
        };
        use materialize::response::{
            validated::{constraint::Type, Binding, Constraint},
            Validated,
        };

        Box::pin(async move {
            let response_bindings = std::mem::take(&mut request.bindings)
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
                        .collect::<BTreeMap<_, _>>();
                    Binding {
                        constraints,
                        resource_path,
                        delta_updates: true,
                    }
                })
                .collect::<Vec<_>>();
            Ok(materialize::Response {
                validated: Some(Validated {
                    bindings: response_bindings,
                }),
                ..Default::default()
            })
        })
    }
}

/// NoOpWrapper wraps another Connectors implementation to selectively
/// enable validations for specific task types.
pub struct NoOpWrapper<C> {
    pub noop_captures: bool,
    pub noop_derivations: bool,
    pub noop_materializations: bool,
    pub inner: C,
}

impl<C: Connectors> Connectors for NoOpWrapper<C> {
    fn validate_capture<'a>(
        &'a self,
        request: capture::Request,
        data_plane: &'a tables::DataPlane,
    ) -> BoxFuture<'a, anyhow::Result<capture::Response>> {
        if self.noop_captures {
            NoOpConnectors.validate_capture(request, data_plane)
        } else {
            self.inner.validate_capture(request, data_plane)
        }
    }
    fn validate_derivation<'a>(
        &'a self,
        request: derive::Request,
        data_plane: &'a tables::DataPlane,
    ) -> BoxFuture<'a, anyhow::Result<derive::Response>> {
        if self.noop_derivations {
            NoOpConnectors.validate_derivation(request, data_plane)
        } else {
            self.inner.validate_derivation(request, data_plane)
        }
    }
    fn validate_materialization<'a>(
        &'a self,
        request: materialize::Request,
        data_plane: &'a tables::DataPlane,
    ) -> BoxFuture<'a, anyhow::Result<materialize::Response>> {
        if self.noop_materializations {
            NoOpConnectors.validate_materialization(request, data_plane)
        } else {
            self.inner.validate_materialization(request, data_plane)
        }
    }
}
