use super::Connectors;
use futures::StreamExt;
use proto_flow::{capture, derive, materialize};
use std::collections::BTreeMap;

/// NoOpConnectors are permissive placeholders for interactions with connectors,
/// that never fail and return the right shape of response.
pub struct NoOpConnectors;

impl Connectors for NoOpConnectors {
    fn capture<'a, R>(
        &'a self,
        _data_plane: &'a tables::DataPlane,
        _task: &'a models::Capture,
        mut request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<capture::Response>> + Send + 'a
    where
        R: futures::Stream<Item = capture::Request> + Send + Unpin + 'static,
    {
        coroutines::try_coroutine(|mut co| async move {
            while let Some(request) = request_rx.next().await {
                if let Some(_spec) = request.spec {
                    () = co
                        .yield_(capture::Response {
                            spec: Some(capture::response::Spec {
                                resource_path_pointers: vec![String::new()],
                                config_schema_json: "true".to_string(),
                                resource_config_schema_json: "true".to_string(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .await;
                    continue;
                }

                let Some(mut validate) = request.validate else {
                    anyhow::bail!("expected Spec or Validate")
                };
                use capture::response::{validated::Binding, Validated};

                let bindings = std::mem::take(&mut validate.bindings)
                    .into_iter()
                    .enumerate()
                    .map(|(i, _)| Binding {
                        resource_path: vec![format!("binding-{}", i)],
                    })
                    .collect::<Vec<_>>();

                () = co
                    .yield_(capture::Response {
                        validated: Some(Validated { bindings }),
                        ..Default::default()
                    })
                    .await;
            }
            Ok(())
        })
    }

    fn derive<'a, R>(
        &'a self,
        _data_plane: &'a tables::DataPlane,
        _task: &'a models::Collection,
        mut request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<derive::Response>> + Send + 'a
    where
        R: futures::Stream<Item = derive::Request> + Send + Unpin + 'static,
    {
        coroutines::try_coroutine(|mut co| async move {
            while let Some(request) = request_rx.next().await {
                if let Some(_spec) = request.spec {
                    () = co
                        .yield_(derive::Response {
                            spec: Some(derive::response::Spec {
                                config_schema_json: "true".to_string(),
                                resource_config_schema_json: "true".to_string(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .await;
                    continue;
                }

                let Some(mut validate) = request.validate else {
                    anyhow::bail!("expected Spec or Validate")
                };
                use derive::response::{validated::Transform, Validated};

                let transforms = std::mem::take(&mut validate.transforms)
                    .into_iter()
                    .map(|_| Transform { read_only: false })
                    .collect::<Vec<_>>();

                () = co
                    .yield_(derive::Response {
                        validated: Some(Validated {
                            transforms,
                            generated_files: BTreeMap::new(),
                        }),
                        ..Default::default()
                    })
                    .await;
            }
            Ok(())
        })
    }

    fn materialize<'a, R>(
        &'a self,
        _data_plane: &'a tables::DataPlane,
        _task: &'a models::Materialization,
        mut request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<materialize::Response>> + Send + 'a
    where
        R: futures::Stream<Item = materialize::Request> + Send + Unpin + 'static,
    {
        coroutines::try_coroutine(|mut co| async move {
            while let Some(request) = request_rx.next().await {
                if let Some(_spec) = request.spec {
                    () = co
                        .yield_(materialize::Response {
                            spec: Some(materialize::response::Spec {
                                config_schema_json: "true".to_string(),
                                resource_config_schema_json: "true".to_string(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .await;
                    continue;
                }

                let Some(mut validate) = request.validate else {
                    anyhow::bail!("expected Spec or Validate")
                };
                use materialize::response::{
                    validated::Binding,
                    validated::{constraint::Type, Constraint},
                    Validated,
                };

                let response_bindings = std::mem::take(&mut validate.bindings)
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

                () = co
                    .yield_(materialize::Response {
                        validated: Some(Validated {
                            bindings: response_bindings,
                        }),
                        ..Default::default()
                    })
                    .await;
            }
            Ok(())
        })
    }
}
