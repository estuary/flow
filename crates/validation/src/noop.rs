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
                                resource_path_pointers: Vec::new(),
                                config_schema_json: "true".to_string(),
                                resource_config_schema_json: "true".to_string(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .await;
                } else if let Some(_validate) = request.validate {
                    () = co
                        .yield_(capture::Response {
                            validated: Some(capture::response::Validated::default()),
                            ..Default::default()
                        })
                        .await;
                } else {
                    anyhow::bail!("expected Spec or Validate")
                }
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
                } else if let Some(_validate) = request.validate {
                    () = co
                        .yield_(derive::Response {
                            validated: Some(derive::response::Validated::default()),
                            ..Default::default()
                        })
                        .await;
                } else {
                    anyhow::bail!("expected Spec or Validate")
                }
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
                } else if let Some(_validate) = request.validate {
                    () = co
                        .yield_(materialize::Response {
                            validated: Some(materialize::response::Validated::default()),
                            ..Default::default()
                        })
                        .await;
                } else {
                    anyhow::bail!("expected Spec or Validate")
                }
            }
            Ok(())
        })
    }
}
