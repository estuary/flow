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
            let response = if let Some(_spec) = request.spec {
                capture::Response {
                    spec: Some(capture::response::Spec {
                        resource_path_pointers: Vec::new(),
                        config_schema_json: "true".into(),
                        resource_config_schema_json: "true".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            } else if let Some(_validate) = request.validate {
                capture::Response {
                    validated: Some(capture::response::Validated::default()),
                    ..Default::default()
                }
            } else {
                anyhow::bail!("expected Spec or Validate")
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
            let response = if let Some(_spec) = request.spec {
                derive::Response {
                    spec: Some(derive::response::Spec {
                        config_schema_json: "true".into(),
                        resource_config_schema_json: "true".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            } else if let Some(_validate) = request.validate {
                derive::Response {
                    validated: Some(derive::response::Validated::default()),
                    ..Default::default()
                }
            } else {
                anyhow::bail!("expected Spec or Validate")
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
            let response = if let Some(_spec) = request.spec {
                materialize::Response {
                    spec: Some(materialize::response::Spec {
                        config_schema_json: "true".into(),
                        resource_config_schema_json: "true".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            } else if let Some(_validate) = request.validate {
                materialize::Response {
                    validated: Some(materialize::response::Validated::default()),
                    ..Default::default()
                }
            } else {
                anyhow::bail!("expected Spec or Validate")
            };
            Ok(response)
        })
    }
}
