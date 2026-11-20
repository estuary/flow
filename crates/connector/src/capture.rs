//! Capture protocol: endpoint extraction.
//! See `protocol.rs` for the start pipeline.
use crate::protocol::{Endpoint, Extracted, Protocol};
use anyhow::Context;
use proto_flow::{
    capture::{Request, request},
    flow,
};

pub(crate) use proto_flow::connector::Capture;

impl Protocol for Capture {
    fn spec_execution(request: &Request) -> Option<flow::ConnectorExecution> {
        let spec = match &request.kind {
            Some(request::Kind::Apply(apply)) => apply.capture.as_ref(),
            Some(request::Kind::Open(open)) => open.capture.as_ref(),
            _ => None,
        }?;
        Some(spec.execution.as_deref().cloned().unwrap_or_default())
    }

    fn extract_endpoint<'r>(
        request: &'r mut Request,
        sqlite_vfs_uri: Option<String>,
    ) -> anyhow::Result<Extracted<'r, Self>> {
        crate::policy::check_connector_sqlite_vfs(false, sqlite_vfs_uri.is_some())
            .map_err(|err| crate::invalid_argument(err.to_string()))?;
        let mut build = None;
        let mut network_ports = Vec::new();

        let (connector_type, config_json, sealed_config_json, secrets) = match &mut request.kind {
            Some(request::Kind::Spec(spec)) => (
                spec.connector_type,
                &mut spec.config_json,
                None,
                &super::EMPTY_SECRETS,
            ),
            Some(request::Kind::Discover(discover)) => (
                discover.connector_type,
                &mut discover.config_json,
                None,
                &discover.secrets,
            ),
            Some(request::Kind::Validate(validate)) => (
                validate.connector_type,
                &mut validate.config_json,
                None,
                &validate.secrets,
            ),
            Some(request::Kind::Apply(apply)) => {
                let inner = apply.capture.as_mut().expect("checked by task_name");
                build = Some(crate::protocol::shard_build(&inner.shard_template)?);
                network_ports = inner.network_ports.iter().map(|port| port.number).collect();
                (
                    inner.connector_type,
                    &mut inner.config_json,
                    None,
                    &inner.secrets,
                )
            }
            Some(request::Kind::Open(open)) => {
                let sealed_config_json = &mut open.sealed_config_json;
                let inner = open.capture.as_mut().expect("checked by task_name");
                build = Some(crate::protocol::shard_build(&inner.shard_template)?);
                network_ports = inner.network_ports.iter().map(|port| port.number).collect();
                (
                    inner.connector_type,
                    &mut inner.config_json,
                    Some(sealed_config_json),
                    &inner.secrets,
                )
            }
            _ => unreachable!("checked by task_name"),
        };

        let endpoint = if connector_type == flow::capture_spec::ConnectorType::Image as i32 {
            let models::ConnectorConfig { image, config } =
                serde_json::from_slice(config_json).context("parsing connector config")?;

            Endpoint::Image { image, config }
        } else if connector_type == flow::capture_spec::ConnectorType::Local as i32 {
            Endpoint::Local {
                config: serde_json::from_slice(config_json).context("parsing local config")?,
            }
        } else {
            anyhow::bail!("invalid connector type: {connector_type}");
        };

        Ok(Extracted {
            build,
            network_ports,
            initial_config_slot: config_json,
            connector_type,
            endpoint,
            initial_sealed_config_slot: sealed_config_json,
            secrets,
        })
    }
}
