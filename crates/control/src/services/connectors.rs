use std::path::Path;

use anyhow::anyhow;
use models::Object;
use tokio::process::Command;

use crate::config::settings;
use crate::controllers::json_api::RawJson;
use crate::models::connector_images::ConnectorImage;
use crate::models::connectors::{Connector, ConnectorOperation};
use crate::models::names::CatalogName;
use crate::services::discovery::{DiscoverResponse, DiscoveredCatalog};
use crate::services::subprocess::Subprocess;

#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    #[error("could not execute the connector")]
    ConnectorFailed(anyhow::Error),
    #[error("failed to parse configuration")]
    MalformedConfig(serde_json::Error),
    #[error("could not parse the connector output")]
    MalformedOutput(serde_json::Error),
    #[error("connector does not support operation: `{0}`")]
    UnsupportedOperation(ConnectorOperation),
}

pub async fn spec(image: &ConnectorImage) -> Result<RawJson, ConnectorError> {
    // TODO: Swap `image.pinned_version()` out with `image.full_name()`?
    let image_output = spec_cmd(&image.pinned_version())
        .execute()
        .await
        .map_err(|e| ConnectorError::ConnectorFailed(anyhow!(e)))?;
    serde_json::from_str(&image_output).map_err(ConnectorError::MalformedOutput)
}

fn spec_cmd(image: &str) -> Command {
    let mut cmd = Command::new("flowctl");
    cmd.arg("api").arg("spec").arg("--image").arg(image);

    cmd
}

/// Settings for controlling names generated as part of the discovered catalog.
pub struct DiscoveryOptions {
    /// The name given to the generated catalog itself. eg. `postgres` in `postgres.flow.json`
    pub catalog_name: CatalogName,
    /// The prefix used for named catalog entities. eg. `acmeCo` in `acmeCo/anvils`
    pub catalog_prefix: CatalogName,
}

pub async fn discover(
    connector: Connector,
    image: ConnectorImage,
    config: Object,
    options: DiscoveryOptions,
) -> Result<DiscoveredCatalog, ConnectorError> {
    if !connector.supports(ConnectorOperation::Discover) {
        return Err(ConnectorError::UnsupportedOperation(
            ConnectorOperation::Discover,
        ));
    }

    // TODO: Use a named pipe so that this file can only be read once.
    let tmpfile =
        tempfile::NamedTempFile::new().map_err(|e| ConnectorError::ConnectorFailed(anyhow!(e)))?;
    serde_json::to_writer(&tmpfile, &config).map_err(ConnectorError::MalformedConfig)?;

    // TODO: Read this output as protobufs, rather than json? There is a
    // `protocol::capture::DiscoverResponse` type we could use directly.
    let image_output = discovery_cmd(&image.pinned_version(), tmpfile.path())
        .execute()
        .await
        .map_err(|e| ConnectorError::ConnectorFailed(anyhow!(e)))?;

    let DiscoverResponse { bindings } =
        serde_json::from_str(&image_output).map_err(ConnectorError::MalformedOutput)?;

    Ok(DiscoveredCatalog::new(
        connector, image, config, bindings, options,
    ))
}

pub fn discovery_cmd(image: &str, config_path: &Path) -> Command {
    let mut cmd = Command::new("flowctl");
    cmd.arg("api")
        .arg("discover")
        .arg("--image")
        .arg(image)
        .arg("--config")
        .arg(config_path)
        .arg("--network")
        .arg(&settings().application.connector_network);

    cmd
}

#[cfg(all(test, feature = "flowctl"))]
mod test {
    use super::*;
    use crate::error::SubprocessError;
    use crate::services::subprocess::Subprocess;

    #[tokio::test]
    async fn connector_spec_works() {
        let mut cmd = spec_cmd("ghcr.io/estuary/source-hello-world:01fb856");
        let output = cmd.execute().await.expect("command output");
        assert_eq!(r#"{"type":"capture","#, &output[0..18]);
    }

    #[tokio::test]
    async fn connector_spec_fails_gracefully() {
        let mut cmd = spec_cmd("ghcr.io/estuary/source-hello-world:non-existant");
        assert!(matches!(
            cmd.execute().await.expect_err("connector should not exist"),
            SubprocessError::Failure { .. },
        ));
    }
}
