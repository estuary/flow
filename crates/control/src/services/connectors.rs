use std::path::Path;

use anyhow::anyhow;
use models::{CompositeKey, JsonPointer, Object, RelativeUrl, Schema};
use tokio::process::Command;

use crate::config::settings;
use crate::controllers::json_api::RawJson;
use crate::models::connector_images::ConnectorImage;
use crate::models::connectors::{Connector, ConnectorOperation};
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

#[derive(Debug, Deserialize)]
pub struct DiscoverResponse {
    pub bindings: Vec<DiscoveredBinding>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredBinding {
    /// A recommended display name for this discovered binding.
    pub recommended_name: String,
    /// JSON-encoded object which specifies the endpoint resource to be captured.
    #[serde(rename = "resourceSpec")]
    pub resource_spec_json: Object,
    /// JSON schema of documents produced by this binding.
    #[serde(rename = "documentSchema")]
    pub document_schema_json: Object,
    /// Composite key of documents (if known), as JSON-Pointers.
    pub key_ptrs: Vec<String>,
}

impl DiscoveredBinding {
    pub fn key(&self) -> models::CompositeKey {
        CompositeKey::new(
            self.key_ptrs
                .iter()
                .map(JsonPointer::new)
                .collect::<Vec<JsonPointer>>(),
        )
    }

    pub fn schema_url(&self) -> Schema {
        Schema::Url(RelativeUrl::new(self.schema_name()))
    }

    pub fn schema_name(&self) -> String {
        format!("{}.schema.json", self.recommended_name)
    }
}

pub async fn discover(
    connector: &Connector,
    image: &ConnectorImage,
    config: &Object,
) -> Result<DiscoverResponse, ConnectorError> {
    if !connector.supports(ConnectorOperation::Discover) {
        return Err(ConnectorError::UnsupportedOperation(
            ConnectorOperation::Discover,
        ));
    }

    // TODO: Use a named pipe so that this file can only be read once.
    let tmpfile =
        tempfile::NamedTempFile::new().map_err(|e| ConnectorError::ConnectorFailed(anyhow!(e)))?;
    serde_json::to_writer(&tmpfile, config).map_err(ConnectorError::MalformedConfig)?;

    // TODO: Read this output as protobufs, rather than json? There is a
    // `protocol::capture::DiscoverResponse` type we could use directly.
    let image_output = discovery_cmd(&image.pinned_version(), tmpfile.path())
        .execute()
        .await
        .map_err(|e| ConnectorError::ConnectorFailed(anyhow!(e)))?;

    Ok(serde_json::from_str(&image_output).map_err(ConnectorError::MalformedOutput)?)
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
