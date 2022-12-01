use crate::config::Config;
use anyhow::Context;
use serde::Deserialize;

#[derive(Deserialize, Clone, PartialEq, Debug)]
pub struct DataPlaneAccess {
    #[serde(rename = "token")]
    pub auth_token: String,
    pub gateway_url: String,
}

/// Fetches connection info for accessing a data plane for the given catalog namespace prefixes.
pub async fn fetch_data_plane_access_token(
    cfg: &Config,
    prefixes: Vec<String>,
) -> anyhow::Result<DataPlaneAccess> {
    let client = cfg.client()?;
    tracing::debug!(?prefixes, "requesting data-plane access token for prefixes");

    let body = serde_json::to_string(&serde_json::json!({
        "prefixes": prefixes,
    }))
    .context("serializing prefix parameters")?;

    let req = client.rpc("gateway_auth_token", body).build();
    tracing::trace!(?req, "built request to execute");
    let resp = req
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .context("requesting data plane gateway auth token")?;
    let json: serde_json::Value = resp.json().await?;
    tracing::trace!(response_body = ?json, "got response from control-plane");
    let mut auths: Vec<DataPlaneAccess> =
        serde_json::from_value(json).context("failed to decode response")?;
    let access = auths.pop().ok_or_else(|| {
        anyhow::anyhow!(
            "no data-plane access tokens were returned for the given prefixes, access is denied"
        )
    })?;
    if !auths.is_empty() {
        let num_tokens = auths.len() + 1;
        anyhow::bail!("received {} tokens for the given set of prefixes: {:?}. This is not yet implemented in flowctl", num_tokens, prefixes);
    }
    Ok(access)
}

/// Returns an authenticated journal client that's authorized to the given prefixes.
pub async fn journal_client_for(
    cfg: &Config,
    prefixes: Vec<String>,
) -> anyhow::Result<journal_client::Client> {
    let DataPlaneAccess {
        auth_token,
        gateway_url,
    } = fetch_data_plane_access_token(cfg, prefixes).await?;
    tracing::debug!(%gateway_url, "acquired data-plane-gateway access token");

    let client =
        journal_client::connect_journal_client(gateway_url.clone(), Some(auth_token)).await?;
    tracing::debug!(%gateway_url, "connected data-plane client");
    Ok(client)
}
