pub(crate) mod connector_tags;
pub mod controllers;
pub(crate) mod controlplane;
mod directives;
mod discovers;
pub mod publications;

#[cfg(test)]
pub(crate) mod integration_tests;

use anyhow::Context;
pub use connector_tags::TagExecutor;
pub use control_plane_api::proxy_connectors::{
    DataPlaneConnectors, DiscoverConnectors, ProxyConnectors,
};
pub use controlplane::{ControlPlane, PGControlPlane};
pub use directives::DirectiveHandler;
pub use discovers::DiscoverExecutor;
use lazy_static::lazy_static;
pub use models::{CatalogType, Id};
use regex::Regex;

lazy_static! {
    static ref NAME_VERSION_RE: Regex = Regex::new(r#".*[_-][vV](\d+)$"#).unwrap();
}

// timeout is a convenience for tokio::time::timeout which merges
// its error with the Future's nested anyhow::Result Output.
async fn timeout<Ok, Fut, C, WC>(
    dur: std::time::Duration,
    fut: Fut,
    with_context: WC,
) -> anyhow::Result<Ok>
where
    C: std::fmt::Display + Send + Sync + 'static,
    Fut: std::future::Future<Output = anyhow::Result<Ok>>,
    WC: FnOnce() -> C,
{
    use anyhow::Context;

    match tokio::time::timeout(dur, fut).await {
        Ok(result) => result,
        Err(err) => Err(anyhow::anyhow!(err)).with_context(with_context),
    }
}

pub async fn decrypt_hmac_keys(dp: &mut tables::DataPlane) -> anyhow::Result<()> {
    let sops = locate_bin::locate("sops").context("failed to locate sops")?;

    if !dp.hmac_keys.is_empty() {
        return Ok(());
    }

    #[derive(serde::Deserialize)]
    struct HMACKeys {
        hmac_keys: Vec<String>,
    }

    // Note that input_output() pre-allocates an output buffer as large as its input buffer,
    // and our decrypted result will never be larger than its input.
    let async_process::Output {
        stderr,
        stdout,
        status,
    } = async_process::input_output(
        async_process::Command::new(sops).args([
            "--decrypt",
            "--input-type",
            "json",
            "--output-type",
            "json",
            "/dev/stdin",
        ]),
        dp.encrypted_hmac_keys.get().as_bytes(),
    )
    .await
    .context("failed to run sops")?;

    let stdout = zeroize::Zeroizing::from(stdout);

    if !status.success() {
        anyhow::bail!(
            "decrypting hmac sops document failed: {}",
            String::from_utf8_lossy(&stderr),
        );
    }

    dp.hmac_keys = serde_json::from_slice::<HMACKeys>(&stdout)
        .context("parsing decrypted sops document")?
        .hmac_keys;

    Ok(())
}
