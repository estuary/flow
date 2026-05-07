use std::time::Duration;

pub async fn check_latest() -> Option<String> {
    match fetch_latest_tag().await {
        Ok(latest) if latest != env!("CARGO_PKG_VERSION") => Some(latest),
        Ok(_) => None,
        Err(err) => {
            tracing::debug!(%err, "version check failed");
            None
        }
    }
}

async fn fetch_latest_tag() -> anyhow::Result<String> {
    #[derive(serde::Deserialize)]
    struct Release {
        tag_name: String,
    }

    let current = env!("CARGO_PKG_VERSION");

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?;

    let release: Release = client
        .get("https://api.github.com/repos/estuary/flow/releases/latest")
        .header("User-Agent", format!("flowctl/{current}"))
        .send()
        .await?
        .json()
        .await?;

    let tag = release
        .tag_name
        .strip_prefix('v')
        .unwrap_or(&release.tag_name);

    Ok(tag.to_owned())
}
