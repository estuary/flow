use std::time::Duration;

pub async fn check_latest() -> Option<(String, String)> {
    let current = env!("CARGO_PKG_VERSION");
    match fetch_latest_tag().await {
        Ok(latest) if is_outdated(current, &latest) => Some((current.to_string(), latest)),
        Ok(_) => None,
        Err(err) => {
            tracing::debug!(%err, "version check failed");
            None
        }
    }
}

/// Whether `current` predates the `latest` release, comparing only the
/// `MAJOR.MINOR.PATCH` of each.
///
/// Release binaries carry the exact release tag, such as `v0.6.13`. Binaries
/// built by CI from master carry a `git describe` version such as
/// `v0.6.13-180-gafc54b0` or `v0.6.13-dirty`, which is at or ahead of the
/// `v0.6.13` release. A local `cargo build` carries the `dev` default from
/// `.cargo/config.toml`, which has no release to compare against.
fn is_outdated(current: &str, latest: &str) -> bool {
    match (parse_release(current), parse_release(latest)) {
        (Some(current), Some(latest)) => current < latest,
        _ => false,
    }
}

/// Parses the leading `MAJOR.MINOR.PATCH` of a version, ignoring a `v` prefix
/// and any `-` suffix.
fn parse_release(version: &str) -> Option<(u64, u64, u64)> {
    let version = version.strip_prefix('v').unwrap_or(version);
    let release = version.split('-').next()?;

    let mut parts = release.split('.').map(str::parse::<u64>);
    let major = parts.next()?.ok()?;
    let minor = parts.next()?.ok()?;
    let patch = parts.next()?.ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
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

    Ok(release.tag_name)
}

#[cfg(test)]
mod test {
    use super::is_outdated;

    #[test]
    fn test_is_outdated() {
        let cases = [
            // Release binaries carry the exact release tag.
            ("v0.6.13", "v0.6.13"),
            ("v0.6.12", "v0.6.13"),
            ("v0.6.14", "v0.6.13"),
            ("v0.6.9", "v0.6.10"),
            ("v0.5.24", "v0.6.0"),
            // CI builds from master carry `git describe` output.
            ("v0.6.13-180-gafc54b0df58", "v0.6.13"),
            ("v0.6.12-180-gafc54b0df58", "v0.6.13"),
            ("v0.6.13-dirty", "v0.6.13"),
            // Local builds carry the `dev` default.
            ("dev", "v0.6.13"),
            ("", "v0.6.13"),
            // Versions without a release triple never warn.
            ("v0.6.13", "dev-next"),
            ("v0.6", "v0.6.13"),
            ("v0.6.13.1", "v0.6.13"),
        ];
        let results: Vec<_> = cases
            .iter()
            .map(|(current, latest)| (current, latest, is_outdated(current, latest)))
            .collect();

        insta::assert_debug_snapshot!(results);
    }
}
