pub mod api;
pub(crate) mod connector_tags;
pub mod controllers;
pub(crate) mod controlplane;
mod directives;
mod discovers;
pub(crate) mod draft;
pub(crate) mod evolution;
mod jobs;
pub(crate) mod live_specs;
pub mod logs;
mod proxy_connectors;
pub mod publications;

#[cfg(test)]
pub(crate) mod integration_tests;

pub use agent_sql::{CatalogType, Id};
use anyhow::Context;
pub use connector_tags::TagExecutor;
pub use controlplane::{ControlPlane, PGControlPlane};
pub use directives::DirectiveHandler;
pub use discovers::DiscoverHandler;
pub use evolution::EvolutionExecutor;
use lazy_static::lazy_static;
pub use proxy_connectors::{DataPlaneConnectors, DiscoverConnectors, ProxyConnectors};
use regex::Regex;

lazy_static! {
    static ref NAME_VERSION_RE: Regex = Regex::new(r#".*[_-][vV](\d+)$"#).unwrap();
}

/// Returns a vector of `EvolveRequest`s for the given incompatible collections.
/// If re-creating a collection is required, then this will use a default name
/// generation function to add or increment a version suffix. If `affected_materializations`
/// are non-empty, then they will always be set on the `EvolveRequest`.
pub fn evolutions_requests(
    incompatible_collections: &[publications::IncompatibleCollection],
) -> Vec<evolution::EvolveRequest> {
    incompatible_collections
        .iter()
        .map(|ic| {
            let req = evolution::EvolveRequest::of(&ic.collection).with_materializations(
                ic.affected_materializations
                    .iter()
                    .map(|ac| ac.name.as_str()),
            );
            if ic.requires_recreation.is_empty() {
                req
            } else {
                req.with_version_increment()
            }
        })
        .collect()
}

/// Takes an existing name and returns a new name with an incremeted version suffix.
/// The name `foo` will become `foo_v2`, and `foo_v2` will become `foo_v3` and so on.
pub fn next_name(current_name: &str) -> String {
    // Does the name already have a version suffix?
    // We try to work with whatever suffix is already present. This way, if a user
    // is starting with a collection like `acmeCo/foo-V3`, they'll end up with
    // `acmeCo/foo-V4` instead of `acmeCo/foo_v4`.
    if let Some(capture) = NAME_VERSION_RE.captures_iter(current_name).next() {
        if let Ok(current_version_num) = capture[1].parse::<u32>() {
            // wrapping_add is just to ensure we don't panic if someone passes
            // a naughty name with a u32::MAX version.
            return format!(
                "{}{}",
                current_name.strip_suffix(&capture[1]).unwrap(),
                // We don't really care what the collection name ends up as if
                // the old name is suffixed by "V-${u32::MAX}", as long as we don't panic.
                current_version_num.wrapping_add(1)
            );
        }
    }
    // We always use an underscore as the separator. This might look a bit
    // unseemly if dashes are already used as separators elsewhere in the
    // name, but any sort of heuristic for determining whether to use dashes
    // or underscores is rife with edge cases and doesn't seem worth the
    // complexity.
    format!("{current_name}_v2")
}

// timeout is a convienence for tokio::time::timeout which merges
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

async fn decrypt_hmac_keys(dp: &mut tables::DataPlane) -> anyhow::Result<()> {
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_evolutions_requests() {
        let incompatible_collections = vec![
            publications::IncompatibleCollection {
                collection: "acmeCo/collectionA".to_string(),
                affected_materializations: vec![publications::AffectedConsumer {
                    name: "acmeCo/bar".to_string(),
                    fields: Vec::new(),
                    resource_path: Vec::new(),
                }],
                requires_recreation: vec![],
            },
            publications::IncompatibleCollection {
                collection: "acmeCo/collectionB_v2".to_string(),
                affected_materializations: vec![publications::AffectedConsumer {
                    name: "acmeCo/baz".to_string(),
                    fields: Vec::new(),
                    resource_path: Vec::new(),
                }],
                requires_recreation: vec![publications::ReCreateReason::KeyChange],
            },
            publications::IncompatibleCollection {
                collection: "acmeCo/collectionC".to_string(),
                affected_materializations: Vec::new(),
                requires_recreation: Vec::new(),
            },
            publications::IncompatibleCollection {
                collection: "acmeCo/collectionD".to_string(),
                affected_materializations: Vec::new(),
                requires_recreation: vec![publications::ReCreateReason::PartitionChange],
            },
        ];
        let requests = evolutions_requests(&incompatible_collections);
        insta::assert_debug_snapshot!(requests, @r###"
        [
            EvolveRequest {
                current_name: "acmeCo/collectionA",
                new_name: None,
                reset: false,
                materializations: [
                    "acmeCo/bar",
                ],
            },
            EvolveRequest {
                current_name: "acmeCo/collectionB_v2",
                new_name: Some(
                    "acmeCo/collectionB_v3",
                ),
                reset: false,
                materializations: [
                    "acmeCo/baz",
                ],
            },
            EvolveRequest {
                current_name: "acmeCo/collectionC",
                new_name: None,
                reset: false,
                materializations: [],
            },
            EvolveRequest {
                current_name: "acmeCo/collectionD",
                new_name: Some(
                    "acmeCo/collectionD_v2",
                ),
                reset: false,
                materializations: [],
            },
        ]
        "###);
    }
}
