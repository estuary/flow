//! Pieces shared by the connectors which serve Python projects: their process
//! entry point, their answers to Spec, and the starters of missing files.

use crate::{Exited, LOCK, MANIFEST, Project, ProjectConfig, entry};
use std::collections::BTreeMap;

/// Documentation URL of every Python connector's Spec response.
pub const DOCUMENTATION_URL: &str = "https://docs.estuary.dev";

/// Run a connector's `run` as the main routine of its process.
pub fn main(run: fn() -> anyhow::Result<()>) {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_env("LOG_LEVEL"))
        .with_writer(std::io::stderr)
        .json()
        .init();

    let Err(error) = run() else {
        return;
    };
    // The final log's message becomes the task's error. A user program which
    // exits with a code has logged its own error, so ours would mask it.
    if let Some(code) = error
        .downcast_ref::<Exited>()
        .and_then(|Exited(status)| status.code())
    {
        std::process::exit(code);
    }
    // Use anyhow's compact format, as the runtime's log decoder cannot structure a multi-line Debug.
    tracing::error!("{error:#}");
    std::process::exit(1);
}

/// The `spec` declared by a ProjectConfig. Any other configuration (the `{}`
/// of a bare image Spec, or an invalid one, which Validate reports) declares
/// nothing, and has the connector's defaults.
pub fn declared_spec(config_json: &[u8]) -> models::custom_connector::Spec {
    serde_json::from_slice::<ProjectConfig>(config_json)
        .map(|config| config.project.spec)
        .unwrap_or_default()
}

/// OAuth2 of a Spec response, which `models::custom_connector::OAuth2` mirrors.
pub fn oauth2(oauth2: &models::custom_connector::OAuth2) -> proto_flow::flow::OAuth2 {
    serde_json::from_value(serde_json::to_value(oauth2).unwrap())
        .expect("OAuth2 of a Spec is a flow::OAuth2")
}

/// Key `files`, whose paths are relative to a project, on their URLs under
/// Validate's `project_root`.
pub fn project_urls(
    project_root: &str,
    files: impl IntoIterator<Item = (String, String)>,
) -> BTreeMap<String, String> {
    files
        .into_iter()
        .map(|(path, content)| (format!("{project_root}/{path}"), content))
        .collect()
}

/// Starters of the listed files of `project` which don't exist yet, keyed on
/// their URLs under `project_root`. The entry file of the task's directory
/// `dir` starts as `entry_starter`, and the manifest as `manifest_starter`.
/// A lock has no starter, and any other file starts empty.
pub fn starter_files(
    project: &Project,
    project_root: &str,
    dir: &str,
    entry_starter: String,
    manifest_starter: String,
) -> BTreeMap<String, String> {
    let entry_path = entry(dir);

    project
        .missing()
        .into_iter()
        .filter(|path| *path != LOCK)
        .map(|path| {
            let starter = if path == entry_path {
                entry_starter.clone()
            } else if path == MANIFEST {
                manifest_starter.clone()
            } else {
                String::new()
            };
            (format!("{project_root}/{path}"), starter)
        })
        .collect()
}
