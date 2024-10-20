pub mod api;
pub(crate) mod connector_tags;
pub mod controllers;
pub(crate) mod controlplane;
mod directives;
mod discovers;
pub(crate) mod draft;
pub(crate) mod evolution;
mod handlers;
mod jobs;
pub mod logs;
mod proxy_connectors;
pub mod publications;
pub(crate) mod resource_configs;

#[cfg(test)]
pub(crate) mod integration_tests;

pub use agent_sql::{CatalogType, Id};
pub use connector_tags::TagHandler;
pub use controlplane::{ControlPlane, PGControlPlane};
pub use directives::DirectiveHandler;
pub use discovers::DiscoverHandler;
pub use evolution::EvolutionHandler;
pub use handlers::{serve, HandleResult, Handler};
use lazy_static::lazy_static;
use proxy_connectors::ProxyConnectors;
use regex::Regex;

// Used during tests.
#[cfg(test)]
const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

lazy_static! {
    static ref NAME_VERSION_RE: Regex = Regex::new(r#".*[_-][vV](\d+)$"#).unwrap();
}

/// Returns true if the given error represents a failure to acquire a lock, as indicated
/// by the "sql state" code.
fn is_acquire_lock_error(err: &anyhow::Error) -> bool {
    let Some(sql_err) = err.downcast_ref::<sqlx::Error>() else {
        return false;
    };
    sql_err
        .as_database_error()
        .filter(|e| e.code().as_ref().map(|c| c.as_ref()) == Some("55P03"))
        .is_some()
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
