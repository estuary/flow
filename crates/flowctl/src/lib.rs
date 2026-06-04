use clap::Parser;
use std::fmt::Debug;

mod alerts;
mod auth;
mod catalog;
mod collection;
mod config;
mod dataplane;
mod discover;
mod draft;
mod generate;
mod graphql;
mod local_specs;
mod ops;
mod output;
mod poll;
mod preview;
mod raw;
mod shuffle_read;
mod version;

use models::authorizations::ControlClaims;
use output::{Output, OutputType};
use poll::poll_while_queued;

/// A command-line tool for working with Estuary Flow.
#[derive(Debug, Parser)]
#[command(author, about, version, next_display_order = None)]
pub struct Cli {
    /// Configuration profile to use.
    ///
    /// Profile are distinct configurations of the `flowctl` tool, and are
    /// completely optional. Use multiple profiles to track multiple accounts
    /// or development endpoints.
    #[clap(long, default_value = "default", env = "FLOWCTL_PROFILE")]
    profile: String,

    #[clap(subcommand)]
    cmd: Command,

    #[clap(flatten)]
    output: Output,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Command {
    /// View and manage alert subscriptions and per-prefix alert configuration
    Alerts(alerts::Alerts),
    /// View and manage subscriptions to alerts and notifications.
    /// (Deprecated: use `flowctl alerts subscriptions` instead.)
    #[clap(hide = true)]
    AlertSubscriptions(alerts::subscriptions::AlertSubscriptions),
    /// Authenticate with Flow.
    Auth(auth::Auth),
    /// Work with the current Flow catalog.
    Catalog(catalog::Catalog),
    /// Work with Flow collections.
    Collections(collection::Collections),
    /// Discover capture bindings from an endpoint.
    ///
    /// Discover runs a discovery operation against a capture's endpoint,
    /// submitting the job to the control plane which runs it on a data-plane.
    /// The discovered bindings are then written to your local Flow catalog files.
    Discover(discover::Discover),
    /// Generate derivation project files and implementation stubs.
    ///
    /// Generate walks your local Flow catalog source file and its imports
    /// to gather collections, derivations, and associated JSON schemas.
    /// Your derivations generate associated project files and supplemental
    /// type implementations which are then written into your project directory,
    /// which is the top-level directory having a flow.yaml or flow.json file.
    ///
    /// You then edit the generated stubs in your preferred editor to fill
    /// out implementations for your derivation transform lambdas.
    Generate(generate::Generate),
    /// Locally run and preview a capture, derivation, or materialization.
    ///
    /// Preview runs a temporary, local instance of your task.
    /// Capture tasks emit captured data to stdout.
    /// Derivations read documents from your source collections, run them
    /// through your derivation connector, and emit the results to stdout.
    /// Materializations read documents from your source collection and
    /// update the configured endpoint system.
    ///
    /// When reading live collection data, preview will process all of your
    /// historical source data and thereafter will emit ongoing updates.
    /// Or, derivations and materializations may alternatively provide a data
    /// --fixture of collection documents to derive or materialize, which is
    /// useful in automated testing contexts.
    ///
    /// Preview provides various knobs for tuning timeouts, simulating back
    /// pressure, and running multiple sessions to exercise connector resumption
    /// and crash recovery, which may be helpful when developing connectors.
    ///
    /// WARNING: previews of captures and materializations make live changes
    /// to their configured endpoints. Be sure that your task does not conflict
    /// or collide with a live task running in the Flow managed service.
    Preview(preview::Preview),
    /// Work with your Flow catalog drafts.
    ///
    /// Drafts are in-progress specifications which are not yet "live".
    /// They can be edited, developed, and tested while still a draft.
    /// Then when you're ready, publish your draft to make your changes live.
    Draft(draft::Draft),
    /// Read operational logs of your tasks (captures, derivations, and materializations).
    Logs(ops::Logs),
    /// Advanced, low-level, and experimental commands which are less common.
    Raw(raw::Advanced),
}

pub struct CliContext {
    /// Base PostgREST client carrying the public `apikey` header. The user's
    /// bearer token is applied per-request via `access_token()`.
    pg: postgrest::Postgrest,
    /// REST client for the control-plane agent API.
    rest: flow_client_next::rest::Client,
    /// Live, auto-refreshing watch of the user's access & refresh tokens.
    user_tokens: tokens::PendingWatch<flow_client_next::user_auth::UserToken>,
    /// Router shared by data-plane (journal/shard) clients.
    router: gazette::Router,
    /// Loaded configuration of the active profile (endpoints, tokens, selected draft).
    config: config::Config,
    /// Selected output format (table / JSON / YAML) for command results.
    output: output::Output,
    /// Tracks in-flight work units; cloned into preview-next connector drivers.
    registry: service_kit::Registry,
}

impl CliContext {
    fn write_all<I, T>(&mut self, items: I, table_alt: T::TableAlt) -> anyhow::Result<()>
    where
        T: output::CliOutput,
        I: IntoIterator<Item = T>,
    {
        match self.get_output_type() {
            OutputType::Json => output::print_json(items),
            OutputType::Yaml => output::print_yaml(items),
            OutputType::Table => output::print_table(table_alt, items),
        }
    }

    fn get_output_type(&mut self) -> OutputType {
        use crossterm::tty::IsTty;

        if let Some(ty) = self.output.output {
            ty
        } else {
            if std::io::stdout().is_tty() {
                OutputType::Table
            } else {
                OutputType::Yaml
            }
        }
    }

    /// The user's current access token from the live token watch, or None if
    /// the user is not authenticated.
    pub(crate) fn access_token(&self) -> Option<String> {
        self.user_tokens
            .watch()
            .token()
            .result()
            .ok()
            .and_then(|t| t.access_ref().map(str::to_string))
    }

    /// Parses the user access token and returns the deserialized claims.
    /// This does not check the validity of the token in any way. As long
    /// the claims can be deserialized, they will be returned as they are.
    fn require_control_claims(&self) -> anyhow::Result<ControlClaims> {
        let Some(token) = self.access_token() else {
            anyhow::bail!("you must be logged in in order to do this. Try `flowctl auth login`");
        };
        let claims = tokens::jwt::parse_unverified::<ControlClaims>(token.as_bytes())?
            .claims()
            .clone();
        Ok(claims)
    }
}

impl Cli {
    pub async fn run(&self, registry: service_kit::Registry) -> anyhow::Result<()> {
        let config = config::Config::load(&self.profile)?;
        let output = self.output.clone();

        let pg = config.build_pg();
        let rest = config.build_rest();
        let router = gazette::Router::new("local");

        // An ambient FLOW_AUTH_TOKEN, if present, overrides the profile's stored
        // tokens. It's used but never persisted (it may still rotate in memory).
        let env_token = config::Config::env_user_token()?;

        // Resolve the watch's initial tokens, whether we may mint a new refresh
        // token (`may_create`, only when explicitly acquiring credentials), and
        // whether the tokens came from the environment (`from_env`, never
        // persisted).
        let (initial_tokens, may_create, from_env) = if let Command::Auth(auth) = &self.cmd
            && auth.acquires_credential()
        {
            // Reject an ambient FLOW_AUTH_TOKEN when acquiring credentials: on a
            // next invocation, it would silently shadow the credential that the
            // user is explicitly providing in this one.
            if env_token.is_some() {
                anyhow::bail!(
                    "FLOW_AUTH_TOKEN is set in your environment and would shadow the credentials you're establishing.\n\
                 Unset FLOW_AUTH_TOKEN and re-run."
                );
            }
            // Allow creation of a new refresh token from the acquired access token.
            (auth.acquire_credential(&config).await?, true, false)
        } else if let Some(env_token) = env_token {
            (env_token, false, true)
        } else {
            (
                flow_client_next::user_auth::UserToken {
                    access_token: config.user_access_token.clone(),
                    refresh_token: config.user_refresh_token.clone(),
                },
                false,
                false,
            )
        };

        // Live, auto-refreshing user-token watch. If `may_create`, the watch
        // will establish a new refresh token. Otherwise, a bare access token is
        // surfaced as-is and allowed to expire, never minting a refresh token.
        let user_tokens = tokens::watch(flow_client_next::user_auth::UserTokenSource {
            pg_client: pg.clone(),
            tokens: initial_tokens,
            may_create,
        });

        // Force the first refresh (creates a refresh token from a bare access
        // token when establishing, or exchanges an existing refresh token),
        // then persist the result so even short-lived commands durably store a
        // freshly-created refresh token. Env-provided tokens are never persisted.
        let user_tokens_watch = user_tokens.clone().ready_owned().await;
        match user_tokens_watch.token().result() {
            Ok(tokens) if tokens.access_token.is_some() && !from_env => {
                config::Config::persist_tokens(&self.profile, tokens)?;
            }
            Ok(_) => {} // Anonymous, or env-provided: nothing to persist.
            Err(status) => anyhow::bail!(
                "Failed to authenticate (run `flowctl auth login`): {}",
                status.message()
            ),
        }

        // Observe token rotations and persist them to config as they happen,
        // rather than only at process exit. Important for single-use refresh
        // tokens, where a consumed-but-unsaved token would lock the user out.
        // Skipped for env-provided tokens, which must never reach the profile.
        let token_observer_stop = tokens::CancellationToken::new();
        let observer = if from_env {
            None
        } else {
            Some(tokio::spawn(persist_token_rotations(
                self.profile.clone(),
                user_tokens_watch.clone(),
                token_observer_stop.clone(),
            )))
        };

        let mut context = CliContext {
            pg,
            rest,
            user_tokens,
            router,
            config,
            output,
            registry,
        };

        // Version check runs concurrently with the command
        let version_check = tokio::spawn(version::check_latest());

        let result = match &self.cmd {
            Command::Alerts(alerts) => alerts.run(&mut context).await,
            Command::AlertSubscriptions(subs) => subs.run(&mut context).await,
            Command::Auth(auth) => auth.run(&mut context).await,
            Command::Catalog(catalog) => catalog.run(&mut context).await,
            Command::Collections(collection) => collection.run(&mut context).await,
            Command::Discover(discover) => discover.run(&mut context).await,
            Command::Generate(generate) => generate.run(&mut context).await,
            Command::Preview(preview) => preview.run(&mut context).await,
            Command::Draft(draft) => draft.run(&mut context).await,
            Command::Logs(logs) => logs.run(&mut context).await,
            Command::Raw(advanced) => advanced.run(&mut context).await,
        };

        // Print before `result?` so the warning is visible even when the command fails
        if let Ok(Some(latest)) = version_check.await {
            tracing::warn!(
                "You're running an outdated version of flowctl — please update to {latest}",
            );
        }

        // Stop the rotation observer and await its final write before persisting
        // the rest of the config, so the two writers never race. The observer
        // returns the freshest tokens it persisted; `Some(observer)` is exactly
        // the "we were persisting" case — env-provided tokens spawn no observer.
        token_observer_stop.cancel();
        let observed_tokens = match observer {
            Some(observer) => observer.await.ok().flatten(),
            None => None,
        };

        result?;

        // Adopt the observer's final tokens so writing the remaining config state
        // (e.g. selected draft) doesn't clobber a rotation that happened during
        // the run.
        if let Some(tokens) = observed_tokens {
            context.config.user_access_token = tokens.access_token;
            context.config.user_refresh_token = tokens.refresh_token;
        }
        context.config.write(&self.profile)?;

        Ok(())
    }
}

/// Background task that persists user-token rotations to the named profile's
/// config as they occur. Stops when `stop` is cancelled (orderly shutdown),
/// returning the freshest tokens it persisted — or `None` if it never observed a
/// valid token (e.g. an anonymous session).
async fn persist_token_rotations(
    profile: String,
    user_tokens_watch: std::sync::Arc<dyn tokens::Watch<flow_client_next::user_auth::UserToken>>,
    stop: tokens::CancellationToken,
) -> Option<flow_client_next::user_auth::UserToken> {
    let mut latest = None;
    loop {
        let refresh = user_tokens_watch.token();

        // Guard against clobbering good on-disk tokens with a `None` access
        // token (e.g. an anonymous startup watch that never authenticated).
        if let Ok(tokens) = refresh.result() {
            if tokens.access_token.is_some() {
                if let Err(err) = config::Config::persist_tokens(&profile, tokens) {
                    tracing::warn!(?err, "failed to persist rotated tokens to config");
                }
                latest = Some(tokens.clone());
            }
        }

        tokio::select! {
            _ = refresh.expired() => continue, // Superseded by a newer rotation.
            _ = stop.cancelled() => break,
        }
    }
    latest
}

// new_table builds a comfy_table with UTF8 styling.
fn new_table(headers: Vec<&str>) -> comfy_table::Table {
    let mut table = comfy_table::Table::new();
    table
        .load_preset(comfy_table::presets::UTF8_FULL)
        .apply_modifier(comfy_table::modifiers::UTF8_ROUND_CORNERS)
        .apply_modifier(comfy_table::modifiers::UTF8_SOLID_INNER_BORDERS);

    table.set_header(headers);
    table
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone)]
pub struct Timestamp(#[serde(with = "time::serde::rfc3339")] time::OffsetDateTime);

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ts = self.0.replace_nanosecond(0).unwrap();
        let ts = ts
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        f.write_str(&ts)
    }
}

impl Timestamp {
    pub fn from_unix_timestamp(epoch_time_seconds: i64) -> Result<Timestamp, anyhow::Error> {
        let offset_date_time = time::OffsetDateTime::from_unix_timestamp(epoch_time_seconds)?;
        Ok(Timestamp(offset_date_time))
    }
}

/// Parse an RFC-3339 timestamp, for use as a clap `value_parser` backing
/// `--not-before` style flags.
pub(crate) fn parse_rfc3339(s: &str) -> Result<time::OffsetDateTime, String> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .map_err(|err| format!("invalid RFC-3339 timestamp: {err}"))
}

/// Resolve the mutually-exclusive `--since` (a duration before now) and
/// `--not-before` (an absolute timestamp) flags into an absolute lower-bound
/// time, or None when neither is set.
pub(crate) fn resolve_not_before(
    since: Option<humantime::Duration>,
    not_before: Option<time::OffsetDateTime>,
) -> Option<time::OffsetDateTime> {
    if let Some(not_before) = not_before {
        Some(not_before)
    } else {
        since.map(|since| time::OffsetDateTime::now_utc() - *since)
    }
}

fn format_user(email: Option<String>, full_name: Option<String>, id: Option<uuid::Uuid>) -> String {
    format!(
        "{full_name} <{email}>\n{id}",
        full_name = full_name.unwrap_or_default(),
        email = email.unwrap_or_default(),
        id = id.map(|id| id.to_string()).unwrap_or_default(),
    )
}

/// Returns a default list of prefixes to use for commands that accept an
/// optional prefix argument. This will return all of the distinct prefixes that
/// the user has at least `min_capability` to. If the user has access to more
/// than `max_count` distinct prefixes, an error will be returned that guides
/// the user to specify a prefix explicitly. This function will never return an
/// empty vec, and will instead return an error if the user does not have access
/// to any prefixes. Note that any `ops/dp/` role grants are ignored.
async fn get_default_prefix_arguments(
    ctx: &mut CliContext,
    min_capability: models::Capability,
    max_count: usize,
) -> anyhow::Result<Vec<String>> {
    // We fetch at least twice the number of roles as requested, so that we can
    // filter out any duplicates and still probably have enough to fill out the
    // list.
    let role_list =
        crate::auth::list::list_authorized_prefixes(ctx, min_capability, (max_count * 2).max(10))
            .await?;

    let prefixes = filter_default_prefixes(role_list, max_count)?;
    if prefixes.is_empty() {
        anyhow::bail!(
            "the current user does not have access to any catalog prefixes, please ask your tenant administrator for help"
        );
    }
    tracing::debug!(
        ?prefixes,
        "no prefix argument provided, determined prefixes automatically"
    );
    Ok(prefixes)
}

/// Accepts a listing of the users role grants, and returns a deduplicated list
/// of prefixes, having a length <= `max`.
fn filter_default_prefixes(
    mut role_list: Vec<auth::list::AuthorizedPrefix>,
    max: usize,
) -> anyhow::Result<Vec<String>> {
    // Filter out `ops/dp/` prefixes because there are never any live specs under that prefix.
    role_list.retain(|r| !r.prefix.starts_with("ops/dp/"));

    // Sort the remaining roles so that we can remove redundant prefixes. Top-level
    // prefixes will sort first, so we can ignore, e.g. `tenant/nested/` if there's
    // also a `tenant/` grant.
    role_list.sort_by(|l, r| l.prefix.cmp(&r.prefix));

    let mut prefixes: Vec<String> = Vec::new();
    for candidate in role_list {
        if prefixes
            .last()
            .is_some_and(|last| candidate.prefix.starts_with(last.as_str()))
        {
            continue;
        }
        prefixes.push(candidate.prefix.to_string());
    }

    if prefixes.len() > max {
        let max_str = if max > 1 {
            format!("{max} prefixes")
        } else {
            "one prefix".to_string()
        };
        anyhow::bail!(
            "an explicit prefix argument is required since you have access to more than {max_str}.\nRun `flowctl auth roles list` to see the prefixes you can access"
        );
    }
    Ok(prefixes)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::auth::list::AuthorizedPrefix;

    #[test]
    fn test_filter_default_prefixes() {
        fn pre(prefix: &str) -> AuthorizedPrefix {
            AuthorizedPrefix {
                prefix: models::Prefix::new(prefix),
                user_capability: models::Capability::Admin, // irrelevant
            }
        }
        let roles = vec![
            pre("wileyCo/"),
            pre("acmeCo/prod/anvils/"),
            pre("acmeCo/dev/anvils/"),
            pre("acmeCo/dev/tnt/"),
            pre("acmeCo/"),
            pre("acmeCo/prod/"),
            pre("acmeCo/foo/"),
            pre("coyoteCo/"),
        ];
        let result = filter_default_prefixes(roles.clone(), 3).expect("should return 3 prefixes");
        assert_eq!(
            vec![
                "acmeCo/".to_string(),
                "coyoteCo/".to_string(),
                "wileyCo/".to_string(),
            ],
            result
        );

        let fail_result = filter_default_prefixes(roles, 2);
        assert!(fail_result.is_err());
    }
}
