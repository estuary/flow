//! User credentials, resolved exactly as flowctl resolves them: an ambient
//! `FLOW_AUTH_TOKEN` overrides the named flowctl profile. Credentials are read
//! and never written, and never reach the run directory.

use anyhow::Context;
use flow_client_next::user_auth::UserToken;

/// Control-plane clients and a live, auto-refreshing user-token watch.
pub struct Session {
    pub pg: postgrest::Postgrest,
    pub rest: flow_client_next::rest::Client,
    pub tokens: tokens::PendingWatch<UserToken>,
}

/// Resolve credentials from `FLOW_AUTH_TOKEN` or else `profile`, and await a
/// first access token.
///
/// Every sidecar of a run resolves and refreshes credentials independently, so
/// a single-use refresh token (which each exchange rotates) would have them
/// invalidating one another. It's rejected here if its first exchange happens
/// at startup: always for a `FLOW_AUTH_TOKEN` refresh token, which comes
/// without an access token. A profile's still-valid access token defers the
/// exchange, and so the check. Tokens which flowctl creates are multi-use.
pub async fn start(profile: &str) -> anyhow::Result<Session> {
    let config = flowctl::config::Config::load(profile)?;

    let initial = match flowctl::config::Config::env_user_token()? {
        Some(token) => token,
        None => UserToken {
            access_token: config.user_access_token.clone(),
            refresh_token: config.user_refresh_token.clone(),
        },
    };
    anyhow::ensure!(
        initial.access_token.is_some() || initial.refresh_token.is_some(),
        "no credentials: set FLOW_AUTH_TOKEN, or pass a --profile having a refresh token"
    );
    let initial_refresh = initial.refresh_token.clone();

    let pg = config.build_pg();
    let tokens = tokens::watch(flow_client_next::user_auth::UserTokenSource {
        pg_client: pg.clone(),
        tokens: initial,
        may_create: false,
    });
    let ready = tokens.clone().ready_owned().await;
    let token = ready.token();
    let token = token
        .result()
        .map_err(|status| anyhow::anyhow!("authenticating: {}", status.message()))?;

    if let (Some(before), Some(after)) = (&initial_refresh, &token.refresh_token) {
        anyhow::ensure!(
            before.id == after.id && before.secret == after.secret,
            "the refresh token is single-use, and was rotated by its first exchange. \
             Provide a multi-use refresh token (as `flowctl auth token` creates)"
        );
    }

    Ok(Session {
        pg,
        rest: config.build_rest(),
        tokens,
    })
}

impl Session {
    pub fn access_token(&self) -> anyhow::Result<String> {
        self.tokens
            .watch()
            .token()
            .result()
            .ok()
            .and_then(|t| t.access_ref().map(str::to_string))
            .context("no current access token")
    }
}
