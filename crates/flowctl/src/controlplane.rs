use crate::config::{Config, RefreshToken};
use crate::{CliContext, api_exec};

use anyhow::Context;
use serde::Deserialize;
use std::fmt::{self, Debug};
use std::ops::Deref;
use std::sync::Arc;

/// A wafer-thin wrapper around a `Postgrest` client that makes it
/// cheaply cloneable and implements `Debug`. This allows us to create
/// a client once and then store it in the `CliContext` for future re-use.
/// This client implements `Deref<Target=Postgrest>`, so you can use it
/// just like you would the normal `Postgrest` client.
#[derive(Clone)]
pub struct Client(Arc<postgrest::Postgrest>);

impl Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // We can't really give a better debug impl since Postgrest
        // keeps all of its members private.
        f.write_str("controlplane::Client")
    }
}

impl Deref for Client {
    type Target = postgrest::Postgrest;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

#[derive(Deserialize)]
struct AccessTokenResponse {
    access_token: String,
}

/// Creates a new client. **you should instead call `CliContext::controlplane_client(&mut Self)`**, which
/// will re-use the existing client if possible.
pub(crate) async fn new_client(config: &mut Config) -> anyhow::Result<Client> {
    match &mut config.api {
        Some(api) => {
            let client = postgrest::Postgrest::new(api.endpoint.as_str());
            let client = client.insert_header("apikey", &api.public_token);

            // Try to give users a more friendly error message if we know their credentials are expired.
            if let Err(e) = check_access_token(&api.access_token) {
                if let Some(refresh_token) = &api.refresh_token {
                    let response = api_exec::<AccessTokenResponse>(
                        client.rpc("generate_access_token", format!(r#"{{"refresh_token_id": "{}", "secret": "{}"}}"#, refresh_token.id, refresh_token.secret))
                    ).await?;
                    api.access_token = response.access_token;
                } else {
                    return Err(e)
                }
            }
            let client =
                client.insert_header("Authorization", format!("Bearer {}", &api.access_token));
            Ok(Client(Arc::new(client)))
        }
        None => {
            anyhow::bail!("You must run `auth login` first")
        }
    }
}

pub async fn configure_new_access_token(ctx: &mut CliContext, token: String) -> anyhow::Result<()> {
    // try to catch issues caused by missing or extra data that may have been accidentally copied
    let jwt = check_access_token(&token)?;
    ctx.config_mut().set_access_token(token);
    let client = ctx.controlplane_client().await?;
    let refresh_token = api_exec::<RefreshToken>(
        client.rpc("create_refresh_token", r#"{"multi_use": true, "valid_for": "14d", "detail": "Created by flowctl"}"#)
    ).await?;
    ctx.config_mut().set_refresh_token(refresh_token);

    let message = if let Some(email) = jwt.email {
        format!("Configured access token for user '{email}'")
    } else {
        "Configured access token".to_string()
    };
    println!("{}", message);
    Ok(())
}

fn check_access_token(token: &str) -> anyhow::Result<JWT> {
    let jwt = parse_jwt(token).context("invalid access_token")?;
    // Try to give users a more friendly error message if we know their credentials are expired.
    if jwt.is_expired() {
        anyhow::bail!("access token is expired, please re-authenticate and then try again");
    }
    Ok(jwt)
}

#[derive(Deserialize)]
struct JWT {
    exp: i64,
    email: Option<String>,
}

impl JWT {
    fn is_expired(&self) -> bool {
        let exp = time::OffsetDateTime::from_unix_timestamp(self.exp).unwrap_or_else(|err| {
            tracing::error!(exp = self.exp, error = %err, "invalid exp in JWT");
            time::OffsetDateTime::UNIX_EPOCH
        });
        time::OffsetDateTime::now_utc() >= exp
    }
}

fn parse_jwt(jwt: &str) -> anyhow::Result<JWT> {
    let payload = jwt
        .split('.')
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("invalid JWT"))?;
    let json_data =
        base64::decode_config(payload, base64::URL_SAFE_NO_PAD).context("invalid JWT")?;
    let data: JWT = serde_json::from_slice(&json_data).context("parsing JWT data")?;
    Ok(data)
}
