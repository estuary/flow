use crate::config::{RefreshToken, ENDPOINT, PUBLIC_TOKEN};
use crate::{api_exec, CliContext};
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
// TODO(johnny): This really needs a deep overhaul. We're not updating refresh
// tokens as we should be, and the structure of the Config is problematic.
// I'm resisting refactoring it more substantially right now, but it needs it.
pub(crate) async fn new_client(ctx: &mut CliContext) -> anyhow::Result<Client> {
    match ctx.config_mut().api {
        Some(ref mut api) => {
            let client = postgrest::Postgrest::new(api.endpoint.as_str());
            let client = client.insert_header("apikey", &api.public_token);

            // Try to give users a more friendly error message if we know their credentials are expired.
            if let Err(e) = check_access_token(&api.access_token) {
                if let Some(refresh_token) = &api.refresh_token {
                    let response = api_exec::<AccessTokenResponse>(client.rpc(
                        "generate_access_token",
                        format!(
                            r#"{{"refresh_token_id": "{}", "secret": "{}"}}"#,
                            refresh_token.id, refresh_token.secret
                        ),
                    ))
                    .await?;
                    api.access_token = response.access_token;
                } else {
                    return Err(e);
                }
            }
            let client =
                client.insert_header("Authorization", format!("Bearer {}", &api.access_token));
            Ok(Client(Arc::new(client)))
        }
        None => {
            // If there has been no prior login, but FLOW_AUTH_TOKEN is available, we use that to
            // generate an access_token and automatically login the user
            if let Ok(env_token) = std::env::var(FLOW_AUTH_TOKEN) {
                let client = postgrest::Postgrest::new(ENDPOINT);
                let client = client.insert_header("apikey", PUBLIC_TOKEN);

                let refresh_token = RefreshToken::from_base64(&env_token)?;
                let response = api_exec::<AccessTokenResponse>(client.rpc(
                    "generate_access_token",
                    format!(
                        r#"{{"refresh_token_id": "{}", "secret": "{}"}}"#,
                        refresh_token.id, refresh_token.secret
                    ),
                ))
                .await?;

                let _jwt = check_access_token(&response.access_token)?;
                ctx.config_mut()
                    .set_access_token(response.access_token.clone());

                let client = client
                    .insert_header("Authorization", format!("Bearer {}", response.access_token));
                Ok(Client(Arc::new(client)))
            } else {
                tracing::warn!("You are not authenticated. Run `auth login` to login to Flow.");

                let client = postgrest::Postgrest::new(ENDPOINT);
                let client = client.insert_header("apikey", PUBLIC_TOKEN);
                Ok(Client(Arc::new(client)))
            }
        }
    }
}

pub async fn configure_new_access_token(
    ctx: &mut CliContext,
    access_token: String,
) -> anyhow::Result<()> {
    let jwt = check_access_token(&access_token)?;
    ctx.config_mut().set_access_token(access_token);
    let client = ctx.controlplane_client().await?;

    let refresh_token = api_exec::<RefreshToken>(client.rpc(
        "create_refresh_token",
        r#"{"multi_use": true, "valid_for": "90d", "detail": "Created by flowctl"}"#,
    ))
    .await?;
    ctx.config_mut().set_refresh_token(refresh_token);

    let message = if let Some(email) = jwt.email {
        format!("Configured access token for user '{email}'")
    } else {
        "Configured access token".to_string()
    };
    println!("{}", message);
    Ok(())
}

fn check_access_token(access_token: &str) -> anyhow::Result<JWT> {
    let jwt = parse_jwt(access_token).context("invalid access_token")?;
    // Try to give users a more friendly error message if we know their credentials are expired.
    if jwt.is_expired() {
        anyhow::bail!("access token is expired, please re-authenticate and then try again");
    }
    Ok(jwt)
}

const FLOW_AUTH_TOKEN: &str = "FLOW_AUTH_TOKEN";
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
