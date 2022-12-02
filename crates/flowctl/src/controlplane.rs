use crate::config::Config;
use crate::CliContext;

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

/// Creates a new client. **you should instead call `CliContext::controlplane_client(&mut Self)`**, which
/// will re-use the existing client if possible.
pub(crate) fn new_client(config: &Config) -> anyhow::Result<Client> {
    match &config.api {
        Some(api) => {
            // Try to give users a more friendly error message if we know their credentials are expired.
            check_access_token(&api.access_token)?;
            let client = postgrest::Postgrest::new(api.endpoint.as_str());
            let client = client.insert_header("apikey", &api.public_token);
            let client =
                client.insert_header("Authorization", format!("Bearer {}", &api.access_token));
            Ok(Client(Arc::new(client)))
        }
        None => {
            anyhow::bail!("You must run `auth login` first")
        }
    }
}

pub fn configure_new_access_token(ctx: &mut CliContext, token: String) -> anyhow::Result<()> {
    // try to catch issues caused by missing or extra data that may have been accidentally copied
    let email = check_access_token(&token)?;
    ctx.config_mut().set_access_token(token);
    println!("Configured access token for user: '{email}'");
    Ok(())
}

fn check_access_token(token: &str) -> anyhow::Result<String> {
    let jwt = parse_jwt(token).context("invalid access_token")?;
    // Try to give users a more friendly error message if we know their credentials are expired.
    if jwt.is_expired() {
        anyhow::bail!("access token is expired, please re-authenticate and then try again");
    }
    Ok(jwt.email)
}

#[derive(Deserialize)]
struct JWT {
    email: String,
    exp: i64,
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
