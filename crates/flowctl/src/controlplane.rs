use crate::{config, CliContext};
use anyhow::Context;
use keyring::Entry;
use serde::{Deserialize, Serialize};

/// A structure containing credentials that can be refreshed. This is represented
/// in serialized form as a base64 encoded JSON object, which is provided by the UI.
#[derive(Deserialize, Serialize)]
struct RefreshableToken {
    /// JWT that is used in the Authorization header for requests to the control plane.
    pub access_token: String,
    /// Can be exchanged for a brand new `RefreshableToken`
    pub refresh_token: String,
    /// The expiration time of the token, unix timestamp in UTC with second precision.
    pub expires_at: i64,
}

impl RefreshableToken {
    /// Returns true if the token expires within `renew_before` of the current time.
    pub fn expires_within(&self, renew_before: time::Duration) -> bool {
        let expiry =
            time::OffsetDateTime::from_unix_timestamp(self.expires_at).unwrap_or_else(|err| {
                tracing::error!(
                    error = %err,
                    expires_at = self.expires_at,
                    "invalid expires_at on auth token"
                );
                time::OffsetDateTime::UNIX_EPOCH // we'll consider this to be expired
            });

        expiry.saturating_sub(renew_before) < time::OffsetDateTime::now_utc()
    }

    pub fn from_base64(encoded_token: &str) -> anyhow::Result<RefreshableToken> {
        let decoded = base64::decode(encoded_token).context("invalid base64")?;
        let tk: RefreshableToken = serde_json::from_slice(&decoded)?;
        Ok(tk)
    }

    pub fn to_base64(&self) -> anyhow::Result<String> {
        let ser = serde_json::to_vec(self)?;
        Ok(base64::encode(&ser))
    }
}

impl From<RefreshResponse> for RefreshableToken {
    fn from(response: RefreshResponse) -> RefreshableToken {
        let expires_at = time::OffsetDateTime::now_utc()
            .saturating_add(time::Duration::seconds(response.expires_in))
            .unix_timestamp();
        RefreshableToken {
            access_token: response.access_token,
            refresh_token: response.refresh_token,
            expires_at,
        }
    }
}

/// private type that matches the response body from the /auth/v1/token endpoint
#[derive(Deserialize)]
struct RefreshResponse {
    // Secret access token of the control-plane API.
    pub access_token: String,
    // Secret refresh token of the control-plane API.
    pub refresh_token: String,
    // Seconds to expiry of access_token
    pub expires_in: i64,
}

fn should_refresh_credentials(credential: &RefreshableToken) -> bool {
    // Add some padding to the expiration in order to deal with potentially
    // long-running operations or imprecision in the expiration time.
    // The environment variable is here to allow for testing the refresh.
    credential.expires_within(time::Duration::minutes(10))
        || std::env::var("FLOWCTL_FORCE_TOKEN_REFRESH").is_ok()
}

/// Create and configure a new client with authentication headers setup for the current user.
/// The current user is identified by the presence of the `user_email` in the config.
/// This requires that the user has previously authenticated `flowctl`.
/// The credentials will be refreshed automatically if required.
/// Credentials are stored in the system keychain. This means that the user may be prompted
/// by the OS when this function is called.
pub async fn new_authenticated_client(
    ctx: &mut CliContext,
) -> anyhow::Result<postgrest::Postgrest> {
    // if the access token is provided by the env variable, then don't try to
    // look up credentials from the system keychain. This allows using flowctl
    // in environments that don't have a keychain, but it's not really intended
    // to support service accounts, since we don't yet know what those will look like.
    if let Ok(access_token) = std::env::var("FLOWCTL_ACCESS_TOKEN") {
        return Ok(client_from_env_credential(ctx.config(), access_token));
    }

    // At this point, we're expecting that credentials must be present in the keychain.
    // They may be expired, though, in which case we'll attempt to refresh them.
    let api_config = ctx.config().api.as_ref().ok_or_else(|| {
        anyhow::anyhow!("missing api configuration, did you forget to run `flowctl auth login`?")
    })?;

    let user_email = api_config.user_email.as_ref().ok_or_else(|| {
        anyhow::anyhow!("config is missing user_email, did you forget to run `flowctl auth login`?")
    })?;

    let mut credentials = retrieve_credentials(&api_config.endpoint, &user_email).await?;
    if should_refresh_credentials(&credentials) {
        tracing::debug!(
            expires_at = credentials.expires_at,
            "current credentials need to be refreshed"
        );
        credentials = fetch_new_credential(&api_config, &credentials.refresh_token)
            .await
            .context("failed to refresh user credentials")?;

        persist_credentials(&api_config.endpoint, &user_email, &credentials).await?;
        tracing::debug!(
            expires_at = credentials.expires_at,
            "successfully refreshed credential"
        );
    }

    Ok(new_client_with_credentials(
        &api_config.endpoint,
        &api_config.public_token,
        &credentials.access_token,
    ))
}

fn client_from_env_credential(
    config: &config::Config,
    access_token: String,
) -> postgrest::Postgrest {
    let api_config = config
        .api
        .as_ref()
        .cloned()
        .unwrap_or_else(config::API::production);
    new_client_with_credentials(
        &api_config.endpoint,
        &api_config.public_token,
        &access_token,
    )
}

fn new_client_with_credentials(
    url: &url::Url,
    public_token: &str,
    access_token: &str,
) -> postgrest::Postgrest {
    postgrest::Postgrest::new(url.to_string())
        .insert_header("apikey", public_token)
        .insert_header("Authorization", format!("Bearer {}", access_token))
}

async fn fetch_new_credential(
    api_config: &config::API,
    refresh_token: &str,
) -> anyhow::Result<RefreshableToken> {
    let mut refresh_url = api_config.endpoint.clone();
    refresh_url.set_path("/auth/v1/token");
    refresh_url.set_query(Some("grant_type=refresh_token"));

    let client = reqwest::Client::new();

    let response = client
        .post(refresh_url)
        .json(&serde_json::json!({ "refresh_token": refresh_token }))
        .header("apikey", &api_config.public_token)
        .send()
        .await?;

    tracing::debug!(headers = ?response.headers(), "got resp headers");
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        anyhow::bail!(
            "Failed to refresh credentials, status: {:?}, response:\n{}",
            status,
            body
        );
    }

    let resp: RefreshResponse =
        serde_json::from_str(&body).context("failed to deserialize token refresh response")?;
    Ok(RefreshableToken::from(resp))
}

pub async fn configure_new_credential(
    ctx: &mut crate::CliContext,
    token: &str,
) -> anyhow::Result<()> {
    // verify that the provided token is valid by decoding it and attempting to make a call
    let api_config = ctx
        .config()
        .api
        .as_ref()
        .cloned()
        .unwrap_or_else(config::API::production);

    let auth = RefreshableToken::from_base64(token).context("unable to parse credential")?;
    let user_email = parse_jwt(&auth.access_token)
        .context("failed to parse jwt")?
        .email;

    persist_credentials(&api_config.endpoint, &user_email, &auth).await?;

    // Only update the config if the email is actually different than what's there.
    if Some(&user_email) != api_config.user_email.as_ref() {
        let mut new_cfg = api_config;
        new_cfg.user_email = Some(user_email);
        ctx.config_mut().api = Some(new_cfg);
    }
    Ok(())
}

async fn retrieve_credentials(
    endpoint: &url::Url,
    user_email: &str,
) -> anyhow::Result<RefreshableToken> {
    let keychain_entry = keychain_auth_entry(endpoint, user_email)?;
    let user_email = user_email.to_string(); // clone so we can use it in the closure
                                             // We use spawn_blocking because accessing the keychain is a blocking call, which may prompt the user
                                             // to allow the access. This could take quite some time, and we don't want to block the executor
                                             // during that period. Note that creating the Entry explicitly does not try to access the keychain.
    let handle = tokio::task::spawn_blocking::<_, anyhow::Result<RefreshableToken>>(move || {
        match keychain_entry.get_password() {
            Err(keyring::Error::NoEntry) => {
                // This is expected to be a common error case, so provide a helpful message.
                anyhow::bail!("no credentials found for user '{user_email}', did you forget to run `flowctl auth login`?");
            }
            Ok(auth) => {
                tracing::debug!("retrieved credentials from keychain");
                let token = RefreshableToken::from_base64(&auth)?;
                Ok(token)
            }
            Err(err) => {
                Err(anyhow::Error::new(err).context("retrieving user credentials from keychain"))
            }
        }
    });
    let cred = handle.await??;
    Ok(cred)
}

async fn persist_credentials(
    endpoint: &url::Url,
    user_email: &str,
    credential: &RefreshableToken,
) -> anyhow::Result<()> {
    let entry = keychain_auth_entry(endpoint, user_email)?;
    let token = credential.to_base64()?;
    // See comment in `retrieve_credentials` for why we need to use `spawn_blocking`
    let handle = tokio::task::spawn_blocking::<_, anyhow::Result<()>>(move || {
        entry
            .set_password(&token)
            .context("persisting user credential to keychain")?;
        tracing::info!("successfully persisted credential in keychain");
        Ok(())
    });
    handle.await??;
    Ok(())
}

fn keychain_auth_entry(endpoint: &url::Url, user_email: &str) -> anyhow::Result<Entry> {
    let hostname = endpoint.domain().ok_or_else(|| {
        anyhow::anyhow!("configured endpoint url '{}' is missing a domain", endpoint)
    })?;
    let entry_name = format!("estuary.dev-flowctl-{hostname}");
    Ok(Entry::new(&entry_name, user_email))
}

#[derive(Deserialize)]
struct JWT {
    email: String,
}

fn parse_jwt(jwt: &str) -> anyhow::Result<JWT> {
    let payload = jwt
        .split('.')
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("invalid JWT"))?;
    let json_data =
        base64::decode_config(payload, base64::STANDARD_NO_PAD).context("invalid JWT")?;
    let data: JWT = serde_json::from_slice(&json_data).context("parsing JWT data")?;
    Ok(data)
}
