//! `git-credential` subcommand: a git credential helper that authenticates to
//! github.com as a GitHub App installation.
//!
//! Git invokes a credential helper with a single operation argument (`get`,
//! `store`, or `erase`), writes the credential context to the helper's stdin,
//! and reads the answer from its stdout. We only implement `get`: we mint (or
//! reuse a cached) installation access token and hand it back as the password
//! for the `x-access-token` user. `store` and `erase` are no-ops.
//!
//! This mirrors how the SSH machine-user key was previously loaded into
//! ssh-agent at container start: authentication stays entirely outside the
//! controller's worker code. See `entrypoint.sh`, which wires this helper up
//! via `credential.https://github.com.helper` in `github-app` auth mode.
//!
//! The token is never passed on a command line or written into a checkout's
//! git config, so it cannot leak into the user-visible command logs. It only
//! ever travels over this stdin/stdout pipe with git.

use anyhow::Context;

/// Environment variable holding the GitHub App ID.
const ENV_APP_ID: &str = "DPC_GITHUB_APP_ID";
/// Environment variable holding the App installation ID.
const ENV_INSTALLATION_ID: &str = "DPC_GITHUB_INSTALLATION_ID";
/// Environment variable holding the App private key (PEM).
const ENV_APP_KEY: &str = "DPC_GITHUB_APP_KEY";
/// Optional override for the token cache file path.
const ENV_TOKEN_CACHE: &str = "DPC_GITHUB_TOKEN_CACHE";

/// Refresh a cached token once it's within this window of expiring, so that a
/// git operation never starts with a token that's about to lapse mid-fetch.
const REFRESH_BEFORE_EXPIRY: chrono::Duration = chrono::Duration::minutes(5);

#[derive(clap::Parser, Debug)]
pub struct GitCredentialArgs {
    /// The git credential operation: `get`, `store`, or `erase`.
    operation: String,
}

/// Run the git credential helper.
pub async fn run_git_credential(args: GitCredentialArgs) -> anyhow::Result<()> {
    // Drain stdin regardless of operation: git writes the credential context
    // and expects the helper to consume it. We don't need any of the fields
    // (the helper is already scoped to github.com by git config), but leaving
    // the pipe unread can cause git to see a broken pipe.
    drain_stdin().await;

    // Only `get` produces output; `store`/`erase` succeed silently.
    if args.operation != "get" {
        return Ok(());
    }

    let token = get_token().await.context("failed to obtain installation token")?;

    // Emit the credential to git. The username is fixed for App tokens.
    print!("username=x-access-token\npassword={token}\n\n");

    Ok(())
}

/// Consume and discard all of stdin.
async fn drain_stdin() {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::new();
    let _ = tokio::io::stdin().read_to_end(&mut buf).await;
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CachedToken {
    token: String,
    expires_at: chrono::DateTime<chrono::Utc>,
}

/// Return a valid installation token, reusing the on-disk cache when possible
/// and otherwise minting a fresh one.
async fn get_token() -> anyhow::Result<String> {
    let cache_path = token_cache_path();
    let now = chrono::Utc::now();

    // Reuse the cached token if it's comfortably in-date.
    if let Some(cached) = read_cache(&cache_path) {
        if cached.expires_at - now > REFRESH_BEFORE_EXPIRY {
            tracing::info!(expires_at = %cached.expires_at, "reusing cached GitHub App token");
            return Ok(cached.token);
        }
    }

    let minted = mint_token().await?;
    tracing::info!(expires_at = %minted.expires_at, "minted new GitHub App installation token");
    write_cache(&cache_path, &minted);
    Ok(minted.token)
}

/// Path of the token cache file. Overridable via [`ENV_TOKEN_CACHE`]; defaults
/// to a file in the system temp directory.
fn token_cache_path() -> std::path::PathBuf {
    if let Ok(path) = std::env::var(ENV_TOKEN_CACHE) {
        return std::path::PathBuf::from(path);
    }
    std::env::temp_dir().join("dpc-github-token.json")
}

/// Read and parse the cache file, returning None on any error (a missing,
/// corrupt, or partially-written cache simply forces a fresh mint).
fn read_cache(path: &std::path::Path) -> Option<CachedToken> {
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Atomically write the cache: write to a per-process temp file in the same
/// directory, then rename over the target. Concurrent readers never see a torn
/// file, and the per-process temp name keeps concurrent writers from clobbering
/// each other's in-progress writes before the rename. The file is created 0600
/// because it holds a live installation token, mirroring the 0400 treatment of
/// the SSH key it replaces.
fn write_cache(path: &std::path::Path, token: &CachedToken) {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let Ok(serialized) = serde_json::to_vec(token) else {
        return;
    };

    let tmp = path.with_extension(format!("json.tmp.{}", std::process::id()));

    let write_result = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)
        .and_then(|mut f| f.write_all(&serialized));

    if write_result.is_ok() {
        let _ = std::fs::rename(&tmp, path);
    } else {
        let _ = std::fs::remove_file(&tmp);
    }
}

/// Claims of the App authentication JWT.
#[derive(serde::Serialize)]
struct Claims {
    /// Issuer: the GitHub App ID.
    iss: String,
    /// Issued-at, backdated slightly to tolerate clock skew.
    iat: i64,
    /// Expiry. GitHub rejects App JWTs with a lifetime over 10 minutes.
    exp: i64,
}

/// Response from the installation access-token endpoint.
#[derive(serde::Deserialize)]
struct AccessTokenResponse {
    token: String,
    expires_at: chrono::DateTime<chrono::Utc>,
}

/// Sign an App JWT and exchange it for a fresh installation access token.
async fn mint_token() -> anyhow::Result<CachedToken> {
    let app_id = std::env::var(ENV_APP_ID)
        .with_context(|| format!("{ENV_APP_ID} is not set"))?;
    let installation_id = std::env::var(ENV_INSTALLATION_ID)
        .with_context(|| format!("{ENV_INSTALLATION_ID} is not set"))?;
    let private_key = std::env::var(ENV_APP_KEY)
        .with_context(|| format!("{ENV_APP_KEY} is not set"))?;

    let jwt = sign_app_jwt(&app_id, &private_key).context("failed to sign App JWT")?;

    let url = format!(
        "https://api.github.com/app/installations/{installation_id}/access_tokens"
    );

    let response = reqwest::Client::new()
        .post(&url)
        .bearer_auth(&jwt)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28")
        // GitHub requires a User-Agent on all API requests.
        .header(reqwest::header::USER_AGENT, "estuary-data-plane-controller")
        .send()
        .await
        .context("failed to request installation access token")?;

    let status = response.status();
    let body = response.text().await.unwrap_or_default();

    if !status.is_success() {
        anyhow::bail!("installation access token request failed ({status}): {body}");
    }

    let parsed: AccessTokenResponse =
        serde_json::from_str(&body).context("failed to parse access token response")?;

    Ok(CachedToken {
        token: parsed.token,
        expires_at: parsed.expires_at,
    })
}

/// Build and sign the RS256 App JWT used to authenticate as the App itself.
fn sign_app_jwt(app_id: &str, private_key: &str) -> anyhow::Result<String> {
    let now = chrono::Utc::now().timestamp();
    let claims = Claims {
        iss: app_id.to_string(),
        iat: now - 60,  // Backdate for clock skew between us and GitHub.
        exp: now + 540, // 9 minutes; under GitHub's 10-minute ceiling.
    };

    let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
    let key = jsonwebtoken::EncodingKey::from_rsa_pem(private_key.as_bytes())
        .context("failed to parse App private key as RSA PEM")?;

    jsonwebtoken::encode(&header, &claims, &key).context("failed to encode App JWT")
}
