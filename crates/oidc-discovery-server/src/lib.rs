use anyhow::Context;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use serde_json::Value;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[derive(clap::Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// URL of the postgres database.
    #[clap(
        long = "database",
        env = "DATABASE_URL",
        default_value = "postgres://postgres:postgres@127.0.0.1:5432/postgres"
    )]
    database_url: url::Url,
    /// Path to CA certificate of the database.
    #[clap(long = "database-ca", env = "DATABASE_CA")]
    database_ca: Option<String>,
    /// Port to bind the HTTP server to.
    #[clap(long = "port", env = "PORT", default_value = "8080")]
    port: u16,
}

#[derive(Clone)]
struct CachedConfig {
    config: Value,
    expires_at: Instant,
}

#[derive(Clone)]
struct AppState {
    pg_pool: PgPool,
    http_client: reqwest::Client,
    google_config_cache: Arc<RwLock<Option<CachedConfig>>>,
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    tracing::info!("starting OIDC discovery server");

    let mut pg_options = args
        .database_url
        .as_str()
        .parse::<sqlx::postgres::PgConnectOptions>()
        .context("parsing database URL")?
        .application_name("oidc-discovery-server");

    // If a database CA was provided, require that we use TLS with full cert verification.
    if let Some(ca) = &args.database_ca {
        pg_options = pg_options
            .ssl_mode(sqlx::postgres::PgSslMode::VerifyFull)
            .ssl_root_cert(ca);
    } else {
        // Otherwise, prefer TLS but don't require it.
        pg_options = pg_options.ssl_mode(sqlx::postgres::PgSslMode::Prefer);
    }

    let pg_pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_secs(30))
        .connect_with(pg_options)
        .await
        .context("connecting to database")?;

    let http_client = reqwest::Client::new();

    let state = AppState {
        pg_pool,
        http_client,
        google_config_cache: Arc::new(RwLock::new(None)),
    };

    let app = Router::new()
        .route(
            "/:data_plane_fqdn/.well-known/openid-configuration",
            get(openid_configuration),
        )
        .with_state(Arc::new(state));

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", args.port))
        .await
        .context("binding to port")?;

    tracing::info!(port = args.port, "OIDC discovery server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serving HTTP")?;

    Ok(())
}

async fn openid_configuration(
    Path(data_plane_fqdn): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, StatusCode> {
    tracing::info!(data_plane_fqdn, "handling OpenID configuration request");

    // Try to get the Google config from cache first
    let google_config = get_cached_google_config(&state).await?;

    // Query the data_planes table for the GCP service account email
    let gcp_service_account_email = match sqlx::query_scalar::<_, String>(
        "SELECT gcp_service_account_email FROM data_planes WHERE data_plane_fqdn = $1",
    )
    .bind(&data_plane_fqdn)
    .fetch_optional(&state.pg_pool)
    .await
    {
        Ok(Some(email)) => email,
        Ok(None) => {
            tracing::warn!(data_plane_fqdn, "data plane not found");
            return Err(StatusCode::NOT_FOUND);
        }
        Err(e) => {
            tracing::error!(error = ?e, data_plane_fqdn, "database query failed");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // Create a modified configuration with the custom jwks_uri and issuer
    let mut modified_config = google_config;
    modified_config["jwks_uri"] = Value::String(format!(
        "https://www.googleapis.com/service_accounts/v1/metadata/jwk/{}",
        gcp_service_account_email
    ));
    modified_config["issuer"] = Value::String(format!(
        "https://estuary.dev/{}/",
        data_plane_fqdn
    ));

    tracing::info!(
        data_plane_fqdn,
        gcp_service_account_email,
        "returning modified OpenID configuration"
    );

    Ok(Json(modified_config))
}

async fn get_cached_google_config(state: &AppState) -> Result<Value, StatusCode> {
    const CACHE_TTL: Duration = Duration::from_secs(60 * 60); // 1 hour cache

    // First, try to read from cache
    {
        let cache = state.google_config_cache.read().await;
        if let Some(cached) = cache.as_ref() {
            if cached.expires_at > Instant::now() {
                tracing::debug!("returning Google OpenID config from cache");
                return Ok(cached.config.clone());
            }
        }
    }

    // Cache miss or expired, fetch new config
    tracing::debug!("fetching fresh Google OpenID configuration");
    let google_config = match state
        .http_client
        .get("https://accounts.google.com/.well-known/openid-configuration")
        .send()
        .await
    {
        Ok(response) => match response.json::<Value>().await {
            Ok(config) => config,
            Err(e) => {
                tracing::error!(error = ?e, "failed to parse Google OpenID configuration");
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        },
        Err(e) => {
            tracing::error!(error = ?e, "failed to fetch Google OpenID configuration");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // Update cache
    {
        let mut cache = state.google_config_cache.write().await;
        *cache = Some(CachedConfig {
            config: google_config.clone(),
            expires_at: Instant::now() + CACHE_TTL,
        });
        tracing::debug!("cached Google OpenID configuration for {} seconds", CACHE_TTL.as_secs());
    }

    Ok(google_config)
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
    tracing::info!("shutdown signal received, stopping server");
}
