use anyhow::Context;
use clap::Parser;
use futures::FutureExt;
use std::sync::Arc;

#[derive(clap::ValueEnum, Debug, Clone)]
enum KeychainType {
    // Encrypt via AGE.
    Age,
    // Encrypt via AWS KMS.
    Aws,
    // Encrypt via GCP KMS.
    Gcp,
}

/// Config-encryption is a stateless service which performs one-way
/// encryption of endpoint configurations.
#[derive(clap::Parser, Debug)]
struct Args {
    /// Port to listen on.
    #[clap(long, env, default_value = "8765")]
    pub api_port: u16,
    /// Type of KMS to use for default encryption.
    #[clap(long, env)]
    pub kms_type: KeychainType,
    /// The fully qualified KMS key to use for encryption.
    /// - For GCP, values are projects/<your-project>/locations/<your-region>/keyRings/<your-keyring>/cryptoKeys/<your-key-name>
    /// - For AWS, values are ARNs like arn:aws:kms:<region>:<account-id>:key/<key-id>
    /// - For AGE, values are the encryption key to use.
    #[clap(long, env)]
    pub kms_key: String,
}

fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    // Use reasonable defaults for printing structured logs to stderr.
    let builder = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env());
    tracing::subscriber::set_global_default(builder.json().finish())
        .expect("setting tracing default failed");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    tracing::info!(?args, "started!");

    let task = runtime.spawn(async move { async_main(args).await });
    let result = runtime.block_on(task);

    tracing::info!(?result, "main function completed, shutting down runtime");
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    result?
}

async fn async_main(
    Args {
        api_port,
        kms_type,
        kms_key,
    }: Args,
) -> anyhow::Result<()> {
    let default_keychain = match kms_type {
        KeychainType::Age => config_encryption::Keychain::Age(kms_key),
        KeychainType::Aws => config_encryption::Keychain::Aws(kms_key),
        KeychainType::Gcp => config_encryption::Keychain::Gcp(kms_key),
    };

    let cors = tower_http::cors::CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_headers([axum::http::header::CONTENT_TYPE, axum::http::header::ACCEPT]);

    let api_listener = tokio::net::TcpListener::bind(format!("[::]:{api_port}"))
        .await
        .context("failed to bind server port")?;

    // Share-able future which completes when the agent should exit.
    let shutdown = tokio::signal::ctrl_c().map(|_| ()).shared();

    let app = axum::Router::new()
        .route(
            "/v1/encrypt-config",
            axum::routing::post(config_encryption::encrypt_config),
        )
        .with_state(Arc::new(default_keychain))
        .layer(cors)
        .layer(tower_http::trace::TraceLayer::new_for_http());

    axum::serve(api_listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;

    Ok(())
}
