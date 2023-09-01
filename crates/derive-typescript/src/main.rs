use tracing_subscriber::{filter::LevelFilter, EnvFilter};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(EnvFilter::from_env("LOG_LEVEL"))
        .with_writer(std::io::stderr)
        .json()
        .init();

    derive_typescript::run()
}
