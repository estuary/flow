use futures::TryStreamExt;
use proto_gazette::broker;
use tracing_subscriber::{filter::LevelFilter, EnvFilter};

#[tokio::test]
async fn foobar() -> gazette::Result<()> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    tracing_subscriber::fmt::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();

    tracing::info!("hello");

    let router = gazette::journal::Router::new(
        "http://localhost:8080",
        gazette::Interceptor::new(None)?,
        "local",
    )?;
    let client = gazette::journal::Client::new(reqwest::Client::new(), router);

    let rr = broker::ReadRequest {
        journal: "demo/wikipedia/recentchange-sampled/pivot=00".to_string(),
        ..Default::default()
    };

    let stream = client.read_docs(rr);
    tokio::pin!(stream);

    let mut bout = std::io::BufWriter::new(std::io::stdout().lock());

    while let Some(doc) = stream.try_next().await? {
        use gazette::journal::Read;

        match doc {
            Read::Doc { offset: _, root } => {
                serde_json::to_writer(&mut bout, &doc::SerPolicy::noop().on(root.get())).unwrap();
            }
            Read::Meta(fragment) => {
                tracing::info!(fragment=?ops::DebugJson(fragment), "started reading a new fragment");
            }
        }
    }
    tracing::info!("goodbye");

    Ok(())
}
