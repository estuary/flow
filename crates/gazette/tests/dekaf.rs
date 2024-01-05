use futures::TryStreamExt;
use proto_gazette::broker;
use tracing_subscriber::{filter::LevelFilter, EnvFilter};

#[tokio::test]
async fn foobar() -> anyhow::Result<()> {
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
        begin_mod_time: 0,
        block: false,
        do_not_proxy: false,
        end_offset: 0,
        header: None,
        journal: "estuary/mahdi-test/events/pivot=00".to_string(),
        metadata_only: true,
        offset: 0,
    };

    let stream = client.read(rr);
    tokio::pin!(stream);

    let mut offset = 0;

    while let Some(mut resp) = stream.try_next().await? {
        let content_len = resp.content.len();
        resp.content = Default::default();

        offset = resp.offset + content_len as i64;
        tracing::info!(?resp, offset, "got resp");
    }

    tracing::info!(offset, "goodbye");

    Ok(())
}

/*
pub fn do_stuff(
    stream: impl futures::Stream<Item = Result<broker::ReadResponse, gazette::Error>>,
) -> impl futures::Stream<Item = Result<broker::ReadResponse, gazette::Error>> {
    use simdjson_sys as ffi;

    coroutines::try_coroutine(move |mut co| async move {
        let mut stream = std::pin::pin!(stream);
        let mut chunk = Vec::new();

        while let Some(mut resp) = stream.try_next().await? {
            if resp.fragment.is_some() {
                () = co.yield_(resp).await;
                continue;
            }
            chunk.extend_from_slice(&resp.content);

            if let None = ::memchr::memrchr(b'\n', &chunk) {
                continue;
            }

            // Add required simdjson padding.
            chunk.extend_from_slice(&[0; 8]);




            //() = co.yield_(resp).await;
        }
        Ok(())
    })
}
*/
