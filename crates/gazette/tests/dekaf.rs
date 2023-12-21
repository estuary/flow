use bytes::Buf;
use core::slice::memchr::memrchr;
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

pub fn do_stuff(
    stream: impl futures::Stream<Item = Result<broker::ReadResponse, gazette::Error>>,
) -> impl futures::Stream<Item = Result<broker::ReadResponse, gazette::Error>> {
    coroutines::try_coroutine(move |mut co| async move {
        let mut stream = std::pin::pin!(stream);
        let mut remainder = Vec::new();

        while let Some(mut resp) = stream.try_next().await? {
            if resp.fragment.is_some() {
                () = co.yield_(resp).await;
                continue;
            }
            let mut chunk = resp.content;

            if !remainder.is_empty() {
                if let Some(index) = memchr::memchr(b'\n', &chunk) {
                    remainder.extend_from_slice(&chunk[..index + 1]);
                    chunk.advance(index + 1);
                } else {
                    remainder.extend_from_slice(&chunk);
                    continue; // Still don't have a full line.
                }

                () = co.yield_(resp.)
                // process remainder now.

                remainder.clear();
            }

            if let Some(index) = memchr::memrchr(b'\n', &chunk) {
                // process chunk[..index+1]

                remainder.extend_from_slice(&chunk[index + 1..]);
            } else {
                // We don't have a full message.
                remainder.extend_from_slice(&chunk);
            }

            //() = co.yield_(resp).await;
        }
        Ok(())
    })
}
