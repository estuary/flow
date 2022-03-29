use async_trait::async_trait;

mod builds;
mod connector_images;
mod discovers;
mod id;
mod jobs;
pub mod logs;

pub use builds::BuildHandler;
pub use connector_images::SpecHandler;
pub use discovers::DiscoverHandler;
pub use id::Id;

/// Handler is the principal trait implemented by the various task-specific
/// event handlers that the agent runs.
#[async_trait]
pub trait Handler {
    type Error: From<tokio_postgres::Error> + std::fmt::Debug;

    // dequeue is a SQL query which returns a single next dequeued row,
    // or no rows if none remain to dequeue.
    fn dequeue() -> &'static str;
    // update is a SQL query which is prepared and provided to on_dequeue.
    fn update() -> &'static str;

    // on_dequeue takes action over the dequeued row.
    async fn on_dequeue(
        &mut self,
        txn: &mut tokio_postgres::Transaction,
        row: tokio_postgres::Row,
        update: &tokio_postgres::Statement,
    ) -> Result<u64, Self::Error>;
}

// build_pg_client builds a tokio_postgres::Client to the named database URL.
// TODO(johnny): Use deadpool_postgres for pooling of connections?
// TODO(johnny): Currently it doesn't handle TLS, but may need to in the future?
#[tracing::instrument(level = "debug", ret, skip(url))]
pub async fn build_pg_client(
    url: &url::Url,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    /*
    let tls_config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
    */

    let (client, connection) = tokio_postgres::connect(url.as_str(), tokio_postgres::NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("postgres connection error: {}", e);
        }
    });

    Ok(client)
}

// serve a Handler until signaled by a ready |exit| future.
// TODO(johnny): This should be be moved and restructured, so that many Handlers
// are run by a single polling event loop using one pg_conn.
#[tracing::instrument(ret, skip_all, fields(handler = std::any::type_name::<H>()))]
pub async fn todo_serve<E, H>(
    mut handler: H,
    mut pg_conn: tokio_postgres::Client,
    exit: E,
) -> Result<(), H::Error>
where
    H: Handler,
    E: std::future::Future<Output = ()> + Send,
{
    let dequeue = pg_conn.prepare(H::dequeue()).await?;
    let update = pg_conn.prepare(H::update()).await?;

    let mut backoff = std::time::Duration::ZERO;

    tokio::pin!(exit);
    loop {
        let sleep = tokio::time::sleep(backoff);

        tokio::select! {
            _ = &mut exit => {
                tracing::debug!("caught signal; exiting...");
                return Ok(()) // All done.
            }
            _ = sleep => (),
        };

        // Begin a |txn| which will scope a held lock on a dequeued row.
        let mut txn = pg_conn.transaction().await?;
        let row = match txn.query_opt(&dequeue, &[]).await? {
            Some(row) => row,
            None => {
                tracing::debug!("found no row to dequeue. Sleeping...");
                backoff = std::time::Duration::from_secs(5);
                continue;
            }
        };

        let affected = handler.on_dequeue(&mut txn, row, &update).await?;
        if affected != 1 {
            panic!("affected is {} not one", affected);
        }
        txn.commit().await?;
    }
}
