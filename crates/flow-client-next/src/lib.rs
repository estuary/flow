use futures::StreamExt;

pub mod postgrest;
pub mod rest;
pub mod user_auth;
pub mod workflows;

/// Adapt a Stream of gazette::RetryResult into a Stream of tonic::Result
/// by unwrapping gRPC errors and rendering other errors as internal gRPC errors.
/// Transient errors are handled by the caller-provided closure, which will
/// usually log a warning with details and return None to suppress the error.
pub fn adapt_gazette_retry_stream<S, T, F>(
    stream: S,
    mut transient: F,
) -> impl futures::Stream<Item = tonic::Result<T>>
where
    S: futures::Stream<Item = gazette::RetryResult<T>>,
    F: FnMut(usize, anyhow::Error) -> Option<tonic::Status> + 'static,
{
    stream.filter_map(move |result| match result {
        Ok(response) => std::future::ready(Some(Ok(response))),

        // Caller provides handling for transient errors.
        Err(gazette::RetryError { attempt, inner }) if inner.is_transient() => {
            if attempt == 0 {
                return std::future::ready(None); // Always suppress first attempt.
            } else {
                std::future::ready(
                    transient(attempt, anyhow::anyhow!(inner)).map(|status| Err(status)),
                )
            }
        }

        // Unwrap gRPC errors.
        Err(gazette::RetryError {
            attempt: _,
            inner: gazette::Error::Grpc(status),
        }) => std::future::ready(Some(Err(status))),

        // Wrap other errors as unknown gRPC errors.
        Err(gazette::RetryError { attempt: _, inner }) => std::future::ready(Some(Err(
            tonic::Status::unknown(format!("{:?}", anyhow::anyhow!(inner))),
        ))),
    })
}

lazy_static::lazy_static! {
    pub static ref DEFAULT_AGENT_URL:  url::Url = url::Url::parse("https://agent-api-1084703453822.us-central1.run.app").unwrap();
    pub static ref DEFAULT_DASHBOARD_URL: url::Url = url::Url::parse("https://dashboard.estuary.dev/").unwrap();
    pub static ref DEFAULT_PG_URL: url::Url = url::Url::parse("https://eyrcnmuzzyriypdajwdk.supabase.co/rest/v1").unwrap();
    pub static ref DEFAULT_CONFIG_ENCRYPTION_URL: url::Url = url::Url::parse("https://config-encryption.estuary.dev/").unwrap();

    // Used only when profile is "local".
    pub static ref LOCAL_AGENT_URL: url::Url = url::Url::parse("http://localhost:8675/").unwrap();
    pub static ref LOCAL_DASHBOARD_URL: url::Url = url::Url::parse("http://localhost:3000/").unwrap();
    pub static ref LOCAL_PG_URL: url::Url = url::Url::parse("http://localhost:5431/rest/v1").unwrap();
    pub static ref LOCAL_CONFIG_ENCRYPTION_URL: url::Url = url::Url::parse("http://localhost:8765/").unwrap();
}

pub const DEFAULT_PG_PUBLIC_TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5cmNubXV6enlyaXlwZGFqd2RrIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NDg3NTA1NzksImV4cCI6MTk2NDMyNjU3OX0.y1OyXD3-DYMz10eGxzo1eeamVMMUwIIeOoMryTRAoco";
pub const LOCAL_PG_PUBLIC_TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0";
pub const LOCAL_DATA_PLANE_HMAC: &str = "c3VwZXJzZWNyZXQ=";
pub const LOCAL_DATA_PLANE_FQDN: &str = "local-cluster.dp.estuary-data.com";
