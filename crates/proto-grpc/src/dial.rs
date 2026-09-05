/// Lazily dial a gRPC endpoint with the workspace defaults: `unix:` sockets
/// (normalizing `unix://authority/path`), plaintext HTTP, and HTTPS with
/// native-roots TLS.
pub fn dial_channel(endpoint: &str) -> Result<tonic::transport::Channel, tonic::transport::Error> {
    use std::time::Duration;

    let endpoint = match url::Url::parse(endpoint) {
        Ok(url) if url.scheme() == "unix" && url.has_host() => {
            std::borrow::Cow::Owned(format!("unix:{}", url.path()))
        }
        _ => std::borrow::Cow::Borrowed(endpoint),
    };

    // Building a TLS connector eagerly initializes rustls even though tonic
    // would not use it for HTTP, so plaintext endpoints must bypass it.
    let is_plaintext = endpoint.starts_with("unix:") || endpoint.starts_with("http://");
    let endpoint = tonic::transport::Endpoint::from_shared(endpoint.to_string())?
        .connect_timeout(Duration::from_secs(60))
        .http2_keep_alive_interval(Duration::from_secs(301))
        .initial_connection_window_size(i32::MAX as u32);

    let endpoint = if is_plaintext {
        endpoint
    } else {
        endpoint.tls_config(
            tonic::transport::ClientTlsConfig::new()
                .with_native_roots()
                .assume_http2(true),
        )?
    };

    Ok(endpoint.connect_lazy())
}
