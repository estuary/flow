//! Included by the launcher and platform tests, each as `rpc`.

use proto_flow::derive;

/// An RPC held open on the connector-init socket at `socket`, from this
/// process, as a busy VMM serves one. connector-init ends a VMM which serves
/// no RPC within seconds, which would race the release a test makes of it.
pub fn hold(
    runtime: &tokio::runtime::Runtime,
    socket: &str,
    timeout: std::time::Duration,
) -> tonic::Streaming<derive::Response> {
    let socket = socket.to_string();
    runtime.block_on(async move {
        let channel = tonic::transport::Endpoint::from_static("http://[::1]:0")
            .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
                let socket = socket.clone();
                async move {
                    let stream = tokio::net::UnixStream::connect(socket).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                }
            }))
            .await
            .expect("connecting over init.sock");
        let mut client = proto_grpc::derive::connector_client::ConnectorClient::new(channel);
        let call = client.derive(futures::stream::pending::<derive::Request>());
        tokio::time::timeout(timeout, call)
            .await
            .expect("the RPC begins")
            .expect("the RPC begins")
            .into_inner()
    })
}
