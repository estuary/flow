use hyper::service::make_service_fn;
use hyperlocal::UnixServerExt;
use log::{debug, error};
use std::convert::Infallible;
use std::fs;
use std::future::Future;
use std::path::Path;
use warp::filters::BoxedFilter;

// Asynchronously serve a warp::Filter over the given Unix Domain Socket path,
// until signaled to gracefully stop.
pub fn serve(
    filter: BoxedFilter<(impl warp::Reply + 'static,)>,
    socket_path: &Path,
    stop: impl Future<Output = ()>,
) -> impl Future<Output = ()> {
    let svc = warp::service(filter);
    let make_svc = make_service_fn(move |stream: &tokio::net::UnixStream| {
        debug!("socket connected {:?}", stream);

        let svc = svc.clone();
        async move { Ok::<_, Infallible>(svc) }
    });

    let incoming = hyper::Server::bind_unix(&socket_path).unwrap();
    let server = incoming.serve(make_svc);
    let server = server.with_graceful_shutdown(stop);

    let socket_path = socket_path.to_owned();
    async move {
        if let Err(err) = server.await {
            error!("error on service stop: {}", err);
        } else {
            debug!("service stop complete");
        }
        if let Err(err) = fs::remove_file(&socket_path) {
            error!(
                "failed to remove unix socket file {:?}: {}",
                &socket_path, err
            );
        };
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use hyper::Client;
    use hyperlocal::{UnixConnector, Uri};
    use std::io::ErrorKind as IOErrorKind;
    use warp::Filter;

    #[tokio::test]
    async fn test_with_simple_server() {
        let _ = pretty_env_logger::init();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test-sock");

        let (tx_stop, rx_stop) = tokio::sync::oneshot::channel::<()>();
        let rx_stop = async move {
            rx_stop.await.unwrap();
        };

        let filter = warp::path::tail()
            .map(|tail: warp::path::Tail| format!("GET {}", tail.as_str()))
            .boxed();

        // Expect |serve| synchronously binds a socket listener, and returns a future to serve it.
        let server = serve(filter, &path, rx_stop);
        let _ = fs::metadata(&path).unwrap(); // Exists.

        // Start serving asynchronously.
        let join_handle = tokio::spawn(server);

        // Build HTTP/1 and HTTP/2 prior-knowledge (h2c) connections, and issue a basic request.
        // Expect both return expected responses.
        for h2 in [false, true].iter() {
            let cli = Client::builder()
                .http2_only(*h2)
                .build::<_, hyper::Body>(UnixConnector);

            let mut resp = cli
                .get(Uri::new(&path, "/hello/world").into())
                .await
                .unwrap();
            let body = hyper::body::to_bytes(resp.body_mut()).await.unwrap();
            assert_eq!(body.as_ref(), "GET hello/world".as_bytes());
        }

        // Graceful shutdown.
        tx_stop.send(()).unwrap();
        join_handle.await.unwrap();

        // Assert socket at |path| was removed.
        assert_eq!(
            fs::metadata(&path).unwrap_err().kind(),
            IOErrorKind::NotFound
        );
    }
}
