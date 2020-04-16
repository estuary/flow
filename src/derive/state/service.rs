use crate::specs::store::Document;
use crate::{derive::state::DocStore, specs::store::GetRequest};
use hyper::server::Builder;
use hyper::service::make_service_fn;
use hyperlocal::SocketIncoming;
use log::info;
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;
use warp::{Reply, filters::BoxedFilter, Filter};

fn rt_hello() -> BoxedFilter<(impl Reply,)> {
    warp::get()
        .and(warp::path::end())
        .map(|| "state service ready")
        .boxed()
}

fn rt_get_doc(store: Arc<Mutex<Box<impl DocStore>>>) -> BoxedFilter<(impl Reply,)> {
    warp::get()
        .and(warp::path("docs"))
        .and(warp::path::tail())
        .and_then(|key: warp::path::Tail| async move {
            info!("get request {:?}", key);

            Ok::<_, Infallible>(key.as_str().to_owned())
        })
        .boxed()
}

fn rt_put_docs(store: Arc<Mutex<Box<impl DocStore>>>) -> BoxedFilter<(impl Reply,)> {
    warp::put()
        .and(warp::body::bytes())
        .map(move |put: bytes::Bytes| -> Box<dyn warp::Reply> {
            let put = serde_json::from_slice::<Document>(put.as_ref());
            let put = match put {
                Ok(put) => put,
                Err(err) => {
                    info!("PUT decode failed: {}", err);

                    return Box::new(warp::reply::with_status(
                        err.to_string(),
                        warp::http::StatusCode::BAD_REQUEST,
                    ));
                }
            };
            info!("PUT document key {:?} doc {:?}", put.key, put.value);

            Box::new(warp::http::StatusCode::OK)
        })
        .boxed()
}

pub fn serve<S, I>(
    incoming: Builder<SocketIncoming>,
    store: Arc<Mutex<Box<S>>>,
    stop: I,
) -> impl Future<Output = ()>
where
    S: DocStore + 'static,
    I: std::future::Future<Output = ()>,
{
    let svc = warp::service(
        rt_put_docs(store.clone())
            .or(rt_hello())
            .or(rt_get_doc(store.clone())),
    );
    let make_svc = make_service_fn(move |stream: &tokio::net::UnixStream| {
        info!("socket connected {:?}", stream);

        let svc = svc.clone();
        async move { Ok::<_, Infallible>(svc) }
    });

    let server = incoming.serve(make_svc);
    let server = server.with_graceful_shutdown(stop);

    async move {
        if let Err(err) = server.await {
            log::error!("error on service stop: {}", err);
        } else {
            log::info!("service stop complete");
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::derive::state::MemoryStore;
    use hyper::{Client, Server};
    use hyperlocal::{UnixConnector, UnixServerExt, Uri};

    async fn with_service<F, R>(
        store: Arc<Mutex<Box<impl DocStore + 'static>>>,
        cb: F,
    ) where
        F: Fn(std::path::PathBuf, Client<UnixConnector>) -> R,
        R: Future<Output=()>,
    {
        let _ = pretty_env_logger::init();
        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("test-sock");
        let listener = Server::bind_unix(&uds).unwrap();

        let (tx_stop, rx_stop) = tokio::sync::oneshot::channel::<()>();
        let rx_stop = async move { rx_stop.await.unwrap(); };

        // Start serving asynchronously.
        let join_handle = tokio::spawn(serve(listener, store, rx_stop));

        // Issue some requests.
        let cli = Client::builder()
            .http2_only(true)
            .build::<_, hyper::Body>(UnixConnector);

        cb(uds, cli).await;

        // Graceful shutdown.
        tx_stop.send(()).unwrap();
        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_hello() {
        let store = Arc::new(Mutex::new(Box::new(MemoryStore::new())));

        with_service(store, |uds, cli| async move {
            let mut resp = cli.get(Uri::new(&uds, "/").into()).await.unwrap();
            let body = hyper::body::to_bytes(resp.body_mut()).await.unwrap();
            assert_eq!(body.as_ref(), "state service ready".as_bytes());
        }).await;
    }

    async fn run_sequence(store: Arc<Mutex<Box<impl DocStore + 'static>>>) {
        with_service(store, |uds, cli| async move {

            // TODO(johnny): Issue some puts.

            let mut resp = cli.get(Uri::new(&uds, "/").into()).await.unwrap();
            let body = hyper::body::to_bytes(resp.body_mut()).await.unwrap();
            assert_eq!(body.as_ref(), "state service ready".as_bytes());
        }).await;
    }

    #[tokio::test]
    async fn test_sequence_memory_store() {
        run_sequence(Arc::new(Mutex::new(Box::new(MemoryStore::new()))));
    }
}

/*

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Error as HyperError, Method, Request, Response, StatusCode, server::Builder};
use hyperlocal::SocketIncoming;
use slog::{info};
use std::future::Future;
use std::io::Error as IOError;
use std::sync::Arc;
use thiserror;
use tokio::sync::Mutex;
use crate::{derive::state::DocStore, log, specs::store::GetRequest};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("HTTP Error: {0}")]
    HyperError(#[from] HyperError),
    #[error("IO Error: {0}")]
    IOError(#[from] IOError),
    #[error("JSON Error: {0}")]
    JsonError(#[from] serde_json::Error),
}



async fn dispatch<S>(
    req: Request<Body>,
    store: Arc<Mutex<Box<S>>>,
) -> Result<Response<Body>, Error>
where
    S: DocStore + Send + Sync + 'static
{
    info!(log(), "dispatching request"; "headers" => format!("{:?}", req.headers()));

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/docs") => {

            let resp = Response::builder();
            let query = req.uri().query().unwrap_or("");
            let query = serde_urlencoded::from_str(query);

            // Did we parse the query correctly?
            let query : GetRequest = match query {
                Err(err) => {
                    return Ok(resp
                        .status(404)
                        .body(Body::from(err.to_string()))
                        .unwrap());
                }
                Ok(query) => query,
            };

            // Fetch a single key.
            if !query.prefix {
                if let Some(doc) = store.lock().await.get(&query.key) {
                    let doc = serde_json::to_vec(&doc).unwrap();
                    return Ok(resp.body(Body::from(doc)).unwrap());
                }
                return Ok(resp.status(404).body(Body::empty()).unwrap());
            }

            // Stream an iteration of the given prefix.
            let (mut send, body) = Body::channel();

            tokio::task::spawn(async move {
                send.send_data("[\n".into()).await?;

                for (ind, doc) in store.lock().await.iter_prefix(&query.key).enumerate() {
                    if ind != 0 {
                        send.send_data(",\n".into()).await?;
                    }
                    let doc = serde_json::to_vec(&doc)?;
                    send.send_data(doc.into()).await?;
                }

                send.send_data("\n]\n".into()).await?;
                Ok::<(), Error>(())
            });

            Ok(resp.body(body).unwrap())
        },

        // Not found handler.
        _ => {
            let mut not_found = Response::default();
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

pub fn serve<S, I>(
    incoming: Builder<SocketIncoming>,
    store: Arc<Mutex<Box<S>>>,
    stop: I,
) -> impl Future<Output = Result<(), HyperError>>
where
    S: DocStore + Send + Sync + 'static,
    I: std::future::Future<Output = ()>,
{
    let service = make_service_fn(move |stream: &tokio::net::UnixStream| {
        info!(log(), "socket connected"; "stream" => format!("{:?}", stream));

        let store = store.clone();
        async move {
            Ok::<_, Error>(service_fn(move |_req: Request<Body>| {
                dispatch(_req, store.clone())
            }))
        }
    });

    let server = incoming.serve(service);
    server.with_graceful_shutdown(stop)
}

    /// // Prepare some signal for when the server should start shutting down...
    /// let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    /// let graceful = server
    ///     .with_graceful_shutdown(async {
    ///         rx.await.ok();
    ///     });
    ///
    /// // Await the `server` receiving the signal...
    /// if let Err(e) = graceful.await {
    ///     eprintln!("server error: {}", e);
    /// }
    ///
    /// // And later, trigger the signal by calling `tx.send(())`.
    /// let _ = tx.send(());

use tokio::net::UnixListener;
use warp::Filter;
use serde_json;
use std::borrow::Cow;
use std::sync::Arc;
use std::collections::HashMap;
use headers::{HeaderMapExt, ContentType};
use serde_json::value::RawValue;
use tokio::sync::Mutex;


#[tokio::main]
async fn main() {
    let mut listener = UnixListener::bind("/home/ubuntu/test-doc-store").unwrap();

    let store : HashMap<String, Box<RawValue>> = HashMap::new();
    let store = Arc::new(Mutex::new(store));

    let m_store = Arc::clone(&store);
    let doc_put = warp::put()
        .and(warp::body::bytes())
        .map(move |put: bytes::Bytes| -> Box<dyn warp::Reply> {

            let put : PutDocument = match serde_json::from_slice(put.as_ref()) {
                Err(err) => {
                    return Box::new(warp::reply::with_status(
                        format!("decoding request body: {}\n", err),
                        warp::http::StatusCode::BAD_REQUEST));
                }
                Ok(put) => put
            };

            let is_borrow = match put.key {
                Cow::Borrowed(_) => true,
                Cow::Owned(_) => false,
            };
            println!("got PUT document {:?} is_borrow {:?}", put, is_borrow);


            let key : String = put.key.to_owned().into();
            let doc = put.doc.to_owned();

            let m_store = Arc::clone(&m_store);
            tokio::task::spawn(async move {
                let store : &mut HashMap<String, Box<RawValue>> = &mut *m_store.lock().await;
                store.insert(key, doc);
            });

            Box::new(warp::http::StatusCode::OK)
        });

    let m_store = Arc::clone(&store);

    let doc_get = warp::get()
        .and(warp::query::query())
        .map(move |get: GetDocument| {
            println!("GET of {:?}\n", get);

            let (mut send, resp_body) = warp::hyper::Body::channel();

            let m_store = Arc::clone(&m_store);
            tokio::task::spawn(async move {
                send.send_data("[\n".into()).await.unwrap();

                let encode = |key, doc| {
                    serde_json::to_vec(&PutDocument{
                        key: Cow::from(key),
                        doc: doc,
                        expire_at: None,
                    }).unwrap().into()
                };

                let store : &mut HashMap<String, Box<RawValue>> = &mut *m_store.lock().await;
                if let Some(true) = get.prefix {
                    let mut first = true;
                    for (key, doc) in store.iter() {
                        if key.starts_with(&get.key) {
                            if !first {
                                send.send_data(",\n".into()).await.unwrap();
                            }
                            first = false;
                            send.send_data(encode(key, doc)).await.unwrap();
                        }
                    }
                } else if let Some(doc) = store.get(&get.key) {
                    send.send_data(encode(&get.key, doc)).await.unwrap();
                }

                send.send_data("\n]\n".into()).await.unwrap();
            });

            let mut resp = warp::http::Response::new(resp_body);
            resp.headers_mut().typed_insert(ContentType::json());
            resp
        });

    let docs = warp::path("docs").and(doc_put.or(doc_get));

    warp::serve(docs).run_incoming(listener.incoming()).await;
}

fn do_ping_pong(
    recv: impl futures::stream::Stream<Item = Result<impl bytes::Buf + Send + Sync, warp::Error>> + Send + 'static,
) -> warp::http::Response<warp::hyper::Body> {

    use headers::{HeaderMapExt, ContentType};
    let (mut send, resp_body) = warp::hyper::Body::channel();

    // Spawn a task which writes to |send| and reads from |recv|.
    tokio::task::spawn(async move {
        let count : u64 = 0;
        let mut recv = Box::pin(recv);

        #[derive(Serialize, Deserialize)]
        struct Echo<'a> {
            num: u64,
            #[serde(borrow, deserialize_with = "rpc_tests::deserialize_cow_str")]
            value: Cow<'a, str>,
        }

        send.send_data(serde_json::to_vec(&Echo{
            num: count,
            value: Cow::from("server"),
        }).unwrap().into()).await.unwrap();

        let mut rem = Vec::<u8>::new();

        use futures::stream::TryStreamExt;
        loop {
            let chunk = match recv.try_next().await {
                Err(err) => {
                    println!("ping-pong read err: {:?}", err);
                    send.abort();
                    break;
                }
                Ok(None) => {
                    println!("ping-pong read None");
                    break;
                }
                Ok(Some(chunk)) => {
                    println!("ping-pong read : {:?}", String::from_utf8_lossy(chunk.bytes()));
                    chunk
                }
            };

            let mut it = if rem.is_empty() {
                serde_json::Deserializer::from_slice(chunk.bytes()).into_iter::<Echo>()
            } else {
                rem.extend_from_slice(chunk.bytes());
                serde_json::Deserializer::from_slice(&rem).into_iter::<Echo>()
            };

            for echo in &mut it {
                let echo = match echo {
                    Ok(echo) => echo,
                    Err(err) if err.is_eof() => {
                        break
                    }
                    Err(err) => {
                        println!("ping-pong read err: {:?}", err);
                        send.abort();
                        return;
                    }
                };
                send.send_data(serde_json::to_vec(&echo).unwrap().into()).await.unwrap();
            }

            let offset = it.byte_offset();
            drop(it);

            let d = chunk.bytes().len() - it.byte_offset();
            drop(it);
            if offset !=  {
                rem.clear();
                rem.reserve()

            }

        }
        println!("ping-pong exit");
    });

    let mut resp = warp::http::Response::new(resp_body);
    resp.headers_mut().typed_insert(ContentType::json());
    resp
}
*/
