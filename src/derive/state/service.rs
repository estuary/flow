use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Error as HyperError, Method, Request, Response, Server, StatusCode};
use hyperlocal::UnixServerExt;
use slog::info;
use std::io::Error as IOError;
use std::path::Path;
use std::sync::Arc;
use thiserror;
use tokio::sync::Mutex;

use crate::{derive::state::DocStore, log};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("HTTP Error: {0}")]
    HyperError(#[from] HyperError),
    #[error("IO Error: {0}")]
    IOError(#[from] IOError),
}

async fn dispatch<S: DocStore>(
    req: Request<Body>,
    _store: Arc<Mutex<Box<S>>>,
) -> Result<Response<Body>, Error> {
    info!(log(), "dispatching request"; "headers" => format!("{:?}", req.headers()));

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => Ok(Response::new(Body::from("Index!"))),

        // Not found handler.
        _ => {
            let mut not_found = Response::default();
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

pub async fn serve<P, S, I>(uds_path: P, store: Arc<Mutex<Box<S>>>, stop: I) -> Result<(), Error>
where
    P: AsRef<Path>,
    S: DocStore + Send + Sync + 'static,
    I: std::future::Future<Output = ()>,
{
    let service = make_service_fn(move |stream: &tokio::net::UnixStream| {
        info!(log(), "socket connected";
            "stream" => format!("{:?}", stream));

        let store = store.clone();
        async move {
            Ok::<_, Error>(service_fn(move |_req: Request<Body>| {
                dispatch(_req, store.clone())
            }))
        }
    });

    let server = Server::bind_unix(uds_path.as_ref())?.serve(service);
    let graceful = server.with_graceful_shutdown(stop);

    info!(log(), "bound socket";
        "uds_path" => uds_path.as_ref().to_str());

    match graceful.await {
        Err(err) => info!(log(), "server error"; "err" => err.to_string()),
        Ok(()) => info!(log(), "server graceful exit"),
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use hyper::Client;
    use hyperlocal::{UnixClientExt, Uri};
    use super::*;
    use crate::derive::state::MemoryStore;


    #[tokio::test]
    async fn my_test() {
        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("test-sock");

        let store = Arc::new(Mutex::new(Box::new(MemoryStore::new())));
        let (stop, rx) = tokio::sync::oneshot::channel::<()>();
        let srv = serve(&uds, store.clone(), async move { rx.await.unwrap(); });

        // Issue some requests.
        let cli = Client::unix();
        
        let mut resp = cli.get(Uri::new(&uds, "/").into()).await.unwrap();
        
        info!(log(), "got response";
            "status" => resp.status().as_str());

        resp.body_mut().data()



        stop.send(()).unwrap();
        srv.await.unwrap();
    }
}

/*
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
