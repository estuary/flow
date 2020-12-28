use serde_json::Value;
use std::path;
use std::str::FromStr;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("hyper error calling lambda: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("failed to decode lambda response json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("parsing lambda response header: {0}")]
    HeaderToStr(#[from] http::header::ToStrError),
    #[error("parsing lambda content-type: {0}")]
    MimeFromStr(#[from] mime::FromStrError),
    #[error("lambda returned an unsupported Content-Type {0:?}")]
    UnsupportedMediaType(Option<String>),
    #[error("lambda returned {status}: {message}")]
    NotOK {
        status: hyper::StatusCode,
        message: String,
    },
    #[error("expected an array")]
    ExpectedArray,
}

#[derive(Clone)]
pub enum Lambda {
    // Noop is a Lambda which does nothing. It's provided as an alternative to
    // using Option<Lambda>, and simplifies usages which would otherwise need
    // to check for None-ness.
    Noop,
    // UnixJson dispatches over a Unix domain socket using JSON encoding.
    UnixJson {
        client: hyper::Client<hyperlocal::UnixConnector>,
        sock: path::PathBuf,
        path: String,
    },
    // WebJson dispatches to an arbitrary Url using JSON encoding.
    WebJson {
        client: hyper::Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>>,
        url: url::Url,
    },
}

impl std::fmt::Debug for Lambda {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Lambda::Noop => write!(f, "noop://"),
            Lambda::UnixJson { sock, path, .. } => {
                write!(f, "uds://{}{}", sock.to_string_lossy(), path)
            }
            Lambda::WebJson { url, .. } => url.fmt(f),
        }
    }
}

impl Lambda {
    pub fn new_web_json(url: url::Url) -> Lambda {
        let https = hyper_tls::HttpsConnector::new();
        let client = hyper::Client::builder().build::<_, hyper::Body>(https);
        Lambda::WebJson { client, url }
    }

    /// Invoke the lambda with the given, encoded `application/json` body.
    #[tracing::instrument(level = "debug", err, skip(body))]
    pub async fn invoke<B>(
        &self,
        mut body: Option<B>,
    ) -> Result<impl Iterator<Item = Result<Vec<Value>, Error>>, Error>
    where
        B: Into<hyper::Body>,
    {
        // Reference |body| to avoid mixing by-ref & by-move in the same pattern.
        let output: Value = match (self, &mut body) {
            (Lambda::Noop, _) | (_, None) => Value::Array(Vec::new()),

            (Lambda::UnixJson { client, sock, path }, Some(_)) => {
                let req = hyper::Request::builder()
                    .method("PUT")
                    .uri(hyperlocal::Uri::new(sock, path))
                    .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                    .body(body.take().unwrap().into())
                    .unwrap();

                unmarshal(client.request(req)).await?
            }
            (Lambda::WebJson { client, url }, Some(_)) => {
                let req = hyper::Request::builder()
                    .method("PUT")
                    .uri(url.as_str())
                    .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                    .body(body.take().unwrap().into())
                    .unwrap();

                unmarshal(client.request(req)).await?
            }
        };

        tracing::trace!(%output);

        let output = match output {
            Value::Array(v) => v,
            _ => return Err(Error::ExpectedArray),
        };
        Ok(output.into_iter().map(|row| match row {
            Value::Array(columns) => Ok(columns),
            _ => Err(Error::ExpectedArray),
        }))
    }
}

async fn unmarshal(resp: hyper::client::ResponseFuture) -> Result<Value, Error> {
    let resp = resp.await?;
    let resp = check_headers(resp, &[mime::APPLICATION_JSON]).await?;

    let body = match resp {
        Some((_media_type, body)) => hyper::body::to_bytes(body).await?,
        None => return Ok(Value::Array(Vec::new())),
    };

    let dom = serde_json::from_slice(&body)?;
    match &dom {
        Value::Array(_) => (),
        _ => return Err(Error::ExpectedArray),
    };
    Ok(dom)
}

async fn check_headers(
    mut resp: hyper::Response<hyper::Body>,
    expect_content_types: &[mime::Mime],
) -> Result<Option<(mime::Mime, hyper::Body)>, Error> {
    if !resp.status().is_success() {
        let body = hyper::body::to_bytes(resp.body_mut()).await?;
        return Err(Error::NotOK {
            status: resp.status(),
            message: String::from_utf8_lossy(&body).into_owned(),
        });
    } else if resp.status() == http::StatusCode::NO_CONTENT {
        return Ok(None);
    }

    let ct = match resp.headers().get(http::header::CONTENT_TYPE) {
        None => return Err(Error::UnsupportedMediaType(None)),
        Some(ct) => mime::Mime::from_str(ct.to_str()?)?,
    };
    match expect_content_types.iter().find(|e| **e == ct) {
        None => return Err(Error::UnsupportedMediaType(Some(ct.to_string()))),
        Some(_) => (),
    };
    Ok(Some((ct, resp.into_body())))
}

#[cfg(test)]
pub mod test {
    use super::*;
    use futures::channel::oneshot;
    use hyper::{Body, Request, Response};
    use serde_json::{json, Value};

    #[tokio::test]
    async fn test_hello_world() {
        // Start a TestServer which adds "added" to it's arguments.
        let srv = TestServer::start_v2(|source, register, previous| {
            vec![
                source,
                register.unwrap_or(&Value::Null),
                previous.unwrap_or(&Value::Null),
            ]
            .into_iter()
            .cloned()
            .chain(std::iter::once(json!("added")))
            .collect::<Vec<_>>()
        });

        let body = json!([
            ["source", "next"],
            [["previous", "register"], ["prev-only"]]
        ])
        .to_string();
        let inv = srv.lambda.invoke(Some(body)).await.unwrap();
        let inv = inv.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(
            serde_json::to_value(&inv).unwrap(),
            json!([
                ["source", "register", "previous", "added"],
                ["next", null, "prev-only", "added"],
            ]),
        );

        // Invoke with no input. The server returns 204: No Content,
        // which we should interpret as an empty output iterator.
        let body = json!([[]]).to_string();
        let inv = srv.lambda.invoke(Some(body)).await.unwrap();
        let inv = inv.collect::<Result<Vec<_>, _>>().unwrap();
        assert!(inv.is_empty());
    }

    pub struct TestServer {
        pub lambda: Lambda,
        tx_stop: Option<oneshot::Sender<()>>,
        // TempDir's drop() deletes the directory.
        _tmpdir: tempfile::TempDir,
    }

    impl TestServer {
        pub fn start_v2(
            func: impl Fn(&Value, Option<&Value>, Option<&Value>) -> Vec<Value> + Send + Clone + 'static,
        ) -> TestServer {
            let handle = move |req: Request<Body>| {
                let func = func.clone();

                async move {
                    match req.headers().get(http::header::CONTENT_TYPE) {
                        Some(hv) if hv.as_bytes() == b"application/json" => (),
                        other @ _ => panic!(format!("invalid content-type: {:?}", other)),
                    };

                    let b = hyper::body::to_bytes(req.into_body()).await.unwrap();

                    let v: Vec<Vec<Value>> = serde_json::from_slice(&b).unwrap();
                    let sources = v.get(0).unwrap();
                    let registers = v.get(1);

                    let mut out = Vec::new();
                    for (ind, src) in sources.iter().enumerate() {
                        let previous = registers.and_then(|r| r[ind].get(0));
                        let register = registers.and_then(|r| r[ind].get(1));

                        out.push(Value::Array(func(src, register, previous)));
                    }

                    let resp = match out.is_empty() {
                        // If there are no output rows, return 204: No content.
                        true => {
                            let mut resp = Response::new(hyper::Body::empty());
                            *resp.status_mut() = http::StatusCode::NO_CONTENT;
                            resp
                        }
                        // Otherwise, return 200: OK with 'application/json' Content-Type.
                        false => {
                            let out = serde_json::to_vec(&out).unwrap();

                            let mut resp = Response::new(hyper::Body::from(out));
                            resp.headers_mut().append(
                                http::header::CONTENT_TYPE,
                                "application/json".parse().unwrap(),
                            );
                            resp
                        }
                    };
                    Ok::<_, std::convert::Infallible>(resp)
                }
            };
            let svc = tower::service_fn(handle);

            let tmpdir = tempfile::tempdir().unwrap();
            let socket_path = tmpdir.path().join("test-sock");

            let (tx_stop, rx_stop) = oneshot::channel::<()>();
            let rx_stop = async move {
                rx_stop.await.unwrap();
            };
            let server = serve::unix_domain_socket(svc, &socket_path, rx_stop);
            tokio::spawn(server);

            let client = hyper::Client::builder()
                .http2_only(true)
                .build::<_, hyper::Body>(hyperlocal::UnixConnector);

            let lambda = Lambda::UnixJson {
                client,
                sock: socket_path,
                path: "/path".to_owned(),
            };

            TestServer {
                lambda,
                tx_stop: Some(tx_stop),
                _tmpdir: tmpdir,
            }
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            // send() may fail if the TestServer already panicked.
            let _ = self.tx_stop.take().unwrap().send(());
        }
    }
}
