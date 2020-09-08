use serde::ser::Serialize;
use serde_json::Value;
use std::path;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("hyper error calling lambda: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("failed to decode lambda response json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("lambda did not return a Content-Type header")]
    MissingMediaType,
    #[error("lambda returned an unsupported Content-Type {0:?}")]
    UnsupportedMediaType(String),
    #[error("lambda returned {status}: {message}")]
    NotOK {
        status: hyper::StatusCode,
        message: String,
    },
    #[error("expected an array")]
    ExpectedArray,
}

#[derive(Debug, Clone)]
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

pub struct Invocation<'l> {
    lambda: &'l Lambda,
    buffer: Vec<u8>,
    row: usize,
    column: usize,
}

impl<'l> Invocation<'l> {
    fn start(&mut self) {
        match self.lambda {
            Lambda::Noop => (),

            Lambda::UnixJson { .. } | Lambda::WebJson { .. } => {
                self.buffer.push(b'[');
            }
        }
    }

    pub fn start_row(&mut self) {
        match self.lambda {
            Lambda::Noop => (),

            Lambda::UnixJson { .. } | Lambda::WebJson { .. } => {
                if self.row != 0 {
                    self.buffer.push(b',');
                }
                self.buffer.push(b'[');
            }
        }
    }

    pub fn add_column(&mut self, c: &Value) -> Result<(), Error> {
        match self.lambda {
            Lambda::Noop => Ok(()),

            Lambda::UnixJson { .. } | Lambda::WebJson { .. } => {
                if self.column != 0 {
                    self.buffer.push(b',');
                }
                self.column += 1;

                let mut ser = serde_json::Serializer::new(&mut self.buffer);
                Ok(c.serialize(&mut ser)?)
            }
        }
    }

    pub fn finish_row(&mut self) {
        match self.lambda {
            Lambda::Noop => (),

            Lambda::UnixJson { .. } | Lambda::WebJson { .. } => {
                self.buffer.push(b']');
                self.row += 1;
                self.column = 0;
            }
        }
    }

    pub async fn finish(self) -> Result<impl Iterator<Item = Result<Vec<Value>, Error>>, Error> {
        let Self {
            lambda,
            mut buffer,
            row,
            ..
        } = self;

        // Buffer "rows" sequence terminator, if needed.
        match lambda {
            Lambda::Noop => (),

            Lambda::UnixJson { .. } | Lambda::WebJson { .. } => {
                buffer.push(b']');
            }
        }

        // Invoke the Lambda.
        let rows: Value = match self.lambda {
            Lambda::Noop => Value::Array(Vec::new()),

            Lambda::UnixJson { client, sock, path } => {
                let req = hyper::Request::builder()
                    .method("PUT")
                    .uri(hyperlocal::Uri::new(sock, path))
                    .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                    .body(hyper::Body::from(buffer))
                    .unwrap();

                unmarshal(client.request(req)).await?
            }
            Lambda::WebJson { client, url } => {
                let req = hyper::Request::builder()
                    .method("PUT")
                    .uri(url.as_str())
                    .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                    .body(hyper::Body::from(buffer))
                    .unwrap();

                unmarshal(client.request(req)).await?
            }
        };

        let rows = match rows {
            Value::Array(v) if v.len() == row => v,
            _ => return Err(Error::ExpectedArray),
        };
        Ok(rows.into_iter().map(|row| match row {
            Value::Array(columns) => Ok(columns),
            _ => Err(Error::ExpectedArray),
        }))
    }
}

impl Lambda {
    pub fn new_web_json(url: url::Url) -> Lambda {
        let https = hyper_tls::HttpsConnector::new();
        let client = hyper::Client::builder().build::<_, hyper::Body>(https);
        Lambda::WebJson { client, url }
    }

    pub fn start_invocation(&'_ self) -> Invocation<'_> {
        let mut inv = Invocation {
            lambda: self,
            buffer: Vec::new(),
            row: 0,
            column: 0,
        };
        inv.start();
        inv
    }
}

async fn unmarshal(resp: hyper::client::ResponseFuture) -> Result<Value, Error> {
    let (ct, body) = check_headers(resp.await?).await?;
    let buf = hyper::body::to_bytes(body).await?;

    let dom: Value = match ct {
        mime::JSON => serde_json::from_slice(&buf)?,
        _ => panic!("content-type verified already"),
    };

    match &dom {
        Value::Array(_) => (),
        _ => return Err(Error::ExpectedArray),
    };
    Ok(dom)
}

async fn check_headers(
    mut resp: hyper::Response<hyper::Body>,
) -> Result<(mime::Name<'static>, hyper::Body), Error> {
    if !resp.status().is_success() {
        let body = hyper::body::to_bytes(resp.body_mut()).await?;
        return Err(Error::NotOK {
            status: resp.status(),
            message: String::from_utf8_lossy(&body).into_owned(),
        });
    }

    let hdr = match resp.headers().get(http::header::CONTENT_TYPE) {
        None => return Err(Error::MissingMediaType),
        Some(v) => v.as_bytes(),
    };
    let ct = match std::str::from_utf8(hdr)
        .ok()
        .and_then(|s| s.parse::<mime::Mime>().ok())
    {
        Some(m) if m.type_() == mime::APPLICATION && m.subtype() == mime::JSON => mime::JSON,
        _ => {
            return Err(Error::UnsupportedMediaType(
                String::from_utf8_lossy(hdr).to_string(),
            ))
        }
    };

    Ok((ct, resp.into_body()))
}

#[cfg(test)]
pub mod test {
    use super::*;
    use hyper::{Body, Request, Response};
    use serde_json::{json, Value};
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn test_hello_world() {
        // Start a TestServer which adds "world" to it's arguments.
        let srv = TestServer::start(|args| {
            let mut out = Vec::new();
            out.extend(args.iter().map(|v| v.clone()));
            out.push(json!("world"));
            out
        });

        let mut inv = srv.lambda.start_invocation();
        inv.start_row();
        inv.add_column(&json!("hello")).unwrap();
        inv.finish_row();

        inv.start_row();
        inv.add_column(&json!("big")).unwrap();
        inv.add_column(&json!("wide")).unwrap();
        inv.finish_row();

        let inv = inv.finish().await.unwrap();
        let inv = inv.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(
            inv,
            vec![
                vec![json!("hello"), json!("world")],
                vec![json!("big"), json!("wide"), json!("world")],
            ]
        );
    }

    pub struct TestServer {
        pub lambda: Lambda,
        tx_stop: Option<oneshot::Sender<()>>,
        // TempDir's drop() deletes the directory.
        _tmpdir: tempfile::TempDir,
    }

    impl TestServer {
        pub fn start(
            func: impl Fn(&[serde_json::Value]) -> Vec<serde_json::Value> + Send + Clone + 'static,
        ) -> TestServer {
            let handle = move |req: Request<Body>| {
                let func = func.clone();

                async move {
                    match req.headers().get(http::header::CONTENT_TYPE) {
                        Some(hv) if hv.as_bytes() == b"application/json" => (),
                        other @ _ => panic!(format!("invalid content-type: {:?}", other)),
                    };

                    let b = hyper::body::to_bytes(req.into_body()).await.unwrap();
                    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();

                    let mut out = Vec::new();
                    for row in v.as_array().unwrap() {
                        out.push(Value::Array(func(row.as_array().unwrap())));
                    }
                    let out = serde_json::to_vec(&out).unwrap();

                    let mut resp = Response::new(hyper::Body::from(out));
                    resp.headers_mut().append(
                        http::header::CONTENT_TYPE,
                        "application/json".parse().unwrap(),
                    );

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
            let server = crate::serve::unix_domain_socket(svc, &socket_path, rx_stop);
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
            self.tx_stop.take().unwrap().send(()).unwrap();
        }
    }
}
