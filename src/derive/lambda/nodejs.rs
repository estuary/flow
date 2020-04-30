use super::Error;
use crate::catalog::{sql_params, ContentType, DB};
use bytes;
use futures::TryStream;
use http_body::Body;
use hyper;
use hyperlocal::{UnixConnector, Uri as UnixUri};
use std::fs;
use std::io::{Read, Write};
use std::path;
use std::process;
use tempfile;
use url;

#[derive(Debug)]
pub struct Service {
    dir: tempfile::TempDir,
    sock: path::PathBuf,
    proc: process::Child,
    client: hyper::Client<UnixConnector>,
}

impl Service {
    pub fn new(db: &DB) -> Result<Service, Error> {
        Self::start(db, tempfile::tempdir()?)
    }

    pub async fn bootstrap(
        &self,
        derivation_id: i64,
        store: &url::Url,
    ) -> Result<(), Error> {

        let req = hyper::Request::builder()
            .uri(UnixUri::new(
                &self.sock,
                &format!("/bootstrap/{}", derivation_id),
            ))
            .header("state-store", store.as_str())
            .body(hyper::Body::empty())?;

        let mut resp = self.client.request(req).await?;

        if !resp.status().is_success() {
            let body = hyper::body::to_bytes(resp.body_mut()).await?;
            return Err(Error::RemoteHTTPError {
                status: resp.status(),
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }
        Ok(())
    }

    pub async fn transform(
        &self,
        transform_id: i64,
        store: &url::Url,
    ) -> Result<
        (
            hyper::body::Sender,
            impl TryStream<Ok = bytes::Bytes, Error = Error> + Unpin,
        ),
        Error,
    > {
        let (req_body_sink, req_body) = hyper::Body::channel();
        let req = hyper::Request::builder()
            .uri(UnixUri::new(
                &self.sock,
                &format!("/transform/{}", transform_id),
            ))
            .header("state-store", store.as_str())
            .body(req_body)?;

        let mut resp = self.client.request(req).await?;

        if !resp.status().is_success() {
            let body = hyper::body::to_bytes(resp.body_mut()).await?;
            return Err(Error::RemoteHTTPError {
                status: resp.status(),
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }

        let foobar = futures::stream::try_unfold(resp.into_body(), |mut body| {
            // Inner closure is !Unpin, so box it to allow it to be moved while still pinned.
            // TODO(johnny): Elide allocation by hoisting to a non-async struct implementing Future + Unpin.
            Box::pin(async move {
                if let Some(chunk) = body.data().await {
                    return Ok(Some((chunk?, body)));
                }

                if let Some(trailers) = body.trailers().await? {
                    log::info!("got trailers! {:?}", trailers);
                // TODO - inspect for errors, and propagate.
                } else {
                    log::error!("missing expected trailers!");
                }
                return Ok(None);
            })
        });

        Ok((req_body_sink, foobar))
    }

    fn start(db: &DB, dir: tempfile::TempDir) -> Result<Service, Error> {
        // Extract catalog pack.tgz to the temp directory.
        let pack: Vec<u8> = db
            .prepare("SELECT content FROM resources WHERE content_type = ?")?
            .query_row(sql_params![ContentType::NpmPack], |r| r.get(0))?;
        fs::write(dir.path().join("pack.tgz"), pack)?;

        // Bootstrap a Node package with the installed pack.
        let cmd = process::Command::new("npm")
            .arg("install")
            .arg("file://./pack.tgz")
            .current_dir(dir.path())
            .output()?;

        if !cmd.status.success() {
            std::io::stderr().write(&cmd.stderr)?;
            return Err(Error::NpmInstallFailed);
        }
        log::info!("installed transform npm pack to {:?}", dir.path());

        let sock = dir.path().join("socket");

        // Start NodeJS subprocess, serving over ${dir}/socket.
        let cmd = dir.path().join("node_modules/.bin/catalog-js-transformer");
        let mut proc = process::Command::new(cmd)
            .stdin(process::Stdio::null())
            .stdout(process::Stdio::piped())
            .current_dir(dir.path())
            .env("SOCKET_PATH", &sock)
            .spawn()?;

        // Wait for subprocess to indicate it's started (and has bound its server socket).
        let mut ready = [0; 6];
        proc.stdout.as_mut().unwrap().read_exact(&mut ready)?;
        assert_eq!(&ready, b"READY\n");

        let client = hyper::Client::builder()
            .http2_only(true)
            .build::<_, hyper::Body>(UnixConnector);

        log::info!("nodejs transform worker is ready {:?}", proc);
        Ok(Service {
            dir,
            proc,
            sock,
            client,
        })
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.proc.kill();
        self.proc.wait().unwrap();
    }
}
