use super::lambda;
use crate::catalog;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("database error: {0}")]
    SQLite(#[from] rusqlite::Error),
    #[error("http error: {0}")]
    Http(#[from] lambda::Error),
    #[error("Failed to install npm package")]
    NpmInstallFailed,
}

pub struct NodeRuntime {
    sock: PathBuf,
    proc: process::Child,
}

impl NodeRuntime {
    /// Start a NodeJS worker using the NPM package extracted from the catalog database.
    pub fn start(db: &catalog::DB, dir: impl AsRef<Path>) -> Result<NodeRuntime, Error> {
        // Extract catalog pack.tgz to a new temp directory.
        let dir = dir.as_ref();
        let pack_path = dir.join("npm-pack.tgz");
        let pack_contents: Vec<u8> = db
            .prepare("SELECT content FROM resources WHERE content_type = ?")?
            .query_row(catalog::sql_params![catalog::ContentType::NpmPack], |r| {
                r.get(0)
            })?;
        fs::write(&pack_path, pack_contents)?;
        log::info!("wrote catalog npm package to {:?}", pack_path);

        let output = process::Command::new("npm")
            .arg("--version")
            .current_dir(dir)
            .output()?;
        log::info!("running NPM version {:?}", &output.stdout);

        // Bootstrap a Node package with the installed pack.
        let cmd = process::Command::new("npm")
            .arg("install")
            .arg("file://./npm-pack.tgz")
            .current_dir(dir)
            .output()?;

        if !cmd.status.success() {
            std::io::stderr().write_all(&cmd.stderr)?;
            return Err(Error::NpmInstallFailed);
        }

        let sock = dir.join("nodejs-socket");

        // Start NodeJS subprocess, serving over ${dir}/socket.
        let cmd = dir.join("node_modules/.bin/catalog-js-transformer");
        let mut proc = process::Command::new(cmd)
            .stdin(process::Stdio::null())
            .stdout(process::Stdio::piped())
            .current_dir(dir)
            .env("SOCKET_PATH", &sock)
            .spawn()?;

        let mut stdout = proc.stdout.take().expect("stdout pipe");

        // Wait for subprocess to indicate it's started (and has bound its server socket).
        let mut ready = [0; 6];
        stdout.read_exact(&mut ready)?;
        assert_eq!(&ready, b"READY\n");

        // Having read "READY\n" header, forward remaining stdout to our own descriptor.
        std::thread::spawn(move || std::io::copy(&mut stdout, &mut std::io::stdout()));

        log::info!("nodejs runtime is ready {:?}", proc);
        Ok(NodeRuntime { proc, sock })
    }

    pub fn new_update_lambda(&self, transform_id: i32) -> lambda::Lambda {
        self.new_lambda(format!("/update/{}", transform_id))
    }

    pub fn new_publish_lambda(&self, transform_id: i32) -> lambda::Lambda {
        self.new_lambda(format!("/publish/{}", transform_id))
    }

    pub async fn invoke_bootstrap(&self, derivation_id: i32) -> Result<(), lambda::Error> {
        let l = self.new_lambda(format!("/bootstrap/{}", derivation_id));
        let _ = l.start_invocation().finish().await?;
        Ok(())
    }

    fn new_lambda(&self, path: String) -> lambda::Lambda {
        let client = hyper::Client::builder()
            .http2_only(true)
            .build::<_, hyper::Body>(hyperlocal::UnixConnector);

        lambda::Lambda::UnixJson {
            client,
            sock: self.sock.clone(),
            path,
        }
    }
}

impl Drop for NodeRuntime {
    fn drop(&mut self) {
        let _ = self.proc.kill();
        self.proc.wait().unwrap();
    }
}
