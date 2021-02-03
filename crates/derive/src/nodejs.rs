use super::lambda;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("http error: {0}")]
    Http(#[from] lambda::Error),
    #[error("Failed to install npm package")]
    NpmInstallFailed,
}

pub struct NodeRuntime {
    sock: PathBuf,
    proc: Option<process::Child>,
}

impl NodeRuntime {
    /// Build a NodeRuntime which uses the given Unix Domain Socket path.
    pub fn from_uds_path(socket: impl AsRef<Path>) -> NodeRuntime {
        NodeRuntime {
            sock: socket.as_ref().into(),
            proc: None,
        }
    }

    /// Start a NodeJS worker using the given NPM package tarball.
    pub fn start(dir: impl AsRef<Path>, package_tgz: &[u8]) -> Result<NodeRuntime, Error> {
        // Extract catalog pack.tgz to a new temp directory.
        let dir = dir.as_ref();
        let package_path = dir.join("npm-package.tgz");
        std::fs::write(&package_path, package_tgz)?;
        tracing::info!("wrote catalog NPM package to {:?}", package_path);

        let output = process::Command::new("npm")
            .arg("--version")
            .current_dir(dir)
            .output()?;
        tracing::info!("running NPM version {:?}", &output.stdout);

        // Bootstrap a Node package with the installed pack.
        let cmd = process::Command::new("npm")
            .arg("install")
            .arg("file://./npm-package.tgz")
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
        Ok(NodeRuntime {
            proc: Some(proc),
            sock,
        })
    }

    pub fn new_lambda(&self, path: impl Into<String>) -> lambda::Lambda {
        let client = hyper::Client::builder()
            .http2_only(true)
            .build::<_, hyper::Body>(hyperlocal::UnixConnector);

        lambda::Lambda::UnixJson {
            client,
            sock: self.sock.clone(),
            path: path.into(),
        }
    }
}

impl Drop for NodeRuntime {
    fn drop(&mut self) {
        if let Some(proc) = &mut self.proc {
            let _ = proc.kill();
            proc.wait().unwrap();
        }
    }
}
