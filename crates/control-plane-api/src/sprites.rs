//! Client for the Fly.io Sprites API, which backs [`crate::sandboxes`].

use anyhow::Context;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

const BASE_URL: &str = "https://api.sprites.dev";

pub struct Client {
    http: reqwest::Client,
    base_url: url::Url,
    token: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct Sprite {
    pub name: String,
    /// Runtime state: `running`, `warm`, or `cold`.
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Checkpoint {
    pub id: String,
}

#[derive(Debug, PartialEq)]
pub struct Output {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
}

impl Client {
    pub fn new(token: String) -> Self {
        let base_url = url::Url::parse(BASE_URL).expect("BASE_URL is a valid URL");
        assert_eq!(base_url.scheme(), "https", "BASE_URL must use HTTPS");
        Self {
            http: reqwest::Client::new(),
            base_url,
            token,
        }
    }

    /// Creates `name`, failing if it already exists.
    pub async fn create_sprite(&self, name: &str) -> anyhow::Result<Sprite> {
        let url = self.url(["v1", "sprites"]);

        // TODO: This request has no timeout, and `http` sets no default. With
        // `wait_for_capacity`, it can block `sandboxCreate` without bound. A
        // client that disconnects meanwhile leaves an unready record holding
        // its catalog name.
        let response = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .json(&serde_json::json!({"name": name, "wait_for_capacity": true}))
            .send()
            .await
            .context("failed to reach the Sprites API")?;

        match response.status() {
            reqwest::StatusCode::CREATED => response
                .json()
                .await
                .context("failed to decode the created sprite"),
            _ => Err(api_error("failed to create sprite", response).await),
        }
    }

    pub async fn delete_sprite(&self, name: &str) -> anyhow::Result<bool> {
        let url = self.url(["v1", "sprites", name]);

        let response = self
            .http
            .delete(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .context("failed to reach the Sprites API")?;

        match response.status() {
            // Deleting a non-existent sprite succeeds, but returns false
            reqwest::StatusCode::NOT_FOUND => Ok(false),
            status if status.is_success() => Ok(true),
            _ => Err(api_error("failed to delete sprite", response).await),
        }
    }

    pub async fn create_checkpoint(&self, name: &str) -> anyhow::Result<()> {
        let url = self.url(["v1", "sprites", name, "checkpoint"]);

        let response = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .timeout(std::time::Duration::from_secs(30))
            .json(&serde_json::json!({}))
            .send()
            .await
            .context("failed to reach the Sprites API")?;

        stream_result("create checkpoint", response).await
    }

    pub async fn list_checkpoints(&self, name: &str) -> anyhow::Result<Vec<Checkpoint>> {
        let url = self.url(["v1", "sprites", name, "checkpoints"]);

        let response = self
            .http
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .context("failed to reach the Sprites API")?;

        if !response.status().is_success() {
            return Err(api_error("failed to list checkpoints", response).await);
        }
        response
            .json()
            .await
            .context("failed to decode checkpoints")
    }

    pub async fn restore_checkpoint(&self, name: &str, id: &str) -> anyhow::Result<()> {
        let url = self.url(["v1", "sprites", name, "checkpoints", id, "restore"]);

        let response = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .timeout(std::time::Duration::from_secs(120))
            .send()
            .await
            .context("failed to reach the Sprites API")?;

        stream_result("restore checkpoint", response).await
    }

    pub async fn read_file(
        &self,
        name: &str,
        working_dir: &str,
        path: &str,
    ) -> anyhow::Result<Option<bytes::Bytes>> {
        let response = self
            .http
            .get(self.url(["v1", "sprites", name, "fs", "read"]))
            .bearer_auth(&self.token)
            .query(&[("workingDir", working_dir), ("path", path)])
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .context("reading sprite file")?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(api_error("reading sprite file", response).await);
        }
        Ok(Some(
            response
                .bytes()
                .await
                .context("reading sprite file contents")?,
        ))
    }

    pub async fn write_file(
        &self,
        name: &str,
        working_dir: &str,
        path: &str,
        contents: String,
    ) -> anyhow::Result<()> {
        let response = self
            .http
            .put(self.url(["v1", "sprites", name, "fs", "write"]))
            .bearer_auth(&self.token)
            .query(&[
                ("workingDir", working_dir),
                ("path", path),
                ("mode", "0600"),
            ])
            .timeout(std::time::Duration::from_secs(30))
            .body(contents)
            .send()
            .await
            .context("writing sprite file")?;
        if !response.status().is_success() {
            return Err(api_error("writing sprite file", response).await);
        }
        Ok(())
    }

    /// Returns the final exit code, or None if the session no longer exists.
    pub async fn kill_exec(&self, name: &str, session_id: &str) -> anyhow::Result<Option<i32>> {
        let response = self
            .http
            .post(self.url(["v1", "sprites", name, "exec", session_id, "kill"]))
            .bearer_auth(&self.token)
            .query(&[("signal", "SIGTERM"), ("timeout", "10s")])
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .context("killing sprite exec")?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(api_error("killing sprite exec", response).await);
        }
        let body = response
            .text()
            .await
            .context("reading exec kill response")?;
        #[derive(serde::Deserialize)]
        #[serde(tag = "type")]
        enum Event {
            #[serde(rename = "complete")]
            Complete { exit_code: i32 },
            #[serde(rename = "error")]
            Error { message: String },
            #[serde(other)]
            Other,
        }
        let mut exit_code = None;
        for line in body.lines().filter(|line| !line.trim().is_empty()) {
            match serde_json::from_str(line).context("decoding exec kill event")? {
                Event::Complete { exit_code: code } => exit_code = Some(code),
                Event::Error { message } => anyhow::bail!("killing sprite exec: {message}"),
                Event::Other => {}
            }
        }
        anyhow::ensure!(
            exit_code.is_some(),
            "exec kill response ended without completing"
        );
        Ok(exit_code)
    }

    /// The timeout applies to the request and response stream; it does not stop the command.
    pub async fn exec_stream(
        &self,
        name: &str,
        argv: &[&str],
        stdin: Option<&str>,
        timeout: std::time::Duration,
    ) -> Result<futures::stream::BoxStream<'static, anyhow::Result<Frame>>, ExecError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut url = self.url(["v1", "sprites", name, "exec"]);
        url.set_scheme("wss").expect("valid WebSocket scheme");
        {
            let mut query = url.query_pairs_mut();
            for arg in argv {
                query.append_pair("cmd", arg);
            }
            query.append_pair("stdin", if stdin.is_some() { "true" } else { "false" });
            query.append_pair("tty", "false");
            // Allow the command to run indefinitely after client disconnects
            query.append_pair("max_run_after_disconnect", "0");
        }
        let mut request = url
            .as_str()
            .into_client_request()
            .context("building exec request")?;
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {}", self.token)
                .parse()
                .context("invalid Sprites token header")?,
        );
        let socket = match tokio::time::timeout_at(
            deadline,
            tokio_tungstenite::connect_async(request),
        )
        .await
        {
            Ok(Ok((socket, _))) => socket,
            Ok(Err(tungstenite::Error::Http(response))) => {
                let status = response.status();
                let body = String::from_utf8_lossy(response.body().as_deref().unwrap_or_default());
                if status == reqwest::StatusCode::NOT_FOUND {
                    return Err(ExecError::SpriteMissing);
                }
                if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                    tracing::warn!(sprite = name, %status, %body, "Sprites exec endpoint unavailable");
                    return Err(ExecError::NotReady);
                }
                return Err(
                    anyhow::anyhow!("failed to start command: HTTP {status}: {body}").into(),
                );
            }
            Ok(Err(err)) => {
                return Err(ExecError::Transport(
                    anyhow::Error::new(err).context("connecting to Sprites exec"),
                ));
            }
            Err(err) => {
                return Err(ExecError::Transport(
                    anyhow::Error::new(err).context("connecting to Sprites exec"),
                ));
            }
        };
        let stdin = stdin.unwrap_or_default().to_owned();
        let (mut writer, mut reader) = socket.split();
        let frames = async_stream::try_stream! {
            // Send input and receive command output concurrently so one doesn't block the other.
            let send_input = async move {
                for chunk in stdin.as_bytes().chunks(32 * 1024) {
                    let mut payload = Vec::with_capacity(chunk.len() + 1);
                    payload.push(0);
                    payload.extend_from_slice(chunk);
                    writer.send(tungstenite::Message::Binary(payload.into())).await?;
                }
                writer.send(tungstenite::Message::Binary(bytes::Bytes::from_static(&[4]))).await?;
                Ok::<_, tungstenite::Error>(())
            };
            tokio::pin!(send_input);
            let mut input_sent = false;
            loop {
                let event = tokio::select! {
                    result = &mut send_input, if !input_sent => futures::future::Either::Left(result),
                    message = tokio::time::timeout_at(deadline, reader.next()) => futures::future::Either::Right(message),
                };
                let message = match event {
                    futures::future::Either::Left(result) => {
                        result.context("sending command stdin")?;
                        input_sent = true;
                        continue;
                    }
                    futures::future::Either::Right(message) => message.context("timed out waiting for Sprites exec")?,
                };
                let Some(message) = message else { break };
                let message = message.context("reading command output")?;
                if matches!(message, tungstenite::Message::Close(_)) {
                    break;
                }
                if let Some(frame) = decode_message(message)? {
                    let exited = matches!(frame, Frame::Exit(_));
                    yield frame;
                    if exited { break; }
                }
            }
        };
        Ok(frames.boxed())
    }

    pub async fn exec(
        &self,
        name: &str,
        argv: &[&str],
        timeout: std::time::Duration,
    ) -> anyhow::Result<Output> {
        collect_output(self.exec_stream(name, argv, None, timeout).await?).await
    }

    fn url<'s>(&self, segments: impl IntoIterator<Item = &'s str>) -> url::Url {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .expect("BASE_URL can be a base")
            .extend(segments);
        url
    }
}

const TAG_STDOUT: u8 = 1;
const TAG_STDERR: u8 = 2;
const TAG_EXIT: u8 = 3;

#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    SessionInfo { session_id: String },
    Stdout(bytes::Bytes),
    Stderr(bytes::Bytes),
    Exit(i32),
}

#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("sprite not found")]
    SpriteMissing,
    #[error("sprite not ready")]
    NotReady,
    /// No response was received. The command may have started, so retrying
    /// could run it twice.
    #[error(transparent)]
    Transport(anyhow::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub async fn collect_output(
    mut frames: futures::stream::BoxStream<'static, anyhow::Result<Frame>>,
) -> anyhow::Result<Output> {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut exit_code = None;

    while let Some(frame) = frames.next().await {
        match frame? {
            Frame::SessionInfo { .. } => {}
            Frame::Stdout(bytes) => stdout.extend_from_slice(&bytes),
            Frame::Stderr(bytes) => stderr.extend_from_slice(&bytes),
            Frame::Exit(code) => exit_code = Some(code),
        }
    }

    let exit_code = exit_code.context("sprite closed the exec response without an exit status")?;
    Ok(Output {
        stdout,
        stderr,
        exit_code,
    })
}

fn decode_message(message: tungstenite::Message) -> anyhow::Result<Option<Frame>> {
    match message {
        tungstenite::Message::Binary(data) => {
            let Some(&tag) = data.first() else {
                return Ok(None);
            };
            let payload = data.slice(1..);
            Ok(Some(match tag {
                TAG_STDOUT => Frame::Stdout(payload),
                TAG_STDERR => Frame::Stderr(payload),
                TAG_EXIT => {
                    anyhow::ensure!(payload.len() == 1, "invalid exec exit frame");
                    Frame::Exit(payload[0] as i32)
                }
                _ => anyhow::bail!("sprite sent an exec frame with unknown stream tag {tag}"),
            }))
        }
        tungstenite::Message::Text(text) => {
            #[derive(serde::Deserialize)]
            #[serde(tag = "type")]
            enum Event {
                #[serde(rename = "session_info")]
                SessionInfo { session_id: String },
                #[serde(rename = "exit")]
                Exit { exit_code: i32 },
                #[serde(rename = "error")]
                Error { error: String },
                #[serde(other)]
                Other,
            }
            match serde_json::from_str(&text).context("decoding Sprites exec event")? {
                Event::SessionInfo { session_id } => Ok(Some(Frame::SessionInfo { session_id })),
                Event::Exit { exit_code } => Ok(Some(Frame::Exit(exit_code))),
                Event::Error { error } => anyhow::bail!("Sprites exec failed: {error}"),
                Event::Other => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

/// Checkpoint and restore report their outcome as NDJSON events
/// 200 doesn't necessarily indicate success
async fn stream_result(context: &str, response: reqwest::Response) -> anyhow::Result<()> {
    let status = response.status();
    let body = response
        .text()
        .await
        .context("reading checkpoint response")?;

    if !status.is_success() {
        anyhow::bail!("{context} failed: HTTP {status}: {body}");
    }

    #[derive(serde::Deserialize)]
    struct Event {
        #[serde(rename = "type")]
        kind: String,
        #[serde(default)]
        error: Option<String>,
    }

    let mut completed = false;
    for line in body.lines() {
        let Ok(event) = serde_json::from_str::<Event>(line.trim()) else {
            continue;
        };
        match event.kind.as_str() {
            "error" => anyhow::bail!(
                "{context} failed: {}",
                event.error.as_deref().unwrap_or("unknown error")
            ),
            "complete" => completed = true,
            _ => {}
        }
    }

    anyhow::ensure!(completed, "{context} ended without completing");
    Ok(())
}

async fn api_error(context: &str, response: reqwest::Response) -> anyhow::Error {
    #[derive(serde::Deserialize)]
    struct Body {
        error: String,
    }

    let status = response.status();
    let body = response.text().await.unwrap_or_default();

    match serde_json::from_str::<Body>(&body) {
        Ok(Body { error }) => anyhow::anyhow!("{context}: {error} (HTTP {status})"),
        Err(_) => anyhow::anyhow!("{context}: HTTP {status}: {body}"),
    }
}
