//! Client for the Fly.io Sprites API, which backs GraphQL sandboxes.
//!
//! A sprite is a persistent, hardware-isolated Linux VM addressed by a name
//! that is unique within a Fly organization. This module speaks the subset of
//! <https://api.sprites.dev/v1> that sandboxes need: create, get, delete, and
//! one-shot command execution.

use anyhow::Context;
use futures::StreamExt;

/// Base URL of the hosted Sprites API.
const BASE_URL: &str = "https://api.sprites.dev";

/// Client carries the organization credential used for Sprites API calls.
pub struct Client {
    http: reqwest::Client,
    base_url: url::Url,
    token: String,
}

/// Sprite is the subset of a sprite's API representation that sandboxes surface.
#[derive(Debug, serde::Deserialize)]
pub struct Sprite {
    pub name: String,
    /// Lifecycle state: `running` while a command is executing, `warm` once the
    /// VM has suspended with its memory frozen, and `cold` once it is fully
    /// stopped. A sprite suspends within a second of its last command exiting;
    /// measured, it falls from warm to cold after about ten minutes.
    /// Waking a warm sprite takes 100-500ms and thaws its processes where they
    /// stopped; waking a cold one takes 1-2s and starts them fresh.
    pub status: String,
    /// Hostname serving ports that the sprite exposes, if it has one.
    #[serde(default)]
    pub url: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// A saved filesystem snapshot of a sprite. The API also reports a synthetic
/// entry with id [`LIVE_STATE_ID`] for the current live state, which is not a
/// saved checkpoint and cannot be deleted.
#[derive(Debug, serde::Deserialize)]
pub struct Checkpoint {
    pub id: String,
    /// Free-text label set when the checkpoint was created, absent on the
    /// synthetic live-state entry and on auto-created checkpoints.
    #[serde(default)]
    pub comment: Option<String>,
}

/// Id of the synthetic checkpoint the API lists for a sprite's current live
/// state. It is not a saved checkpoint.
pub const LIVE_STATE_ID: &str = "Current";

/// Output of a command that ran to completion in a sprite.
#[derive(Debug, PartialEq)]
pub struct Output {
    /// Raw stdout bytes. A command may emit anything, including invalid UTF-8.
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
}

impl Client {
    pub fn new(token: String) -> Self {
        Self::with_base_url(
            token,
            url::Url::parse(BASE_URL).expect("BASE_URL is a valid URL"),
        )
    }

    /// A client for an API served at `base_url`, which tests point at a server
    /// of their own.
    pub fn with_base_url(token: String, base_url: url::Url) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url,
            token,
        }
    }

    /// Creates `name` and returns it, treating an already-existing sprite of
    /// that name as success. A conflict means the sprite was already
    /// provisioned.
    ///
    /// The call returns once VM capacity is reserved, so the sprite is ready to
    /// boot on its first exec rather than answering 503 while capacity is found.
    pub async fn create_sprite(&self, name: &str) -> anyhow::Result<Sprite> {
        let url = self.url(["v1", "sprites"]);

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
            reqwest::StatusCode::CONFLICT => self.get_sprite(name).await,
            _ => Err(api_error("failed to create sprite", response).await),
        }
    }

    /// Fetches `name`, which must exist.
    pub async fn get_sprite(&self, name: &str) -> anyhow::Result<Sprite> {
        let url = self.url(["v1", "sprites", name]);

        let response = self
            .http
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .context("failed to reach the Sprites API")?;

        if !response.status().is_success() {
            return Err(api_error("failed to fetch sprite", response).await);
        }
        response.json().await.context("failed to decode sprite")
    }

    /// Deletes `name` and its storage, returning whether it existed.
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
            reqwest::StatusCode::NOT_FOUND => Ok(false),
            status if status.is_success() => Ok(true),
            _ => Err(api_error("failed to delete sprite", response).await),
        }
    }

    /// Snapshots `name`'s filesystem into a new checkpoint, labeled `comment`.
    pub async fn create_checkpoint(&self, name: &str, comment: &str) -> anyhow::Result<()> {
        let url = self.url(["v1", "sprites", name, "checkpoint"]);

        let response = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .timeout(std::time::Duration::from_secs(30))
            .json(&serde_json::json!({ "comment": comment }))
            .send()
            .await
            .context("failed to reach the Sprites API")?;

        stream_result("create checkpoint", response).await
    }

    /// Lists `name`'s saved checkpoints, plus the synthetic [`LIVE_STATE_ID`]
    /// entry for its current state.
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

    /// Deletes checkpoint `id` of `name`, treating an already-absent checkpoint
    /// as success.
    pub async fn delete_checkpoint(&self, name: &str, id: &str) -> anyhow::Result<()> {
        let url = self.url(["v1", "sprites", name, "checkpoints", id]);

        let response = self
            .http
            .delete(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .context("failed to reach the Sprites API")?;

        match response.status() {
            reqwest::StatusCode::NOT_FOUND => Ok(()),
            status if status.is_success() => Ok(()),
            _ => Err(api_error("failed to delete checkpoint", response).await),
        }
    }

    /// Restores `name` to checkpoint `id`, reverting its filesystem and
    /// restarting its processes from that snapshot.
    ///
    /// The API snapshots the current state into a new checkpoint before it
    /// reverts, so a caller that wants a single steady-state checkpoint prunes
    /// afterward. Restore restarts the sprite's container, so it takes longer
    /// than the other calls.
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

    /// Streams the output of `argv` running in `name`. `argv[0]` is the
    /// executable and the rest are its arguments.
    ///
    /// The command gets no stdin, and `timeout` bounds the whole exchange. A
    /// command that outlives the timeout keeps running in the sprite: the API
    /// detaches it rather than killing it when the response is abandoned.
    ///
    /// The returned stream is the seam between transports. This implementation
    /// reads the API's chunked HTTP response; attaching to an existing session
    /// requires the WebSocket endpoint, which carries the same frames and so
    /// would yield the same stream.
    pub async fn exec_stream(
        &self,
        name: &str,
        argv: &[&str],
        timeout: std::time::Duration,
    ) -> Result<futures::stream::BoxStream<'static, anyhow::Result<Frame>>, ExecError> {
        self.exec_stream_with_stdin(name, argv, None, timeout).await
    }

    /// Sends finite stdin in the HTTP body, never in the command URL. The
    /// caller must consume it before detaching if execution needs it later.
    pub async fn exec_stream_with_stdin(
        &self,
        name: &str,
        argv: &[&str],
        stdin: Option<&str>,
        timeout: std::time::Duration,
    ) -> Result<futures::stream::BoxStream<'static, anyhow::Result<Frame>>, ExecError> {
        let started = std::time::Instant::now();
        let mut url = self.url(["v1", "sprites", name, "exec"]);
        {
            let mut query = url.query_pairs_mut();
            for arg in argv {
                query.append_pair("cmd", arg);
            }
            query.append_pair("stdin", if stdin.is_some() { "true" } else { "false" });
        }

        let response = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .timeout(timeout)
            .body(stdin.unwrap_or_default().to_owned())
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .context("failed to reach the Sprites API")
            .map_err(ExecError::Transport)?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(ExecError::SpriteMissing);
        } else if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            // Never log a free-form provider response: it may echo command
            // arguments or credentials. Only fixed diagnostic categories leave
            // this boundary, even for requests without stdin.
            let diagnostic = read_unavailable_reason(response).await;
            tracing::warn!(
                sprite = name,
                status = 503,
                reason = diagnostic,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Sprites exec endpoint unavailable"
            );
            return Err(ExecError::NotReady);
        } else if !response.status().is_success() {
            // A provider error may echo its request body, which can contain
            // credentials. Keep that body out of errors callers may log.
            if stdin.is_some() {
                return Err(
                    anyhow::anyhow!("failed to start command: HTTP {}", response.status()).into(),
                );
            }
            return Err(api_error("failed to start command", response).await.into());
        }

        // Frames carry no length and the transport may split one across several
        // deliveries, so decoding threads state through the body. See [`Decoder`].
        let frames = response
            .bytes_stream()
            .scan(Decoder::default(), |decoder, chunk| {
                futures::future::ready(Some(match chunk {
                    Ok(chunk) => decoder.step(chunk).transpose(),
                    Err(err) => Some(Err(anyhow::Error::new(err.without_url())
                        .context("failed to read the command's output"))),
                }))
            })
            .filter_map(futures::future::ready);

        Ok(frames.boxed())
    }

    /// Runs `argv` to completion in `name` and returns its whole output.
    ///
    /// This is [`Client::exec_stream`] folded into a buffer, for callers that
    /// want the result rather than the progress.
    pub async fn exec(
        &self,
        name: &str,
        argv: &[&str],
        timeout: std::time::Duration,
    ) -> anyhow::Result<Output> {
        fold_output(self.exec_stream(name, argv, timeout).await?).await
    }

    /// Best-effort observations after persistent 503s. Session representations
    /// contain commands, so only their count and a known lifecycle state are
    /// logged. Diagnostic failures must not replace the operation's error.
    pub async fn log_unavailable_state(&self, name: &str) {
        for (resource, segments) in [
            ("sprite", vec!["v1", "sprites", name]),
            ("sessions", vec!["v1", "sprites", name, "exec"]),
        ] {
            let result = self
                .http
                .get(self.url(segments))
                .bearer_auth(&self.token)
                .timeout(std::time::Duration::from_secs(3))
                .send()
                .await;
            let Ok(mut response) = result else {
                tracing::warn!(sprite = name, resource, "Sprites diagnostic request failed");
                continue;
            };
            let status = response.status().as_u16();
            if !response.status().is_success() {
                tracing::warn!(
                    sprite = name,
                    resource,
                    status,
                    "Sprites diagnostic endpoint unavailable"
                );
                continue;
            }
            // Session listings can include large command arguments. Bound the
            // diagnostic itself and never print a decoding error or raw body.
            let mut body = Vec::new();
            let complete = loop {
                match response.chunk().await {
                    Ok(Some(chunk)) if body.len() + chunk.len() <= 65536 => {
                        body.extend_from_slice(&chunk)
                    }
                    Ok(None) => break true,
                    _ => break false,
                }
            };
            let value = complete
                .then(|| serde_json::from_slice::<serde_json::Value>(&body).ok())
                .flatten();
            let Some(value) = value else {
                tracing::warn!(
                    sprite = name,
                    resource,
                    status,
                    "Sprites diagnostic response unreadable"
                );
                continue;
            };
            if resource == "sessions" {
                tracing::warn!(sprite = name, session_count = ?value.as_array().map(Vec::len), "Sprites exec session diagnostic");
            } else {
                let state = match value.get("status").and_then(serde_json::Value::as_str) {
                    Some("running") => "running",
                    Some("warm") => "warm",
                    Some("cold") => "cold",
                    _ => "unknown",
                };
                tracing::warn!(sprite = name, state, "Sprites lifecycle diagnostic");
            }
        }
    }

    /// Builds an API URL from percent-encoded path segments.
    fn url<'s>(&self, segments: impl IntoIterator<Item = &'s str>) -> url::Url {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .expect("BASE_URL can be a base")
            .extend(segments);
        url
    }
}

/// Stream tags of the exec framing protocol.
const TAG_STDOUT: u8 = 1;
const TAG_STDERR: u8 = 2;
const TAG_EXIT: u8 = 3;

/// One frame of a command's output.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    Stdout(bytes::Bytes),
    Stderr(bytes::Bytes),
    /// The command's exit status, which is always the final frame.
    Exit(i32),
}

/// Why a command could not be started in a sprite.
#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    /// The named sprite does not exist. A caller that provisions sandboxes on
    /// demand creates it and retries.
    #[error("sprite not found")]
    SpriteMissing,
    /// The exec endpoint returned HTTP 503. This may be a wake transition or
    /// a provider failure; it does not establish that the sprite is sleeping.
    #[error("sprite not ready")]
    NotReady,
    /// The request failed before the API answered it: the connection could
    /// not be made, broke, or timed out while the response was pending.
    /// Whether the sprite received the command is unknown, so a caller that
    /// must not run it twice checks the sprite before retrying.
    #[error(transparent)]
    Transport(anyhow::Error),
    /// The API answered and refused the command, or the response was
    /// malformed. The command did not start.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Bound both diagnostic latency and memory, and emit only fixed labels.
async fn read_unavailable_reason(mut response: reqwest::Response) -> &'static str {
    let read = async {
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.ok()? {
            if body.len() + chunk.len() > 8192 {
                return Some("response_too_large");
            }
            body.extend_from_slice(&chunk);
        }
        Some(unavailable_reason(&body))
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), read)
        .await
        .ok()
        .flatten()
        .unwrap_or("response_unreadable")
}

fn unavailable_reason(body: &[u8]) -> &'static str {
    let text = String::from_utf8_lossy(body).to_ascii_lowercase();
    for (needles, reason) in [
        (&["capacity"][..], "capacity"),
        (&["too many", "rate limit"][..], "limit"),
        (&["out of memory", "oom killed"][..], "memory"),
        (&["timeout", "timed out", "deadline"][..], "timeout"),
        (
            &["connection refused", "failed to connect", "unreachable"][..],
            "connection",
        ),
        (&["not ready", "booting", "waking"][..], "not_ready"),
        (&["unavailable"][..], "unavailable"),
    ] {
        if needles.iter().any(|needle| text.contains(*needle)) {
            return reason;
        }
    }
    "unclassified"
}

/// Folds a frame stream into the command's whole output.
pub async fn fold_output(
    mut frames: futures::stream::BoxStream<'static, anyhow::Result<Frame>>,
) -> anyhow::Result<Output> {
    let mut output = Output {
        stdout: Vec::new(),
        stderr: Vec::new(),
        exit_code: -1,
    };
    let mut exited = false;

    while let Some(frame) = frames.next().await {
        match frame? {
            Frame::Stdout(bytes) => output.stdout.extend_from_slice(&bytes),
            Frame::Stderr(bytes) => output.stderr.extend_from_slice(&bytes),
            Frame::Exit(code) => (output.exit_code, exited) = (code, true),
        }
    }

    // Without an exit frame the command's fate is unknown -- the sprite may
    // still be running it -- so any reported status would be a guess.
    anyhow::ensure!(
        exited,
        "sprite closed the exec response without an exit status"
    );
    Ok(output)
}

/// Which stream a run of payload bytes belongs to.
#[derive(Debug, Clone, Copy)]
enum OpenStream {
    Stdout,
    Stderr,
}

impl OpenStream {
    fn frame(self, payload: bytes::Bytes) -> Frame {
        match self {
            OpenStream::Stdout => Frame::Stdout(payload),
            OpenStream::Stderr => Frame::Stderr(payload),
        }
    }
}

/// Decodes the exec framing protocol across the deliveries of a response body.
///
/// A frame is a one-byte stream tag followed by that stream's payload, carrying
/// no length, so a frame runs until the next one begins. The endpoint speaks
/// HTTP/2, whose DATA frames may split one of the sprite's writes across several
/// deliveries, and a delivery carrying the middle of a payload opens with an
/// ordinary payload byte.
///
/// A delivery therefore continues the open stream unless it starts with a known
/// tag. Text output decodes exactly, because the tags are control bytes. Binary
/// output split immediately before a 0x01, 0x02, or 0x03 byte joins the stream
/// that was already open. The WebSocket exec endpoint carries the sprite's write
/// boundaries as message boundaries, and decoding it needs none of this state.
#[derive(Debug, Default)]
enum Decoder {
    /// Nothing has been read yet, so the next byte is a tag.
    #[default]
    Start,
    /// Untagged bytes continue this stream.
    Continuing(OpenStream),
    /// An exit tag arrived without its status byte, which is the next byte.
    AwaitingExitCode,
    /// The exit frame has been read, and nothing further is expected.
    Done,
}

impl Decoder {
    /// Decodes one delivery of the response body into at most one frame.
    fn step(&mut self, chunk: bytes::Bytes) -> anyhow::Result<Option<Frame>> {
        // An empty delivery carries no bytes, and so no frame.
        let Some(&first) = chunk.first() else {
            return Ok(None);
        };

        if let Decoder::AwaitingExitCode = self {
            *self = Decoder::Done;
            return Ok(Some(Frame::Exit(first as i32)));
        }

        if let Decoder::Continuing(stream) = *self
            && !matches!(first, TAG_STDOUT | TAG_STDERR | TAG_EXIT)
        {
            return Ok(Some(stream.frame(chunk)));
        }

        let payload = chunk.slice(1..);

        Ok(Some(match first {
            TAG_STDOUT => {
                *self = Decoder::Continuing(OpenStream::Stdout);
                Frame::Stdout(payload)
            }
            TAG_STDERR => {
                *self = Decoder::Continuing(OpenStream::Stderr);
                Frame::Stderr(payload)
            }
            TAG_EXIT => match payload.first() {
                Some(&code) => {
                    *self = Decoder::Done;
                    Frame::Exit(code as i32)
                }
                // The tag arrived alone, so its status byte opens the next delivery.
                None => {
                    *self = Decoder::AwaitingExitCode;
                    return Ok(None);
                }
            },
            tag => anyhow::bail!("sprite sent an exec frame with unknown stream tag {tag}"),
        }))
    }
}

/// Resolves a checkpoint or restore call, whose outcome the API streams as
/// NDJSON events under HTTP 200.
///
/// Success ends with a `complete` event and failure carries an `error` event,
/// so the HTTP status alone does not report the outcome. This reads the whole
/// stream and maps those events to a `Result`.
async fn stream_result(context: &str, response: reqwest::Response) -> anyhow::Result<()> {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();

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

/// Builds an error from a failed Sprites API response, folding in the
/// `{"error": "..."}` message that the API returns for 4xx and 5xx.
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

#[cfg(test)]
mod test {
    use super::{Decoder, Frame, Output};

    #[test]
    fn test_unavailable_diagnostics_never_echo_input() {
        for (body, expected) in [
            ("{\"error\":\"capacity unavailable\"}", "capacity"),
            ("upstream request timed out", "timeout"),
            ("failed to connect to VM", "connection"),
            ("too many sessions", "limit"),
            ("sprite not ready", "not_ready"),
            ("out of memory", "memory"),
            ("service unavailable", "unavailable"),
            ("reflected-command invented-secret", "unclassified"),
            ("", "unclassified"),
        ] {
            assert_eq!(super::unavailable_reason(body.as_bytes()), expected);
        }
    }

    /// Folds `chunks` the way [`super::Client::exec`] folds a response body.
    fn decode(chunks: &[&[u8]]) -> anyhow::Result<Output> {
        let mut output = Output {
            stdout: Vec::new(),
            stderr: Vec::new(),
            exit_code: -1,
        };
        let mut exited = false;
        let mut decoder = Decoder::default();

        for chunk in chunks {
            match decoder.step(bytes::Bytes::copy_from_slice(chunk))? {
                Some(Frame::Stdout(bytes)) => output.stdout.extend_from_slice(&bytes),
                Some(Frame::Stderr(bytes)) => output.stderr.extend_from_slice(&bytes),
                Some(Frame::Exit(code)) => (output.exit_code, exited) = (code, true),
                None => (),
            }
        }
        anyhow::ensure!(
            exited,
            "sprite closed the exec response without an exit status"
        );
        Ok(output)
    }

    #[test]
    fn test_decode_exec_frames() {
        // Chunking observed on the wire for
        // `bash -lc 'echo one; echo two >&2; echo three; exit 3'`.
        let output = decode(&[b"\x01one\n", b"\x02two\n", b"\x01three\n", b"\x03\x03"]).unwrap();

        assert_eq!(
            output,
            Output {
                stdout: b"one\nthree\n".to_vec(),
                stderr: b"two\n".to_vec(),
                exit_code: 3,
            }
        );
    }

    #[test]
    fn test_decode_empty_command() {
        // A zero-length chunk carries no frame and must not end the fold.
        let output = decode(&[b"", b"\x03\x00"]).unwrap();

        assert_eq!(
            output,
            Output {
                stdout: Vec::new(),
                stderr: Vec::new(),
                exit_code: 0,
            }
        );
    }

    #[test]
    fn test_decode_payload_split_across_deliveries() {
        // HTTP/2 may split one of the sprite's writes across several
        // deliveries. Only the first carries a tag, and the rest continue it.
        // The third delivery here opens with a space, which is the byte that
        // a tag-per-delivery reading mistakes for an unknown stream tag.
        let output = decode(&[
            b"\x01total 48\ndrwx",
            b"r-xr-x 1 root root",
            b" 4096 Aug 29 14:00 .\n",
            b"\x02ls: cannot access\n",
            b"\x03\x02",
        ])
        .unwrap();

        assert_eq!(
            output,
            Output {
                stdout: b"total 48\ndrwxr-xr-x 1 root root 4096 Aug 29 14:00 .\n".to_vec(),
                stderr: b"ls: cannot access\n".to_vec(),
                exit_code: 2,
            }
        );
    }

    #[test]
    fn test_decode_split_exit_frame() {
        // The exit tag and its status byte can land in separate deliveries.
        let output = decode(&[b"\x01done\n", b"\x03", b"\x07"]).unwrap();

        assert_eq!(
            output,
            Output {
                stdout: b"done\n".to_vec(),
                stderr: Vec::new(),
                exit_code: 7,
            }
        );
    }

    #[test]
    fn test_decode_rejects_malformed_bodies() {
        let cases: [(&str, &[&[u8]]); 4] = [
            ("truncated before exit", &[b"\x01hi\n"]),
            ("empty body", &[]),
            ("exit frame without a code", &[b"\x03"]),
            ("unknown stream tag", &[b"\x09what\n", b"\x03\x00"]),
        ];

        let errors: Vec<String> = cases
            .into_iter()
            .map(|(case, chunks)| {
                let err = decode(chunks).expect_err(case);
                format!("{case}: {err:#}")
            })
            .collect();

        insta::assert_debug_snapshot!(errors);
    }
}

/// Exercises the client against the live Sprites API. Ignored by default: these
/// tests need a `SPRITES_TOKEN` for a Fly organization, and they create and
/// destroy a real VM. Run with:
///
///     cargo test -p control-plane-api sprites::live -- --ignored --nocapture
#[cfg(test)]
mod live {
    use super::{Client, ExecError};

    /// Name of the sprite these tests own. It is created and deleted by the
    /// test, and is deliberately outside the `sbx-` sandbox namespace.
    const SPRITE: &str = "control-plane-api-live-test";

    #[tokio::test]
    #[ignore]
    async fn test_create_exec_delete() {
        let token = std::env::var("SPRITES_TOKEN").expect("SPRITES_TOKEN must be set");
        let client = Client::new(token);
        let timeout = std::time::Duration::from_secs(120);

        let sprite = client.create_sprite(SPRITE).await.unwrap();
        assert_eq!(sprite.name, SPRITE);

        // Creating again resolves to the same sprite rather than conflicting.
        assert_eq!(client.create_sprite(SPRITE).await.unwrap().name, SPRITE);

        // Interleaved writes to both streams confirm that the response's frame
        // boundaries survive the HTTP client: a coalesced read would attribute
        // one stream's bytes to the other.
        let output = client
            .exec(
                SPRITE,
                &[
                    "bash",
                    "-lc",
                    "echo one; echo two >&2; echo three; echo four >&2; exit 3",
                ],
                timeout,
            )
            .await
            .unwrap();

        assert_eq!(String::from_utf8_lossy(&output.stdout), "one\nthree\n");
        assert_eq!(String::from_utf8_lossy(&output.stderr), "two\nfour\n");
        assert_eq!(output.exit_code, 3);

        assert!(client.delete_sprite(SPRITE).await.unwrap());
        // A second delete reports that the sprite is already gone.
        assert!(!client.delete_sprite(SPRITE).await.unwrap());
    }

    #[tokio::test]
    #[ignore]
    async fn test_exec_stream_arrives_incrementally() {
        use super::Frame;
        use futures::StreamExt;

        let token = std::env::var("SPRITES_TOKEN").expect("SPRITES_TOKEN must be set");
        let client = Client::new(token);
        let sprite = "control-plane-api-live-stream";

        client.create_sprite(sprite).await.unwrap();

        let mut frames = client
            .exec_stream(
                sprite,
                &[
                    "bash",
                    "-lc",
                    "echo one; sleep 2; echo two; sleep 2; echo three",
                ],
                std::time::Duration::from_secs(120),
            )
            .await
            .unwrap();

        let start = std::time::Instant::now();
        let mut arrivals = Vec::new();
        let mut exit_code = None;

        while let Some(frame) = frames.next().await {
            match frame.unwrap() {
                Frame::Stdout(bytes) => arrivals.push((
                    start.elapsed().as_secs(),
                    String::from_utf8_lossy(&bytes).trim().to_string(),
                )),
                Frame::Stderr(bytes) => panic!("unexpected stderr: {bytes:?}"),
                Frame::Exit(code) => exit_code = Some(code),
            }
        }

        assert_eq!(exit_code, Some(0));
        assert_eq!(
            arrivals
                .iter()
                .map(|(_, line)| line.as_str())
                .collect::<Vec<_>>(),
            ["one", "two", "three"],
        );

        // The point of the stream: lines surface as the command produces them.
        // A buffered response would deliver all three at the same instant.
        let (last_at, _) = arrivals[2];
        assert!(
            last_at >= 3,
            "third line arrived after {last_at}s; output was buffered, not streamed"
        );

        assert!(client.delete_sprite(sprite).await.unwrap());
    }

    #[tokio::test]
    #[ignore]
    async fn test_exec_on_missing_sprite() {
        let token = std::env::var("SPRITES_TOKEN").expect("SPRITES_TOKEN must be set");
        let client = Client::new(token);

        let err = client
            .exec_stream(
                "control-plane-api-no-such-sprite",
                &["bash", "-lc", "true"],
                std::time::Duration::from_secs(30),
            )
            .await
            .err()
            .expect("sprite does not exist");

        // Sandboxes provision themselves on this variant, so an absent sprite
        // must stay distinguishable from every other failure.
        assert!(matches!(err, ExecError::SpriteMissing), "{err:#}");
    }
}
