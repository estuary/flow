//! Client for the Fly.io Sprites API

use anyhow::Context;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

const BASE_URL: &str = "https://api.sprites.dev";

pub struct Client {
    http: reqwest::Client,
    base_url: url::Url,
    token: String,
}

/// A command's exec WebSocket. Its protocol is decoded by [`decode_message`].
pub type ExecSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// The Sprites API refused to start a command, answering its WebSocket
/// upgrade with this HTTP status. A 404 means the sprite doesn't exist.
#[derive(Debug, thiserror::Error)]
#[error("failed to start command: HTTP {status}: {body}")]
pub struct ExecRejected {
    pub status: tungstenite::http::StatusCode,
    pub body: String,
}

impl Client {
    pub fn new(token: String) -> Self {
        let base_url = url::Url::parse(BASE_URL).expect("BASE_URL is a valid URL");
        Self::with_base_url(token, base_url)
    }

    /// A client of the Sprites API at `base_url`, such as a test server.
    /// Exec WebSockets use `ws` for an `http` base, and `wss` otherwise.
    pub fn with_base_url(token: String, base_url: url::Url) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("building Sprites HTTP client"),
            base_url,
            token,
        }
    }

    pub async fn create_sprite(&self, name: &str) -> anyhow::Result<()> {
        let url = self.url(["v1", "sprites"]);

        let response = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            // Provisioning can take longer than ordinary API calls, as in Fly's SDK.
            .timeout(std::time::Duration::from_secs(120))
            .json(&serde_json::json!({"name": name, "wait_for_capacity": true}))
            .send()
            .await
            .context("failed to reach the Sprites API")?;

        match response.status() {
            reqwest::StatusCode::CREATED => Ok(()),
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

    /// Names of the organization's sprites which start with `prefix`.
    pub async fn list_sprites(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let mut names = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self
                .http
                .get(self.url(["v1", "sprites"]))
                .bearer_auth(&self.token)
                .query(&[("prefix", prefix), ("max_results", "500")]);
            if let Some(token) = &continuation_token {
                request = request.query(&[("continuation_token", token)]);
            }
            let response = request
                .send()
                .await
                .context("failed to reach the Sprites API")?;

            if !response.status().is_success() {
                return Err(api_error("failed to list sprites", response).await);
            }
            let page: SpriteList = response.json().await.context("decoding sprite list")?;

            // Callers delete the sprites listed, so the API's filtering is checked.
            for sprite in page.sprites {
                anyhow::ensure!(
                    sprite.name.starts_with(prefix),
                    "sprite list for prefix {prefix:?} included {:?}",
                    sprite.name
                );
                names.push(sprite.name);
            }
            if !page.has_more {
                return Ok(names);
            }
            continuation_token = Some(
                page.next_continuation_token
                    .context("sprite list has more results but no continuation token")?,
            );
        }
    }

    /// URL of the exec WebSocket which runs `argv` with stdin and without a TTY.
    ///
    /// After the client disconnects, Sprites waits `max_run_after_disconnect`
    /// (a duration such as `5s`) and then SIGTERMs the command's process group.
    pub fn exec_url(&self, name: &str, argv: &[&str], max_run_after_disconnect: &str) -> url::Url {
        let mut url = self.url(["v1", "sprites", name, "exec"]);
        let scheme = if url.scheme() == "http" { "ws" } else { "wss" };
        url.set_scheme(scheme).expect("valid WebSocket scheme");
        {
            let mut query = url.query_pairs_mut();
            for arg in argv {
                query.append_pair("cmd", arg);
            }
            query.append_pair("stdin", "true");
            query.append_pair("tty", "false");
            query.append_pair("max_run_after_disconnect", max_run_after_disconnect);
        }
        url
    }

    /// Opens an exec WebSocket of [`Client::exec_url`]. An HTTP response
    /// other than the upgrade is an [`ExecRejected`] error.
    pub async fn connect_exec(
        &self,
        url: url::Url,
        deadline: tokio::time::Instant,
    ) -> anyhow::Result<ExecSocket> {
        let mut request = url
            .as_str()
            .into_client_request()
            .context("building command request")?;
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {}", self.token)
                .parse()
                .context("invalid Sprites token header")?,
        );
        match tokio::time::timeout_at(deadline, tokio_tungstenite::connect_async(request))
            .await
            .context("timed out connecting to Sprites command")?
        {
            Ok((socket, _)) => Ok(socket),
            Err(tungstenite::Error::Http(response)) => Err(ExecRejected {
                status: response.status(),
                body: String::from_utf8_lossy(response.body().as_deref().unwrap_or_default())
                    .into_owned(),
            }
            .into()),
            Err(err) => Err(anyhow::Error::new(err).context("connecting to Sprites command")),
        }
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
    Stdout(bytes::Bytes),
    Stderr(bytes::Bytes),
    Exit(i32),
}

/// Decodes a message of an exec WebSocket. Messages without a frame,
/// such as pings and unknown events, are None.
pub(crate) fn decode_message(message: tungstenite::Message) -> anyhow::Result<Option<Frame>> {
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
                    anyhow::ensure!(payload.len() == 1, "invalid command exit frame");
                    Frame::Exit(payload[0] as i32)
                }
                _ => anyhow::bail!("sprite sent an command frame with unknown stream tag {tag}"),
            }))
        }
        tungstenite::Message::Text(text) => {
            #[derive(serde::Deserialize)]
            #[serde(tag = "type")]
            enum Event {
                #[serde(rename = "exit")]
                Exit { exit_code: i32 },
                #[serde(rename = "error")]
                Error { error: String },
                #[serde(other)]
                Other,
            }
            match serde_json::from_str(&text).context("decoding Sprites command event")? {
                Event::Exit { exit_code } => Ok(Some(Frame::Exit(exit_code))),
                Event::Error { error } => anyhow::bail!("Sprites command failed: {error}"),
                Event::Other => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

#[derive(Debug, serde::Deserialize)]
struct SpriteList {
    sprites: Vec<SpriteListEntry>,
    has_more: bool,
    #[serde(default)]
    next_continuation_token: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct SpriteListEntry {
    name: String,
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
