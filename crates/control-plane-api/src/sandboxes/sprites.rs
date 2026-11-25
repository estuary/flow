//! A [`Provider`] of Fly.io Sprites, through the client in `crate::sprites`.
//!
//! Each command runs over its own exec WebSocket. Sprites doesn't close a
//! command's stdin when its client disconnects. So when the Exec's stdin is
//! dropped, the provider sends a stdin EOF message (see `relay`). If that
//! message is lost, as when this process dies, Sprites SIGTERMs the command's
//! process group once [`MAX_RUN_AFTER_DISCONNECT`] has passed.

use super::provider::{Error, Exec, Output, Provider, State};
use crate::sprites::{ExecSocket, Frame};
use anyhow::Context;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite;

/// How long Sprites lets a command run after its client disconnects,
/// before it SIGTERMs the command's process group.
pub const MAX_RUN_AFTER_DISCONNECT: &str = "5s";

pub struct Sprites {
    pub client: std::sync::Arc<crate::sprites::Client>,
    /// Download URL of the flowctl binary, which [`Sprites::setup`] installs
    /// at `/opt/estuary/bin/flowctl`. A `file://` URL names a file which is
    /// already on the sprite.
    pub flowctl_url: url::Url,
    /// Prefix of the handle of every sprite of this provider.
    /// `list` returns each sprite whose name has it.
    pub prefix: String,
}

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
/// Bounds writing the remaining stdin, its EOF, and the close message, once
/// an [`Exec`] is dropped.
const CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const SETUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
/// Stdin data is sent in messages of at most this many bytes.
const STDIN_CHUNK: usize = 32 * 1024;

const STDIN_DATA: u8 = 0;
const STDIN_EOF: u8 = 4;

/// Brings a sprite to the baseline, running as its default user. `$1` is
/// the flowctl download URL. `flowctl raw sandbox-exec --help` fails if the
/// download isn't a flowctl which can run sandbox commands.
///
/// The sprite's DNS proxy answers without name compression, truncates large
/// replies, and refuses the TCP retry that follows. Hosts with many address
/// records, such as the brokers of a large data plane, then fail to resolve.
/// With EDNS0 the resolver advertises a larger UDP buffer and the reply fits.
const SETUP: &str = r#"set -euo pipefail
download="$(mktemp)"
curl --fail --silent --show-error --location --retry 3 --output "$download" "$1"
sudo -n install -D --mode 0755 "$download" /opt/estuary/bin/flowctl
rm "$download"
/opt/estuary/bin/flowctl raw sandbox-exec --help >/dev/null
sudo -n install -d --owner "$(id -un)" --group "$(id -gn)" /work
sudo -n tee /etc/profile.d/estuary.sh >/dev/null <<'PROFILE'
case ":${PATH}:" in *:/opt/estuary/bin:*) ;; *) export PATH="/opt/estuary/bin:${PATH}" ;; esac
export RES_OPTIONS=edns0
PROFILE
"#;

#[async_trait::async_trait]
impl Provider for Sprites {
    fn kind(&self) -> &'static str {
        "sprites"
    }

    async fn provision(&self, handle: &str) -> Result<State, Error> {
        self.client.create_sprite(handle).await?;

        if let Err(err) = self.setup(handle).await {
            self.client
                .delete_sprite(handle)
                .await
                .with_context(|| format!("deleting sprite after its setup failed: {err:#}"))?;
            return Err(err);
        }
        tracing::info!(handle, "provisioned sprite");

        Ok(serde_json::json!({}))
    }

    async fn destroy(&self, handle: &str, _state: &State) -> Result<bool, Error> {
        Ok(self.client.delete_sprite(handle).await?)
    }

    async fn exec(&self, handle: &str, _state: &State, argv: &[&str]) -> Result<Exec, Error> {
        tracing::debug!(handle, ?argv, "starting sprite exec");

        let url = self.client.exec_url(handle, argv, MAX_RUN_AFTER_DISCONNECT);
        let deadline = tokio::time::Instant::now() + CONNECT_TIMEOUT;

        let socket = match self.client.connect_exec(url, deadline).await {
            Ok(socket) => socket,
            Err(err)
                if err
                    .downcast_ref::<crate::sprites::ExecRejected>()
                    .is_some_and(|rejected| {
                        rejected.status == tungstenite::http::StatusCode::NOT_FOUND
                    }) =>
            {
                return Err(Error::Gone);
            }
            Err(err) => return Err(err.into()),
        };
        super::set_keepalive(socket.get_ref().get_ref())
            .context("failed to enable TCP keep-alive")?;

        let (stdin_tx, stdin_rx) = futures::channel::mpsc::channel(1);
        // Dropped with the stdin sink. `relay` can't otherwise tell that it
        // was dropped until `stdin_rx` is drained.
        let (stdin_guard, stdin_dropped) = tokio::sync::oneshot::channel::<()>();
        let (output_tx, output_rx) = tokio::sync::mpsc::channel(16);
        tokio::spawn(relay(socket, stdin_rx, stdin_dropped, output_tx));

        // Like Docker's, the sink closes stdin only when dropped:
        // `unfold` flushes on close but keeps its state.
        let stdin = futures::sink::unfold(
            (stdin_tx, stdin_guard),
            |(mut stdin_tx, stdin_guard), data: bytes::Bytes| async {
                stdin_tx
                    .send(data)
                    .await
                    .map_err(|_| anyhow::anyhow!("sprite exec connection is closed"))?;
                Ok::<_, Error>((stdin_tx, stdin_guard))
            },
        );
        let output = futures::stream::unfold(output_rx, |mut output_rx| async {
            let item = output_rx.recv().await?;
            Some((item, output_rx))
        });

        Ok(Exec {
            stdin: Box::pin(stdin),
            output: output.boxed(),
        })
    }

    async fn list(&self) -> Result<Vec<String>, Error> {
        Ok(self.client.list_sprites(&self.prefix).await?)
    }
}

impl Sprites {
    /// Brings existing sprite `handle` to the baseline, which `provision`
    /// does for each sprite it creates. Its steps can be repeated.
    pub async fn setup(&self, handle: &str) -> Result<(), Error> {
        let argv = ["bash", "-c", SETUP, "setup", self.flowctl_url.as_str()];
        let Exec { stdin, mut output } = self.exec(handle, &serde_json::json!({}), &argv).await?;
        drop(stdin);

        let run = async {
            let mut stderr = Vec::new();
            while let Some(item) = output.next().await {
                match item? {
                    Output::Stdout(_) => {}
                    Output::Stderr(data) => stderr.extend_from_slice(&data),
                    Output::Exit(0) => return Ok(()),
                    Output::Exit(code) => {
                        return Err(anyhow::anyhow!(
                            "sprite setup failed with exit code {code}: {}",
                            String::from_utf8_lossy(&stderr).trim()
                        )
                        .into());
                    }
                }
            }
            Err(anyhow::anyhow!("sprite setup output ended without an exit status").into())
        };
        tokio::time::timeout(SETUP_TIMEOUT, run)
            .await
            .context("timed out setting up the sprite")?
    }
}

/// Relays between `socket` and the parts of an [`Exec`], and then closes
/// `socket`. Relaying ends when the command exits or the connection fails.
/// It also ends once the Exec's stdin and output are both dropped.
///
/// When the Exec's stdin is dropped, `stdin` ends after its buffered data.
/// Then `write_stdin` sends the stdin EOF message, which Sprites delivers to
/// the command at once. Output is still relayed until the command exits or the
/// Exec's output is dropped too.
///
/// Once both are dropped, `write_stdin` has until a deadline to finish.
/// Its writes stall if the command doesn't read its stdin. Either way the
/// close message follows. Sprites doesn't reply to it, so `socket` is dropped
/// once it's written, which closes the TCP connection. Sprites' own
/// disconnect timeout covers an EOF message which wasn't sent.
async fn relay(
    socket: ExecSocket,
    stdin: futures::channel::mpsc::Receiver<bytes::Bytes>,
    stdin_dropped: tokio::sync::oneshot::Receiver<()>,
    output: tokio::sync::mpsc::Sender<Result<Output, Error>>,
) {
    let (mut writer, mut reader) = socket.split();
    let deadline;
    {
        let write = write_stdin(&mut writer, stdin);
        let read = read_output(&mut reader, &output);
        tokio::pin!(write, read, stdin_dropped);
        let (mut stdin_done, mut stdin_dropped_seen) = (false, false);

        let exited = loop {
            tokio::select! {
                // A failed write means the connection failed. `read_output`
                // reports that when it next reads.
                _ = &mut write, if !stdin_done => stdin_done = true,
                _ = &mut stdin_dropped, if !stdin_dropped_seen => stdin_dropped_seen = true,
                () = &mut read => break true,
                () = output.closed(), if stdin_dropped_seen => break false,
            }
        };
        deadline = tokio::time::Instant::now() + CLOSE_TIMEOUT;

        // Output is still read and discarded, so Sprites isn't blocked writing it.
        if !exited && !stdin_done {
            tokio::select! {
                _ = &mut write => {}
                () = &mut read => {}
                () = tokio::time::sleep_until(deadline) => {}
            }
        }
    }
    // The connection is being abandoned, so a failure to close it changes nothing.
    let _ = tokio::time::timeout_at(deadline, writer.send(tungstenite::Message::Close(None))).await;
}

/// Writes stdin data until `stdin` ends, and then the stdin EOF message.
async fn write_stdin(
    writer: &mut futures::stream::SplitSink<ExecSocket, tungstenite::Message>,
    mut stdin: futures::channel::mpsc::Receiver<bytes::Bytes>,
) -> Result<(), tungstenite::Error> {
    while let Some(data) = stdin.next().await {
        for chunk in data.chunks(STDIN_CHUNK) {
            let mut message = Vec::with_capacity(chunk.len() + 1);
            message.push(STDIN_DATA);
            message.extend_from_slice(chunk);
            writer
                .send(tungstenite::Message::Binary(message.into()))
                .await?;
        }
    }
    writer
        .send(tungstenite::Message::Binary(bytes::Bytes::from_static(&[
            STDIN_EOF,
        ])))
        .await
}

/// Sends the command's output to `output`, ending with its exit status or an
/// error. After `output` is dropped, output is still read and discarded, so
/// Sprites isn't blocked writing it while stdin is relayed.
async fn read_output(
    reader: &mut futures::stream::SplitStream<ExecSocket>,
    output: &tokio::sync::mpsc::Sender<Result<Output, Error>>,
) {
    while let Some(message) = reader.next().await {
        let frame = message
            .context("reading sprite exec output")
            .and_then(crate::sprites::decode_message);

        let item = match frame {
            Ok(None) => continue,
            Ok(Some(Frame::Stdout(data))) => Ok(Output::Stdout(data)),
            Ok(Some(Frame::Stderr(data))) => Ok(Output::Stderr(data)),
            Ok(Some(Frame::Exit(code))) => Ok(Output::Exit(code)),
            Err(err) => Err(Error::Other(err)),
        };
        let last = !matches!(item, Ok(Output::Stdout(_) | Output::Stderr(_)));

        // Fails only if `output` was dropped.
        let _ = output.send(item).await;
        if last {
            return;
        }
    }
    let _ = output
        .send(Err(anyhow::anyhow!(
            "sprite exec connection closed without an exit status"
        )
        .into()))
        .await;
}
