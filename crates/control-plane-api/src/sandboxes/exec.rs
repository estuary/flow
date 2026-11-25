//! Runs one command on a sandbox machine through the command wrapper
//! (`flowctl raw sandbox-exec`, see `crates/sandbox-exec`), and relays the
//! wrapper's output frames.
//!
//! [`relay`] owns the provider's [`Exec`], and does all of its work as it's
//! polled. Dropping the stream drops the `Exec`, which hangs up on the
//! wrapper, and the wrapper then cancels the command.

use super::provider::{Error, Exec, Output};
use futures::{SinkExt, StreamExt};

/// Maximum payload of each Stdin frame.
const STDIN_CHUNK: usize = 64 << 10;

/// Bytes of the wrapper's own stderr which are kept for an Error frame.
const STDERR_TAIL: usize = 2 << 10;

/// A command's stdin, as the client sends it. Its end is stdin EOF.
/// An error abandons the command.
pub type Stdin = futures::stream::BoxStream<'static, anyhow::Result<bytes::Bytes>>;

/// Encodes the Header frame which opens a command.
/// Fails if it exceeds the maximum frame payload.
pub fn header_frame(header: sandbox_exec::Header) -> anyhow::Result<bytes::Bytes> {
    let mut buf = bytes::BytesMut::new();
    sandbox_exec::Input::Header(header).encode(&mut buf)?;
    Ok(buf.freeze())
}

/// Tracks the wrapper's output frames as they're relayed. It forwards only
/// whole frames, and knows whether the wrapper sent its terminal frame
/// (`Exit` or `Error`).
#[derive(Debug, Default)]
pub struct Relay {
    /// Bytes of the wrapper's output which don't yet complete a frame.
    partial: bytes::BytesMut,
    /// The wrapper's terminal frame, once it's relayed.
    terminal: Option<sandbox_exec::Output>,
    /// The latest bytes of the wrapper's own stderr.
    stderr_tail: std::collections::VecDeque<u8>,
}

impl Relay {
    /// Accepts bytes of the wrapper's stdout, and appends the encoding of each
    /// whole frame they complete to `out`. Bytes after the terminal frame are
    /// discarded. Fails if the bytes aren't valid frames, in which case `out`
    /// holds the whole frames before the invalid one.
    pub fn stdout(&mut self, data: &[u8], out: &mut bytes::BytesMut) -> anyhow::Result<()> {
        if self.terminal.is_some() {
            return Ok(());
        }
        self.partial.extend_from_slice(data);

        while let Some(frame) = sandbox_exec::Output::decode(&mut self.partial)? {
            frame.encode(out).expect("a decoded frame encodes");

            if matches!(
                frame,
                sandbox_exec::Output::Exit { .. } | sandbox_exec::Output::Error { .. }
            ) {
                self.terminal = Some(frame);
                self.partial.clear();
                break;
            }
        }
        Ok(())
    }

    /// Accepts bytes of the wrapper's own stderr: its diagnostics.
    pub fn stderr(&mut self, data: &[u8]) {
        let data = &data[data.len().saturating_sub(STDERR_TAIL)..];
        let excess = (self.stderr_tail.len() + data.len()).saturating_sub(STDERR_TAIL);
        self.stderr_tail.drain(..excess);
        self.stderr_tail.extend(data);
    }

    /// The wrapper's terminal frame, if it was relayed.
    pub fn terminal(&self) -> Option<&sandbox_exec::Output> {
        self.terminal.as_ref()
    }

    /// The message of an Error frame which ends a relay that has no terminal
    /// frame, because of `reason`.
    pub fn failure(&self, reason: &str) -> String {
        let mut message = reason.to_string();

        if !self.partial.is_empty() {
            message.push_str(&format!(
                " ({} bytes of an incomplete output frame were dropped)",
                self.partial.len()
            ));
        }
        let (front, back) = self.stderr_tail.as_slices();
        let tail = String::from_utf8_lossy(&[front, back].concat()).into_owned();
        let tail = tail.trim();

        if !tail.is_empty() {
            message.push_str(&format!(". The command wrapper's diagnostics: {tail}"));
        }
        message
    }
}

/// Items of [`relay`].
#[derive(Debug, PartialEq)]
pub enum Relayed {
    /// Whole output frames of the wrapper.
    Frames(bytes::Bytes),
    /// The wrapper's terminal frame was the last of the preceding `Frames`.
    /// Last item.
    Finished(sandbox_exec::Output),
    /// The relay ended without a terminal frame from the wrapper, and its
    /// consumer must end the stream with an Error frame of `message`.
    /// `gone` is true if the provider reported the machine gone.
    /// Last item.
    Failed { message: String, gone: bool },
}

/// Relays the command of `exec`, a wrapper started by `Provider::exec`.
///
/// The stream writes `header` (from [`header_frame`]) to the wrapper's input,
/// and then `stdin` as Stdin frames and a StdinEof, while it relays the
/// wrapper's output. Then it holds the input open. It ends after the wrapper's
/// terminal frame, when the provider's output ends or fails without one, or
/// when `stdin` fails.
///
/// The stream owns `exec`. It ends or is dropped, and then `exec` is dropped.
/// That closes the wrapper's input, the only teardown signal of
/// `Provider::exec`, and the wrapper cancels the command.
pub fn relay(
    exec: Exec,
    header: bytes::Bytes,
    stdin: Stdin,
) -> impl futures::Stream<Item = Relayed> + Send + 'static {
    async_stream::stream! {
        let Exec { stdin: mut input, mut output } = exec;
        let writer = write_input(&mut input, header, stdin);
        let mut writer = std::pin::pin!(writer);
        let mut writing = true;

        let mut relay = Relay::default();
        let mut frames = bytes::BytesMut::new();

        let (reason, gone) = loop {
            let item = tokio::select! {
                result = writer.as_mut(), if writing => {
                    writing = false;
                    match result {
                        Ok(()) => continue,
                        Err(InputError::Stdin(err)) => {
                            break (format!("failed to read the command's stdin: {err:#}"), false);
                        }
                        Err(InputError::Provider(Error::Gone)) => {
                            break ("the sandbox machine no longer exists".to_string(), true);
                        }
                        // The output tells how the wrapper fared without its input.
                        Err(InputError::Provider(Error::Other(err))) => {
                            tracing::debug!(?err, "failed to write sandbox command input");
                            continue;
                        }
                    }
                }
                item = output.next() => item,
            };

            match item {
                Some(Ok(Output::Stdout(data))) => {
                    let decoded = relay.stdout(&data, &mut frames);
                    if !frames.is_empty() {
                        yield Relayed::Frames(frames.split().freeze());
                    }
                    if let Err(err) = decoded {
                        break (format!("the command wrapper sent an invalid output frame: {err:#}"), false);
                    }
                    if let Some(terminal) = relay.terminal() {
                        yield Relayed::Finished(terminal.clone());
                        return;
                    }
                }
                Some(Ok(Output::Stderr(data))) => relay.stderr(&data),
                Some(Ok(Output::Exit(status))) => {
                    break (format!("the command wrapper exited with status {status} before the command finished"), false);
                }
                Some(Err(Error::Gone)) => break ("the sandbox machine no longer exists".to_string(), true),
                Some(Err(Error::Other(err))) => {
                    break (format!("lost the connection to the sandbox: {err:#}"), false);
                }
                None => {
                    break ("the connection to the sandbox closed before the command finished".to_string(), false);
                }
            }
        };

        yield Relayed::Failed { message: relay.failure(&reason), gone };
    }
}

enum InputError {
    Provider(Error),
    Stdin(anyhow::Error),
}

/// Writes `header` to the wrapper's `input`, then each chunk of `stdin` as
/// Stdin frames, and then StdinEof.
///
/// A send waits while the wrapper's input is full. `relay` still reads output
/// meanwhile.
async fn write_input(
    input: &mut std::pin::Pin<Box<dyn futures::Sink<bytes::Bytes, Error = Error> + Send>>,
    header: bytes::Bytes,
    mut stdin: Stdin,
) -> Result<(), InputError> {
    input.send(header).await.map_err(InputError::Provider)?;
    let mut buf = bytes::BytesMut::new();

    while let Some(data) = stdin.next().await {
        let data = data.map_err(InputError::Stdin)?;

        for chunk in data.chunks(STDIN_CHUNK) {
            sandbox_exec::Input::Stdin(data.slice_ref(chunk))
                .encode(&mut buf)
                .expect("chunk is within MAX_PAYLOAD");
            input
                .send(buf.split().freeze())
                .await
                .map_err(InputError::Provider)?;
        }
    }
    sandbox_exec::Input::StdinEof
        .encode(&mut buf)
        .expect("StdinEof encodes");
    input.send(buf.freeze()).await.map_err(InputError::Provider)
}

#[cfg(test)]
mod test {
    use super::{Relay, Relayed};
    use crate::sandboxes::fake::{Fake, Process};
    use crate::sandboxes::provider::{Error, Exec, Output, Provider, WRAPPER_ARGV};
    use futures::StreamExt;
    use sandbox_exec::{Input, Output as Frame};

    /// Starts a process of the fake provider, returning the relay's end and
    /// the test's end of it.
    async fn start() -> (Exec, Process) {
        let fake = Fake::new("fake");
        fake.inner
            .lock()
            .unwrap()
            .machines
            .insert("handle".to_string());
        let mut processes = fake.processes();
        let exec = fake
            .exec("handle", &serde_json::json!({}), &WRAPPER_ARGV)
            .await
            .unwrap();
        (exec, processes.recv().await.unwrap())
    }

    fn header() -> bytes::Bytes {
        super::header_frame(sandbox_exec::Header {
            version: sandbox_exec::PROTOCOL_VERSION,
            workspace: "ws".to_string(),
            command: "cat".to_string(),
            env: [("FLOW_AUTH_TOKEN".to_string(), "token".to_string())].into(),
        })
        .unwrap()
    }

    fn stdin_of(data: &[u8]) -> super::Stdin {
        futures::stream::iter([Ok(bytes::Bytes::copy_from_slice(data))]).boxed()
    }

    fn encode(frames: &[Frame]) -> Vec<u8> {
        let mut buf = bytes::BytesMut::new();
        for frame in frames {
            frame.encode(&mut buf).unwrap();
        }
        buf.to_vec()
    }

    /// Describes each item of the relay, decoding the frames of each `Frames`.
    async fn describe(relayed: impl futures::Stream<Item = Relayed>) -> Vec<String> {
        relayed
            .map(|item| match item {
                Relayed::Frames(frames) => {
                    let mut buf = bytes::BytesMut::from(&frames[..]);
                    let mut decoded = Vec::new();
                    while let Some(frame) = Frame::decode(&mut buf).unwrap() {
                        decoded.push(frame);
                    }
                    assert!(buf.is_empty(), "Frames holds only whole frames");
                    format!("Frames {decoded:?}")
                }
                other => format!("{other:?}"),
            })
            .collect()
            .await
    }

    #[tokio::test]
    async fn test_relays_whole_frames() {
        let (exec, mut process) = start().await;
        assert_eq!(process.argv, WRAPPER_ARGV);

        // Stdin of more than one chunk.
        let mut input = vec![b'x'; 100 << 10];
        input.extend_from_slice(b" and more");

        let relayed = tokio::spawn(describe(super::relay(exec, header(), stdin_of(&input))));

        // The wrapper responds once it has read its input.
        let mut inputs = Vec::new();
        let mut buf = bytes::BytesMut::new();
        while !matches!(inputs.last(), Some(Input::StdinEof)) {
            buf.extend_from_slice(&process.stdin.recv().await.unwrap());
            while let Some(input) = Input::decode(&mut buf).unwrap() {
                inputs.push(input);
            }
        }

        let mut wrapper_output = encode(&[
            Frame::Stdout(bytes::Bytes::from_static(b"hello")),
            Frame::Stderr(bytes::Bytes::from_static(b"warning")),
            Frame::Exit {
                code: Some(0),
                signal: None,
            },
        ]);
        // Bytes after the terminal frame are discarded.
        wrapper_output.extend_from_slice(b"\x01\0\0\0\x05extra");

        // Arbitrary splits, which begin with a partial prefix.
        for piece in [
            &wrapper_output[..3],
            &wrapper_output[3..14],
            &wrapper_output[14..],
        ] {
            process
                .output
                .send(Ok(Output::Stdout(bytes::Bytes::copy_from_slice(piece))))
                .unwrap();
        }
        process
            .output
            .send(Ok(Output::Stderr(bytes::Bytes::from_static(
                b"diagnostics",
            ))))
            .unwrap();

        let items = relayed.await.unwrap();

        // The relay ended, and dropped the Exec.
        assert!(process.stdin.recv().await.is_none());
        assert!(process.output.is_closed());

        let inputs: Vec<String> = inputs
            .into_iter()
            .map(|input| match input {
                Input::Stdin(data) => format!("Stdin of {} bytes", data.len()),
                other => format!("{other:?}"),
            })
            .collect();
        insta::assert_debug_snapshot!((items, inputs));
    }

    #[tokio::test(start_paused = true)]
    async fn test_holds_input_open() {
        let (exec, mut process) = start().await;
        let relayed = tokio::spawn(describe(super::relay(exec, header(), stdin_of(b""))));

        let mut received = Vec::new();
        let mut buf = bytes::BytesMut::new();
        while !matches!(received.last(), Some(Input::StdinEof)) {
            buf.extend_from_slice(&process.stdin.recv().await.unwrap());
            while let Some(input) = Input::decode(&mut buf).unwrap() {
                received.push(input);
            }
        }

        // Closing the input would cancel the command, so it stays open while
        // the command runs, however long that is.
        let idle =
            tokio::time::timeout(std::time::Duration::from_secs(3600), process.stdin.recv()).await;
        assert!(idle.is_err(), "input must stay open: {idle:?}");

        process
            .output
            .send(Ok(Output::Stdout(
                encode(&[Frame::Exit {
                    code: Some(0),
                    signal: None,
                }])
                .into(),
            )))
            .unwrap();
        let items = relayed.await.unwrap();

        // The relay ended, and closed the input.
        assert!(process.stdin.recv().await.is_none());
        insta::assert_debug_snapshot!(items, @r#"
        [
            "Frames [Exit { code: Some(0), signal: None }]",
            "Finished(Exit { code: Some(0), signal: None })",
        ]
        "#);
    }

    #[tokio::test]
    async fn test_ends_without_terminal_frame() {
        let stdout = |frames: &[Frame]| Ok(Output::Stdout(encode(frames).into()));
        let partial = || {
            let mut data = encode(&[Frame::Stdout(bytes::Bytes::from_static(b"partial"))]);
            data.truncate(8);
            Ok(Output::Stdout(data.into()))
        };
        let out = || stdout(&[Frame::Stdout(bytes::Bytes::from_static(b"out"))]);
        let diagnostics = || {
            Ok(Output::Stderr(bytes::Bytes::from_static(
                b"Error: input closed (client hung up)\n",
            )))
        };

        let cases: Vec<(&str, Vec<Result<Output, Error>>, bool)> = vec![
            (
                "closed within a frame",
                vec![out(), partial(), diagnostics()],
                false,
            ),
            (
                "wrapper exited",
                vec![out(), diagnostics(), Ok(Output::Exit(1))],
                false,
            ),
            ("gone", vec![out(), Err(Error::Gone)], false),
            (
                "connection error",
                vec![Err(anyhow::anyhow!("connection reset").into())],
                false,
            ),
            (
                "invalid frame",
                vec![Ok(Output::Stdout(bytes::Bytes::from_static(
                    b"\x09\0\0\0\0",
                )))],
                false,
            ),
            (
                "wrapper error",
                vec![stdout(&[Frame::Error {
                    message: "invalid workspace".to_string(),
                }])],
                false,
            ),
            (
                "stdin closed",
                vec![stdout(&[Frame::Exit {
                    code: None,
                    signal: Some(9),
                }])],
                true,
            ),
        ];

        let mut outcomes = Vec::new();
        for (name, items, close_stdin) in cases {
            let (exec, process) = start().await;
            let Process { stdin, output, .. } = process;
            if close_stdin {
                drop(stdin);
            }
            for item in items {
                output.send(item).unwrap();
            }
            // Without a terminal frame, only the end of the output ends the relay.
            drop(output);

            let items = describe(super::relay(exec, header(), stdin_of(b""))).await;
            outcomes.push((name, items));
        }

        insta::assert_debug_snapshot!(outcomes);
    }

    #[tokio::test]
    async fn test_stdin_error_fails() {
        let (exec, mut process) = start().await;
        let stdin = futures::stream::iter([
            Ok(bytes::Bytes::from_static(b"partial")),
            Err(anyhow::anyhow!("connection reset")),
        ])
        .boxed();
        let items = describe(super::relay(exec, header(), stdin)).await;

        // The relay ended, closing the wrapper's input before any StdinEof.
        let mut buf = bytes::BytesMut::new();
        while let Some(data) = process.stdin.recv().await {
            buf.extend_from_slice(&data);
        }
        let mut inputs = Vec::new();
        while let Some(input) = Input::decode(&mut buf).unwrap() {
            inputs.push(match input {
                Input::Header(_) => "Header".to_string(),
                other => format!("{other:?}"),
            });
        }
        insta::assert_debug_snapshot!((inputs, items), @r#"
        (
            [
                "Header",
                "Stdin(b\"partial\")",
            ],
            [
                "Failed { message: \"failed to read the command's stdin: connection reset\", gone: false }",
            ],
        )
        "#);
    }

    #[test]
    fn test_header_size_and_stderr_tail() {
        let oversize = super::header_frame(sandbox_exec::Header {
            version: sandbox_exec::PROTOCOL_VERSION,
            workspace: "ws".to_string(),
            command: "x".repeat(sandbox_exec::MAX_PAYLOAD),
            env: Default::default(),
        });
        assert!(oversize.is_err());

        let mut relay = Relay::default();
        relay.stderr(&vec![b'a'; 3000]);
        relay.stderr(b"bbb");
        relay.stderr(&vec![b'c'; 1000]);
        let message = relay.failure("it ended");
        assert_eq!(
            message.len(),
            "it ended. The command wrapper's diagnostics: ".len() + 2048
        );
        assert!(message.ends_with(&format!("{}bbb{}", "a".repeat(1045), "c".repeat(1000))));
    }
}
