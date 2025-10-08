use super::codec::{Codec, reader_to_message_stream};
use anyhow::Context;
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tonic::Status;

pub fn new_command<S: AsRef<str>>(entrypoint: &[S]) -> async_process::Command {
    // Split `entrypoint` into the binary and its arguments.
    let entrypoint = entrypoint.iter().map(AsRef::as_ref).collect::<Vec<_>>();
    let (binary, args) = entrypoint.split_first().unwrap();

    let mut cmd = async_process::Command::new(binary);
    cmd.args(args);
    cmd
}

/// Process a unary RPC `op` which is delegated to the connector at `entrypoint`.
pub async fn unary<In, Out, H>(
    connector: async_process::Command,
    codec: Codec,
    request: In,
    log_handler: H,
) -> tonic::Result<Out>
where
    In: prost::Message + serde::Serialize + 'static,
    Out: prost::Message + for<'de> serde::Deserialize<'de> + Default + Unpin,
    H: Fn(&ops::Log) + Send + Sync + 'static,
{
    let requests = futures::stream::once(async { Ok(request) });
    let responses = bidi(connector, codec, requests, log_handler)?;
    let mut responses: Vec<Out> = responses.try_collect().await?;

    let response = responses.pop();
    match (response, responses.is_empty()) {
        (Some(response), true) => Ok(response),
        (Some(_), false) => Err(Status::internal(
            "rpc is expected to be unary but it returned multiple responses",
        )),
        (None, _) => Err(Status::internal(
            "rpc is expected to be unary but it returned no response",
        )),
    }
}

/// Process a bi-directional RPC which is delegated to the connector at `entrypoint`.
pub fn bidi<In, Out, InStream, H>(
    mut connector: async_process::Command,
    codec: Codec,
    requests: InStream,
    log_handler: H,
) -> tonic::Result<impl futures::Stream<Item = tonic::Result<Out>>>
where
    In: prost::Message + serde::Serialize + 'static,
    Out: prost::Message + for<'de> serde::Deserialize<'de> + Default,
    InStream: futures::Stream<Item = tonic::Result<In>> + Send + 'static,
    H: Fn(&ops::Log) + Send + Sync + 'static,
{
    let args: Vec<String> = std::iter::once(connector.get_program())
        .chain(connector.get_args())
        .map(|s| s.to_string_lossy().to_string())
        .collect();

    let mut connector: async_process::Child = connector
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .with_context(|| format!("running command {args:?}"))
        .map_err(|err| map_status("could not start connector entrypoint", err))?
        .into();

    // Map connector's stdout into a stream of output messages.
    let responses = reader_to_message_stream(
        codec,
        connector.stdout.take().expect("stdout is piped"),
        32 * 1024, // Minimum buffer capacity.
    )
    .map_err(|err| map_status("failed to process connector output", err));
    // Spawn a concurrent task that services the connector and forwards to its stdin.
    let connector = tokio::spawn(service_connector(connector, codec, requests, log_handler));
    // Ensure `connector` is aborted (and the process killed) if our response stream is dropped.
    let connector = AutoAbortHandle(connector);
    // Map to a Stream that awaits `connector` and returns EOF, or returns its error.
    let connector = futures::stream::try_unfold(connector, |connector| async move {
        let () = connector.await.expect("service_connector finishes")?;
        Ok(None)
    });
    // Chain `responses` with the final success (empty) or error Status of `connector`.
    Ok(responses.chain(connector))
}

/// Service connector by jointly waiting for it to exit, and for its stderr to complete.
/// While waiting, also read from `stream` and forward to the connector's stdin.
///
/// Note that the connector _should_ but is not *obligated* to consume its stdin.
/// As such, an I/O error (e.x. a broken pipe) or unconsumed stream remainder
/// is logged but is not considered an error.
async fn service_connector<M, S, H>(
    mut connector: async_process::Child,
    codec: Codec,
    stream: S,
    log_handler: H,
) -> tonic::Result<()>
where
    M: prost::Message + serde::Serialize + 'static,
    S: futures::Stream<Item = tonic::Result<M>>,
    H: Fn(&ops::Log) + Send + Sync + 'static,
{
    let mut stdin = connector.stdin.take().expect("connector stdin is a pipe");
    let stderr = connector.stderr.take().expect("connector stderr is a pipe");

    // Future which processes decoded logs from the connector's stderr, forwarding to
    // our `log_handler` and, when stderr closes, resolving to a final Log.
    let last_log = process_logs(stderr, log_handler, std::time::SystemTime::now);

    // Future which awaits the connector's exit and stderr result, and returns Ok(())
    // if it exited successfully or an error with embedded stderr content otherwise.
    let exit = async {
        let (wait, last_log) = futures::join!(connector.wait(), last_log);
        let status = wait.map_err(|err| map_status("failed to wait for connector", err))?;

        if !status.success() {
            tracing::error!(%status, "connector failed");

            let mut status = Status::internal(&last_log.message);
            status.metadata_mut().insert_bin(
                "last-log-bin",
                tonic::metadata::MetadataValue::from_bytes(&last_log.encode_to_vec()),
            );
            Err(status)
        } else {
            tracing::debug!(%status, "connector exited");
            Ok(())
        }
    };

    tokio::pin!(exit, stream);
    let mut buffer = Vec::new();

    loop {
        let message: Option<tonic::Result<M>> = tokio::select! {
            biased;

            // Should we exit?
            exit = &mut exit => {
                tracing::warn!("connector exited with unconsumed input stream remainder");
                return exit;
            }

            // Proxy a next, ready message?
            message = stream.next(), if buffer.len() < 1<<15 => message,

            // No message is ready. Should we flush?
            _ = async {}, if !buffer.is_empty() => {
                if let Err(error) = stdin.write_all(&buffer).await {
                    tracing::warn!(%error, "i/o error writing to connector stdin");
                }
                buffer.clear();
                continue;
            }
        };

        let Some(Ok(message)) = message else {
            if let Err(error) = stdin.write_all(&buffer).await {
                tracing::warn!(%error, "i/o error writing to connector stdin");
            }
            if let Some(Err(error)) = message {
                tracing::error!(%error, "failed to read next runtime request");
            }
            std::mem::drop(stdin); // Forward EOF to connector.
            return exit.await;
        };

        codec.encode(&message, &mut buffer);
    }
}

/// Decode ops::Logs from the AsyncRead, passing each to the given handler,
/// and also accumulate up to `ring_capacity` of final stderr output
/// which is returned upon the first clean EOF or other error of the reader.
async fn process_logs<R, H, T>(reader: R, handler: H, timesource: T) -> ops::Log
where
    R: tokio::io::AsyncRead + Unpin,
    H: Fn(&ops::Log),
    T: Fn() -> std::time::SystemTime,
{
    let mut reader = tokio::io::BufReader::new(reader);
    let mut line = String::new();
    let decoder = ops::decode::Decoder::new(timesource);
    let mut last_log = ops::Log {
        level: ops::LogLevel::Warn as i32,
        message: "connector exited with no log output".to_string(),
        ..Default::default()
    };

    loop {
        line.clear();

        match reader.read_line(&mut line).await {
            Err(error) => {
                tracing::error!(%error, "failed to read from connector stderr");
                break;
            }
            Ok(0) => break, // Clean EOF.
            Ok(_) => (),
        }

        // Extract the next log line, passing a look-ahead buffer that allows
        // multiple unstructured lines to map into a single Log instance.
        let (log, consume) = decoder.line_to_log(&line, reader.buffer());
        reader.consume(consume);

        handler(&log);
        last_log = log;
    }
    last_log
}

fn map_status<E: Into<anyhow::Error>>(message: &'static str, err: E) -> Status {
    Status::internal(format!("{:#}", anyhow::anyhow!(err).context(message)))
}

#[cfg(test)]
mod test {
    use super::{Codec, bidi, new_command, process_logs, unary};
    use futures::{StreamExt, TryStreamExt};
    use proto_flow::flow::TestSpec;

    #[tokio::test]
    async fn test_log_processing() {
        let fixture = [
            "hi",
            r#"{"some_log":"value"}"#,
            "a failed walrus appears\t\r",
            r#"{"msg":"hi","lvl":"debug","other":["thing one","thing two"],"field":"with a long value which overflows the ring"}  "#,
            "to boldly go",
            "   smol(1)",
        ]
        .join("\n") + "\n";

        let seq = std::cell::RefCell::new(0);
        let timesource = || {
            let mut seq = seq.borrow_mut();
            *seq += 10;
            time::OffsetDateTime::from_unix_timestamp(1660000000 + *seq)
                .unwrap()
                .into()
        };

        let logs = std::cell::RefCell::new(Vec::new());
        let last_log = process_logs(
            fixture.as_bytes(),
            |log| logs.borrow_mut().push(log.clone()),
            timesource,
        )
        .await;

        // Expect the last log is returned.
        insta::assert_debug_snapshot!(last_log, @r###"
        Log {
            meta: None,
            shard: None,
            timestamp: Some(
                Timestamp {
                    seconds: 1660000050,
                    nanos: 0,
                },
            ),
            level: Warn,
            message: "to boldly go\n   smol(1)",
            fields_json_map: {},
            spans: [],
        }
        "###);

        // All logs were decoded and mapped into their structured equivalents.
        insta::assert_snapshot!(serde_json::to_string_pretty(&logs).unwrap(), @r###"
        [
          {
            "ts": "2022-08-08T23:06:50+00:00",
            "level": "warn",
            "message": "hi"
          },
          {
            "ts": "2022-08-08T23:07:00+00:00",
            "level": "warn",
            "fields": {
              "some_log": "value"
            }
          },
          {
            "ts": "2022-08-08T23:07:10+00:00",
            "level": "error",
            "message": "a failed walrus appears"
          },
          {
            "ts": "2022-08-08T23:07:20+00:00",
            "level": "debug",
            "message": "hi",
            "fields": {
              "field": "with a long value which overflows the ring",
              "other": ["thing one","thing two"]
            }
          },
          {
            "ts": "2022-08-08T23:07:30+00:00",
            "level": "warn",
            "message": "to boldly go\n   smol(1)"
          }
        ]
        "###);
    }

    #[tokio::test]
    async fn test_bidi_cat() {
        for codec in [Codec::Proto, Codec::Json] {
            let requests = futures::stream::repeat_with(|| {
                Ok(TestSpec {
                    name: "hello world".to_string(),
                    ..Default::default()
                })
            })
            .take(2); // Bounded stream of two inputs.

            // Let "cat" run to completion and collect its output messages.
            // Note that "cat" will only exit if we properly close its stdin after sending all inputs.
            let responses: Vec<Result<TestSpec, _>> = bidi(
                new_command(&["cat".to_string(), "-".to_string()]),
                codec,
                requests,
                ops::stderr_log_handler,
            )
            .unwrap()
            .collect()
            .await;

            insta::allow_duplicates! {
            insta::assert_debug_snapshot!(responses, @r###"
            [
                Ok(
                    TestSpec {
                        name: "hello world",
                        steps: [],
                    },
                ),
                Ok(
                    TestSpec {
                        name: "hello world",
                        steps: [],
                    },
                ),
            ]
            "###);
            }
        }
    }

    #[tokio::test]
    async fn test_bidi_true() {
        let requests = futures::stream::repeat_with(|| {
            Ok(TestSpec {
                name: "hello world".to_string(),
                ..Default::default()
            })
        }); // Unbounded stream.

        // "true" exits immediately with success, without reading our unbounded stream of inputs.
        let responses: Vec<Result<TestSpec, _>> = bidi(
            new_command(&["true".to_string()]),
            Codec::Proto,
            requests,
            ops::stderr_log_handler,
        )
        .unwrap()
        .collect()
        .await;

        insta::assert_debug_snapshot!(responses, @r###"
        []
        "###);
    }

    #[tokio::test]
    async fn test_bidi_cat_error() {
        for codec in [Codec::Proto, Codec::Json] {
            let requests = futures::stream::repeat_with(|| {
                Ok(TestSpec {
                    name: "hello world".to_string(),
                    ..Default::default()
                })
            }); // Unbounded stream.

            let responses: Vec<Result<TestSpec, _>> = bidi(
                new_command(&["cat".to_string(), "/this/path/does/not/exist".to_string()]),
                codec,
                requests,
                ops::stderr_log_handler,
            )
            .unwrap()
            .map_err(strip_log)
            .collect()
            .await;

            insta::allow_duplicates! {
            insta::assert_debug_snapshot!(responses, @r###"
            [
                Err(
                    Status {
                        code: Internal,
                        message: "cat: /this/path/does/not/exist: No such file or directory",
                        metadata: MetadataMap {
                            headers: {
                                "last-log-bin": "dmFs",
                            },
                        },
                        source: None,
                    },
                ),
            ]
            "###);
            }
        }
    }

    #[tokio::test]
    async fn test_bidi_cat_bad_output_and_error() {
        let requests = futures::stream::repeat_with(|| {
            Ok(TestSpec {
                name: "hello world".to_string(),
                ..Default::default()
            })
        }); // Unbounded stream.

        // Model a connector that both writes bad output, and also fails with an error.
        // We'll map this into two errors of the response stream, though tonic is only
        // able to log the first of these. We additionally have tracing::error logging
        // which ensures both make it to the ops log collection. Unfortunately there's
        // no reliable way to conjoin these errors, as a connector can write bad output
        // or even close its stdout without actually exiting.
        let responses: Vec<Result<TestSpec, _>> = bidi(
            new_command(&[
                "cat".to_string(),
                "/etc/hosts".to_string(),
                "/this/path/does/not/exist".to_string(),
            ]),
            Codec::Proto,
            requests,
            ops::stderr_log_handler,
        )
        .unwrap()
        .map_err(strip_log)
        .collect()
        .await;

        insta::assert_debug_snapshot!(responses, @r###"
        [
            Err(
                Status {
                    code: Internal,
                    message: "failed to process connector output: connector wrote a partial message and then closed its output",
                    source: None,
                },
            ),
            Err(
                Status {
                    code: Internal,
                    message: "cat: /this/path/does/not/exist: No such file or directory",
                    metadata: MetadataMap {
                        headers: {
                            "last-log-bin": "dmFs",
                        },
                    },
                    source: None,
                },
            ),
        ]
        "###);
    }

    #[tokio::test]
    async fn test_unary_cat() {
        for codec in [Codec::Proto, Codec::Json] {
            let fixture = TestSpec {
                name: "hello world".to_string(),
                ..Default::default()
            };

            let out: TestSpec = unary(
                new_command(&["cat".to_string(), "-".to_string()]),
                codec,
                fixture.clone(),
                ops::stderr_log_handler,
            )
            .await
            .unwrap();
            assert_eq!(out, fixture);
        }
    }

    #[tokio::test]
    async fn test_unary_too_few_outputs() {
        for codec in [Codec::Proto, Codec::Json] {
            let fixture = TestSpec {
                name: "hello world".to_string(),
                ..Default::default()
            };

            let out: Result<TestSpec, _> = unary(
                new_command(&["true".to_string()]),
                codec,
                fixture.clone(),
                ops::stderr_log_handler,
            )
            .await;

            insta::allow_duplicates! {
            insta::assert_debug_snapshot!(out, @r###"
            Err(
                Status {
                    code: Internal,
                    message: "rpc is expected to be unary but it returned no response",
                    source: None,
                },
            )
            "###);
            }
        }
    }

    fn strip_log(mut status: tonic::Status) -> tonic::Status {
        if let Some(m) = status.metadata_mut().get_bin_mut("last-log-bin") {
            *m = tonic::metadata::MetadataValue::from_bytes(b"val");
        }
        status
    }
}

struct AutoAbortHandle<T>(tokio::task::JoinHandle<T>);

impl<T> std::future::Future for AutoAbortHandle<T> {
    type Output = Result<T, tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        unsafe { std::pin::Pin::new_unchecked(&mut self.0) }.poll(cx)
    }
}

impl<T> Drop for AutoAbortHandle<T> {
    fn drop(&mut self) {
        self.0.abort()
    }
}
