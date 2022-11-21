use anyhow::Context;
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt};

/// Code is an error code.
type Code = tonic::Code;

/// Status is an error representation that combines a well-known error
/// code with a descriptive error message.
type Status = tonic::Status;

/// Process a unary RPC `op` which is delegated to the connector at `entrypoint`.
pub async fn unary<In, Out>(entrypoint: &[String], op: &str, request: In) -> Result<Out, Status>
where
    In: Message + 'static,
    Out: Message + Default + Unpin,
{
    let requests = futures::stream::once(async { Ok(request) });
    let responses = bidi(entrypoint, op, requests)?;
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

/// Process a bi-directional RPC `op` which is delegated to the connector at `entrypoint`.
pub fn bidi<In, Out, InStream>(
    entrypoint: &[String],
    op: &str,
    requests: InStream,
) -> Result<impl futures::Stream<Item = Result<Out, Status>>, Status>
where
    In: Message + 'static,
    Out: Message + Default,
    InStream: futures::Stream<Item = Result<In, Status>> + Send + 'static,
{
    // Extend `entrypoint` with `op`, then split into the binary and its arguments.
    let entrypoint = entrypoint
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(op))
        .collect::<Vec<_>>();
    let (binary, args) = entrypoint.split_first().unwrap();

    tracing::info!(?binary, ?args, "invoking connector");

    let mut connector = tokio::process::Command::new(binary)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .args(args)
        .kill_on_drop(true)
        .spawn()
        .map_err(|err| {
            map_status(
                Code::Unimplemented,
                "could not start connector entrypoint",
                err,
            )
        })?;

    // Map connector's stdout into a stream of output messages.
    let responses = reader_to_message_stream(connector.stdout.take().expect("stdout is piped"));
    // Spawn a concurrent task that services the connector and forwards to its stdin.
    let connector = tokio::spawn(service_connector(connector, requests));
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
#[tracing::instrument(err, ret, skip_all, level = "debug")]
async fn service_connector<M, S>(
    mut connector: tokio::process::Child,
    stream: S,
) -> Result<(), Status>
where
    M: Message,
    S: futures::Stream<Item = Result<M, Status>>,
{
    let mut stdin = connector.stdin.take().expect("connector stdin is a pipe");
    let stderr = connector.stderr.take().expect("connector stderr is a pipe");

    // Future which processes decoded logs from the connector's stderr, forwarding to
    // our own stderr and, when stderr closes, resolving to a smallish ring-buffer of
    // the very last stderr output.
    let stderr = process_logs(
        stderr,
        ops::stderr_log_handler,
        time::OffsetDateTime::now_utc,
        8192,
    );

    // Future which awaits the connector's exit and stderr result, and returns Ok(())
    // if it exited successfully or an error with embedded stderr content otherwise.
    let exit = async {
        let (wait, stderr) = futures::join!(connector.wait(), stderr);
        let status =
            wait.map_err(|err| map_status(Code::Internal, "failed to wait for connector", err))?;

        if !status.success() {
            let code = status.code().unwrap_or_default();
            return Err(Status::internal(format!(
                "connector failed (exit status {code}) with stderr:\n{stderr}"
            )));
        }
        Ok(())
    };

    tokio::pin!(exit, stream);

    loop {
        let message: Option<Result<M, Status>> = tokio::select! {
            exit = &mut exit => {
                tracing::warn!("connector exited with unconsumed input stream remainder");
                return exit;
            }
            message = stream.next() => message,
        };

        let Some(Ok(message)) = message else {
            if let Some(Err(error)) = message {
                tracing::error!(%error, "error while reading next request");
            }
            std::mem::drop(stdin); // Forward EOF to connector.
            return exit.await;
        };

        if let Err(err) = stdin.write_all(&encode_message(&message)).await {
            tracing::warn!(%err, "i/o error writing to connector stdin");
        }
    }
}

// Maps an AsyncRead into a Stream of decoded messages.
fn reader_to_message_stream<M, R>(reader: R) -> impl futures::Stream<Item = Result<M, Status>>
where
    M: Message + Default,
    R: AsyncRead + Unpin,
{
    futures::stream::try_unfold(reader, |mut reader| async move {
        let next = decode_message::<M, _>(&mut reader).await.map_err(|err| {
            map_status(Code::Internal, "failed to decode message from reader", err)
        })?;

        match next {
            Some(next) => Ok(Some((next, reader))),
            None => Ok(None),
        }
    })
}

/// Decode ops::Logs from the AsyncRead, passing each to the given handler,
/// and also accumulate up to `ring_capacity` of final stderr output
/// which is returned upon the first clean EOF or other error of the reader.
async fn process_logs<R, H, T>(reader: R, handler: H, timesource: T, ring_capacity: usize) -> String
where
    R: tokio::io::AsyncRead + Unpin,
    H: Fn(ops::Log),
    T: Fn() -> time::OffsetDateTime,
{
    let mut reader = tokio::io::BufReader::new(reader);
    let mut ring = std::collections::VecDeque::<u8>::with_capacity(ring_capacity);
    let mut line = String::new();
    let decoder = ops::decode::Decoder::new(timesource);

    loop {
        line.clear();

        match reader.read_line(&mut line).await {
            Err(err) => {
                tracing::error!(%err, "failed to read from connector stderr");
                break;
            }
            Ok(0) => break, // Clean EOF.
            Ok(_) => (),
        }

        // Drop lines from the head of the ring while there's insufficient
        // capacity for the current `line`. Then push `line`.
        // We (currently) allow a single line to violate `ring_capacity`.
        while !ring.is_empty() && ring.len() + line.len() > ring_capacity {
            match ring.iter().position(|c| *c == b'\n') {
                Some(ind) => {
                    ring.drain(..ind + 1);
                }
                None => ring.clear(),
            }
        }
        ring.extend(line.as_bytes().iter());

        handler(decoder.line_to_log(&line));
    }

    String::from_utf8_lossy(ring.make_contiguous()).to_string()
}

fn map_status<E: Into<anyhow::Error>>(code: tonic::Code, message: &str, err: E) -> Status {
    Status::with_details(code, message, format!("{:#}", anyhow::anyhow!(err)).into())
}

/// Encode a message into a returned buffer.
/// The message is prefixed with a fix four-byte little endian length header.
pub fn encode_message<T: Message>(message: &T) -> Vec<u8> {
    let length = message.encoded_len();
    let mut buf = Vec::with_capacity(4 + length);
    buf.extend_from_slice(&(length as u32).to_le_bytes());
    message
        .encode(&mut buf)
        .expect("buf has pre-allocated capacity");
    buf
}

/// Decode a single message of type T from the AsyncRead.
/// If the reader returns EOF prior to a length header being read,
/// the EOF is mapped into an Ok(None). Other errors, including an
/// unexpected EOF _after_ reading the length header, are returned.
pub async fn decode_message<T, R>(mut reader: R) -> anyhow::Result<Option<T>>
where
    T: Message + Default,
    R: AsyncRead + Unpin,
{
    let length = match reader.read_u32_le().await {
        Err(err) => match err.kind() {
            // UnexpectedEof indicates the ending of the stream.
            std::io::ErrorKind::UnexpectedEof => return Ok(None),
            _ => return Err(err).context("decoding message header"),
        },
        Ok(l) if l > 1 << 27 => anyhow::bail!("decoded message length {l} is too large"),
        Ok(l) => l,
    };

    let mut buf: Vec<u8> = vec![0; length as usize];
    reader.read_exact(&mut buf).await?;

    Ok(Some(
        T::decode(buf.as_slice()).context("decoding message body")?,
    ))
}

#[cfg(test)]
mod test {
    use super::{bidi, decode_message, encode_message, reader_to_message_stream};
    use super::{process_logs, unary};
    use futures::StreamExt;

    use futures::TryStreamExt;
    use proto_flow::flow::TestSpec;

    #[tokio::test]
    async fn test_encode_and_decode() {
        let buf = encode_message(&TestSpec {
            test: "hello world".to_string(),
            ..Default::default()
        });
        let mut r = buf.as_slice();

        // Expect we decode our fixture.
        assert_eq!(
            decode_message(&mut r).await.unwrap(),
            Some(TestSpec {
                test: "hello world".to_string(),
                ..Default::default()
            })
        );
        // Next attempt maps EOF => None.
        assert_eq!(decode_message::<TestSpec, _>(&mut r).await.unwrap(), None,);

        // Malformed input (ends prematurely given header).
        let buf = vec![1, 2, 3, 4, 5, 6];
        assert_eq!(
            format!("{:?}", decode_message::<TestSpec, _>(buf.as_slice()).await),
            "Err(early eof)",
        );
        // Malformed input (length is longer than our allowed maximum).
        let buf = vec![0xee, 0xee, 0xee, 0xee, 1];
        assert_eq!(
            format!("{:?}", decode_message::<TestSpec, _>(buf.as_slice()).await),
            "Err(decoded message length 4008636142 is too large)",
        );
    }

    #[tokio::test]
    async fn test_byte_reader_to_stream() {
        let fixture = TestSpec {
            test: "hello world".to_string(),
            ..Default::default()
        };
        let mut buf = encode_message(&fixture);

        // We can collect multiple encoded items as a stream, and then read a clean EOF.
        let three = buf.repeat(3);
        let stream = reader_to_message_stream(three.as_slice());
        let three: Vec<TestSpec> = stream.try_collect().await.unwrap();
        assert_eq!(three.len(), 3);

        // If the stream bytes are malformed, we read a message and then an appropriate error.
        buf.extend_from_slice(&[0xee, 0xee, 0xee, 0xee, 1]);
        let stream = reader_to_message_stream::<TestSpec, _>(buf.as_slice());
        tokio::pin!(stream);

        assert_eq!(
            &format!("{:?}", stream.next().await),
            "Some(Ok(TestSpec { test: \"hello world\", steps: [] }))"
        );
        assert_eq!(
            &format!("{:?}", stream.next().await),
            "Some(Err(Status { code: Internal, message: \"failed to decode message from reader\", details: b\"decoded message length 4008636142 is too large\", source: None }))",
        );
    }

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
        .join("\n");

        let seq = std::cell::RefCell::new(0);
        let timesource = || {
            let mut seq = seq.borrow_mut();
            *seq += 10;
            time::OffsetDateTime::from_unix_timestamp(1660000000 + *seq).unwrap()
        };

        let logs = std::cell::RefCell::new(Vec::new());
        let recent = process_logs(
            fixture.as_bytes(),
            |log| logs.borrow_mut().push(log),
            timesource,
            64,
        )
        .await;

        // Expect a bounded amount of recent logs are returned.
        insta::assert_snapshot!(recent, @r###"
        to boldly go
           smol(1)
        "###);

        // All logs were decoded and mapped into their structured equivalents.
        insta::assert_snapshot!(serde_json::to_string_pretty(&logs).unwrap(), @r###"
        [
          {
            "ts": "2022-08-08T23:06:50Z",
            "level": "warn",
            "message": "hi"
          },
          {
            "ts": "2022-08-08T23:07:00Z",
            "level": "warn",
            "message": "",
            "fields": {
              "some_log": "value"
            }
          },
          {
            "ts": "2022-08-08T23:07:10Z",
            "level": "error",
            "message": "a failed walrus appears"
          },
          {
            "ts": "2022-08-08T23:07:20Z",
            "level": "debug",
            "message": "hi",
            "fields": {
              "field": "with a long value which overflows the ring",
              "other": ["thing one","thing two"]
            }
          },
          {
            "ts": "2022-08-08T23:07:30Z",
            "level": "warn",
            "message": "to boldly go"
          },
          {
            "ts": "2022-08-08T23:07:40Z",
            "level": "warn",
            "message": "   smol(1)"
          }
        ]
        "###);
    }

    #[tokio::test]
    async fn test_bidi_cat() {
        let requests = futures::stream::repeat_with(|| {
            Ok(TestSpec {
                test: "hello world".to_string(),
                ..Default::default()
            })
        })
        .take(2); // Bounded stream of two inputs.

        // Let "cat" run to completion and collect its output messages.
        // Note that "cat" will only exit if we properly close its stdin after sending all inputs.
        let responses: Vec<Result<TestSpec, _>> = bidi(&["cat".to_string()], "-", requests)
            .unwrap()
            .collect()
            .await;

        insta::assert_debug_snapshot!(responses, @r###"
        [
            Ok(
                TestSpec {
                    test: "hello world",
                    steps: [],
                },
            ),
            Ok(
                TestSpec {
                    test: "hello world",
                    steps: [],
                },
            ),
        ]
        "###);
    }

    #[tokio::test]
    async fn test_bidi_true() {
        let requests = futures::stream::repeat_with(|| {
            Ok(TestSpec {
                test: "hello world".to_string(),
                ..Default::default()
            })
        }); // Unbounded stream.

        // "true" exits immediately with success, without reading our unbounded stream of inputs.
        let responses: Vec<Result<TestSpec, _>> = bidi(&["true".to_string()], "", requests)
            .unwrap()
            .collect()
            .await;

        insta::assert_debug_snapshot!(responses, @r###"
        []
        "###);
    }

    #[tokio::test]
    async fn test_bidi_cat_error() {
        let requests = futures::stream::repeat_with(|| {
            Ok(TestSpec {
                test: "hello world".to_string(),
                ..Default::default()
            })
        }); // Unbounded stream.

        let responses: Vec<Result<TestSpec, _>> =
            bidi(&["cat".to_string()], "/this/path/does/not/exist", requests)
                .unwrap()
                .collect()
                .await;

        insta::assert_debug_snapshot!(responses, @r###"
        [
            Err(
                Status {
                    code: Internal,
                    message: "connector failed (exit status 1) with stderr:\ncat: /this/path/does/not/exist: No such file or directory\n",
                    source: None,
                },
            ),
        ]
        "###);
    }

    #[tokio::test]
    async fn test_unary_cat() {
        let fixture = TestSpec {
            test: "hello world".to_string(),
            ..Default::default()
        };

        let out: TestSpec = unary(&["cat".to_string()], "-", fixture.clone())
            .await
            .unwrap();
        assert_eq!(out, fixture);
    }

    #[tokio::test]
    async fn test_unary_too_few_outputs() {
        let fixture = TestSpec {
            test: "hello world".to_string(),
            ..Default::default()
        };

        let out: Result<TestSpec, _> = unary(&["true".to_string()], "", fixture.clone()).await;
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
