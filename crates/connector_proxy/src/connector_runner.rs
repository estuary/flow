use crate::apis::{FlowCaptureOperation, FlowMaterializeOperation, InterceptorStream};
use crate::errors::{
    self, interceptor_stream_to_io_stream, io_stream_to_interceptor_stream, Error,
};
use crate::interceptors::airbyte_source_interceptor::AirbyteSourceInterceptor;
use crate::libs::command::{
    check_exit_status, invoke_connector_delayed, invoke_connector_direct, parse_child,
};
use crate::libs::protobuf::{decode_message, encode_message};
use futures::channel::oneshot;
use futures::{stream, StreamExt};
use proto_flow::capture::PullResponse;
use proto_flow::flow::DriverCheckpoint;
use tokio::io::copy;
use tokio::process::{ChildStdin, ChildStdout};
use tokio_util::io::{ReaderStream, StreamReader};

pub async fn run_flow_capture_connector(
    op: &FlowCaptureOperation,
    entrypoint: Vec<String>,
) -> Result<(), Error> {
    let (entrypoint, mut args) = parse_entrypoint(&entrypoint)?;
    args.push(op.to_string());

    let (mut child, child_stdin, child_stdout) =
        parse_child(invoke_connector_direct(entrypoint, args)?)?;

    let adapted_request_stream = request_stream();

    let adapted_response_stream = response_stream(child_stdout);

    let streaming_all_task =
        streaming_all(child_stdin, adapted_request_stream, adapted_response_stream);

    let exit_status_task =
        async move {
            let r = check_exit_status("flow capture connector:", child.wait().await);
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            r
        };

    tokio::select! {
        Err(e) = streaming_all_task => Err(e),
        resp = exit_status_task => resp,
    }
}

pub async fn run_flow_materialize_connector(
    op: &FlowMaterializeOperation,
    entrypoint: Vec<String>,
) -> Result<(), Error> {
    let (entrypoint, mut args) = parse_entrypoint(&entrypoint)?;
    args.push(op.to_string());

    let (mut child, child_stdin, child_stdout) =
        parse_child(invoke_connector_direct(entrypoint, args)?)?;

    let adapted_request_stream = request_stream();

    let adapted_response_stream = response_stream(child_stdout);

    let streaming_all_task =
        streaming_all(child_stdin, adapted_request_stream, adapted_response_stream);

    let exit_status_task =
        async move {
            let r = check_exit_status("flow materialize connector:", child.wait().await);
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            r
        };

    tokio::select! {
        Err(e) = streaming_all_task => Err(e),
        resp = exit_status_task => resp,
    }
}

pub async fn run_airbyte_source_connector(
    op: &FlowCaptureOperation,
    entrypoint: Vec<String>,
) -> Result<(), Error> {
    let mut airbyte_interceptor = AirbyteSourceInterceptor::new();

    let (entrypoint, args) = parse_entrypoint(&entrypoint)?;
    let args = airbyte_interceptor.adapt_command_args(op, args)?;

    let (mut child, child_stdin, child_stdout) =
        parse_child(invoke_connector_delayed(entrypoint, args)?)?;

    let adapted_request_stream = airbyte_interceptor.adapt_request_stream(op, request_stream())?;

    let res_stream =
        airbyte_interceptor.adapt_response_stream(op, response_stream(child_stdout))?;

    // Keep track of whether we did send a Driver Checkpoint as the final message of the response stream
    // See the comment of the block below for why this is necessary
    let (tp_sender, tp_receiver) = oneshot::channel::<bool>();
    let res_stream = if *op == FlowCaptureOperation::Pull {
        Box::pin(stream::try_unfold(
            (false, res_stream, tp_sender),
            |(transaction_pending, mut stream, sender)| async move {
                let (message, raw) = match stream.next().await {
                    Some(bytes) => {
                        let bytes = bytes?;
                        let mut buf = &bytes[..];
                        // This is infallible because we must encode a PullResponse in response to
                        // a PullRequest. See airbyte_source_interceptor.adapt_pull_response_stream
                        let msg = decode_message::<PullResponse, _>(&mut buf)
                            .await
                            .unwrap()
                            .unwrap();
                        (msg, bytes)
                    }
                    None => {
                        sender
                            .send(transaction_pending)
                            .map_err(|_| errors::Error::AirbyteCheckpointPending)?;
                        return Ok(None);
                    }
                };

                Ok(Some((raw, (!message.checkpoint.is_some(), stream, sender))))
            },
        ))
    } else {
        res_stream
    };

    let adapted_response_stream = res_stream;

    let streaming_all_task =
        streaming_all(child_stdin, adapted_request_stream, adapted_response_stream);

    let cloned_op = op.clone();
    let exit_status_task = async move {
        let exit_status_result = check_exit_status("airbyte source connector:", child.wait().await);

        // There are some Airbyte connectors that write records, and exit successfully, without ever writing
        // a state (checkpoint). In those cases, we want to provide a default empty checkpoint. It's important that
        // this only happens if the connector exit successfully, otherwise we risk double-writing data.
        if exit_status_result.is_ok() && cloned_op == FlowCaptureOperation::Pull {
            // the received value (transaction_pending) is true if the connector writes output messages and exits _without_ writing
            // a final state checkpoint.
            if tp_receiver.await.unwrap() {
                // We generate a synthetic commit now, and the empty checkpoint means the assumed behavior
                // of the next invocation will be "full refresh".
                tracing::warn!("go.estuary.dev/W001: connector exited without writing a final state checkpoint, writing an empty object {{}} merge patch driver checkpoint.");
                let mut resp = PullResponse::default();
                resp.checkpoint = Some(DriverCheckpoint {
                    driver_checkpoint_json: b"{}".to_vec(),
                    rfc7396_merge_patch: true,
                });
                let encoded_response = &encode_message(&resp)?;
                let mut buf = &encoded_response[..];
                copy(&mut buf, &mut tokio::io::stdout()).await?;
            }
        }

        // Once the airbyte connector has exited, we must close stdout of connector_proxy
        // so that the runtime knows the RPC is over. In turn, the runtime will close the stdin
        // from their end. This is necessary to avoid a deadlock where runtime is waiting for
        // connector_proxy to close stdout, and connector_proxy is waiting for runtime to close
        // stdin.
        if exit_status_result.is_ok() {
            // We wait a few seconds to let any remaining writes to be done
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            std::process::exit(0);
        }

        exit_status_result
    };

    tokio::try_join!(streaming_all_task, exit_status_task)?;

    Ok(())
}

fn parse_entrypoint(entrypoint: &Vec<String>) -> Result<(String, Vec<String>), Error> {
    if entrypoint.len() == 0 {
        return Err(Error::EmptyEntrypointError);
    }

    return Ok((entrypoint[0].clone(), entrypoint[1..].to_vec()));
}

fn request_stream() -> InterceptorStream {
    Box::pin(io_stream_to_interceptor_stream(ReaderStream::new(
        tokio::io::stdin(),
    )))
}

fn response_stream(child_stdout: ChildStdout) -> InterceptorStream {
    Box::pin(io_stream_to_interceptor_stream(ReaderStream::new(
        child_stdout,
    )))
}

async fn streaming_all(
    mut request_stream_writer: ChildStdin,
    request_stream: InterceptorStream,
    response_stream: InterceptorStream,
) -> Result<(), Error> {
    let mut request_stream_reader =
        StreamReader::new(interceptor_stream_to_io_stream(request_stream));
    let mut response_stream_reader =
        StreamReader::new(interceptor_stream_to_io_stream(response_stream));
    let mut response_stream_writer = tokio::io::stdout();

    let request_stream_copy =
        async move { copy(&mut request_stream_reader, &mut request_stream_writer).await };

    let response_stream_copy =
        async move { copy(&mut response_stream_reader, &mut response_stream_writer).await };

    let (req_stream_bytes, resp_stream_bytes) =
        tokio::try_join!(request_stream_copy, response_stream_copy)?;

    tracing::debug!(
        req_stream = req_stream_bytes,
        resp_stream = resp_stream_bytes,
        "streaming_all finished"
    );
    Ok(())
}

#[cfg(test)]
mod test {
    use std::pin::Pin;

    use bytes::Bytes;
    use futures::{stream, TryStream};

    use super::*;

    fn create_stream<T>(
        input: Vec<T>,
    ) -> Pin<Box<impl TryStream<Item = Result<T, Error>, Ok = T, Error = Error>>> {
        Box::pin(stream::iter(input.into_iter().map(Ok::<T, Error>)))
    }

    #[tokio::test]
    async fn test_streaming_all_eof_in_stdin() {
        let input = "hello\n".as_bytes();
        let req_stream = create_stream(vec![Bytes::from(input)]);

        // `tail -f` will not exit until EOF has been reached in its stdin
        // This test ensures that once we reach end of the input stream, an EOF is sent to stdin of the proxy process
        // even if the stdout of the process is blocked. In this case, tail -f will not terminate its stdout until
        // stdin has received EOF.
        let (_, stdin, stdout) = parse_child(
            invoke_connector_direct("tail".to_string(), vec!["-f".to_string()]).unwrap(),
        )
        .unwrap();

        let res_stream = response_stream(stdout);

        assert!(streaming_all(stdin, req_stream, res_stream).await.is_ok());
    }

    /* This test will still hang because there is a detached streaming task lingering around
     * that prevents it from shutting down. In the code itself we use `std::process::exit` when we see an error such as this
     * however here we can't do that as it breaks the test runner.
     *
     * Uncommenting and running this test should result in the `done!` message in stderr which means we do propagate the Err
     * upstream.
     *
    #[tokio::test]
    async fn test_run_connector_exit_process_before_eof() {
        let script = vec!["sh".to_string(), "-c".to_string(), "exit 2".to_string()];

        let result =
            run_flow_materialize_connector(&FlowMaterializeOperation::Transactions, script).await;
        assert!(result.is_err());
        eprintln!("done! {:#?}", result);
    }*/
}
