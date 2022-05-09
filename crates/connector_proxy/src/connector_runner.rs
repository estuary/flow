use crate::apis::{FlowCaptureOperation, FlowMaterializeOperation, InterceptorStream};
use crate::errors::Error;
use crate::interceptors::{
    airbyte_source_interceptor::AirbyteSourceInterceptor,
    network_tunnel_capture_interceptor::NetworkTunnelCaptureInterceptor,
    network_tunnel_materialize_interceptor::NetworkTunnelMaterializeInterceptor,
};
use crate::libs::command::{
    check_exit_status, invoke_connector_delayed, invoke_connector_direct, parse_child,
};
use flow_cli_common::LogArgs;
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

    let adapted_request_stream =
        NetworkTunnelCaptureInterceptor::adapt_request_stream(op, request_stream())?;

    let adapted_response_stream =
        NetworkTunnelCaptureInterceptor::adapt_response_stream(op, response_stream(child_stdout))?;

    streaming_all(child_stdin, adapted_request_stream, adapted_response_stream).await?;

    check_exit_status("flow capture connector:", child.wait().await)
}

pub async fn run_flow_materialize_connector(
    op: &FlowMaterializeOperation,
    entrypoint: Vec<String>,
) -> Result<(), Error> {
    let (entrypoint, mut args) = parse_entrypoint(&entrypoint)?;
    args.push(op.to_string());

    let (mut child, child_stdin, child_stdout) =
        parse_child(invoke_connector_direct(entrypoint, args)?)?;

    let adapted_request_stream =
        NetworkTunnelMaterializeInterceptor::adapt_request_stream(op, request_stream())?;

    let adapted_response_stream = NetworkTunnelMaterializeInterceptor::adapt_response_stream(
        op,
        response_stream(child_stdout),
    )?;

    streaming_all(child_stdin, adapted_request_stream, adapted_response_stream).await?;

    check_exit_status("flow materialize connector:", child.wait().await)
}

pub async fn run_airbyte_source_connector(
    op: &FlowCaptureOperation,
    entrypoint: Vec<String>,
    log_args: LogArgs,
) -> Result<(), Error> {
    let mut airbyte_interceptor = AirbyteSourceInterceptor::new();

    let (entrypoint, args) = parse_entrypoint(&entrypoint)?;
    let args = airbyte_interceptor.adapt_command_args(op, args)?;

    let (mut child, child_stdin, child_stdout) =
        parse_child(invoke_connector_delayed(entrypoint, args, log_args)?)?;

    let adapted_request_stream = airbyte_interceptor.adapt_request_stream(
        op,
        NetworkTunnelCaptureInterceptor::adapt_request_stream(op, request_stream())?,
    )?;

    let adapted_response_stream = NetworkTunnelCaptureInterceptor::adapt_response_stream(
        op,
        airbyte_interceptor.adapt_response_stream(op, response_stream(child_stdout))?,
    )?;

    streaming_all(child_stdin, adapted_request_stream, adapted_response_stream).await?;

    check_exit_status("airbyte source connector:", child.wait().await)
}

fn parse_entrypoint(entrypoint: &Vec<String>) -> Result<(String, Vec<String>), Error> {
    if entrypoint.len() == 0 {
        return Err(Error::EmptyEntrypointError);
    }

    return Ok((entrypoint[0].clone(), entrypoint[1..].to_vec()));
}

fn request_stream() -> InterceptorStream {
    Box::pin(ReaderStream::new(tokio::io::stdin()))
}

fn response_stream(child_stdout: ChildStdout) -> InterceptorStream {
    Box::pin(ReaderStream::new(child_stdout))
}

async fn streaming_all(
    mut request_stream_writer: ChildStdin,
    request_stream: InterceptorStream,
    response_stream: InterceptorStream,
) -> Result<(), Error> {
    let mut request_stream_reader = StreamReader::new(request_stream);
    let mut response_stream_reader = StreamReader::new(response_stream);
    let mut response_stream_writer = tokio::io::stdout();

    let request_stream_copy =
        tokio::spawn(
            async move { copy(&mut request_stream_reader, &mut request_stream_writer).await },
        );

    let response_stream_copy = tokio::spawn(async move {
        copy(&mut response_stream_reader, &mut response_stream_writer).await
    });

    let (a, b) = tokio::try_join!(request_stream_copy, response_stream_copy)?;
    let req_stream_bytes = a?;
    let resp_stream_bytes = b?;

    tracing::info!(
        req_stream = req_stream_bytes,
        resp_stream = resp_stream_bytes,
        message = "Done streaming"
    );
    Ok(())
}

#[cfg(test)]
mod test {
    use std::pin::Pin;

    use bytes::Bytes;
    use futures::{stream, StreamExt, TryStream};

    use super::*;

    fn create_stream<T>(
        input: Vec<T>,
    ) -> Pin<Box<impl TryStream<Item = std::io::Result<T>, Ok = T, Error = std::io::Error>>> {
        Box::pin(stream::iter(input.into_iter().map(Ok::<T, std::io::Error>)))
    }

    #[tokio::test]
    async fn test_streaming_all_eof() {
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
}
