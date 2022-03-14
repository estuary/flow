use crate::apis::{FlowCaptureOperation, FlowMaterializeOperation, InterceptorStream};
use crate::errors::Error;
use crate::interceptors::{
    airbyte_source_interceptor::AirbyteSourceInterceptor,
    network_proxy_capture_interceptor::NetworkProxyCaptureInterceptor,
    network_proxy_materialize_interceptor::NetworkProxyMaterializeInterceptor,
};
use crate::libs::command::{check_exit_status, invoke_connector_direct, invoke_delayed_connector};
use tokio::io::copy;
use tokio::process::{ChildStderr, ChildStdin, ChildStdout};
use tokio_util::io::{ReaderStream, StreamReader};

pub async fn run_flow_capture_connector(
    op: &FlowCaptureOperation,
    entrypoint: Vec<String>,
) -> Result<(), Error> {
    let (entrypoint, mut args) = parse_entrypoint(&entrypoint)?;
    args.push(op.to_string());

    let (mut child, child_stdin, child_stdout, child_stderr) =
        invoke_connector_direct(entrypoint, args)?;

    let adapted_request_stream =
        NetworkProxyCaptureInterceptor::adapt_request_stream(op, request_stream())?;

    let adapted_response_stream =
        NetworkProxyCaptureInterceptor::adapt_response_stream(op, response_stream(child_stdout))?;

    streaming_all(
        child_stdin,
        child_stderr,
        adapted_request_stream,
        adapted_response_stream,
    )
    .await?;

    check_exit_status("flow capture connector:", child.wait().await)
}

pub async fn run_flow_materialize_connector(
    op: &FlowMaterializeOperation,
    entrypoint: Vec<String>,
) -> Result<(), Error> {
    let (entrypoint, mut args) = parse_entrypoint(&entrypoint)?;
    args.push(op.to_string());

    let (mut child, child_stdin, child_stdout, child_stderr) =
        invoke_connector_direct(entrypoint, args)?;

    let adapted_request_stream =
        NetworkProxyMaterializeInterceptor::adapt_request_stream(op, request_stream())?;

    let adapted_response_stream = NetworkProxyMaterializeInterceptor::adapt_response_stream(
        op,
        response_stream(child_stdout),
    )?;

    streaming_all(
        child_stdin,
        child_stderr,
        adapted_request_stream,
        adapted_response_stream,
    )
    .await?;

    check_exit_status("flow materialize connector:", child.wait().await)
}

pub async fn run_airbyte_source_connector(
    op: &FlowCaptureOperation,
    entrypoint: Vec<String>,
) -> Result<(), Error> {
    let mut airbyte_interceptor = AirbyteSourceInterceptor::new();

    let (entrypoint, args) = parse_entrypoint(&entrypoint)?;
    let args = airbyte_interceptor.adapt_command_args(op, args)?;

    let (mut child, child_stdin, child_stdout, child_stderr) =
        invoke_delayed_connector(entrypoint, args).await?;

    let adapted_request_stream = airbyte_interceptor.adapt_request_stream(
        child.id().ok_or(Error::MissingPid)?,
        op,
        NetworkProxyCaptureInterceptor::adapt_request_stream(op, request_stream())?,
    )?;

    let adapted_response_stream = airbyte_interceptor.adapt_response_stream(
        &op,
        NetworkProxyCaptureInterceptor::adapt_response_stream(op, response_stream(child_stdout))?,
    )?;

    streaming_all(
        child_stdin,
        child_stderr,
        adapted_request_stream,
        adapted_response_stream,
    )
    .await?;

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
    mut error_reader: ChildStderr,
    request_stream: InterceptorStream,
    response_stream: InterceptorStream,
) -> Result<(), Error> {
    let mut request_stream_reader = StreamReader::new(request_stream);
    let mut response_stream_reader = StreamReader::new(response_stream);
    let mut response_stream_writer = tokio::io::stdout();
    let mut error_writer = tokio::io::stderr();

    let (a, b, c) = tokio::join!(
        copy(&mut request_stream_reader, &mut request_stream_writer),
        copy(&mut response_stream_reader, &mut response_stream_writer),
        copy(&mut error_reader, &mut error_writer),
    );
    a?;
    b?;
    c?;
    Ok(())
}
