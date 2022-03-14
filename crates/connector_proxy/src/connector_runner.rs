use crate::apis::{FlowCaptureOperation, FlowMaterializeOperation, InterceptorStream};
use crate::errors::Error;
use crate::interceptors::{
    airbyte_capture_interceptor::AirbyteCaptureInterceptor,
    network_proxy_capture_interceptor::NetworkProxyCaptureInterceptor,
    network_proxy_materialize_interceptor::NetworkProxyMaterializeInterceptor,
};
use crate::libs::command::{check_exit_status, invoke_connector_direct, invoke_delayed_connector};
use tokio::io::copy;
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio_util::io::{ReaderStream, StreamReader};

pub async fn run_flow_capture_connector(
    op: &FlowCaptureOperation,
    entrypoint: Vec<String>,
) -> Result<(), Error> {
    let (entrypoint, mut args) = parse_entrypoint(&entrypoint)?;
    args.push(op.to_string());

    let (child, child_stdin, child_stdout) = invoke_connector_direct(entrypoint, args)?;

    let adapted_request_stream =
        NetworkProxyCaptureInterceptor::adapt_request_stream(op, request_stream())?;

    let adapted_response_stream =
        NetworkProxyCaptureInterceptor::adapt_response_stream(op, response_stream(child_stdout))?;

    streaming_bidirectional(
        child,
        child_stdin,
        adapted_request_stream,
        adapted_response_stream,
    )
    .await
}

pub async fn run_flow_materialize_connector(
    op: &FlowMaterializeOperation,
    entrypoint: Vec<String>,
) -> Result<(), Error> {
    let (entrypoint, mut args) = parse_entrypoint(&entrypoint)?;
    args.push(op.to_string());

    let (child, child_stdin, child_stdout) = invoke_connector_direct(entrypoint, args)?;

    let adapted_request_stream =
        NetworkProxyMaterializeInterceptor::adapt_request_stream(op, request_stream())?;

    let adapted_response_stream = NetworkProxyMaterializeInterceptor::adapt_response_stream(
        op,
        response_stream(child_stdout),
    )?;

    streaming_bidirectional(
        child,
        child_stdin,
        adapted_request_stream,
        adapted_response_stream,
    )
    .await
}

pub async fn run_airbyte_source_connector(
    op: &FlowCaptureOperation,
    entrypoint: Vec<String>,
) -> Result<(), Error> {
    let mut airbyte_interceptor = AirbyteCaptureInterceptor::new();

    let (entrypoint, args) = parse_entrypoint(&entrypoint)?;
    let args = airbyte_interceptor.adapt_command_args(op, args)?;

    let (child, child_stdin, child_stdout) = invoke_delayed_connector(entrypoint, args).await?;

    let adapted_request_stream = airbyte_interceptor.adapt_request_stream(
        child.id().ok_or(Error::MissingPid)?,
        op,
        NetworkProxyCaptureInterceptor::adapt_request_stream(op, request_stream())?,
    )?;

    let adapted_response_stream = airbyte_interceptor.adapt_response_stream(
        &op,
        NetworkProxyCaptureInterceptor::adapt_response_stream(op, response_stream(child_stdout))?,
    )?;

    streaming_bidirectional(
        child,
        child_stdin,
        adapted_request_stream,
        adapted_response_stream,
    )
    .await
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

async fn streaming_bidirectional(
    mut child: Child,
    mut request_stream_writer: ChildStdin,
    request_stream: InterceptorStream,
    response_stream: InterceptorStream,
) -> Result<(), Error> {
    let mut request_stream_reader = StreamReader::new(request_stream);
    let mut response_stream_reader = StreamReader::new(response_stream);
    let mut response_stream_writer = tokio::io::stdout();

    let (a, b) = tokio::join!(
        copy(&mut request_stream_reader, &mut request_stream_writer),
        copy(&mut response_stream_reader, &mut response_stream_writer)
    );
    a?;
    b?;

    check_exit_status("connector runner:", child.wait().await)
}
