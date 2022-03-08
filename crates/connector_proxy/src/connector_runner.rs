use crate::apis::{FlowOperation, Interceptor};
use crate::errors::Error;
use crate::libs::command::{check_exit_status, invoke_connector_wrapper};
use tokio::io::copy;
use tokio_util::io::{ReaderStream, StreamReader};

pub async fn run_connector<T: std::fmt::Display + FlowOperation>(
    operation: T,
    entrypoint: Vec<String>,
    mut intercepter: Box<dyn Interceptor<T>>,
) -> Result<(), Error> {
    // prepare entrypoint and args.
    if entrypoint.len() == 0 {
        return Err(Error::EmptyEntrypointError);
    }
    let args = intercepter.convert_command_args(&operation, (&entrypoint[1..]).to_vec())?;

    let entrypoint = entrypoint[0].clone();

    // invoke the connector and converts the request/response streams.
    let mut child = invoke_connector_wrapper(entrypoint, args)?;

    // Perform conversions on requests and responses and starts bi-directional copying.
    let mut request_source = StreamReader::new(intercepter.convert_request(
        child.id(),
        &operation,
        Box::pin(ReaderStream::new(tokio::io::stdin())),
    )?);
    let mut request_destination = child.stdin.take().ok_or(Error::MissingIOPipe)?;

    let response_stream_out = child.stdout.take().ok_or(Error::MissingIOPipe)?;
    let mut response_source = StreamReader::new(
        intercepter
            .convert_response(&operation, Box::pin(ReaderStream::new(response_stream_out)))?,
    );
    let mut response_destination = tokio::io::stdout();

    let (a, b) = tokio::join!(
        copy(&mut request_source, &mut request_destination),
        copy(&mut response_source, &mut response_destination)
    );
    a?;
    b?;

    check_exit_status("connector runner:", child.wait().await)
}
