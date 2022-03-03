use crate::apis::RequestResponseConverterPair;
use crate::errors::Error;
use crate::libs::command::{check_exit_status, invoke_connector};
use tokio::io::copy;
use tokio_util::io::{ReaderStream, StreamReader};

pub async fn run_connector<T: std::fmt::Display>(
    operation: T,
    entrypoint: Vec<String>,
    converter_pair: RequestResponseConverterPair<T>,
) -> Result<(), Error> {
    // prepare entrypoint and args.
    if entrypoint.len() == 0 {
        return Err(Error::EmptyEntrypointError);
    }
    let mut args = Vec::new();
    args.extend_from_slice(&entrypoint[1..]);
    args.push(operation.to_string());

    let entrypoint = entrypoint[0].clone();

    // invoke the connector and converts the request/response streams.
    let mut child = invoke_connector(entrypoint, &args)?;

    let (request_converter, response_converter) = converter_pair;
    // Perform conversions on requests and responses and starts bi-directional copying.
    let mut request_source = StreamReader::new((request_converter)(
        &operation,
        Box::pin(ReaderStream::new(tokio::io::stdin())),
    )?);
    let mut request_destination = child.stdin.take().ok_or(Error::MissingIOPipe)?;

    let response_stream_out = child.stdout.take().ok_or(Error::MissingIOPipe)?;
    let mut response_source = StreamReader::new((response_converter)(
        &operation,
        Box::pin(ReaderStream::new(response_stream_out)),
    )?);
    let mut response_destination = tokio::io::stdout();

    let (a, b) = tokio::join!(
        copy(&mut request_source, &mut request_destination),
        copy(&mut response_source, &mut response_destination)
    );
    a?;
    b?;

    check_exit_status(child.wait().await)
}
