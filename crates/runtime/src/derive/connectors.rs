use futures::Stream;
use proto_flow::derive::{Request, Response};
use proto_flow::ops;
use std::path::Path;

pub fn typescript_connector<L, R>(
    _peek_request: &Request,
    log_handler: L,
    request_rx: R,
) -> tonic::Result<impl Stream<Item = tonic::Result<Response>>>
where
    L: Fn(ops::Log) + Send + Sync + 'static,
    R: futures::stream::Stream<Item = tonic::Result<Request>> + Send + 'static,
{
    // Look for `flowctl` alongside the current running program (or perhaps it *is* the
    // current running program).
    let this_program = std::env::args().next().unwrap();
    let mut flowctl = Path::new(&this_program).parent().unwrap().join("flowctl");

    // Fall back to the $PATH.
    if !flowctl.exists() {
        flowctl = "flowctl".into();
    } else {
        // If the executable does exist, then we need to pass it as an absolute path,
        // because the `Command` does not handle relative paths.
        flowctl = flowctl.canonicalize().unwrap();
    }
    let cmd = connector_init::rpc::new_command(&[flowctl.to_str().unwrap(), "raw", "deno-derive"]);

    let response_rx = connector_init::rpc::bidi::<Request, Response, _, _>(
        cmd,
        connector_init::Codec::Json,
        request_rx,
        log_handler,
    )?;

    Ok(response_rx)
}

/*
pub const DERIVE_DENO_PATH: &[&str] = &[
    "bash",
    "-c",
    "tee deno.input | flowctl raw deno-derive | tee deno.output",
];
*/
