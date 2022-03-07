use crate::apis::{FlowCaptureOperation, Interceptor, InterceptorStream};
use crate::errors::Error;
use crate::libs::airbyte_catalog::Message;
use crate::libs::command::resume_process;
use crate::libs::protobuf::{decode_message, encode_message};
use crate::libs::stream::stream_all_airbyte_messages;

use async_stream::stream;
use protocol::capture::{
    ApplyRequest, DiscoverRequest, DiscoverResponse, PullRequest, SpecResponse, ValidateRequest,
};

use futures_util::pin_mut;
use futures_util::StreamExt;
use serde_json::value::RawValue;
use std::fs::File;
use std::io::Write;
use tokio_util::io::StreamReader;

const INPUT_CONFIG_FILE_PATH: &str = "/tmp/config.json";
pub struct AirbyteCaptureInterceptor {}
impl AirbyteCaptureInterceptor {
    fn convert_spec_request(pid: u32, stream: InterceptorStream) -> InterceptorStream {
        resume_process(pid);
        stream
    }

    fn convert_discover_request(pid: u32, in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream! {
            let mut reader = StreamReader::new(in_stream);
            let mut request = decode_message::<DiscoverRequest, _>(&mut reader).await.expect("expected request is not received.").unwrap();

            write!(File::create(INPUT_CONFIG_FILE_PATH).unwrap(), "{}",  request.endpoint_spec_json).unwrap();

            resume_process(pid).unwrap();
            yield Err(std::io::Error::new(std::io::ErrorKind::Other, "Unexpected read to this empty capture discover stream."))
        })
    }

    fn convert_discover_response(in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream! {
            let mut airbyte_message_stream = Box::pin(stream_all_airbyte_messages(in_stream));
            loop {
                let message = match airbyte_message_stream.next().await {
                    None => break,
                    Some(message) => {
                        match message {
                            Ok(m) => m,
                            Err(e) => panic!("failed receiving discover airbyte message: {:?}", e)
                        }
                    }
                };
                let catalog = message.catalog.expect("failed to fetch expected catalog object");
                let resp = DiscoverResponse::default();
                // fill resp with message.
                yield encode_message(&resp);
           }
        })
    }
}

impl Interceptor<FlowCaptureOperation> for AirbyteCaptureInterceptor {
    fn convert_command_args(
        &self,
        op: &FlowCaptureOperation,
        args: Vec<String>,
    ) -> Result<Vec<String>, Error> {
        let mut new_args = args.clone();
        new_args.extend_from_slice(&["--config".to_string(), INPUT_CONFIG_FILE_PATH.to_string()]);
        Ok(new_args)
    }

    fn convert_request(
        &self,
        pid: Option<u32>,
        op: &FlowCaptureOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        match pid {
            None => Err(Error::MissingPid),
            Some(pid) => match op {
                FlowCaptureOperation::Discover => {
                    Ok(Self::convert_discover_request(pid, in_stream))
                }
                _ => Ok(in_stream),
            },
        }
    }

    fn convert_response(
        &self,
        op: &FlowCaptureOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        match op {
            FlowCaptureOperation::Discover => Ok(Self::convert_discover_response(in_stream)),
            _ => Ok(in_stream),
        }
    }
}
