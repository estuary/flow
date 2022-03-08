use crate::apis::{FlowCaptureOperation, Interceptor, InterceptorStream};
use crate::errors::Error;
use crate::libs::airbyte_catalog::{Message, ResourceSpec};
use crate::libs::command::resume_process;
use crate::libs::json::create_root_schema;
use crate::libs::protobuf::{decode_message, encode_message};
use crate::libs::stream::{stream_all_airbyte_messages, stream_all_bytes};

use async_stream::stream;
use protocol::capture::{
    validate_response, ApplyRequest, ApplyResponse, DiscoverRequest, DiscoverResponse, PullRequest,
    SpecResponse, ValidateRequest, ValidateResponse,
};
use std::sync::Arc;
use tokio::sync::Mutex;

use futures_util::pin_mut;
use futures_util::StreamExt;
use serde_json::value::RawValue;
use std::fs::File;
use std::io::Write;
use tokio_util::io::StreamReader;

const INPUT_CONFIG_FILE_PATH: &str = "/tmp/config.json";
pub struct AirbyteCaptureInterceptor {
    validate_request: Arc<Mutex<Option<ValidateRequest>>>,
}
impl AirbyteCaptureInterceptor {
    pub fn new() -> Self {
        AirbyteCaptureInterceptor {
            validate_request: Arc::new(Mutex::new(None)),
        }
    }

    fn convert_spec_request(&mut self, pid: u32, stream: InterceptorStream) -> InterceptorStream {
        resume_process(pid);
        stream
    }

    fn convert_spec_response(&mut self, in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(stream! {
            let mut airbyte_message_stream = Box::pin(stream_all_airbyte_messages(in_stream));
            let message = match airbyte_message_stream
                .next()
                .await
                .expect("missing expected spec response.")
            {
                Ok(m) => m,
                Err(e) => panic!("failed receiving discover airbyte message: {:?}", e),
            };
            let spec = message.spec.expect("failed to fetch expected spec object");
            let mut resp = SpecResponse::default();
            resp.endpoint_spec_schema_json = spec.connection_specification.to_string();
            resp.resource_spec_schema_json = serde_json::to_string_pretty(&create_root_schema::<ResourceSpec>()).expect("unexpected failure in encoding ResourceSpec schema.");
            if let Some(url) = spec.documentation_url {
                resp.documentation_url = url;
            }

            yield encode_message(&resp);
        })
    }

    fn convert_discover_request(
        &mut self,
        pid: u32,
        in_stream: InterceptorStream,
    ) -> InterceptorStream {
        Box::pin(stream! {
            let mut reader = StreamReader::new(in_stream);
            let mut request = decode_message::<DiscoverRequest, _>(&mut reader).await.expect("expected request is not received.").unwrap();

            write!(File::create(INPUT_CONFIG_FILE_PATH).unwrap(), "{}",  request.endpoint_spec_json).unwrap();

            resume_process(pid).unwrap();
            yield Err(std::io::Error::new(std::io::ErrorKind::Other, "Unexpected read to this empty capture discover stream."))
        })
    }
    fn convert_discover_response(&mut self, in_stream: InterceptorStream) -> InterceptorStream {
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

    fn convert_validate_request(
        &mut self,
        pid: u32,
        validate_request: Arc<Mutex<Option<ValidateRequest>>>,
        in_stream: InterceptorStream,
    ) -> InterceptorStream {
        Box::pin(stream! {
            let mut reader = StreamReader::new(in_stream);
            let mut request = decode_message::<ValidateRequest, _>(&mut reader).await.expect("expected request is not received.").unwrap();
            *validate_request.lock().await = Some(request.clone());

            write!(File::create(INPUT_CONFIG_FILE_PATH).unwrap(), "{}",  request.endpoint_spec_json).unwrap();

            resume_process(pid).unwrap();
            //yield Err(std::io::Error::new(std::io::ErrorKind::Other, "Unexpected read to this empty capture validate stream."))
            //yield Err(std::io::Error::new(std::io::ErrorKind::Other, "Unexpected read to this empty capture validate stream."))

            // TODO: simplify this logic. It exists only to satisfy the stream type definition.
            let s = stream_all_bytes(reader);
            pin_mut!(s);
            while let Some(value) = s.next().await {
                yield value;
            }

        })
    }

    fn convert_validate_response(
        &mut self,
        validate_request: Arc<Mutex<Option<ValidateRequest>>>,
        in_stream: InterceptorStream,
    ) -> InterceptorStream {
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
                let connection_status = message.connection_status.expect("failed to fetch expected connection_status object");
                // TODO: fix constants.
                if connection_status.status !=  "SUCCEEDED" {
                    panic!("validation failed {}, {}", connection_status.status, connection_status.message);
                }

                let req = validate_request.lock().await;
                let req = req.as_ref().expect("unexpectedly missing validate request.");
                let mut resp = ValidateResponse::default();
                for binding in &req.bindings {
                    let stream: ResourceSpec = serde_json::from_str(&binding.resource_spec_json).expect("failed serialize resource");
                    resp.bindings.push(validate_response::Binding {
                                    resource_path: vec![stream.stream]
                    });
                }
                yield encode_message(&resp);
           }
        })
    }
}

impl Interceptor<FlowCaptureOperation> for AirbyteCaptureInterceptor {
    fn convert_command_args(
        &mut self,
        op: &FlowCaptureOperation,
        args: Vec<String>,
    ) -> Result<Vec<String>, Error> {
        let op_arg = match op {
            FlowCaptureOperation::Spec => "spec",
            FlowCaptureOperation::Discover => "discover",
            FlowCaptureOperation::Validate => "check",
            FlowCaptureOperation::Pull => "read",
            _ => return Err(Error::UnexpectedOperation(op.to_string())),
        };
        Ok([
            vec![op_arg.to_string()],
            args,
            vec!["--config".to_string(), INPUT_CONFIG_FILE_PATH.to_string()],
        ]
        .concat())
    }

    fn convert_request(
        &mut self,
        pid: Option<u32>,
        op: &FlowCaptureOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        match pid {
            None => Err(Error::MissingPid),
            Some(pid) => match op {
                FlowCaptureOperation::Spec => Ok(self.convert_spec_request(pid, in_stream)),
                FlowCaptureOperation::Discover => Ok(self.convert_discover_request(pid, in_stream)),
                FlowCaptureOperation::Validate => Ok(self.convert_validate_request(
                    pid,
                    Arc::clone(&self.validate_request),
                    in_stream,
                )),
                FlowCaptureOperation::ApplyUpsert | FlowCaptureOperation::ApplyDelete => {
                    Err(Error::UnexpectedOperation(op.to_string()))
                }

                _ => Ok(in_stream),
            },
        }
    }

    fn convert_response(
        &mut self,
        op: &FlowCaptureOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        match op {
            FlowCaptureOperation::Spec => Ok(self.convert_spec_response(in_stream)),
            FlowCaptureOperation::Discover => Ok(self.convert_discover_response(in_stream)),
            FlowCaptureOperation::Validate => {
                Ok(self.convert_validate_response(Arc::clone(&self.validate_request), in_stream))
            }
            FlowCaptureOperation::ApplyUpsert | FlowCaptureOperation::ApplyDelete => {
                Err(Error::UnexpectedOperation(op.to_string()))
            }

            _ => Ok(in_stream),
        }
    }
}
