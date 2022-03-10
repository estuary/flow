use crate::apis::{FlowCaptureOperation, Interceptor, InterceptorStream};

use crate::errors::{create_custom_error, Error};
use crate::libs::airbyte_catalog::{
    self, ConfiguredCatalog, ConfiguredStream, Range, ResourceSpec,
};
use crate::libs::command::resume_process;
use crate::libs::json::create_root_schema;
use crate::libs::protobuf::{decode_message, encode_message};
use crate::libs::stream::stream_all_airbyte_messages;

use async_stream::stream;
use bytes::Bytes;
use protocol::capture::{
    discover_response, validate_response, DiscoverRequest, DiscoverResponse, Documents,
    PullRequest, PullResponse, SpecResponse, ValidateRequest, ValidateResponse,
};
use protocol::flow::{DriverCheckpoint, Slice};
use std::collections::HashMap;
use std::sync::Arc;
// TODO: switch to std::sync::Mutex;
use tokio::sync::Mutex;

use futures_util::StreamExt;
use serde_json::value::RawValue;
use std::fs::File;
use std::io::Write;
use tempfile::{Builder, TempDir};
use tokio_util::io::StreamReader;

const CONFIG_FILE_NAME: &str = "config.json";
const CATALOG_FILE_NAME: &str = "catalog.json";
const STATE_FILE_NAME: &str = "state.json";

pub struct AirbyteCaptureInterceptor {
    validate_request: Arc<Mutex<Option<ValidateRequest>>>,
    stream_to_binding: Arc<Mutex<HashMap<String, usize>>>,
    tmp_dir: TempDir,
}

impl AirbyteCaptureInterceptor {
    pub fn new() -> Self {
        // TODO: keep tempdir inside container only, not mounted to any local dir.
        // -- tmpfs mount? https://docs.docker.com/storage/tmpfs/
        AirbyteCaptureInterceptor {
            validate_request: Arc::new(Mutex::new(None)),
            stream_to_binding: Arc::new(Mutex::new(HashMap::new())),
            tmp_dir: Builder::new()
                .prefix("airbyte-capture")
                .tempdir()
                .expect("failed to create temp dir."),
        }
    }

    fn convert_spec_request(
        &mut self,
        pid: u32,
        in_stream: InterceptorStream,
    ) -> InterceptorStream {
        resume_process(pid).unwrap();
        in_stream
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
                Err(e) => {
                    yield Err(create_custom_error(&format!("failed receiving discover airbyte message: {:?}", e)));
                    return
                },
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
        config_file_path: String,
        in_stream: InterceptorStream,
    ) -> InterceptorStream {
        Box::pin(stream! {
            let mut reader = StreamReader::new(in_stream);
            let request = decode_message::<DiscoverRequest, _>(&mut reader).await.expect("expected request is not received.").unwrap();

            write!(File::create(config_file_path).unwrap(), "{}",  request.endpoint_spec_json).unwrap();

            resume_process(pid).unwrap();
            yield Ok(Bytes::from(""));
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
                            Err(e) => {
                                yield Err(create_custom_error(&format!("failed receiving discover airbyte message: {:?}", e)));
                                return;
                            }
                        }
                    }
                };

                let mut resp = DiscoverResponse::default();
                // TODO: check message.log.
                if let Some(catalog) = message.catalog {
                    for stream in catalog.streams {
                        let mode = if stream.supported_sync_modes.contains(&"incremental".to_string()) {"incremental"} else {"full_refresh"};
                        let resource_spec = ResourceSpec {
                            stream: stream.name.clone(),
                            namespace: stream.namespace,
                            sync_mode: mode.into()
                        };
                        let key_ptrs: Vec<String> = Vec::new();  // TODO derive key_prs from stream.source_defined_primary_key
                        resp.bindings.push(discover_response::Binding{
                            recommended_name: stream.name.clone(),
                            resource_spec_json: serde_json::to_string(&resource_spec)?,
                            key_ptrs: key_ptrs,
                            document_schema_json: stream.json_schema.to_string(),
                        })
                    }
                }

                yield encode_message(&resp);
           }
        })
    }

    fn convert_validate_request(
        &mut self,
        pid: u32,
        config_file_path: String,
        validate_request: Arc<Mutex<Option<ValidateRequest>>>,
        in_stream: InterceptorStream,
    ) -> InterceptorStream {
        Box::pin(stream! {
            let mut reader = StreamReader::new(in_stream);
            let request = decode_message::<ValidateRequest, _>(&mut reader).await.expect("expected request is not received.").unwrap();
            *validate_request.lock().await = Some(request.clone());

            write!(File::create(config_file_path).unwrap(), "{}",  request.endpoint_spec_json).unwrap();

            resume_process(pid).unwrap();
            yield Ok(Bytes::from(""));
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
                            Err(e) => {
                                yield Err(create_custom_error(&format!("failed receiving discover airbyte message: {:?}", e)));
                                return
                            }
                        }
                    }
                };

                let connection_status = message.connection_status.expect("failed to fetch expected connection_status object");
                // TODO: fix constants.
                if connection_status.status !=  "SUCCEEDED" {
                    yield Err(create_custom_error(&format!("validation failed {}, {}", connection_status.status, connection_status.message)));
                }

                let req = validate_request.lock().await;
                let req = req.as_ref().expect("unexpectedly missing validate request.");
                let mut resp = ValidateResponse::default();
                for binding in &req.bindings {
                    let resource: ResourceSpec = serde_json::from_str(&binding.resource_spec_json).expect("failed serialize resource");
                    resp.bindings.push(validate_response::Binding {
                                    resource_path: vec![resource.stream]
                    });
                }
                drop(req);
                yield encode_message(&resp);
           }
        })
    }

    fn convert_pull_request(
        &mut self,
        pid: u32,
        config_file_path: String,
        catalog_file_path: String,
        state_file_path: String,
        stream_to_binding: Arc<Mutex<HashMap<String, usize>>>,
        in_stream: InterceptorStream,
    ) -> InterceptorStream {
        Box::pin(stream! {
            let mut reader = StreamReader::new(in_stream);
            let mut request = decode_message::<PullRequest, _>(&mut reader).await.expect("expected request is not received.").unwrap();
            if let Some(ref mut o) = request.open {
                File::create(state_file_path).unwrap().write_all(&o.driver_checkpoint_json).unwrap();

                if let Some(ref mut c) = o.capture {
                    write!(File::create(config_file_path).unwrap(), "{}", c.endpoint_spec_json).unwrap();

                    let mut catalog = ConfiguredCatalog {
                        streams: Vec::new(), // nil???
                        tail: o.tail,
                        range: Range {
                            begin: format!("{:x}", o.key_begin),
                            end: format!("{:x}", o.key_end),
                        }
                    };

                    let mut stream_to_binding = stream_to_binding.lock().await;

                    for (i, binding) in c.bindings.iter().enumerate() {
                        let resource: ResourceSpec = serde_json::from_str(&binding.resource_spec_json).expect("failed serialize resource.");
                        stream_to_binding.insert(resource.stream.clone(), i);

                        let mut projections = HashMap::new();
                        if let Some(ref collection) = binding.collection {
                            for p in &collection.projections {
                                projections.insert(p.field.clone(), p.ptr.clone());
                            }
                            // TODO: extract primary_key from collection.key_ptrs.
                            //       could use docs/src/ptr.rs.
                            catalog.streams.push(ConfiguredStream{
                                sync_mode: resource.sync_mode.clone(),
                                destination_sync_mode: "append".to_string(),
                                cursor_field: None,
                                primary_key: None, // TODO.
                                stream: airbyte_catalog::Stream{
                                    name:  resource.stream,
                                    namespace:          resource.namespace,
                                    json_schema:         RawValue::from_string(collection.schema_json.clone())?,
                                    supported_sync_modes: vec![resource.sync_mode.clone()],
                                    default_cursor_field: None,
                                    source_defined_cursor: None,
                                    source_defined_primary_key: None,
                                },
                                projections: projections,
                            });
                        }
                    }
                    serde_json::to_writer(File::create(catalog_file_path)?, &catalog)?
                }

                // release the lock.
                drop(stream_to_binding);

                // Resume the connector process.
                resume_process(pid).unwrap();

                yield Ok(Bytes::from(""));
            }
        })
    }

    fn convert_pull_response(
        &mut self,
        stream_to_binding: Arc<Mutex<HashMap<String, usize>>>,
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
                            Err(e) => {
                                yield Err(create_custom_error(&format!("failed receiving airbyte pull message: {:?}", e)));
                                return
                            }
                        }
                    }
                };

                let mut resp = PullResponse::default();
                // TODO: check message.log.
                if let Some(state) = message.state {
                    resp.checkpoint = Some(DriverCheckpoint{
                            driver_checkpoint_json: state.data.get().as_bytes().to_vec(),
                            rfc7396_merge_patch: match state.ns_merge {
                                Some(b) => b,
                                None => false, // TODO: figure out the right value.
                            },
                    })
                } else if let Some(record) = message.record {
                    let stream_to_binding = stream_to_binding.lock().await;
                    match stream_to_binding.get(&record.stream) {
                        None => {
                            yield Err(create_custom_error(&format!("connector record with unknown stream {}", record.stream)));
                            return
                        }
                        Some(binding) => {
                            let arena = record.data.get().as_bytes().to_vec();
                            let arena_len: u32 = arena.len() as u32;
                            resp.documents = Some(Documents {
                                binding: *binding as u32,
                                arena: arena,
                                docs_json: vec![Slice{begin: 0, end: arena_len}]
                            })
                        }
                    }
                    drop(stream_to_binding);
                } else {
                    continue
                }
                yield encode_message(&resp);
            }

        })
    }

    fn input_file_path(&mut self, file_name: &str) -> String {
        self.tmp_dir
            .path()
            .join(file_name)
            .to_str()
            .expect("failed construct config file name.")
            .into()
    }
}

impl Interceptor<FlowCaptureOperation> for AirbyteCaptureInterceptor {
    fn convert_command_args(
        &mut self,
        op: &FlowCaptureOperation,
        args: Vec<String>,
    ) -> Result<Vec<String>, Error> {
        let config_file_path = self.input_file_path(CONFIG_FILE_NAME);
        let catalog_file_path = self.input_file_path(CATALOG_FILE_NAME);
        let state_file_path = self.input_file_path(STATE_FILE_NAME);

        let airbyte_args = match op {
            FlowCaptureOperation::Spec => vec!["spec"],
            FlowCaptureOperation::Discover => vec!["discover", "--config", &config_file_path],
            FlowCaptureOperation::Validate => vec!["check", "--config", &config_file_path],
            FlowCaptureOperation::Pull => {
                vec![
                    "read",
                    "--config",
                    &config_file_path,
                    "--catalog",
                    &catalog_file_path,
                    "--state",
                    &state_file_path,
                ]
            }

            _ => return Err(Error::UnexpectedOperation(op.to_string())),
        };

        let airbyte_args: Vec<String> = airbyte_args.into_iter().map(Into::into).collect();
        Ok([airbyte_args, args].concat())
    }

    fn convert_request(
        &mut self,
        pid: Option<u32>,
        op: &FlowCaptureOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        let config_file_path = self.input_file_path(CONFIG_FILE_NAME);
        let catalog_file_path = self.input_file_path(CATALOG_FILE_NAME);
        let state_file_path = self.input_file_path(STATE_FILE_NAME);

        match pid {
            None => Err(Error::MissingPid),
            Some(pid) => match op {
                FlowCaptureOperation::Spec => Ok(self.convert_spec_request(pid, in_stream)),
                FlowCaptureOperation::Discover => {
                    Ok(self.convert_discover_request(pid, config_file_path, in_stream))
                }
                FlowCaptureOperation::Validate => Ok(self.convert_validate_request(
                    pid,
                    config_file_path,
                    Arc::clone(&self.validate_request),
                    in_stream,
                )),
                FlowCaptureOperation::Pull => Ok(self.convert_pull_request(
                    pid,
                    config_file_path,
                    catalog_file_path,
                    state_file_path,
                    Arc::clone(&self.stream_to_binding),
                    in_stream,
                )),

                _ => Err(Error::UnexpectedOperation(op.to_string())),
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
            FlowCaptureOperation::Pull => {
                Ok(self.convert_pull_response(Arc::clone(&self.stream_to_binding), in_stream))
            }
            _ => Err(Error::UnexpectedOperation(op.to_string())),
        }
    }
}
