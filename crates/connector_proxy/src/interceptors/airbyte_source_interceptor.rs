use crate::apis::{FlowCaptureOperation, InterceptorStream};

use crate::errors::{create_custom_error, raise_custom_error, Error};
use crate::libs::airbyte_catalog::{
    self, ConfiguredCatalog, ConfiguredStream, DestinationSyncMode, Range, ResourceSpec, Status,
    SyncMode,
};
use crate::libs::command::READY;
use crate::libs::json::{create_root_schema, tokenize_jsonpointer};
use crate::libs::protobuf::{decode_message, encode_message};
use crate::libs::stream::stream_all_airbyte_messages;

use async_stream::try_stream;
use bytes::Bytes;
use protocol::capture::{
    discover_response, validate_response, DiscoverRequest, DiscoverResponse, Documents,
    PullRequest, PullResponse, SpecRequest, SpecResponse, ValidateRequest, ValidateResponse,
};
use protocol::flow::{DriverCheckpoint, Slice};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use validator::Validate;

use futures_util::StreamExt;
use json_pointer::JsonPointer;
use serde_json::value::RawValue;
use std::fs::File;
use std::io::Write;
use tempfile::{Builder, TempDir};
use tokio_util::io::StreamReader;

const CONFIG_FILE_NAME: &str = "config.json";
const CATALOG_FILE_NAME: &str = "catalog.json";
const STATE_FILE_NAME: &str = "state.json";

pub struct AirbyteSourceInterceptor {
    validate_request: Arc<Mutex<Option<ValidateRequest>>>,
    stream_to_binding: Arc<Mutex<HashMap<String, usize>>>,
    tmp_dir: TempDir,
}

impl AirbyteSourceInterceptor {
    pub fn new() -> Self {
        AirbyteSourceInterceptor {
            validate_request: Arc::new(Mutex::new(None)),
            stream_to_binding: Arc::new(Mutex::new(HashMap::new())),
            tmp_dir: Builder::new()
                .prefix("airbyte-source-")
                .tempdir_in("/var/tmp")
                .expect("failed to create temp dir."),
        }
    }

    fn adapt_spec_request_stream(&mut self, in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(try_stream! {
            let mut reader = StreamReader::new(in_stream);
            decode_message::<SpecRequest, _>(&mut reader).await?.ok_or(create_custom_error("missing spec request."))?;

            yield Bytes::from(READY);
        })
    }

    fn adapt_spec_response_stream(&mut self, in_stream: InterceptorStream) -> InterceptorStream {
        Box::pin(try_stream! {
            let mut airbyte_message_stream = Box::pin(stream_all_airbyte_messages(in_stream));
            loop {
                let message = match airbyte_message_stream.next().await {
                    None => break,
                    Some(message) => message?
                };
                if let Some(spec) = message.spec {
                    let mut resp = SpecResponse::default();
                    resp.endpoint_spec_schema_json = spec.connection_specification.to_string();
                    resp.resource_spec_schema_json = serde_json::to_string_pretty(&create_root_schema::<ResourceSpec>())?;
                    if let Some(url) = spec.documentation_url {
                        resp.documentation_url = url;
                    }
                    yield encode_message(&resp)?;
                } else if let Some(mlog) = message.log {
                    mlog.log();
                } else {
                    raise_custom_error("unexpected spec response.")?;
                }
            }
        })
    }

    fn adapt_discover_request(
        &mut self,
        config_file_path: String,
        in_stream: InterceptorStream,
    ) -> InterceptorStream {
        Box::pin(try_stream! {
            let mut reader = StreamReader::new(in_stream);
            let request = decode_message::<DiscoverRequest, _>(&mut reader).await?.ok_or(create_custom_error("missing discover request."))?;

            File::create(config_file_path)?.write_all(request.endpoint_spec_json.as_bytes())?;

            yield Bytes::from(READY);
        })
    }

    fn adapt_discover_response_stream(
        &mut self,
        in_stream: InterceptorStream,
    ) -> InterceptorStream {
        Box::pin(try_stream! {
            let mut airbyte_message_stream = Box::pin(stream_all_airbyte_messages(in_stream));
            loop {
                let message = match airbyte_message_stream.next().await {
                    None => break,
                    Some(message) => message?
                };

                if let Some(catalog) = message.catalog {
                    let mut resp = DiscoverResponse::default();
                    for stream in catalog.streams {
                        let mode = if stream.supported_sync_modes.map(|modes| modes.contains(&SyncMode::Incremental)).unwrap_or(false) {SyncMode::Incremental} else {SyncMode::FullRefresh};
                        let resource_spec = ResourceSpec {
                            stream: stream.name.clone(),
                            namespace: stream.namespace,
                            sync_mode: mode
                        };

                        let key_ptrs = match stream.source_defined_primary_key {
                            None => Vec::new(),
                            Some(keys) => keys.iter().map(|k| JsonPointer::new(k).to_string()).collect()
                        };
                        resp.bindings.push(discover_response::Binding{
                            recommended_name: stream.name.clone(),
                            resource_spec_json: serde_json::to_string(&resource_spec)?,
                            key_ptrs: key_ptrs,
                            document_schema_json: stream.json_schema.to_string(),
                        })
                    }

                    yield encode_message(&resp)?;
                } else if let Some(mlog) = message.log {
                    mlog.log();
                } else {
                    raise_custom_error("unexpected discover response.")?;
                }
           }
        })
    }

    fn adapt_validate_request_stream(
        &mut self,
        config_file_path: String,
        validate_request: Arc<Mutex<Option<ValidateRequest>>>,
        in_stream: InterceptorStream,
    ) -> InterceptorStream {
        Box::pin(try_stream! {
            let mut reader = StreamReader::new(in_stream);
            let request = decode_message::<ValidateRequest, _>(&mut reader).await?.ok_or(create_custom_error("missing validate request"))?;
            *validate_request.lock().await = Some(request.clone());

            File::create(config_file_path)?.write_all(request.endpoint_spec_json.as_bytes())?;

            yield Bytes::from(READY);
        })
    }

    fn adapt_validate_response_stream(
        &mut self,
        validate_request: Arc<Mutex<Option<ValidateRequest>>>,
        in_stream: InterceptorStream,
    ) -> InterceptorStream {
        Box::pin(try_stream! {
            let mut airbyte_message_stream = Box::pin(stream_all_airbyte_messages(in_stream));
            loop {
                let message = match airbyte_message_stream.next().await {
                    None => break,
                    Some(message) => message?
                };

                if let Some(connection_status) = message.connection_status {
                    if connection_status.status !=  Status::Succeeded {
                        raise_custom_error(&format!("validation failed {:?}", connection_status))?;
                    }

                    let req = validate_request.lock().await;
                    let req = req.as_ref().ok_or(create_custom_error("missing validate request."))?;
                    let mut resp = ValidateResponse::default();
                    for binding in &req.bindings {
                        let resource: ResourceSpec = serde_json::from_str(&binding.resource_spec_json)?;
                        resp.bindings.push(validate_response::Binding {resource_path: vec![resource.stream]});
                    }
                    drop(req);
                    yield encode_message(&resp)?;
                } else if let Some(mlog) = message.log {
                    mlog.log();
                } else {
                    raise_custom_error("unexpected validate response.")?;
                }
           }
        })
    }

    fn adapt_pull_request_stream(
        &mut self,
        config_file_path: String,
        catalog_file_path: String,
        state_file_path: String,
        stream_to_binding: Arc<Mutex<HashMap<String, usize>>>,
        in_stream: InterceptorStream,
    ) -> InterceptorStream {
        Box::pin(try_stream! {
            let mut reader = StreamReader::new(in_stream);
            let mut request = decode_message::<PullRequest, _>(&mut reader).await?.ok_or(create_custom_error("missing pull request"))?;
            if let Some(ref mut o) = request.open {
                File::create(state_file_path)?.write_all(&o.driver_checkpoint_json)?;

                if let Some(ref mut c) = o.capture {
                    File::create(config_file_path)?.write_all(&c.endpoint_spec_json.as_bytes())?;

                    let mut catalog = ConfiguredCatalog {
                        streams: Vec::new(),
                        tail: o.tail,
                        range: Range { begin: o.key_begin, end: o.key_end }
                    };

                    let mut stream_to_binding = stream_to_binding.lock().await;

                    for (i, binding) in c.bindings.iter().enumerate() {
                        let resource: ResourceSpec = serde_json::from_str(&binding.resource_spec_json)?;
                        stream_to_binding.insert(resource.stream.clone(), i);

                        let mut projections = HashMap::new();
                        if let Some(ref collection) = binding.collection {
                            for p in &collection.projections {
                                projections.insert(p.field.clone(), p.ptr.clone());
                            }

                            let primary_key: Vec<Vec<String>> = collection.key_ptrs.iter().map(|ptr| tokenize_jsonpointer(ptr)).collect();
                            catalog.streams.push(ConfiguredStream{
                                sync_mode: resource.sync_mode.clone(),
                                destination_sync_mode: DestinationSyncMode::Append,
                                cursor_field: None,
                                primary_key: Some(primary_key),
                                stream: airbyte_catalog::Stream{
                                    name:  resource.stream,
                                    namespace:          resource.namespace,
                                    json_schema:         RawValue::from_string(collection.schema_json.clone())?,
                                    supported_sync_modes: Some(vec![resource.sync_mode.clone()]),
                                    default_cursor_field: None,
                                    source_defined_cursor: None,
                                    source_defined_primary_key: None,
                                },
                                projections: projections,
                            });
                        }
                    }

                    if let Err(e) = catalog.validate() {
                        raise_custom_error(&format!("invalid config_catalog: {:?}", e))?
                    }

                    serde_json::to_writer(File::create(catalog_file_path)?, &catalog)?
                }

                // release the lock.
                drop(stream_to_binding);

                yield Bytes::from(READY);
            }
        })
    }

    fn adapt_pull_response_stream(
        &mut self,
        stream_to_binding: Arc<Mutex<HashMap<String, usize>>>,
        in_stream: InterceptorStream,
    ) -> InterceptorStream {
        Box::pin(try_stream! {
            let mut airbyte_message_stream = Box::pin(stream_all_airbyte_messages(in_stream));
            // transaction_pending is true if the connector writes output messages and exits _without_ writing
            // a final state checkpoint.
            let mut transaction_pending = false;

            loop {
                let message = match airbyte_message_stream.next().await {
                    None => break,
                    Some(message) => message?
                };

                let mut resp = PullResponse::default();
                if let Some(state) = message.state {
                    resp.checkpoint = Some(DriverCheckpoint{
                            driver_checkpoint_json: state.data.get().as_bytes().to_vec(),
                            rfc7396_merge_patch: match state.merge {
                                Some(m) => m,
                                None => false,
                            },
                    });

                    yield encode_message(&resp)?;
                    transaction_pending = false;
                } else if let Some(record) = message.record {
                    let stream_to_binding = stream_to_binding.lock().await;
                    match stream_to_binding.get(&record.stream) {
                        None => {
                            raise_custom_error(&format!("connector record with unknown stream {}", record.stream))?;
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
                    yield encode_message(&resp)?;
                    transaction_pending = true;
                } else if let Some(mlog) = message.log {
                    mlog.log();
                } else {
                    raise_custom_error("unexpected pull response.")?;
                }
            }

            if transaction_pending {
                // We generate a synthetic commit now, and the empty checkpoint means the assumed behavior
                // of the next invocation will be "full refresh".
                let mut resp = PullResponse::default();
                resp.checkpoint = Some(DriverCheckpoint{
                    driver_checkpoint_json: Vec::new(),
                    rfc7396_merge_patch: false
                });
                yield encode_message(&resp)?;
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

impl AirbyteSourceInterceptor {
    pub fn adapt_command_args(
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
        Ok([args, airbyte_args].concat())
    }

    pub fn adapt_request_stream(
        &mut self,
        op: &FlowCaptureOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        let config_file_path = self.input_file_path(CONFIG_FILE_NAME);
        let catalog_file_path = self.input_file_path(CATALOG_FILE_NAME);
        let state_file_path = self.input_file_path(STATE_FILE_NAME);

        match op {
            FlowCaptureOperation::Spec => Ok(self.adapt_spec_request_stream(in_stream)),
            FlowCaptureOperation::Discover => {
                Ok(self.adapt_discover_request(config_file_path, in_stream))
            }
            FlowCaptureOperation::Validate => Ok(self.adapt_validate_request_stream(
                config_file_path,
                Arc::clone(&self.validate_request),
                in_stream,
            )),
            FlowCaptureOperation::Pull => Ok(self.adapt_pull_request_stream(
                config_file_path,
                catalog_file_path,
                state_file_path,
                Arc::clone(&self.stream_to_binding),
                in_stream,
            )),

            _ => Err(Error::UnexpectedOperation(op.to_string())),
        }
    }

    pub fn adapt_response_stream(
        &mut self,
        op: &FlowCaptureOperation,
        in_stream: InterceptorStream,
    ) -> Result<InterceptorStream, Error> {
        match op {
            FlowCaptureOperation::Spec => Ok(self.adapt_spec_response_stream(in_stream)),
            FlowCaptureOperation::Discover => Ok(self.adapt_discover_response_stream(in_stream)),
            FlowCaptureOperation::Validate => {
                Ok(self
                    .adapt_validate_response_stream(Arc::clone(&self.validate_request), in_stream))
            }
            FlowCaptureOperation::Pull => {
                Ok(self.adapt_pull_response_stream(Arc::clone(&self.stream_to_binding), in_stream))
            }
            _ => Err(Error::UnexpectedOperation(op.to_string())),
        }
    }
}
