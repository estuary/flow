use crate::{Convert, FromMessage};
use connector_protocol::{
    capture::{DiscoveredBinding, Response, ValidatedBinding},
    OAuth2,
};
use proto_flow::{
    capture::{self, discover_response, pull_response, validate_response},
    flow,
};

impl FromMessage for capture::SpecResponse {
    type Message = Response;
    fn from_message(msg: Self::Message, out: &mut Vec<Self>) -> anyhow::Result<()> {
        let Response::Spec{
            documentation_url,
            config_schema,
            resource_config_schema,
            oauth2,
        } = msg else {
            anyhow::bail!("expected spec, not: {}", serde_json::to_string_pretty(&msg).unwrap());
        };

        Ok(out.push(Self {
            documentation_url,
            endpoint_spec_schema_json: config_schema.to_string(),
            resource_spec_schema_json: resource_config_schema.to_string(),
            oauth2_spec: oauth2.map(Convert::convert),
        }))
    }
}

impl FromMessage for capture::DiscoverResponse {
    type Message = Response;
    fn from_message(msg: Self::Message, out: &mut Vec<Self>) -> anyhow::Result<()> {
        let Response::Discovered {
            bindings,
        } = msg else {
            anyhow::bail!("expected discovered, not: {}", serde_json::to_string_pretty(&msg).unwrap());
        };

        Ok(out.push(Self {
            bindings: bindings.into_iter().map(Convert::convert).collect(),
        }))
    }
}

impl FromMessage for capture::ValidateResponse {
    type Message = Response;
    fn from_message(msg: Self::Message, out: &mut Vec<Self>) -> anyhow::Result<()> {
        let Response::Validated { bindings } = msg else {
            anyhow::bail!("expected validated, not: {}", serde_json::to_string_pretty(&msg).unwrap());
        };

        Ok(out.push(Self {
            bindings: bindings.into_iter().map(Convert::convert).collect(),
        }))
    }
}

impl FromMessage for capture::ApplyResponse {
    type Message = Response;
    fn from_message(msg: Self::Message, out: &mut Vec<Self>) -> anyhow::Result<()> {
        let Response::Applied { action_description } = msg else {
            anyhow::bail!("expected applied, not: {}", serde_json::to_string_pretty(&msg).unwrap());
        };

        Ok(out.push(Self { action_description }))
    }
}

impl FromMessage for capture::PullResponse {
    type Message = Response;
    fn from_message(msg: Self::Message, out: &mut Vec<Self>) -> anyhow::Result<()> {
        match msg {
            Response::Opened {
                explicit_acknowledgements,
            } => out.push(capture::PullResponse {
                opened: Some(pull_response::Opened {
                    explicit_acknowledgements,
                }),
                ..Default::default()
            }),
            Response::Document { binding, doc } => {
                let documents = match out.last_mut() {
                    Some(capture::PullResponse {
                        documents: Some(documents),
                        ..
                    }) if binding == documents.binding
                        && documents.arena.capacity()
                            >= documents.arena.len() + doc.get().len() =>
                    {
                        documents
                    }
                    _ => {
                        out.push(capture::PullResponse {
                            documents: Some(capture::Documents {
                                binding,
                                arena: Vec::with_capacity(1 << 16), // 64K
                                ..Default::default()
                            }),
                            ..Default::default()
                        });
                        out.last_mut().unwrap().documents.as_mut().unwrap()
                    }
                };
                let begin = documents.arena.len() as u32;
                documents.arena.extend_from_slice(doc.get().as_bytes());
                let end = documents.arena.len() as u32;
                documents.docs_json.push(flow::Slice { begin, end });
            }
            Response::Checkpoint {
                driver_checkpoint,
                merge_patch,
            } => {
                let driver_checkpoint: Box<str> = driver_checkpoint.0.into();

                out.push(capture::PullResponse {
                    checkpoint: Some(flow::DriverCheckpoint {
                        driver_checkpoint_json: driver_checkpoint.into_boxed_bytes().into_vec(),
                        rfc7396_merge_patch: merge_patch,
                    }),
                    ..Default::default()
                });
            }
            msg => {
                anyhow::bail!(
                    "expected opened, document, or checkpoint, not: {}",
                    serde_json::to_string_pretty(&msg).unwrap()
                );
            }
        }

        Ok(())
    }
}

impl Convert for DiscoveredBinding {
    type Target = discover_response::Binding;
    fn convert(self: Self) -> Self::Target {
        let DiscoveredBinding {
            recommended_name,
            resource_config,
            document_schema,
            key: key_ptrs,
        } = self;
        Self::Target {
            recommended_name,
            resource_spec_json: resource_config.to_string(),
            document_schema_json: document_schema.to_string(),
            key_ptrs,
        }
    }
}

impl Convert for ValidatedBinding {
    type Target = validate_response::Binding;
    fn convert(self: Self) -> Self::Target {
        let Self { resource_path } = self;
        Self::Target { resource_path }
    }
}

impl Convert for OAuth2 {
    type Target = flow::OAuth2Spec;
    fn convert(self: Self) -> Self::Target {
        let Self {
            provider,
            auth_url_template,
            access_token_url_template,
            access_token_method,
            access_token_body,
            access_token_headers,
            access_token_response_map,
            refresh_token_url_template,
            refresh_token_method,
            refresh_token_body,
            refresh_token_headers,
            refresh_token_response_map,
        } = self;

        Self::Target {
            provider,
            auth_url_template,
            access_token_url_template,
            access_token_method,
            access_token_body,
            access_token_headers_json: serde_json::to_string(&access_token_headers).unwrap(),
            access_token_response_map_json: serde_json::to_string(&access_token_response_map)
                .unwrap(),
            refresh_token_url_template,
            refresh_token_method,
            refresh_token_body,
            refresh_token_headers_json: serde_json::to_string(&refresh_token_headers).unwrap(),
            refresh_token_response_map_json: serde_json::to_string(&refresh_token_response_map)
                .unwrap(),
        }
    }
}
