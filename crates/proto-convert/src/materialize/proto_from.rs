use crate::{Convert, FromMessage};
use anyhow::Context;
use connector_protocol::materialize::{Constraint, Response, ValidatedBinding};
use proto_flow::{
    flow,
    materialize::{self, transaction_response, validate_response},
};

impl FromMessage for materialize::SpecResponse {
    type Message = Response;
    fn from_message(msg: Self::Message, out: &mut Vec<Self>) -> anyhow::Result<()> {
        let Response::Spec {
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

impl FromMessage for materialize::ValidateResponse {
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

impl FromMessage for materialize::ApplyResponse {
    type Message = Response;
    fn from_message(msg: Self::Message, out: &mut Vec<Self>) -> anyhow::Result<()> {
        let Response::Applied { action_description } = msg else {
            anyhow::bail!("expected applied, not: {}", serde_json::to_string_pretty(&msg).unwrap());
        };

        Ok(out.push(Self { action_description }))
    }
}

impl FromMessage for materialize::TransactionResponse {
    type Message = Response;
    fn from_message(msg: Self::Message, out: &mut Vec<Self>) -> anyhow::Result<()> {
        match msg {
            Response::Opened { runtime_checkpoint } => {
                let runtime_checkpoint = base64::decode(runtime_checkpoint)
                    .context("decoding runtime checkpoint base64")?;
                out.push(materialize::TransactionResponse {
                    opened: Some(transaction_response::Opened { runtime_checkpoint }),
                    ..Default::default()
                });
            }
            Response::Acknowledged {} => {
                out.push(materialize::TransactionResponse {
                    acknowledged: Some(transaction_response::Acknowledged {}),
                    ..Default::default()
                });
            }
            Response::Loaded { binding, doc } => {
                let loaded = match out.last_mut() {
                    Some(materialize::TransactionResponse {
                        loaded: Some(loaded),
                        ..
                    }) if binding == loaded.binding
                        && loaded.arena.capacity() >= loaded.arena.len() + doc.get().len() =>
                    {
                        loaded
                    }
                    _ => {
                        out.push(materialize::TransactionResponse {
                            loaded: Some(transaction_response::Loaded {
                                binding,
                                arena: Vec::with_capacity(1 << 16), // 64K
                                ..Default::default()
                            }),
                            ..Default::default()
                        });
                        out.last_mut().unwrap().loaded.as_mut().unwrap()
                    }
                };
                let begin = loaded.arena.len() as u32;
                loaded.arena.extend_from_slice(doc.get().as_bytes());
                let end = loaded.arena.len() as u32;
                loaded.docs_json.push(flow::Slice { begin, end });
            }
            Response::Flushed {} => {
                out.push(materialize::TransactionResponse {
                    flushed: Some(transaction_response::Flushed {}),
                    ..Default::default()
                });
            }
            Response::StartedCommit {
                driver_checkpoint,
                merge_patch,
            } => {
                let driver_checkpoint: Box<str> = driver_checkpoint.0.into();

                out.push(materialize::TransactionResponse {
                    started_commit: Some(transaction_response::StartedCommit {
                        driver_checkpoint: Some(flow::DriverCheckpoint {
                            driver_checkpoint_json: driver_checkpoint.into_boxed_bytes().into_vec(),
                            rfc7396_merge_patch: merge_patch,
                        }),
                    }),
                    ..Default::default()
                });
            }
            msg => {
                anyhow::bail!(
                    "expected opened, acknowledged, loaded, flushed, or startedCommit, not: {}",
                    serde_json::to_string_pretty(&msg).unwrap()
                );
            }
        }

        Ok(())
    }
}

impl Convert for ValidatedBinding {
    type Target = validate_response::Binding;
    fn convert(self: Self) -> Self::Target {
        let Self {
            resource_path,
            constraints,
            delta_updates,
        } = self;

        Self::Target {
            resource_path,
            constraints: constraints
                .into_iter()
                .map(|(field, Constraint { r#type, reason })| {
                    (
                        field,
                        materialize::Constraint {
                            r#type: r#type as i32,
                            reason,
                        },
                    )
                })
                .collect(),
            delta_updates,
        }
    }
}
