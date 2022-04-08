use crate::JsonError;
use prost::Message;
use proto_flow::flow::{self, schema_api::Code};
use url::Url;

#[derive(thiserror::Error, Debug, serde::Serialize)]
pub enum Error {
    #[error("parsing URL: {0:?}")]
    #[serde(serialize_with = "crate::serialize_as_display")]
    Url(#[from] url::ParseError),
    #[error("schema index: {0}")]
    SchemaIndex(#[from] json::schema::index::Error),
    #[error(transparent)]
    Json(#[from] JsonError),
    #[error("Protobuf decoding error")]
    #[serde(serialize_with = "crate::serialize_as_display")]
    ProtoDecode(#[from] prost::DecodeError),
    #[error("protocol error (invalid state or invocation)")]
    InvalidState,
    #[error(transparent)]
    #[serde(serialize_with = "crate::serialize_as_display")]
    Anyhow(#[from] anyhow::Error),
}

pub struct API {}

impl cgo::Service for API {
    type Error = Error;

    fn create() -> Self {
        Self {}
    }

    fn invoke(
        &mut self,
        code: u32,
        data: &[u8],
        arena: &mut Vec<u8>,
        out: &mut Vec<cgo::Out>,
    ) -> Result<(), Self::Error> {
        let code = match Code::from_i32(code as i32) {
            Some(c) => c,
            None => return Err(Error::InvalidState),
        };
        tracing::trace!(?code, "invoke");

        match code {
            Code::BuildIndex => {
                // Parse bundle into SchemaDocs table.
                let flow::SchemaBundle { bundle } = flow::SchemaBundle::decode(data)?;

                let mut table = tables::SchemaDocs::new();
                for (uri, dom) in bundle {
                    let dom: serde_json::Value =
                        serde_json::from_str(&dom).map_err(|e| JsonError::new(dom, e))?;
                    table.insert_row(Url::parse(&uri)?, dom);
                }
                let index = tables::SchemaDoc::leak_index(&table)?;

                // Send an encoding of the |index| memory address.
                let cfg = &flow::schema_api::BuiltIndex {
                    schema_index_memptr: index as *const doc::SchemaIndex<'static> as u64,
                };
                cgo::send_message(1, cfg, arena, out);

                Ok(())
            }
            _ => return Err(Error::InvalidState),
        }
    }
}

#[cfg(test)]
pub mod test {
    use super::API;
    use cgo::Service;
    use prost::Message;
    use proto_flow::flow::{self, schema_api::Code};

    #[test]
    fn test_schema_api() {
        let mut svc = API::create();

        let mut arena = Vec::new();
        let mut out = Vec::new();

        svc.invoke_message(
            Code::BuildIndex as u32,
            flow::SchemaBundle {
                bundle: vec![("https://example".to_string(), "true".to_string())]
                    .into_iter()
                    .collect(),
            },
            &mut arena,
            &mut out,
        )
        .unwrap();

        assert_eq!(out[0].code, 1);
        flow::schema_api::BuiltIndex::decode(arena.as_slice()).unwrap();
    }
}
