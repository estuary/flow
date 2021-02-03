use crate::setup_env_tracing;

use models::tables;
use prost::Message;
use protocol::{cgo, flow};
use url::Url;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("parsing URL: {0:?}")]
    Url(#[from] url::ParseError),
    #[error("schema index: {0}")]
    SchemaIndex(#[from] json::schema::index::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Protobuf decoding error")]
    ProtoDecode(#[from] prost::DecodeError),
    #[error("Invalid state code {0} for schema API")]
    InvalidState(u32),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

pub struct API {}

impl cgo::Service for API {
    type Error = Error;

    fn create() -> Self {
        setup_env_tracing();
        Self {}
    }

    fn invoke(
        &mut self,
        code: u32,
        data: &[u8],
        arena: &mut Vec<u8>,
        out: &mut Vec<cgo::Out>,
    ) -> Result<(), Self::Error> {
        match code {
            1 => {
                // Parse bundle into SchemaDocs table.
                let flow::SchemaBundle { bundle } = flow::SchemaBundle::decode(data)?;

                let mut table = tables::SchemaDocs::new();
                for (uri, dom) in bundle {
                    let dom: serde_json::Value = serde_json::from_str(&dom)?;
                    table.push_row(Url::parse(&uri)?, dom);
                }
                let index = tables::SchemaDoc::leak_index(&table)?;

                // Send an encoding of the |index| memory address.
                let cfg = &flow::schema_api::BuiltIndex {
                    schema_index_memptr: index as *const doc::SchemaIndex<'static> as u64,
                };
                cgo::send_message(1, cfg, arena, out);

                Ok(())
            }
            _ => return Err(Error::InvalidState(code)),
        }
    }
}

#[cfg(test)]
pub mod test {
    use super::API;
    use prost::Message;
    use protocol::{cgo::Service, flow};

    #[test]
    fn test_schema_api() {
        let mut svc = API::create();

        let mut arena = Vec::new();
        let mut out = Vec::new();

        svc.invoke_message(
            1,
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
