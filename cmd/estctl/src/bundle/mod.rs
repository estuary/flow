use super::specs;
use estuary_json::schema::{self};
use serde_json;
use std::collections::BTreeMap;
use url;

pub mod loader;

pub struct Bundle {
    pub root: url::Url,

    pub collections: BTreeMap<url::Url, specs::Collection>,
    pub materializations: BTreeMap<url::Url, specs::Materialization>,
    pub schemas: BTreeMap<url::Url, Box<Schema>>,
}

pub struct Schema {
    raw: serde_json::Value,
    parsed: schema::Schema<specs::Annotation>,
}

impl Schema {
    fn new(url: url::Url, raw: serde_json::Value) -> Result<Schema, schema::BuildError> {
        let parsed: schema::Schema<specs::Annotation> = schema::build::build_schema(url, &raw)?;

        Ok(Schema {
            raw: raw,
            parsed: parsed,
        })
    }
}
