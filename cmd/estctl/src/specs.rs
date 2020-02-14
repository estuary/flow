use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path;
use url;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Collection {
    pub name: String,
    pub schema: String,
    #[serde(default)]
    pub examples: String,
    #[serde(default)]
    pub partitions: BTreeMap<String, String>,
    #[serde(default)]
    pub group_by: Vec<String>,
    #[serde(default)]
    pub derive_from: Vec<Transform>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Transform {
    pub source: String,
    #[serde(default)]
    pub with_jq: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Materialization {
    pub collection: String,
    #[serde(default)]
    pub additional_projections: BTreeMap<String, String>,
    pub target: Target,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", deny_unknown_fields, rename_all = "camelCase")]
pub enum Target {
    Postgres { endpoint: String, table: String },
    Elastic { endpoint: String, index: String },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Project {
    pub collections: Vec<Collection>,
    pub materializations: Vec<Materialization>,
}
