use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Collection {
    pub name: String,
    pub schema: String,
    pub key: Vec<String>,

    #[serde(default)]
    pub examples: String,
    #[serde(default)]
    pub partitions: BTreeMap<String, String>,
    pub derivation: Option<Derivation>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "runtime", deny_unknown_fields, rename_all = "camelCase")]
pub enum Derivation {
    Jq(JQDerivation),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", deny_unknown_fields, rename_all = "camelCase")]
pub enum InnerState {
    Durable { parallelism: u16 },
    Ephemeral,
}

impl Default for InnerState {
    fn default() -> Self {
        InnerState::Ephemeral
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct JQDerivation {
    #[serde(default)]
    pub inner_state: InnerState,
    pub transforms: Vec<JQTransform>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct JQTransform {
    pub source: String,
    #[serde(default)]
    pub shuffle: Shuffle,
    #[serde(default)]
    pub function: String,
    #[serde(default)]
    pub function_path: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Shuffle {
    #[serde(default)]
    pub key: Vec<String>,
    #[serde(default)]
    pub broadcast: u16,
    #[serde(default)]
    pub choose: u16,
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
    #[serde(default)]
    pub materializations: Vec<Materialization>,
}
