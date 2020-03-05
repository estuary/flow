use serde::{Deserialize, Serialize};
use serde_json;

use estuary_json::schema;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Collection {
    pub name: String,
    pub schema: String,
    pub key: Vec<String>,
    pub fixtures: Vec<String>,
    #[serde(default)]
    pub projections: Vec<Projection>,
    pub derivation: Option<Derivation>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Fixture {
    pub document: serde_json::Value,
    pub key: Vec<serde_json::Value>,
    #[serde(default)]
    pub projections: serde_json::Map<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Projection {
    pub name: String,
    pub ptr: String,
    #[serde(default)]
    pub partition: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Derivation {
    #[serde(default)]
    pub inner_state: InnerState,
    pub transform: Vec<Transform>,
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
pub struct Transform {
    pub source: String,
    pub source_schema: Option<String>,
    pub shuffle: Option<Shuffle>,
    pub lambda: Lambda,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum Lambda {
    Jq(String),
    JqBlock(String),
    Sqlite{
        bootstrap: Option<String>,
        body: String,
    },
    SqliteBlock{
        bootstrap: Option<String>,
        body: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Shuffle {
    pub key: Option<Vec<String>>,
    pub broadcast: Option<u16>,
    pub choose: Option<u16>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Materialization {
    pub collection: String,
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
pub struct Node {
    #[serde(default)]
    pub import: Vec<String>,
    #[serde(default)]
    pub collections: Vec<Collection>,
    #[serde(default)]
    pub materializations: Vec<Materialization>,
}

/// TODO: Move to json_ext ?

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "strategy", deny_unknown_fields, rename_all = "camelCase")]
pub enum Reducer {
    Minimize {},
    Maximize {},
    Sum {},
    Merge {},
    FirstWriteWins,
    LastWriteWins,
}

#[derive(Debug)]
pub enum Annotation {
    Core(schema::CoreAnnotation),
    Reduce(Reducer),
}

impl schema::Annotation for Annotation {
    fn as_core(&self) -> Option<&schema::CoreAnnotation> {
        match self {
            Annotation::Core(annot) => Some(annot),
            _ => None,
        }
    }
}

impl schema::build::AnnotationBuilder for Annotation {
    fn uses_keyword(keyword: &str) -> bool {
        if keyword == "reduce" {
            true
        } else {
            schema::CoreAnnotation::uses_keyword(keyword)
        }
    }

    fn from_keyword(
        keyword: &str,
        value: &serde_json::Value,
    ) -> Result<Self, schema::build::Error> {
        use schema::BuildError::AnnotationErr;
        use schema::CoreAnnotation as Core;

        if keyword == "reduce" {
            match Reducer::deserialize(value) {
                Err(e) => Err(AnnotationErr(Box::new(e))),
                Ok(r) => Ok(Annotation::Reduce(r)),
            }
        } else {
            Ok(Annotation::Core(Core::from_keyword(keyword, value)?))
        }
    }
}