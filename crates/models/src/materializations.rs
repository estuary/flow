use protocol::flow::EndpointType;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;

use super::{
    Collection, ConnectorConfig, Field, Object, PartitionSelector, RelativeUrl, ShardTemplate,
};

/// A Materialization binds a Flow collection with an external system & target
/// (e.x, a SQL table) into which the collection is to be continuously materialized.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MaterializationDef {
    /// # Endpoint to materialize into.
    pub endpoint: MaterializationEndpoint,
    /// # Bound collections to materialize into the endpoint.
    pub bindings: Vec<MaterializationBinding>,
    /// # Template for shards of this materialization task.
    #[serde(default)]
    pub shards: ShardTemplate,
}

impl MaterializationDef {
    pub fn example() -> Self {
        Self {
            endpoint: MaterializationEndpoint::FlowSink(ConnectorConfig::example()),
            bindings: vec![MaterializationBinding::example()],
            shards: ShardTemplate::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(example = "MaterializationBinding::example")]
pub struct MaterializationBinding {
    /// # Endpoint resource to materialize into.
    pub resource: Object,
    /// # Name of the collection to be materialized.
    pub source: Collection,
    /// # Selector over partitions of the source collection to read.
    #[serde(default)]
    #[schemars(default = "PartitionSelector::example")]
    pub partitions: Option<PartitionSelector>,
    /// # Selected projections for this materialization.
    #[serde(default)]
    pub fields: MaterializationFields,
}

impl MaterializationBinding {
    fn example() -> Self {
        Self {
            resource: json!({"table": "a_table"}).as_object().unwrap().clone(),
            source: Collection::new("source/collection"),
            partitions: None,
            fields: MaterializationFields::default(),
        }
    }
}

/// MaterializationFields defines a selection of projections to materialize,
/// as well as optional per-projection, driver-specific configuration.
#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "MaterializationFields::example")]
pub struct MaterializationFields {
    /// # Fields to include.
    /// This supplements any recommended fields, where enabled.
    /// Values are passed through to the driver, e.x. for customization
    /// of the driver's schema generation or runtime behavior with respect
    /// to the field.
    #[serde(default)]
    pub include: BTreeMap<Field, Object>,
    /// # Fields to exclude.
    /// This removes from recommended projections, where enabled.
    #[serde(default)]
    pub exclude: Vec<Field>,
    /// # Should recommended projections for the endpoint be used?
    pub recommended: bool,
}

impl MaterializationFields {
    pub fn example() -> Self {
        MaterializationFields {
            include: vec![(Field::new("added"), Object::new())]
                .into_iter()
                .collect(),
            exclude: vec![Field::new("removed")],
            recommended: true,
        }
    }
}

impl Default for MaterializationFields {
    fn default() -> Self {
        Self {
            include: BTreeMap::new(),
            exclude: Vec::new(),
            recommended: true,
        }
    }
}

/// An Endpoint connector used for Flow materializations.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum MaterializationEndpoint {
    /// # A Flow sink.
    FlowSink(ConnectorConfig),
    /// # A SQLite database.
    Sqlite(SqliteConfig),
}

impl MaterializationEndpoint {
    pub fn endpoint_type(&self) -> EndpointType {
        match self {
            Self::FlowSink(_) => EndpointType::FlowSink,
            Self::Sqlite(_) => EndpointType::Sqlite,
        }
    }
}

/// Sqlite endpoint configuration.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct SqliteConfig {
    /// # Path of the database, relative to this catalog source.
    /// The path may include query arguments. See:
    /// https://github.com/mattn/go-sqlite3#connection-string
    pub path: RelativeUrl,
}
