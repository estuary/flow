use super::{
    is_false, Capture, ConnectorConfig, Field, RawValue, RelativeUrl, ShardTemplate, Source,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;

/// A Materialization binds a Flow collection with an external system & target
/// (e.x, a SQL table) into which the collection is to be continuously materialized.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MaterializationDef {
    /// # Automatically materialize new bindings from a named capture
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_capture: Option<Capture>,
    /// # Endpoint to materialize into.
    pub endpoint: MaterializationEndpoint,
    /// # Bound collections to materialize into the endpoint.
    pub bindings: Vec<MaterializationBinding>,
    /// # Template for shards of this materialization task.
    #[serde(default, skip_serializing_if = "ShardTemplate::is_empty")]
    pub shards: ShardTemplate,
}

/// An Endpoint connector used for Flow materializations.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum MaterializationEndpoint {
    /// # A Connector.
    #[serde(alias = "flowSink")]
    Connector(ConnectorConfig),
    /// # A SQLite database.
    /// TODO(johnny): Remove.
    #[schemars(skip)]
    Sqlite(SqliteConfig),
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(example = "MaterializationBinding::example")]
pub struct MaterializationBinding {
    /// # Endpoint resource to materialize into.
    pub resource: RawValue,
    /// # The collection to be materialized.
    pub source: Source,
    /// # Whether to disable the binding
    /// Disabled bindings are inactive, and not validated.
    #[serde(default, skip_serializing_if = "is_false")]
    pub disable: bool,
    /// # Priority applied to documents processed by this binding.
    /// When all bindings are of equal priority, Flow processes documents
    /// according to their associated publishing time, as encoded in the
    /// document UUID.
    ///
    /// However, when one binding has a higher priority than others,
    /// then *all* ready documents are processed through the binding
    /// before *any* documents of other bindings are processed.
    #[serde(
        default,
        skip_serializing_if = "MaterializationBinding::priority_is_zero"
    )]
    pub priority: u32,
    /// # Selected projections for this materialization.
    #[serde(default)]
    pub fields: MaterializationFields,
}

/// MaterializationFields defines a selection of projections to materialize,
/// as well as optional per-projection, driver-specific configuration.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "MaterializationFields::example")]
pub struct MaterializationFields {
    /// # Fields to include.
    /// This supplements any recommended fields, where enabled.
    /// Values are passed through to the driver, e.x. for customization
    /// of the driver's schema generation or runtime behavior with respect
    /// to the field.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub include: BTreeMap<Field, RawValue>,
    /// # Fields to exclude.
    /// This removes from recommended projections, where enabled.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<Field>,
    /// # Should recommended projections for the endpoint be used?
    pub recommended: bool,
}

impl MaterializationDef {
    pub fn example() -> Self {
        Self {
            source_capture: None,
            endpoint: MaterializationEndpoint::Connector(ConnectorConfig::example()),
            bindings: vec![MaterializationBinding::example()],
            shards: ShardTemplate::default(),
        }
    }
}

impl MaterializationBinding {
    fn example() -> Self {
        Self {
            resource: serde_json::from_value(json!({"table": "a_table"})).unwrap(),
            source: Source::example(),
            disable: false,
            priority: 0,
            fields: MaterializationFields::default(),
        }
    }

    fn priority_is_zero(p: &u32) -> bool {
        *p == 0
    }
}

impl MaterializationFields {
    pub fn example() -> Self {
        MaterializationFields {
            include: vec![(Field::new("added"), serde_json::from_str("{}").unwrap())]
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

/// Sqlite endpoint configuration.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SqliteConfig {
    /// # Path of the database, relative to this catalog source.
    /// The path may include query arguments. See:
    /// https://github.com/mattn/go-sqlite3#connection-string
    pub path: RelativeUrl,
}
