use crate::{source::OnIncompatibleSchemaChange, Collection, Id};

use super::{
    Capture, ConnectorConfig, Field, LocalConfig, RawValue, RelativeUrl, ShardTemplate, Source,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;

/// A Materialization binds a Flow collection with an external system & target
/// (e.x, a SQL table) into which the collection is to be continuously materialized.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MaterializationDef {
    /// # Automatically materialize new bindings from a named capture
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_capture: Option<Capture>,
    /// # Default handling of schema changes that are incompatible with the target resource.
    /// This can be overridden on a per-binding basis.
    #[serde(
        default,
        skip_serializing_if = "OnIncompatibleSchemaChange::is_default"
    )]
    pub on_incompatible_schema_change: OnIncompatibleSchemaChange,
    /// # Endpoint to materialize into.
    pub endpoint: MaterializationEndpoint,
    /// # Bound collections to materialize into the endpoint.
    pub bindings: Vec<MaterializationBinding>,
    /// # Template for shards of this materialization task.
    #[serde(default, skip_serializing_if = "ShardTemplate::is_empty")]
    pub shards: ShardTemplate,
    /// # Expected publication ID of this materialization within the control plane.
    /// When present, a publication of the materialization will fail if the
    /// last publication ID in the control plane doesn't match this value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect_pub_id: Option<Id>,
    /// # Delete this materialization within the control plane.
    /// When true, a publication will delete this materialization.
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub delete: bool,
}

/// An Endpoint connector used for Flow materializations.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum MaterializationEndpoint {
    /// # A Connector.
    #[serde(alias = "flowSink")]
    Connector(ConnectorConfig),
    /// # A local command (development only).
    Local(LocalConfig),
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "MaterializationBinding::example")]
pub struct MaterializationBinding {
    /// # Endpoint resource to materialize into.
    pub resource: RawValue,
    /// # The collection to be materialized.
    pub source: Source,
    /// # Whether to disable the binding
    /// Disabled bindings are inactive, and not validated.
    #[serde(default, skip_serializing_if = "super::is_false")]
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
    /// # Backfill counter for this binding.
    /// Every increment of this counter will result in a new backfill of this
    /// binding from its source collection to its materialized resource.
    /// For example when materializing to a SQL table, incrementing this counter
    /// causes the table to be dropped and then rebuilt by re-reading the source
    /// collection.
    #[serde(default, skip_serializing_if = "super::is_u32_zero")]
    pub backfill: u32,

    /// # Action to take when a schema change is rejected due to incompatibility.
    /// This setting is used to determine the action to take when a schema change
    /// is rejected due to incompatibility with the target resource. By default,
    /// the binding will have its `backfill` counter incremented, causing it to
    /// be re-materialized from the source collection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_incompatible_schema_change: Option<OnIncompatibleSchemaChange>,
}

/// MaterializationFields defines a selection of projections to materialize,
/// as well as optional per-projection, driver-specific configuration.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
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
            expect_pub_id: None,
            delete: false,
            on_incompatible_schema_change: OnIncompatibleSchemaChange::default(),
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
            backfill: 0,
            on_incompatible_schema_change: None,
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

impl super::ModelDef for MaterializationDef {
    fn sources(&self) -> impl Iterator<Item = &crate::Source> {
        self.bindings
            .iter()
            .filter(|b| !b.disable)
            .map(|binding| &binding.source)
    }
    fn targets(&self) -> impl Iterator<Item = &Collection> {
        std::iter::empty()
    }

    fn catalog_type(&self) -> crate::CatalogType {
        crate::CatalogType::Materialization
    }

    fn is_enabled(&self) -> bool {
        !self.shards.disable
    }

    fn materialization_source_capture(&self) -> Option<&crate::Capture> {
        self.source_capture.as_ref()
    }

    fn connector_image(&self) -> Option<&str> {
        match &self.endpoint {
            MaterializationEndpoint::Connector(cfg) => Some(&cfg.image),
            _ => None,
        }
    }
}
