use crate::DeriveUsing;

use super::{CompositeKey, Derivation, Field, Id, JournalTemplate, JsonPointer, Schema};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::collections::BTreeMap;

/// Collection describes a set of related documents, where each adheres to a
/// common schema and grouping key. Collections are append-only: once a document
/// is added to a collection, it is never removed. However, it may be replaced
/// or updated (either in whole, or in part) by a future document sharing its
/// key. Each new document of a given key is "reduced" into existing documents
/// of the key. By default, this reduction is achieved by completely replacing
/// the previous document, but much richer reduction behaviors can be specified
/// through the use of annotated reduction strategies of the collection schema.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "CollectionDef::example")]
pub struct CollectionDef {
    /// # Schema against which collection documents are validated and reduced on write and read.
    #[schemars(example = "Schema::example_relative")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Schema>,
    /// # Schema against which collection documents are validated and reduced on write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_schema: Option<Schema>,
    /// # Schema against which collection documents are validated and reduced on read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_schema: Option<Schema>,
    /// # Composite key of this collection.
    pub key: CompositeKey,
    /// # Projections and logical partitions of this collection.
    #[schemars(schema_with = "projections_schema")]
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub projections: BTreeMap<Field, Projection>,
    /// # Template for journals of this collection.
    #[serde(default, skip_serializing_if = "JournalTemplate::is_empty")]
    pub journals: JournalTemplate,
    // # Derivation which builds this collection as transformations of other collections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derive: Option<Derivation>,
    /// # Expected publication ID of this collection within the control plane.
    /// When present, a publication of the collection will fail if the
    /// last publication ID in the control plane doesn't match this value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect_pub_id: Option<Id>,
    /// # Delete this collection within the control plane.
    /// When true, a publication will delete this collection.
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub delete: bool,
}

impl CollectionDef {
    pub fn example() -> Self {
        Self {
            schema: Some(Schema::example_inline_basic()),
            write_schema: None,
            read_schema: None,
            key: CompositeKey::example(),
            projections: BTreeMap::new(),
            journals: JournalTemplate::default(),
            derive: None,
            expect_pub_id: None,
            delete: false,
        }
    }
}

/// Projections are named locations within a collection document which
/// may be used for logical partitioning or directly exposed to databases
/// into which collections are materialized.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(untagged, deny_unknown_fields, rename_all = "camelCase")]
pub enum Projection {
    Pointer(JsonPointer),
    Extended {
        /// # Location of this projection.
        location: JsonPointer,
        /// # Is this projection a logical partition?
        #[serde(default)]
        partition: bool,
    },
}

impl Projection {
    pub fn as_parts(&self) -> (&JsonPointer, bool) {
        match self {
            Self::Pointer(location) => (location, false),
            Self::Extended {
                location,
                partition,
            } => (location, *partition),
        }
    }

    fn example_pointer() -> Self {
        Self::Pointer(JsonPointer::example())
    }

    fn example_extended() -> Self {
        Projection::Extended {
            location: JsonPointer::example(),
            partition: true,
        }
    }
}

fn projections_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    let schema = Field::json_schema(gen);
    gen.definitions_mut().insert(Field::schema_name(), schema);

    let schema = Projection::json_schema(gen);
    gen.definitions_mut()
        .insert(Projection::schema_name(), schema);

    from_value(json!({
        "type": "object",
        "patternProperties": {
            Field::schema_pattern(): {
                "$ref": format!("#/definitions/{}", Projection::schema_name()),
            },
        },
        "additionalProperties": false,
        "examples": [{
            "a_field": Projection::example_pointer(),
            "a_partition": Projection::example_extended(),
        }],
    }))
    .unwrap()
}

impl super::ModelDef for CollectionDef {
    fn sources(&self) -> impl Iterator<Item = &crate::Source> {
        self.derive
            .iter()
            .map(|derive| {
                derive
                    .transforms
                    .iter()
                    .filter(|t| !t.disable)
                    .map(|transform| &transform.source)
            })
            .flatten()
    }
    fn targets(&self) -> impl Iterator<Item = &crate::Collection> {
        std::iter::empty()
    }

    fn catalog_type(&self) -> crate::CatalogType {
        crate::CatalogType::Collection
    }

    fn is_enabled(&self) -> bool {
        self.derive
            .as_ref()
            .map(|d| !d.shards.disable)
            .unwrap_or(true)
    }
    fn connector_image(&self) -> Option<String> {
        self.derive.as_ref().and_then(|d| match &d.using {
            DeriveUsing::Connector(cfg) => Some(cfg.image.to_owned()),
            _ => None,
        })
    }
}
