use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::collections::BTreeMap;

use super::{CompositeKey, Derivation, Field, JournalTemplate, JsonPointer, RelativeUrl, Schema};

/// Collection describes a set of related documents, where each adheres to a
/// common schema and grouping key. Collections are append-only: once a document
/// is added to a collection, it is never removed. However, it may be replaced
/// or updated (either in whole, or in part) by a future document sharing its
/// key. Each new document of a given key is "reduced" into existing documents
/// of the key. By default, this reduction is achieved by completely replacing
/// the previous document, but much richer reduction behaviors can be specified
/// through the use of annotated reduction strategies of the collection schema.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "CollectionDef::example")]
pub struct CollectionDef {
    /// # Schema against which collection documents are validated and reduced on write and read.
    #[schemars(example = "Schema::example_relative")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Schema>,
    /// # Schema against which collection documents are validated and reduced on write.
    #[schemars(example = "Schema::example_relative")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_schema: Option<Schema>,
    /// # Schema against which collection documents are validated and reduced on read.
    #[schemars(example = "Schema::example_relative")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_schema: Option<Schema>,
    /// # Composite key of this collection.
    pub key: CompositeKey,
    /// # Projections and logical partitions of this collection.
    #[schemars(schema_with = "projections_schema")]
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub projections: BTreeMap<Field, Projection>,
    /// # Derivation which builds this collection from others.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derivation: Option<Derivation>,
    /// # Template for journals of this collection.
    #[serde(default, skip_serializing_if = "JournalTemplate::is_empty")]
    pub journals: JournalTemplate,
}

impl CollectionDef {
    pub fn example() -> Self {
        Self {
            schema: Some(Schema::Url(RelativeUrl::example_relative())),
            write_schema: None,
            read_schema: None,
            key: CompositeKey::example(),
            projections: BTreeMap::new(),
            derivation: None,
            journals: JournalTemplate::default(),
        }
    }
}

/// Projections are named locations within a collection document which
/// may be used for logical partitioning or directly exposed to databases
/// into which collections are materialized.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
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
