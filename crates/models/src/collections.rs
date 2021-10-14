use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use tuple_vec_map;

use super::{CompositeKey, Derivation, Field, JournalTemplate, JsonPointer, RelativeUrl, Schema};

/// Collection describes a set of related documents, where each adheres to a
/// common schema and grouping key. Collections are append-only: once a document
/// is added to a collection, it is never removed. However, it may be replaced
/// or updated (either in whole, or in part) by a future document sharing its
/// key. Each new document of a given key is "reduced" into existing documents
/// of the key. By default, this reduction is achieved by completely replacing
/// the previous document, but much richer reduction behaviors can be specified
/// through the use of annotated reduction strategies of the collection schema.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "CollectionDef::example")]
pub struct CollectionDef {
    /// # Schema against which collection documents are validated and reduced.
    #[schemars(example = "Schema::example_relative")]
    pub schema: Schema,
    /// # Composite key of this collection.
    pub key: CompositeKey,
    /// # Projections and logical partitions of this collection.
    #[serde(default, with = "tuple_vec_map")]
    #[schemars(schema_with = "projections_schema")]
    pub projections: Vec<(Field, Projection)>,
    /// # Derivation which builds this collection from others.
    pub derivation: Option<Derivation>,
    /// # Template for journals of this collection.
    #[serde(default)]
    pub journals: JournalTemplate,
}

impl CollectionDef {
    pub fn example() -> Self {
        from_value(json!({
            "schema": RelativeUrl::example_relative(),
            "key": CompositeKey::example(),
        }))
        .unwrap()
    }
}

/// Projections are named locations within a collection document which
/// may be used for logical partitioning or directly exposed to databases
/// into which collections are materialized.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
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
    pub fn example_pointer() -> Self {
        Self::Pointer(JsonPointer::example())
    }

    pub fn example_extended() -> Self {
        Projection::Extended {
            location: JsonPointer::example(),
            partition: true,
        }
    }
}

fn projections_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    from_value(json!({
        "type": "object",
        "patternProperties": {
            Field::schema_pattern(): {
                "$ref": "#/definitions/Projection",
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
