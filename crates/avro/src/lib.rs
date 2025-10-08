mod encode;
mod schema;

pub use schema::{AVRO_FIELD_RE, key_to_avro, shape_to_avro as located_shape_to_avro};

// Re-export fundamental schema types so crates don't have to depend on apache_avro.
pub use apache_avro::{
    Schema,
    schema::{Name as RecordName, RecordField, RecordSchema, UnionSchema},
};

// FLOW_KEY_NAME names a field which contains a sub-record of the
// collection key components. Records are nested in this manner because
// some systems compose top-level fields of a Kafka key and value,
// and placing key components in this sub-field makes the relationship
// clearer and minimizes the chance of collision with document fields.
pub const FLOW_KEY_NAME: &str = "_flow_key";

// FLOW_EXTRA_NAME names a field which contains dynamic properties of a
// document object which are unknown or could not be mapped by the schema.
// For example, properties which don't conform to Avro naming restrictions
// are placed into `_flow_extra`.
pub const FLOW_EXTRA_NAME: &str = "_flow_extra";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    BuildError(#[from] json::schema::build::Errors<doc::Annotation>),
    #[error(transparent)]
    IndexError(#[from] json::schema::index::Error),
    #[error("at {ptr}, value {actual} does not conform to AVRO schema {}", serde_json::to_string(expected).unwrap())]
    NotMatched {
        ptr: String,
        expected: apache_avro::Schema,
        actual: serde_json::Value,
    },
    #[error("schema field {} is not a map and must be", FLOW_EXTRA_NAME)]
    ExtraPropertiesMap,
    #[error("key schema is malformed")]
    KeySchemaMalformed,
    #[error("key components mismatch: expected {expected} but found {actual}")]
    KeyComponentsMismatch { expected: usize, actual: usize },
    #[error("failed to parse string {0:?} into double")]
    ParseFloat(String, #[source] std::num::ParseFloatError),
}

/// Map a [`doc::Shape`] into its equivalent AVRO schema.
pub fn shape_to_avro(shape: doc::Shape) -> apache_avro::Schema {
    schema::shape_to_avro(json::Location::Root, shape, true)
}

/// Map a JSON schema bundle and key pointers into its equivalent AVRO schema.
pub fn json_schema_to_avro(
    schema: &str,
    key: &[json::Pointer],
) -> Result<(apache_avro::Schema, apache_avro::Schema), Error> {
    let schema = doc::validation::build_bundle(schema.as_bytes())?;
    let validator = doc::Validator::new(schema)?;
    let shape = doc::Shape::infer(validator.schema(), validator.schema_index());

    Ok((key_to_avro(key, shape.clone()), shape_to_avro(shape)))
}

/// Encode a document into a binary AVRO representation using the given schema.
pub fn encode<'s, 'n, N: json::AsNode>(
    b: &mut Vec<u8>,
    schema: &'s Schema,
    node: &'n N,
) -> Result<(), Error> {
    encode::encode(json::Location::Root, b, schema, node)
}

pub use encode::encode_key;
