pub mod ptr;
mod varint;
pub use ptr::Pointer;

pub mod inference;
pub mod reduce;

mod annotation;
pub use annotation::{extract_reduce_annotations, Annotation};

// Specialize estuary_json templates for the estuary `Annotation` type.
pub type Schema = estuary_json::schema::Schema<Annotation>;
pub type SchemaIndex<'sm> = estuary_json::schema::index::Index<'sm, Annotation>;
pub type Validator<'sm, C> = estuary_json::validator::Validator<'sm, Annotation, C>;

#[derive(Debug, serde::Serialize)]
pub struct FailedValidation {
    document: serde_json::Value,
    basic_output: serde_json::Value,
}

pub fn validate<C>(
    val: &mut Validator<C>,
    schema: &url::Url,
    doc: &serde_json::Value,
) -> Result<(), FailedValidation>
where
    C: estuary_json::validator::Context,
{
    val.prepare(schema)
        .expect("attempt to use invalid register schema URL for validation");
    estuary_json::de::walk(doc, val).unwrap();

    if val.invalid() {
        return Err(FailedValidation {
            document: doc.clone(),
            basic_output: estuary_json::validator::build_basic_output(val.outcomes()),
        });
    } else {
        return Ok(());
    }
}
