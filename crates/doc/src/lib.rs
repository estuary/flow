pub mod ptr;
mod varint;
pub use ptr::Pointer;

pub mod inference;
pub mod reduce;

mod annotation;
pub use annotation::{extract_reduce_annotations, Annotation};

// Specialize json templates for the estuary `Annotation` type.
pub type Schema = json::schema::Schema<Annotation>;
pub type SchemaIndex<'sm> = json::schema::index::Index<'sm, Annotation>;
pub type Validator<'sm, C> = json::validator::Validator<'sm, Annotation, C>;
pub type FullContext = json::validator::FullContext;

mod diff;
pub use diff::Diff;

#[derive(Debug, serde::Serialize)]
pub struct FailedValidation {
    pub document: serde_json::Value,
    pub basic_output: serde_json::Value,
}

pub fn validate<C>(
    val: &mut Validator<C>,
    schema: &url::Url,
    doc: &serde_json::Value,
) -> Result<json::Span, FailedValidation>
where
    C: json::validator::Context,
{
    val.prepare(schema)
        .expect("attempt to use invalid register schema URL for validation");
    let span = json::de::walk(doc, val).unwrap();

    if val.invalid() {
        Err(FailedValidation {
            document: doc.clone(),
            basic_output: json::validator::build_basic_output(val.outcomes()),
        })
    } else {
        Ok(span)
    }
}
