pub mod ptr;
mod varint;
pub use ptr::Pointer;

pub mod reduce;

mod annotation;
pub use annotation::{extract_reduce_annotations, Annotation};

// Specialize estuary_json templates for the estuary `Annotation` type.
pub type Schema = estuary_json::schema::Schema<Annotation>;
pub type Validator<'sm, C> = estuary_json::validator::Validator<'sm, Annotation, C>;
