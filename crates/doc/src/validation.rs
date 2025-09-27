use super::{Annotation, SerPolicy};
use std::pin::Pin;

// Specialize json types for doc::Annotation.
pub type Schema = json::schema::Schema<Annotation>;
pub type SchemaIndexBuilder<'sm> = json::schema::index::Builder<'sm, Annotation>;
pub type SchemaIndex<'sm> = json::schema::index::Index<'sm, Annotation>;
pub type RawValidator<'sm> = json::Validator<'sm, Annotation>;
pub type Outcome<'sm> = json::validator::Outcome<'sm, Annotation>;
pub type ScopedOutcome<'sm> = json::validator::ScopedOutcome<'sm, Annotation>;

// Build an already-bundled Schema.
pub fn build_bundle(bundle: &[u8]) -> Result<Schema, json::schema::build::Errors<Annotation>> {
    let id = url::Url::parse("schema://bundle").unwrap();

    let mut bundle: serde_json::Value = match serde_json::from_slice(bundle) {
        Ok(bundle) => bundle,
        Err(err) => {
            return Err(json::schema::build::Errors(vec![
                json::schema::build::ScopedError {
                    scope: id,
                    inner: json::schema::build::Error::Json(err),
                },
            ]))
        }
    };

    // Take a valid URI $id from the bundle root and use it as the `id`.
    // We do this to avoid indexing the placeholder schema, which otherwise
    // makes for confusing errors during validation.
    let id = bundle
        .as_object_mut()
        .and_then(|obj| obj.remove(json::schema::keywords::ID))
        .and_then(|v| {
            if let serde_json::Value::String(id) = v {
                Some(id)
            } else {
                None
            }
        })
        .and_then(|id| url::Url::parse(&id).ok())
        .unwrap_or(id);

    json::schema::build(&id, &bundle)
}

// Validator wraps a json::Validator and manages ownership of the schemas under validation.
pub struct Validator {
    // Careful, order matters! Fields are dropped in declaration order.
    inner: json::Validator<'static, Annotation>,
    index: Pin<Box<SchemaIndex<'static>>>,
    schema_static: &'static Schema,
    schema: Pin<Box<Schema>>,
}

impl Validator {
    pub fn new(schema: Schema) -> Result<Self, json::schema::index::Error> {
        let schema: Pin<Box<Schema>> = Box::pin(schema);

        // Safety: we manually keep an owned schema alongside the associated
        // index and validator, and ensure it's dropped last.
        let schema_static = unsafe { std::mem::transmute::<&'_ Schema, &'static Schema>(&schema) };

        let mut index = SchemaIndexBuilder::new();
        index.add(schema_static)?;
        index.verify_references()?;

        // Safety: we manually keep the owned index alongside the associated validator,
        // and drop it before the validator.
        let index = Box::pin(index.into_index());
        let index_static =
            unsafe { std::mem::transmute::<&'_ SchemaIndex, &'static SchemaIndex>(&index) };

        Ok(Self {
            inner: json::validator::Validator::new(index_static),
            index,
            schema_static,
            schema,
        })
    }

    /// Fetch the SchemaIndex of this Validator.
    pub fn schema_index(&self) -> &SchemaIndex<'static> {
        &self.index
    }

    /// Fetch the Schema indexed by this Validator.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Validate the given document, collecting annotations as indicated by the filter.
    /// If the document is invalid, a FailedValidation is returned with details
    /// and a redacted version of the document.
    #[inline]
    pub fn validate<'n, 'v, N, F>(
        &'v mut self,
        doc: &'n N,
        filter: F,
    ) -> Result<Vec<ScopedOutcome<'v>>, FailedValidation>
    where
        N: json::AsNode,
        F: for<'o> Fn(Outcome<'o>) -> Option<Outcome<'o>>,
    {
        let (valid, outcomes) = self.inner.validate(self.schema_static, doc, filter);

        if valid {
            // Transmute 'static outcomes back to ones which cannot outlive 'v.
            let outcomes = unsafe {
                std::mem::transmute::<Vec<ScopedOutcome<'static>>, Vec<ScopedOutcome<'v>>>(outcomes)
            };
            Ok(outcomes)
        } else {
            Err(self.build_failed_validation(doc))
        }
    }

    #[cold]
    #[inline(never)]
    fn build_failed_validation<N: json::AsNode>(&mut self, doc: &N) -> FailedValidation {
        // Validate again, collecting non-annotation error outcomes.
        let (_valid, outcomes) = self.inner.validate(self.schema_static, doc, error_filter);
        let basic_output = json::validator::build_basic_output(doc, &outcomes);

        // Validate once more: this time, collect redact annotations.
        let (_valid, outcomes) = self.inner.validate(self.schema_static, doc, redact_filter);

        // Re-allocate and redact the document as per collected annotations.
        let alloc = crate::Allocator::new();
        let mut doc = crate::HeapNode::from_node(doc, &alloc);
        let _result = crate::redact::redact(&mut doc, &outcomes, &alloc, &[]);

        FailedValidation {
            // TODO: It might be a good idea to add a field on `FailedValidation` to indicate
            // whether the document serialized here has been truncated.
            document: serde_json::to_value(SerPolicy::debug().on(&doc)).unwrap(),
            basic_output,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FailedValidation {
    pub basic_output: serde_json::Value,
    pub document: serde_json::Value,
}

impl std::fmt::Display for FailedValidation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        serde_json::to_string_pretty(self).unwrap().fmt(f)
    }
}
impl std::error::Error for FailedValidation {}

#[inline(always)]
pub fn redact_filter<'s>(outcome: Outcome<'s>) -> Option<Outcome<'s>> {
    match &outcome {
        Outcome::Annotation(crate::Annotation::Redact(_)) => Some(outcome),
        _ => None,
    }
}

#[inline(always)]
pub fn reduce_filter<'s>(outcome: Outcome<'s>) -> Option<Outcome<'s>> {
    match &outcome {
        Outcome::Annotation(crate::Annotation::Reduce(_)) => Some(outcome),
        _ => None,
    }
}

pub fn error_filter<'s>(outcome: Outcome<'s>) -> Option<Outcome<'s>> {
    match &outcome {
        Outcome::Annotation(_) => None,
        _ => Some(outcome),
    }
}
