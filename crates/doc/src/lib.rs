pub mod ptr;
mod varint;
pub use ptr::Pointer;

pub mod inference;
pub mod reduce;

mod annotation;
pub use annotation::Annotation;

// Specialize json templates for the estuary `Annotation` type.
pub type Schema = json::schema::Schema<Annotation>;
pub type SchemaIndexBuilder<'sm> = json::schema::index::IndexBuilder<'sm, Annotation>;
pub type SchemaIndex<'sm> = json::schema::index::Index<'sm, Annotation>;
pub type FullContext = json::validator::FullContext;
pub type SpanContext = json::validator::SpanContext;
pub type Validator<'sm> = json::validator::Validator<'sm, Annotation, SpanContext>;

mod diff;
pub use diff::Diff;

/// Validation represents the outcome of a document validation.
pub struct Validation<'sm, 'v> {
    /// Schema which was validated.
    pub schema: &'v url::Url,
    /// Document which was validated.
    pub document: serde_json::Value,
    /// Validator which holds the validation outcome.
    // Note use of Validator in a loop requires that we separate these lifetimes.
    pub validator: &'v mut Validator<'sm>,
    /// Walked document span.
    pub span: json::Span,
}

// Validation is a new-type wrapper of a Validation having a valid outcome.
pub struct Valid<'sm, 'v>(pub Validation<'sm, 'v>);

impl<'sm, 'v> Validation<'sm, 'v> {
    /// Validate validates the given document against the given schema.
    pub fn validate(
        validator: &'v mut Validator<'sm>,
        schema: &'v url::Url,
        document: serde_json::Value,
    ) -> Result<Self, json::schema::index::Error> {
        validator.prepare(schema)?;
        // Deserialization of Value cannot fail.
        let span = json::de::walk(&document, validator).unwrap();

        Ok(Self {
            schema,
            document,
            validator,
            span,
        })
    }

    /// Ok returns returns FailedValidation if the validation failed, or Valid otherwise.
    pub fn ok(self) -> Result<Valid<'sm, 'v>, FailedValidation> {
        if !self.validator.invalid() {
            return Ok(Valid(self));
        }

        let Self {
            schema,
            document,
            validator,
            span,
        } = self;

        // Repeat the validation, but this time with FullContext for better error generation.
        let mut full_validator =
            json::validator::Validator::<Annotation, FullContext>::new(validator.schema_index());
        full_validator.prepare(schema).unwrap();
        let full_span = json::de::walk(&document, &mut full_validator).unwrap();

        // Sanity check that we got the same validation result.
        assert!(full_validator.invalid());
        assert_eq!(span, full_span);

        Err(FailedValidation {
            document,
            basic_output: json::validator::build_basic_output(full_validator.outcomes()),
        })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct FailedValidation {
    pub document: serde_json::Value,
    pub basic_output: serde_json::Value,
}

impl std::fmt::Display for FailedValidation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        serde_json::json!({
            "document": &self.document,
            "basic_output": &self.basic_output,
        })
        .fmt(f)
    }
}
impl std::error::Error for FailedValidation {}
