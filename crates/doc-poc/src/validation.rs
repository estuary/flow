use super::{reduce, walker::walk_document, Annotation, AsNode};
use json::validator::Context;

// Specialize json templates for the Flow `Annotation` type.
pub type Schema = json::schema::Schema<Annotation>;
pub type SchemaIndexBuilder<'sm> = json::schema::index::IndexBuilder<'sm, Annotation>;
pub type SchemaIndex<'sm> = json::schema::index::Index<'sm, Annotation>;
pub type FullContext = json::validator::FullContext;
pub type SpanContext = json::validator::SpanContext;
pub type Validator<'sm> = json::validator::Validator<'sm, Annotation, SpanContext>;

/// Validation represents the outcome of a document validation.
pub struct Validation<'schema, 'doc, 'tmp, N: AsNode> {
    /// Schema which was validated.
    pub schema: &'tmp url::Url,
    /// Document which was validated.
    pub document: &'doc N,
    /// Validator which holds the validation outcome.
    // Note use of Validator in a loop requires that we separate these lifetimes.
    pub validator: &'tmp mut Validator<'schema>,
    /// Walked document span.
    pub span: json::Span,
}

// Valid is a Validation known to have had a valid outcome.
pub struct Valid<'schema, 'tmp> {
    /// Validator which holds the validation outcome.
    pub validator: &'tmp mut Validator<'schema>,
    /// Walked document span.
    pub span: json::Span,
}

impl<'schema, 'doc, 'tmp, N: AsNode> Validation<'schema, 'doc, 'tmp, N> {
    /// Validate validates the given document against the given schema.
    pub fn validate(
        validator: &'tmp mut Validator<'schema>,
        schema: &'tmp url::Url,
        document: &'doc N,
    ) -> Result<Self, json::schema::index::Error> {
        validator.prepare(schema)?;

        let root = json::Location::Root;
        let span = walk_document(document, validator, &root, 0);

        Ok(Self {
            schema,
            document,
            validator,
            span,
        })
    }

    /// Ok returns returns FailedValidation if the validation failed, or Valid otherwise.
    pub fn ok(self) -> Result<Valid<'schema, 'tmp>, FailedValidation> {
        if !self.validator.invalid() {
            return Ok(Valid {
                span: self.span,
                validator: self.validator,
            });
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

        let root = json::Location::Root;
        let full_span = walk_document(document, &mut full_validator, &root, 0);

        // Sanity check that we got the same validation result.
        assert!(full_validator.invalid());
        assert_eq!(span, full_span);

        Err(FailedValidation {
            document: serde_json::to_value(document.as_node()).unwrap(),
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

impl<'schema> Valid<'schema, '_> {
    pub fn extract_reduce_annotations(&self) -> Vec<(&'schema reduce::Strategy, u64)> {
        let mut idx = std::iter::repeat((reduce::DEFAULT_STRATEGY, 0))
            .take(self.span.end)
            .collect::<Vec<_>>();

        for (outcome, ctx) in self.validator.outcomes() {
            let subspan = ctx.span();

            if let ::json::validator::Outcome::Annotation(Annotation::Reduce(strategy)) = outcome {
                idx[subspan.begin] = (strategy, subspan.hashed);
            }
        }
        idx
    }
}
