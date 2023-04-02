use super::{reduce, walker::walk_document, Annotation, AsNode};
use json::validator::Context;
use std::pin::Pin;

// Specialize json templates for the Flow `Annotation` type.
pub type Schema = json::schema::Schema<Annotation>;
pub type SchemaIndexBuilder<'sm> = json::schema::index::IndexBuilder<'sm, Annotation>;
pub type SchemaIndex<'sm> = json::schema::index::Index<'sm, Annotation>;
pub type FullContext = json::validator::FullContext;
pub type SpanContext = json::validator::SpanContext;
pub type RawValidator<'sm> = json::validator::Validator<'sm, Annotation, SpanContext>;

// Re-export build_schema for lower-level usages.
pub use json::schema::build::build_schema;

// Build an already-bundled Schema.
pub fn build_bundle(bundle: &str) -> Result<Schema, json::schema::build::Error> {
    let mut schema = build_schema(
        url::Url::parse("schema://bundle").unwrap(),
        &serde_json::from_str(bundle).unwrap(),
    )?;

    // Tweak scope to remove a synthetic resource pointer previously
    // embedded in the $id during schema bundling.
    for (key, value) in schema.curi.query_pairs() {
        if key != "ptr" {
            continue;
        }
        let fragment = value.to_string();
        schema.curi.set_fragment(Some(&fragment));
        schema.curi.set_query(None);
        break;
    }

    Ok(schema)
}

// Validator wraps a json::Validator and manages ownership of the schemas under validation.
pub struct Validator {
    // Careful, order matters! Fields are dropped in declaration order.
    inner: json::validator::Validator<'static, Annotation, SpanContext>,
    index: Pin<Box<SchemaIndex<'static>>>,
    schemas: Pin<Box<[Schema]>>,
}

impl Validator {
    pub fn new(schema: Schema) -> Result<Self, json::schema::index::Error> {
        Self::new_from_iter(std::iter::once(schema))
    }

    pub fn new_from_iter<I>(it: I) -> Result<Self, json::schema::index::Error>
    where
        I: IntoIterator<Item = Schema>,
    {
        let schemas: Vec<Schema> = it.into_iter().collect();
        let schemas: Pin<Box<[Schema]>> = Pin::new(schemas.into_boxed_slice());

        // Safety: we manually keep owned schemas alongside the associated index and validator,
        // and ensure they're dropped last.
        let schemas_static =
            unsafe { std::mem::transmute::<&'_ [Schema], &'static [Schema]>(&schemas) };

        let mut index = SchemaIndexBuilder::new();
        for schema in schemas_static {
            index.add(schema)?;
        }
        index.verify_references()?;

        // Safety: we manually keep the owned index alongside the associated validator,
        // and drop it before the validator.
        let index = Box::pin(index.into_index());
        let index_static =
            unsafe { std::mem::transmute::<&'_ SchemaIndex, &'static SchemaIndex>(&index) };

        Ok(Self {
            inner: json::validator::Validator::new(index_static),
            index,
            schemas,
        })
    }

    /// Fetch the SchemaIndex of this Validator.
    pub fn schema_index(&self) -> &SchemaIndex<'static> {
        &self.index
    }

    /// Fetch the Schemas indexed by this Validator.
    pub fn schemas(&self) -> &[Schema] {
        &self.schemas
    }

    /// Validate validates the given document against the given schema.
    /// If schema is None, than the root_curi() of this Validator is validated.
    pub fn validate<'doc, 'v, N: AsNode>(
        &'v mut self,
        schema: Option<&'v url::Url>,
        document: &'doc N,
    ) -> Result<Validation<'static, 'doc, 'v, N>, json::schema::index::Error> {
        let effective_schema = match schema {
            Some(schema) => schema,
            None if self.schemas.len() == 1 => &self.schemas[0].curi,
            None => {
                panic!("root_curi() may only be used with Validators having a single root schema")
            }
        };
        self.inner.prepare(effective_schema)?;

        let root = json::Location::Root;
        let span = walk_document(document, &mut self.inner, &root, 0);

        Ok(Validation {
            document,
            schema: effective_schema,
            span,
            validator: &mut self.inner,
        })
    }
}

/// Validation represents the outcome of a document validation.
pub struct Validation<'schema, 'doc, 'tmp, N: AsNode> {
    /// Document which was validated.
    pub document: &'doc N,
    /// Schema which was validated.
    pub schema: &'tmp url::Url,
    /// Walked document span.
    pub span: json::Span,
    /// Validator which holds the validation outcome.
    // Note use of Validator in a loop requires that we separate these lifetimes.
    pub validator: &'tmp mut RawValidator<'schema>,
}

// Valid is a Validation known to have had a valid outcome.
pub struct Valid<'schema, 'tmp> {
    /// Validator which holds the validation outcome.
    pub validator: &'tmp mut RawValidator<'schema>,
    /// Walked document span.
    pub span: json::Span,
}

impl<'schema, 'doc, 'tmp, N: AsNode> Validation<'schema, 'doc, 'tmp, N> {
    /// Validate is a lower-level API for verifying a given document against the given schema.
    /// You probably want to use Validator::validate() instead of this function.
    pub fn validate(
        validator: &'tmp mut RawValidator<'schema>,
        schema: &'tmp url::Url,
        document: &'doc N,
    ) -> Result<Self, json::schema::index::Error> {
        validator.prepare(schema)?;

        let root = json::Location::Root;
        let span = walk_document(document, validator, &root, 0);

        Ok(Self {
            document,
            schema,
            span,
            validator,
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

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
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
