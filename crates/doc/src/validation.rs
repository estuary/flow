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
            ]));
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

#[cfg(test)]
mod tests {
    use crate::HeapNode;

    #[test]
    fn test_duplicate_properties() {
        // HeapNode preserves duplicate properties (unlike serde_json::Value).
        // This tests that each occurrence of a duplicate property is validated.
        // We test this functionality here to avoid introducing `doc` as a
        // dev-dependency of the `json` crate.

        struct Case {
            name: &'static str,
            doc: &'static str,
            schema: serde_json::Value,
            expect_valid: bool,
            expect_type_errors: usize,
            expect_missing_errors: usize,
        }

        let cases = [
            Case {
                name: "both duplicates valid",
                doc: r#"{"foo": "a", "foo": "b"}"#,
                schema: serde_json::json!({"properties": {"foo": {"type": "string"}}}),
                expect_valid: true,
                expect_type_errors: 0,
                expect_missing_errors: 0,
            },
            Case {
                name: "one duplicate invalid",
                doc: r#"{"foo": "a", "foo": 42}"#,
                schema: serde_json::json!({"properties": {"foo": {"type": "string"}}}),
                expect_valid: false,
                expect_type_errors: 1,
                expect_missing_errors: 0,
            },
            Case {
                name: "both duplicates invalid",
                doc: r#"{"foo": 42, "foo": 99}"#,
                schema: serde_json::json!({"properties": {"foo": {"type": "string"}}}),
                expect_valid: false,
                expect_type_errors: 2,
                expect_missing_errors: 0,
            },
            Case {
                name: "required with duplicates present (both invalid type)",
                doc: r#"{"foo": 1, "foo": 2}"#,
                schema: serde_json::json!({
                    "properties": {"foo": {"type": "string"}},
                    "required": ["foo"]
                }),
                expect_valid: false,
                expect_type_errors: 2,
                expect_missing_errors: 0, // foo IS present, just wrong type
            },
            Case {
                name: "missing required after duplicates",
                doc: r#"{"aaa": 1, "aaa": 2, "zzz": 3}"#,
                schema: serde_json::json!({
                    "properties": {
                        "aaa": {"type": "integer"},
                        "missing": {"type": "integer"}
                    },
                    "required": ["missing"]
                }),
                expect_valid: false,
                expect_type_errors: 0,
                expect_missing_errors: 1,
            },
            Case {
                name: "three duplicates all invalid",
                doc: r#"{"foo": 1, "foo": 2, "foo": 3}"#,
                schema: serde_json::json!({"properties": {"foo": {"type": "string"}}}),
                expect_valid: false,
                expect_type_errors: 3,
                expect_missing_errors: 0,
            },
            Case {
                name: "multiple properties each with duplicates",
                doc: r#"{"aaa": 1, "aaa": 2, "bbb": 3, "bbb": 4}"#,
                schema: serde_json::json!({
                    "properties": {
                        "aaa": {"type": "string"},
                        "bbb": {"type": "string"}
                    }
                }),
                expect_valid: false,
                expect_type_errors: 4,
                expect_missing_errors: 0,
            },
            Case {
                name: "duplicates not matching any schema property",
                doc: r#"{"unknown": 1, "unknown": 2}"#,
                schema: serde_json::json!({"properties": {"other": {"type": "integer"}}}),
                expect_valid: true, // additionalProperties defaults to true
                expect_type_errors: 0,
                expect_missing_errors: 0,
            },
        ];

        for case in cases {
            // Parse doc as HeapNode to preserve duplicates
            let alloc = HeapNode::new_allocator();
            let mut de = serde_json::Deserializer::from_str(case.doc);
            let heap_doc = HeapNode::from_serde(&mut de, &alloc)
                .unwrap_or_else(|e| panic!("{}: failed to parse doc: {}", case.name, e));

            let schema = json::schema::build::build_schema::<crate::Annotation>(
                &url::Url::parse("https://example.com/test.json").unwrap(),
                &case.schema,
            )
            .unwrap_or_else(|e| panic!("{}: failed to build schema: {:?}", case.name, e));

            let mut builder = json::schema::index::Builder::new();
            builder.add(&schema).unwrap();
            builder.verify_references().unwrap();
            let index = builder.into_index();

            let mut validator = json::Validator::new(&index);
            let (valid, outcomes) = validator.validate(&schema, &heap_doc, |o| Some(o));

            assert_eq!(
                valid, case.expect_valid,
                "{}: expected valid={}, got valid={}\noutcomes: {:?}",
                case.name, case.expect_valid, valid, outcomes
            );

            let type_errors = outcomes
                .iter()
                .filter(|o| matches!(o.outcome, json::validator::Outcome::TypeNotMet(_)))
                .count();
            assert_eq!(
                type_errors, case.expect_type_errors,
                "{}: expected {} type errors, got {}\noutcomes: {:?}",
                case.name, case.expect_type_errors, type_errors, outcomes
            );

            let missing_errors = outcomes
                .iter()
                .filter(|o| {
                    matches!(
                        o.outcome,
                        json::validator::Outcome::MissingRequiredProperty(_)
                    )
                })
                .count();
            assert_eq!(
                missing_errors, case.expect_missing_errors,
                "{}: expected {} missing errors, got {}\noutcomes: {:?}",
                case.name, case.expect_missing_errors, missing_errors, outcomes
            );
        }
    }
}
