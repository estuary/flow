use crate::{
    schema,
    validator::{Outcome, ScopedOutcome},
    Node,
};
use itertools::Itertools;
use serde_json::json;

fn outcome_detail<'s, A: schema::Annotation>(outcome: &Outcome<'s, A>) -> String {
    match outcome {
        Outcome::Annotation(ann) => {
            format!("Annotation: {ann:?}")
        }
        Outcome::AnyOfNotMatched => {
            "Location does not match any of the expected schemas".to_string()
        }
        Outcome::ConstNotMatched => "Location does not match the expected constant".to_string(),
        Outcome::EnumNotMatched => "Location is not one of the enumerated constants".to_string(),
        Outcome::ExclusiveMaximumExceeded => "Number exceeds the exclusive maximum".to_string(),
        Outcome::ExclusiveMinimumNotMet => {
            "Number is not greater than the exclusive minimum".to_string()
        }
        Outcome::False => "This location is not allowed to exist".to_string(),
        Outcome::FormatNotMatched(format) => {
            format!("Format mismatch: expected a {format:?}")
        }
        Outcome::ItemsNotUnique => {
            "Array contains duplicate items when uniqueItems is required".to_string()
        }
        Outcome::MaxContainsExceeded(expect, actual) => {
            format!(
                "Array contains too many matching items: expected at most {expect}, found {actual}"
            )
        }
        Outcome::MaxItemsExceeded(expect, actual) => {
            format!("Array has too many items: expected at most {expect}, found {actual}")
        }
        Outcome::MaxLengthExceeded(expect, actual) => {
            format!("String is too long: expected at most {expect} characters, found {actual}")
        }
        Outcome::MaxPropertiesExceeded(expect, actual) => {
            format!("Object has too many properties: expected at most {expect}, found {actual}")
        }
        Outcome::MaximumExceeded => "Number exceeds the maximum".to_string(),
        Outcome::MinContainsNotMet(expect, actual) => {
            format!(
                "Array contains too few matching items: expected at least {expect}, found {actual}"
            )
        }
        Outcome::MinItemsNotMet(expect, actual) => {
            format!("Array has too few items: expected at least {expect}, found {actual}")
        }
        Outcome::MinLengthNotMet(expect, actual) => {
            format!("String is too short: expected at least {expect} characters, found {actual}")
        }
        Outcome::MinPropertiesNotMet(expect, actual) => {
            format!("Object has too few properties: expected at least {expect}, found {actual}")
        }
        Outcome::MinimumNotMet => "Number is below the minimum".to_string(),
        Outcome::MissingRequiredProperty(prop) => {
            let prop: &str = &(*prop)[1..];
            format!("Missing required property: {prop}")
        }
        Outcome::MultipleOfNotMet => "Number is not a multiple of the required factor".to_string(),
        Outcome::NotIsValid => "Location matches a schema that should not be matched".to_string(),
        Outcome::OneOfMultipleMatched => {
            "Location matches multiple schemas when exactly one match is required".to_string()
        }
        Outcome::OneOfNotMatched => {
            "Location must match exactly one of the required schemas, but matched none".to_string()
        }
        Outcome::PatternNotMatched => "String does not match the required pattern".to_string(),
        Outcome::RecursionDepthExceeded => {
            "Recursion depth exceeded while validating the document".to_string()
        }
        Outcome::ReferenceNotFound(reference) => {
            let reference: &str = &*reference;
            format!("Schema reference not found: {reference}")
        }
        Outcome::TypeNotMet(expected) => {
            format!("Type mismatch: expected a {}", expected.iter().join(" or "))
        }
    }
}

pub fn build_basic_output<'s, N, A>(doc: &N, outcomes: &[ScopedOutcome<'s, A>]) -> serde_json::Value
where
    N: crate::AsNode,
    A: schema::Annotation,
{
    let errors: Vec<serde_json::Value> = outcomes
        .iter()
        .map(
            |ScopedOutcome {
                 outcome,
                 schema_curi,
                 tape_index,
             }| {
                let schema_curi: &str = &**schema_curi;

                let (instance_location, instance_value) =
                    crate::location::find_tape_index(doc, *tape_index, |location, node| {
                        let value = match node.as_node() {
                            Node::Array(_) => json!("<array>"),
                            Node::Object(_) => json!("<object>"),
                            Node::Bool(b) => {
                                if b {
                                    json!(true)
                                } else {
                                    json!(false)
                                }
                            }
                            Node::Null => json!(null),
                            Node::Bytes(_) => json!("<bytes>"),
                            Node::String(s) => {
                                if s.len() < 256 {
                                    json!(s)
                                } else {
                                    let s = s.chars().take(256).collect::<String>();
                                    json!(format!("{s} ... (trimmed)"))
                                }
                            }
                            Node::Float(f) => json!(f),
                            Node::NegInt(i) => json!(i),
                            Node::PosInt(i) => json!(i),
                        };

                        (location.pointer_str().to_string(), value)
                    })
                    .unwrap_or_else(|| ("<unknown location>".to_string(), json!("<unknown>")));

                let detail = outcome_detail(outcome);

                serde_json::json!({
                    "absoluteKeywordLocation": schema_curi,
                    "instanceLocation": instance_location,
                    "instanceValue": instance_value,
                    "detail": detail,
                })
            },
        )
        .collect();

    serde_json::Value::Array(errors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        schema::{self, build, CoreAnnotation},
        Validator,
    };
    use serde_json::json;

    #[test]
    fn test_build_basic_output_complex_failures() {
        // Complex schema with multiple validation requirements and annotations
        let schema_json = json!({
            "$id": "http://example.com/test-schema",
            "type": "object",
            "title": "Test Schema",
            "description": "A schema to test multiple validation failures",
            "required": ["name", "age", "email", "items", "status", "missing"],
            "dependentRequired": {
                "score": ["dependent-missing"]
            },
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 3,
                    "maxLength": 20,
                    "pattern": "^[A-Za-z]+$",
                    "title": "Person Name",
                    "description": "Must be alphabetic only"
                },
                "age": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 120,
                    "exclusiveMinimum": 0,
                    "multipleOf": 5,
                    "deprecated": true
                },
                "email": {
                    "type": "string",
                    "format": "email",
                    "default": "user@example.com"
                },
                "score": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 100.0,
                    "exclusiveMaximum": 100.0
                },
                "items": {
                    "type": "array",
                    "minItems": 2,
                    "maxItems": 5,
                    "uniqueItems": true,
                    "items": {
                        "type": "string",
                        "minLength": 1
                    },
                    "contains": {
                        "type": "string",
                        "const": "special"
                    },
                    "minContains": 1,
                    "maxContains": 2
                },
                "status": {
                    "enum": ["active", "pending", "inactive"],
                    "readOnly": true
                },
                "metadata": {
                    "type": "object",
                    "minProperties": 1,
                    "maxProperties": 3,
                    "additionalProperties": false,
                    "properties": {
                        "created": {
                            "type": "string",
                            "format": "date-time"
                        }
                    }
                },
                "tags": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "config": {
                    "oneOf": [
                        {"type": "string"},
                        {"type": "object", "properties": {"enabled": {"type": "boolean"}}}
                    ]
                },
                "legacy": {
                    "not": {
                        "type": "null"
                    }
                },
                "code": {
                    "anyOf": [
                        {"type": "string", "pattern": "^[A-Z]{3}$"},
                        {"type": "integer", "minimum": 1000}
                    ]
                },
                "missing": true,
            },
            "additionalProperties": false,
            "if": {
                "properties": {
                    "status": {"const": "active"}
                }
            },
            "then": {
                "required": ["score"]
            },
            "examples": [
                {
                    "name": "John",
                    "age": 25,
                    "email": "john@example.com",
                    "items": ["special"],
                    "status": "active",
                    "score": 85.5
                }
            ]
        });

        // Document fixture that violates multiple constraints
        let doc = json!({
            "name": "ab1",  // Too short, contains non-alphabetic
            "age": 3,  // Not a multiple of 5, violates exclusiveMinimum
            "email": "not-an-email",  // Invalid format
            "score": 100.0,  // Exceeds exclusive maximum
            "items": ["item1", "item1", "item2", "special", "special", "special"],  // Duplicates, too many items, too many "special"
            "status": "unknown",  // Not in enum
            "metadata": {  // Too many properties
                "created": "not-a-date",  // Invalid date format
                "updated": "2024-01-01",
                "deleted": "2024-01-02",
                "extra": "field"
            },
            "tags": [123, 456],  // Wrong type for array items
            "config": true,  // Matches neither oneOf option
            "legacy": null,  // Violates "not" constraint
            "code": "AB",  // Doesn't match anyOf patterns
            "extraField": "not allowed ".repeat(100), // Additional property not allowed
        });

        let url = url::Url::parse("http://example.com/test-schema").unwrap();
        let schema = build::build_schema::<CoreAnnotation>(&url, &schema_json).unwrap();

        let mut index_builder = schema::index::Builder::new();
        index_builder.add(&schema).unwrap();
        let index = index_builder.into_index();

        let mut validator = Validator::new(&index);
        let (is_valid, outcomes) = validator.validate(&schema, &doc, |o| Some(o));

        assert!(!is_valid);
        assert!(!outcomes.is_empty());

        let output = build_basic_output(&doc, &outcomes);
        insta::assert_json_snapshot!(output);
    }
}
