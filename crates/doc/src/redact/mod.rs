use super::{AsNode, HeapField, HeapNode, Valid};
use crate::{BumpStr, BumpVec};
use itertools::Itertools;
use json::validator::{self, Context};
use sha2::Digest;

#[derive(thiserror::Error, Debug, serde::Serialize)]
pub enum Error {
    #[error("sha256 strategy cannot be applied to {0} nodes")]
    UnsupportedSha256NodeType(&'static str),

    #[error("conflicting strategies at this location: {first:?} vs {second:?}")]
    ConflictingStrategies { first: Strategy, second: Strategy },

    #[error("while redacting {:?}", .ptr)]
    WithLocation {
        ptr: String,
        #[source]
        detail: Box<Error>,
    },
}

impl Error {
    fn with_location(self, loc: json::Location) -> Self {
        Error::WithLocation {
            ptr: loc.pointer_str().to_string(),
            detail: Box::new(self),
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

/// Outcome of applying a redaction operation to a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Node was not modified.
    Unchanged,
    /// Node should be removed by its parent.
    /// Contains the tape length of the removed node.
    Remove { tape_length: i32 },
    /// Node was modified in place or had children modified.
    /// Contains the delta in tape length.
    Modified { tape_delta: i32 },
}

/// Strategy for redaction of document locations.
#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(tag = "strategy", deny_unknown_fields, rename_all = "camelCase")]
pub enum Strategy {
    /// Block causes an object property or array item to be removed altogether.
    Block,
    /// Sha256 hashes string values to a hex digest.
    Sha256,
}

impl Strategy {
    fn apply<'alloc>(
        &self,
        node: &mut HeapNode<'alloc>,
        alloc: &'alloc bumpalo::Bump,
        salt: &[u8],
    ) -> Result<Outcome> {
        match self {
            Strategy::Block => {
                // Block strategy removes the node entirely.
                // The parent is responsible for actually removing it from its container.
                let tape_length = node.tape_length();
                Ok(Outcome::Remove { tape_length })
            }
            Strategy::Sha256 => match node {
                HeapNode::String(s)
                    // Check if its already a sha256:digest.
                    // See also json::schema::Format::validate() for Sha256.
                    if s.len() == 71 && &s.as_bytes()[0..7] == b"sha256:"
                        && s[7..].bytes().all(|b| b.is_ascii_hexdigit()) =>
                {
                    Ok(Outcome::Unchanged) // Already a SHA-256 hex digest with prefix.
                }
                HeapNode::String(s) => {
                    let mut sha256 = sha2::Sha256::new();
                    sha256.update(salt);
                    sha256.update(s.as_bytes());
                    let digest = sha256.finalize();
                    *node = HeapNode::String(BumpStr::from_str(&format!("sha256:{digest:x}"), alloc));
                    Ok(Outcome::Modified { tape_delta: 0 })
                }
                HeapNode::Array(_, _) => Err(Error::UnsupportedSha256NodeType("Array")),
                HeapNode::Bool(_) => Err(Error::UnsupportedSha256NodeType("Bool")),
                HeapNode::Bytes(_) => Err(Error::UnsupportedSha256NodeType("Bytes")),
                HeapNode::Float(_) => Err(Error::UnsupportedSha256NodeType("Float")),
                HeapNode::NegInt(_) => Err(Error::UnsupportedSha256NodeType("NegInt")),
                HeapNode::Null => Err(Error::UnsupportedSha256NodeType("Null")),
                HeapNode::Object(_, _) => Err(Error::UnsupportedSha256NodeType("Object")),
                HeapNode::PosInt(_) => Err(Error::UnsupportedSha256NodeType("PosInt")),
            },
        }
    }
}

impl std::convert::TryFrom<&serde_json::Value> for Strategy {
    type Error = serde_json::Error;

    fn try_from(v: &serde_json::Value) -> std::result::Result<Self, Self::Error> {
        <Strategy as serde::Deserialize>::deserialize(v)
    }
}

/// Apply redact annotations to a HeapNode document based on a validation outcome.
/// Returns an Outcome indicating the modification state of the document.
pub fn redact<'alloc>(
    doc: &mut HeapNode<'alloc>,
    valid: Valid,
    alloc: &'alloc bumpalo::Bump,
    salt: &[u8],
) -> Result<Outcome> {
    // Extract sparse tape of redact annotations and their applicable [begin, end) spans.
    let tape: Vec<(i32, i32, &Strategy)> = (valid.validator.outcomes().iter())
        .filter_map(|(outcome, ctx)| {
            if let validator::Outcome::Annotation(crate::Annotation::Redact(strategy)) = outcome {
                let span = ctx.span();
                Some((span.begin as i32, span.end as i32, strategy))
            } else {
                None
            }
        })
        // Order by span `begin`.
        .sorted_by_key(|(begin, _, _)| *begin)
        .collect();

    let mut tape_index = 0i32;

    redact_node(
        &mut tape.as_slice(),
        &mut tape_index,
        json::Location::Root,
        doc,
        alloc,
        salt,
    )
}

// Slice of sparse (span-begin, span-end, redact Strategy) annotations.
// As redaction progresses, matched entries are discarded from the head.
type Tape<'a> = &'a [(i32, i32, &'a Strategy)];

fn redact_node<'alloc, 'schema>(
    tape: &mut Tape<'schema>,
    tape_index: &mut i32,
    loc: json::Location<'_>,
    node: &mut HeapNode<'alloc>,
    alloc: &'alloc bumpalo::Bump,
    salt: &[u8],
) -> Result<Outcome> {
    let next_begin = loop {
        match tape.get(0).copied() {
            None => return Ok(Outcome::Unchanged), // Tape is empty. No further annotations apply.

            Some((span_begin, _, _)) if span_begin < *tape_index => {
                // This can happen if a parent and its child both have `redact` annotations:
                // the parent's annotation is applied and `tape_index` is then advanced past
                // the child. Discard this entry and continue.
                *tape = &tape[1..];
            }
            Some((span_begin, span_end, strategy)) if span_begin == *tape_index => {
                *tape = &tape[1..];

                // Pop additional strategies at the same tape index and check for conflicts.
                while !tape.is_empty() && tape[0].0 == span_begin {
                    let (_, other_end, other_strategy) = tape[0];
                    assert_eq!(span_end, other_end);
                    *tape = &tape[1..];

                    if strategy != other_strategy {
                        return Err(Error::ConflictingStrategies {
                            first: strategy.clone(),
                            second: other_strategy.clone(),
                        }
                        .with_location(loc));
                    }
                }

                *tape_index += node.tape_length();
                assert_eq!(*tape_index, span_end);

                return match strategy.apply(node, alloc, salt) {
                    Ok(outcome) => Ok(outcome),
                    Err(e) => Err(e.with_location(loc)),
                };
            }
            Some((begin, _, _)) => break begin, // Must be greater than tape_index.
        }
    };

    let mut built_delta = 0;
    let mut modified = false;

    match node {
        HeapNode::Object(tape_length, fields) => {
            // Can we prove no redact annotations apply to this node's subtree?
            if *tape_index + *tape_length < next_begin {
                *tape_index += *tape_length;
                return Ok(Outcome::Unchanged);
            }
            *tape_index += 1; // Consume container.

            let mut fields_new = BumpVec::with_capacity_in(fields.len(), alloc);

            for field in fields.iter_mut() {
                match redact_node(
                    tape,
                    tape_index,
                    loc.push_prop(field.property.as_str()),
                    &mut field.value,
                    alloc,
                    salt,
                )? {
                    Outcome::Unchanged => (),
                    Outcome::Remove {
                        tape_length: child_length,
                    } => {
                        built_delta -= child_length;
                        modified = true;
                        continue;
                    }
                    Outcome::Modified {
                        tape_delta: child_delta,
                    } => {
                        built_delta += child_delta;
                        modified = true;
                    }
                };

                fields_new.push(
                    HeapField {
                        property: field.property,
                        value: std::mem::replace(&mut field.value, HeapNode::Null),
                    },
                    alloc,
                );
            }
            std::mem::swap(fields, &mut fields_new);
            *tape_length += built_delta;
        }
        HeapNode::Array(tape_length, items) => {
            // Can we prove no redact annotations apply to this node's subtree?
            if *tape_index + *tape_length < next_begin {
                *tape_index += *tape_length;
                return Ok(Outcome::Unchanged);
            }
            *tape_index += 1; // Consume container.

            let mut items_new = BumpVec::with_capacity_in(items.len(), alloc);

            for (index, item) in items.iter_mut().enumerate() {
                match redact_node(tape, tape_index, loc.push_item(index), item, alloc, salt)? {
                    Outcome::Unchanged => (),
                    Outcome::Remove {
                        tape_length: child_length,
                    } => {
                        built_delta -= child_length;
                        modified = true;
                        continue;
                    }
                    Outcome::Modified {
                        tape_delta: child_delta,
                    } => {
                        built_delta += child_delta;
                        modified = true;
                    }
                };

                items_new.push(std::mem::replace(item, HeapNode::Null), alloc);
            }
            std::mem::swap(items, &mut items_new);
            *tape_length += built_delta;
        }
        _ => {
            *tape_index += 1; // Consume scalar.
        }
    }

    Ok(if modified {
        Outcome::Modified {
            tape_delta: built_delta,
        }
    } else {
        Outcome::Unchanged
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{HeapNode, Validator};
    use json::schema::build::build_schema;
    use serde_json::json;

    #[test]
    fn test_redact_scenarios() {
        const TEST_SALT: &[u8] = b"test-salt";

        // Expected hash value for "hash-me" with TEST_SALT
        const HASH_ME_SHA256: &str =
            "sha256:a9ec3b6826ee77d1577b7e9c5ea49255ae1fbb24245b87130569aa2d928b1398";
        const OTHER_SHA256: &str =
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

        // Recursive schema that:
        // - blocks "secret"
        // - blocks objects or arrays containing "nested-secret"
        // - hashes "hash-me"
        let schema = json!({
            "$defs": {
                "node": {
                    "anyOf": [
                        // Pure recursion for objects - always matches objects, applies recursion
                        {
                            "type": "object",
                            "additionalProperties": {"$ref": "#/$defs/node"}
                        },
                        // Pure recursion for arrays - always matches arrays, applies recursion
                        {
                            "type": "array",
                            "items": {"$ref": "#/$defs/node"}
                        },
                        // All scalar types
                        {
                            "type": ["string", "number", "boolean", "null"]
                        },
                        // Redact scalar "secret" values
                        {
                            "const": "secret",
                            "redact": {"strategy": "block"}
                        },
                        // Hash "hash-me" string values using Sha256
                        {
                            "const": "hash-me",
                            "redact": {"strategy": "sha256"}
                        },
                        // Hash nodes which are already sha256 digests, but we expect this to be idempotent.
                        {
                            "type": "string",
                            "format": "sha256",
                            "redact": {"strategy": "sha256"}
                        },
                        // Redact objects that have a property with "nested-secret" value
                        // Using double-negative: NOT(all properties are NOT "nested-secret")
                        {
                            "type": "object",
                            "not": {
                                "additionalProperties": {"not": {"const": "nested-secret"}}
                            },
                            "redact": {"strategy": "block"}
                        },
                        // Redact arrays containing "nested-secret"
                        {
                            "type": "array",
                            "contains": {"const": "nested-secret"},
                            "redact": {"strategy": "block"}
                        }
                    ]
                }
            },
            "$ref": "#/$defs/node"
        });

        // Table of test cases: (name, input, expected_output, expected_outcome)
        let cases: Vec<(&str, serde_json::Value, serde_json::Value, Outcome)> = vec![
            (
                "simple object with secret field",
                json!({"public": "visible", "hidden": "secret", "another": {"public": "field"}}),
                json!({"public": "visible", "another": {"public": "field"}}),
                Outcome::Modified { tape_delta: -1 }, // Removed 1 scalar "secret"
            ),
            (
                "nested secrets",
                json!({
                    "level1": {
                        "level2": {"secret_val": "secret", "public_val": "visible"},
                        "another_secret": "secret"
                    },
                    "top_level": ["i'm", "public"]
                }),
                json!({
                    "level1": {"level2": {"public_val": "visible"}},
                    "top_level": ["i'm", "public"]
                }),
                Outcome::Modified { tape_delta: -2 }, // Removed 2 scalars "secret"
            ),
            (
                "array with secret elements",
                json!(["public1", "secret", {"nested": "secret"}, "public2", ["inner", "secret", "value"]]),
                json!(["public1", {}, "public2", ["inner", "value"]]),
                Outcome::Modified { tape_delta: -3 }, // Removed 3 scalars "secret"
            ),
            (
                "empty object after redaction",
                json!({"only_secret": "secret"}),
                json!({}),
                Outcome::Modified { tape_delta: -1 }, // Removed 1 scalar "secret"
            ),
            (
                "empty array after redaction",
                json!(["secret", "secret", "secret"]),
                json!([]),
                Outcome::Modified { tape_delta: -3 }, // Removed 3 scalars
            ),
            (
                "nested empty containers",
                json!({"a": {"b": {"c": "secret"}}, "d": ["secret"]}),
                json!({"a": {"b": {}}, "d": []}),
                Outcome::Modified { tape_delta: -2 }, // Removed 2 scalars
            ),
            (
                "object with nested-secret child is redacted entirely",
                json!({"parent": {"child": "nested-secret", "other": "value"}}),
                json!({}), // Parent object with nested-secret child is removed entirely
                Outcome::Modified { tape_delta: -3 }, // Removed entire object (1) + its 2 properties
            ),
            (
                "array with nested-secret in nested array",
                json!([["value1", "nested-secret", "value2"], "other"]),
                json!(["other"]),
                Outcome::Modified { tape_delta: -4 }, // Removed inner array (1) + its 3 elements
            ),
            (
                "nested-secret at different levels",
                json!({
                    "keep": {"normal": "value"},
                    "remove_me": {"special": "nested-secret"},
                    "also_remove": ["a", "nested-secret", "b"]
                }),
                json!({"keep": {"normal": "value"}}),
                Outcome::Modified { tape_delta: -6 }, // Removed object(1) + 2 fields, array(1) + 3 items = 6
            ),
            (
                "mixed secret and nested-secret",
                json!({
                    "a": "secret",
                    "b": {"c": "nested-secret"},
                    "d": ["nested-secret", "value"],
                    "e": {"f": "secret", "g": "keep"}
                }),
                json!({"e": {"g": "keep"}}),
                Outcome::Modified { tape_delta: -7 }, // Removed: "secret"(1), object+child(2), array+2 items(3), "secret"(1) = 7
            ),
            (
                "deeply nested with nested-secret",
                json!({
                    "level1": {
                        "level2": {
                            "level3": {"special": "nested-secret"}
                        }
                    }
                }),
                json!({"level1": {"level2": {}}}),
                Outcome::Modified { tape_delta: -2 }, // Removed level3 object with nested-secret child
            ),
            (
                "complex nested structure",
                json!({
                    "users": [
                        {
                            "name": "Alice",
                            "password": "secret",
                            "metadata": {"role": "admin", "token": "secret"}
                        },
                        {
                            "name": "Bob",
                            "password": "secret",
                            "metadata": {"role": "user"}
                        }
                    ],
                    "config": {
                        "api_key": "secret",
                        "endpoint": "https://api.example.com",
                        "settings": {"debug": false, "secret_flag": "secret"}
                    }
                }),
                json!({
                    "users": [
                        {"name": "Alice", "metadata": {"role": "admin"}},
                        {"name": "Bob", "metadata": {"role": "user"}}
                    ],
                    "config": {
                        "endpoint": "https://api.example.com",
                        "settings": {"debug": false}
                    }
                }),
                Outcome::Modified { tape_delta: -5 }, // Removed 5 scalars "secret"
            ),
            (
                "sha256 hash string values",
                json!({"text": "hash-me", "keep": "normal", "nested": {"value": "hash-me"}}),
                json!({"text": HASH_ME_SHA256, "keep": "normal", "nested": {"value": HASH_ME_SHA256}}),
                Outcome::Modified { tape_delta: 0 }, // No tape length change for Sha256
            ),
            (
                "sha256 idempotency",
                json!({
                    "already_hashed": OTHER_SHA256,
                    "needs_hash": "hash-me",
                    "nested": {
                        "also_hashed": "hash-me",
                        "pre_hashed": HASH_ME_SHA256
                    }
                }),
                json!({
                    "already_hashed": OTHER_SHA256,  // Unchanged.
                    "needs_hash": HASH_ME_SHA256,
                    "nested": {
                        "also_hashed": HASH_ME_SHA256,
                        "pre_hashed": HASH_ME_SHA256  // Unchanged.
                    }
                }),
                Outcome::Modified { tape_delta: 0 },
            ),
            (
                "sha256 in arrays",
                json!(["hash-me", "normal", OTHER_SHA256, "hash-me"]),
                json!([HASH_ME_SHA256, "normal", OTHER_SHA256, HASH_ME_SHA256]),
                Outcome::Modified { tape_delta: 0 },
            ),
            (
                "sha256 deeply nested",
                json!({
                    "level1": {
                        "level2": {
                            "hash_str": "hash-me",
                            "already_hashed": OTHER_SHA256,
                            "level3": {
                                "another": "hash-me",
                                "normal": "keep this"
                            }
                        }
                    }
                }),
                json!({
                    "level1": {
                        "level2": {
                            "hash_str": HASH_ME_SHA256,
                            "already_hashed": OTHER_SHA256,
                            "level3": {
                                "another": HASH_ME_SHA256,
                                "normal": "keep this"
                            }
                        }
                    }
                }),
                Outcome::Modified { tape_delta: 0 },
            ),
            (
                "mixed block and sha256 strategies",
                json!({
                    "public": "visible",
                    "secret_field": "secret",
                    "hash_string": "hash-me",
                    "keep_int": 1234,
                    "nested": {
                        "remove_me": "secret",
                        "hash_another": "hash-me",
                        "keep": "normal"
                    },
                    "array": ["secret", "hash-me", "keep-me", "normal"]
                }),
                json!({
                    "public": "visible",
                    "hash_string": HASH_ME_SHA256,
                    "keep_int": 1234,
                    "nested": {
                        "hash_another": HASH_ME_SHA256,
                        "keep": "normal"
                    },
                    "array": [HASH_ME_SHA256, "keep-me", "normal"]
                }),
                Outcome::Modified { tape_delta: -3 }, // Removed 3 scalars: "secret_field": "secret" (1), "remove_me": "secret" (1), and "secret" in array (1)
            ),
            (
                "unchanged - no redaction annotations apply",
                json!({"normal": "data", "nested": {"also": "normal"}, "array": [1, 2, 3]}),
                json!({"normal": "data", "nested": {"also": "normal"}, "array": [1, 2, 3]}),
                Outcome::Unchanged, // No modifications
            ),
            (
                "unchanged - sha256 already hashed",
                json!({"hash": OTHER_SHA256, "another": HASH_ME_SHA256}),
                json!({"hash": OTHER_SHA256, "another": HASH_ME_SHA256}),
                Outcome::Unchanged, // Already hashed strings are unchanged
            ),
            (
                "root scalar removed",
                json!("secret"),
                json!("secret"),
                Outcome::Remove { tape_length: 1 }, // Root scalar should be removed
            ),
            (
                "root object removed due to nested-secret",
                json!({"has": "nested-secret", "other": "value"}),
                json!({"has": "nested-secret", "other": "value"}),
                Outcome::Remove { tape_length: 3 }, // Root object (1) + 2 properties
            ),
            (
                "root array removed due to nested-secret",
                json!(["item1", "nested-secret", "item2"]),
                json!(["item1", "nested-secret", "item2"]),
                Outcome::Remove { tape_length: 4 }, // Root array (1) + 3 items
            ),
        ];

        let curi = url::Url::parse("http://example/schema").unwrap();
        let mut validator = Validator::new(build_schema(curi, &schema).unwrap()).unwrap();
        let alloc = HeapNode::new_allocator();

        for (name, input, expected, expected_outcome) in cases {
            let mut heap_doc = HeapNode::from_node(&input, &alloc);
            let valid = validator.validate(None, &input).unwrap().ok().unwrap();
            let outcome = redact(&mut heap_doc, valid, &alloc, TEST_SALT).unwrap();

            assert_eq!(
                outcome, expected_outcome,
                "Test case '{name}': outcome mismatch"
            );

            let result = serde_json::to_value(crate::SerPolicy::noop().on(&heap_doc)).unwrap();
            assert_eq!(result, expected, "Test case '{name}' failed");
            assert_eq!(heap_doc.tape_length(), expected.tape_length());
        }
    }

    #[test]
    fn test_conflicting_strategies_error() {
        let schema = json!({
            "anyOf": [
                { "redact": {"strategy": "block"} },
                { "redact": {"strategy": "sha256"} }
            ]
        });

        let curi = url::Url::parse("http://example").unwrap();
        let mut validator = Validator::new(build_schema(curi, &schema).unwrap()).unwrap();
        let alloc = HeapNode::new_allocator();

        let mut doc = HeapNode::from_node(&json!("conflict"), &alloc);
        let valid = validator.validate(None, &doc).unwrap().ok().unwrap();
        let result = redact(&mut doc, valid, &alloc, &[]);

        // Should get an error about conflicting strategies
        insta::assert_debug_snapshot!(result, @r###"
        Err(
            WithLocation {
                ptr: "",
                detail: ConflictingStrategies {
                    first: Sha256,
                    second: Block,
                },
            },
        )
        "###);
    }

    #[test]
    fn test_sha256_unsupported_types_error() {
        const TEST_SALT: &[u8] = b"test-salt";

        // Test cases: (name, input, expected_error_location, expected_error_type)
        let error_cases = vec![
            ("sha256 on null", json!({"field": null}), "/field", "Null"),
            (
                "sha256 on boolean",
                json!({"field": true}),
                "/field",
                "Bool",
            ),
            (
                "sha256 on positive integer",
                json!({"field": 1234}),
                "/field",
                "PosInt",
            ),
            (
                "sha256 on negative integer",
                json!({"field": -1234}),
                "/field",
                "NegInt",
            ),
            ("sha256 on float", json!({"field": 3.14}), "/field", "Float"),
            (
                "sha256 on array",
                json!({"field": [1, 2, 3]}),
                "/field",
                "Array",
            ),
            (
                "sha256 on object",
                json!({"field": {"inner": "value"}}),
                "/field",
                "Object",
            ),
        ];

        for (name, input, expected_location, expected_type) in error_cases {
            // Create a schema that applies sha256 to specific field values
            let schema = json!({
                "type": "object",
                "properties": {
                    "field": {
                        "redact": {"strategy": "sha256"}
                    },
                }
            });

            let curi = url::Url::parse("http://example/schema").unwrap();
            let mut validator = Validator::new(build_schema(curi, &schema).unwrap()).unwrap();
            let alloc = HeapNode::new_allocator();

            let mut heap_doc = HeapNode::from_node(&input, &alloc);
            let valid = validator.validate(None, &input).unwrap().ok().unwrap();
            let result = redact(&mut heap_doc, valid, &alloc, TEST_SALT);

            match result {
                Err(Error::WithLocation { ptr, detail }) => {
                    assert_eq!(ptr, expected_location, "Test {name}");
                    match &*detail {
                        Error::UnsupportedSha256NodeType(node_type) => {
                            assert_eq!(*node_type, expected_type, "Test {name}");
                        }
                        detail => panic!("Test {name}: unexpected {detail:?}"),
                    }
                }
                result => panic!("Test {name}: unexpected {result:?}"),
            }
        }
    }
}
