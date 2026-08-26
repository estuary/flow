//! The Verify comparator.
//!
//! Actual and expected documents are both in collection-key order — the caller
//! combines actuals by key, and `validation` rejects at build time a verify step
//! whose documents are not in key order (`Error::TestVerifyOrder`) — so the
//! comparator walks the two sequences in lock-step, reporting mismatched,
//! missing, and unexpected documents by index.
//!
//! Each pair is compared with [`doc::diff`], which ignores properties of the
//! actual document that the expectation doesn't mention and *locates* each
//! difference — so a failure reports the differing locations rather than two
//! whole documents.

use serde_json::Value;

/// A single verification failure at `doc_index`.
#[derive(Debug, Clone, PartialEq)]
pub enum Mismatch {
    /// Actual and expected documents at this index differed.
    Mismatched {
        doc_index: usize,
        actual: Value,
        expected: Value,
    },
    /// An expected document had no corresponding actual document.
    Missing { doc_index: usize, expected: Value },
    /// An actual document had no corresponding expected document.
    Unexpected { doc_index: usize, actual: Value },
}

impl Mismatch {
    pub fn doc_index(&self) -> usize {
        match self {
            Mismatch::Mismatched { doc_index, .. }
            | Mismatch::Missing { doc_index, .. }
            | Mismatch::Unexpected { doc_index, .. } => *doc_index,
        }
    }
}

/// Compare `actual` documents against `expected`, both already combined by key
/// and ordered by collection key. Returns the failures, in document order;
/// an empty result is a passing verification.
pub fn compare_documents(actual: &[Value], expected: &[Value]) -> Vec<Mismatch> {
    let mut failures = Vec::new();

    let common = actual.len().min(expected.len());
    for index in 0..common {
        if doc::diff(Some(&actual[index]), Some(&expected[index])).is_empty() {
            continue;
        }
        failures.push(Mismatch::Mismatched {
            doc_index: index,
            actual: actual[index].clone(),
            expected: expected[index].clone(),
        });
    }

    for index in common..expected.len() {
        failures.push(Mismatch::Missing {
            doc_index: index,
            expected: expected[index].clone(),
        });
    }
    for index in common..actual.len() {
        failures.push(Mismatch::Unexpected {
            doc_index: index,
            actual: actual[index].clone(),
        });
    }

    failures
}

/// Render verification `failures` as a readable report. Callers prepend the
/// failing test / step context.
///
/// A mismatch renders only its differing locations: the documents are otherwise
/// equal by construction, so printing them whole would bury the difference. A
/// missing or unexpected document has no counterpart to diff against, so the
/// document *is* the message.
pub fn render_failures(failures: &[Mismatch]) -> String {
    use std::fmt::Write;

    let mut out = String::from("actual and expected document(s) did not match:\n");
    for failure in failures {
        match failure {
            Mismatch::Mismatched {
                doc_index,
                actual,
                expected,
            } => {
                let _ = writeln!(out, "mismatched document at index {doc_index}:");
                for difference in doc::diff(Some(actual), Some(expected)) {
                    let _ = writeln!(out, "  {}", render_difference(&difference));
                }
            }
            Mismatch::Missing {
                doc_index,
                expected,
            } => {
                let _ = writeln!(out, "missing expected document at index {doc_index}:");
                let _ = writeln!(out, "  {}", render_value(expected));
            }
            Mismatch::Unexpected { doc_index, actual } => {
                let _ = writeln!(out, "unexpected actual document at index {doc_index}:");
                let _ = writeln!(out, "  {}", render_value(actual));
            }
        }
    }
    out
}

/// Render one located difference as `/at/location: actual X, expect Y`.
/// Either side may be absent, when a location appears in only one document.
fn render_difference(difference: &doc::diff::Diff<'_, '_, Value, Value>) -> String {
    let doc::diff::Diff {
        location,
        actual,
        expect,
        note,
    } = difference;

    let mut parts = Vec::new();
    if let Some(actual) = actual {
        parts.push(format!("actual {}", render_value(actual)));
    }
    if let Some(expect) = expect {
        parts.push(format!("expect {}", render_value(expect)));
    }
    if let Some(note) = note {
        parts.push(note.to_string());
    }
    format!("{location}: {}", parts.join(", "))
}

/// Render a document or value under [`doc::SerPolicy::debug`], which bounds long
/// strings, large arrays, and wide objects. Both consumers need that bound: a
/// terminal for `flowctl raw test`, and a publication's job logs for the agent.
fn render_value(value: &Value) -> String {
    serde_json::to_string(&doc::SerPolicy::debug().on(value))
        .unwrap_or_else(|_| "<unserializable>".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compare_documents_reports_mismatch_missing_unexpected() {
        let actual = vec![
            json!({"k": 1, "v": "a", "extra": true}),
            json!({"k": 2, "v": "WRONG"}),
        ];
        let expected = vec![
            json!({"k": 1, "v": "a"}), // Extra actual properties are ignored.
            json!({"k": 2, "v": "b"}), // Mismatch.
            json!({"k": 3, "v": "c"}), // Missing.
        ];

        let failures = compare_documents(&actual, &expected);
        assert_eq!(failures.len(), 2);
        assert!(matches!(
            &failures[0],
            Mismatch::Mismatched { doc_index: 1, .. }
        ));
        assert!(matches!(
            &failures[1],
            Mismatch::Missing { doc_index: 2, .. }
        ));

        insta::assert_snapshot!(render_failures(&failures), @r###"
        actual and expected document(s) did not match:
        mismatched document at index 1:
          /v: actual "WRONG", expect "b"
        missing expected document at index 2:
          {"k":3,"v":"c"}
        "###);
    }

    #[test]
    fn compare_documents_reports_unexpected_extra_actuals() {
        let actual = vec![json!({"k": 1}), json!({"k": 2})];
        let expected = vec![json!({"k": 1})];

        let failures = compare_documents(&actual, &expected);
        assert_eq!(failures.len(), 1);

        insta::assert_snapshot!(render_failures(&failures), @r###"
        actual and expected document(s) did not match:
        unexpected actual document at index 1:
          {"k":2}
        "###);
    }

    #[test]
    fn passing_verification_is_empty() {
        // A derived document's `_meta` is an extra property of the actual,
        // which the expectation need not mention.
        let actual = vec![json!({"k": 1, "_meta": {"uuid": "flow-uuid"}})];
        let expected = vec![json!({"k": 1})];
        assert!(compare_documents(&actual, &expected).is_empty());

        // Numbers compare across representations, within an epsilon.
        assert!(compare_documents(&[json!({"v": 1.0})], &[json!({"v": 1})]).is_empty());
    }

    #[test]
    fn differences_render_with_their_locations() {
        let actual = vec![json!({"k": 1, "nested": {"a": 1}, "arr": [1, 2]})];
        let expected = vec![json!({"k": 2, "nested": {"a": 2, "b": 3}, "arr": [1, 2, 3]})];

        insta::assert_snapshot!(render_failures(&compare_documents(&actual, &expected)), @r###"
        actual and expected document(s) did not match:
        mismatched document at index 0:
          /arr/2: expect 3, missing in actual document
          /k: actual 1, expect 2
          /nested/a: actual 1, expect 2
          /nested/b: expect 3, missing in actual document
        "###);
    }
}
