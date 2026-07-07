//! The Verify comparator, ported from `go/testing/driver.go`.
//!
//! Verify combines a collection's actual documents by key (done by the caller,
//! producing one document per key in collection-key order) and compares them
//! against the step's expected documents, which build-time validation
//! guarantees are also in collection-key order. The comparison:
//!
//! - **superset match** — an actual document passes if it is equal to, or a
//!   superset of, the expected document (actual may carry extra fields);
//! - **scaled-epsilon float compare** — numbers compare with a string fast-path,
//!   then an `f64` compare with `FLT_EPSILON` scaled to magnitude (ports
//!   `compareNumbers`), tolerating precision drift from float math;
//! - **UUID masking** — document UUIDs are replaced with the placeholder
//!   `"flow-uuid"` before comparison, so tests are deterministic.
//!
//! The comparator walks the actual and expected lists in lock-step (as V1
//! does), reporting mismatched, missing, and unexpected documents.

use serde_json::Value;

/// `FLT_EPSILON`, the "machine epsilon": `nextafter(1.0, 2.0) - 1.0`.
const EPSILON: f64 = f64::EPSILON;

/// The placeholder substituted for document UUIDs before comparison.
pub const UUID_MASK: &str = "flow-uuid";

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
        if !superset_match(&actual[index], &expected[index]) {
            failures.push(Mismatch::Mismatched {
                doc_index: index,
                actual: actual[index].clone(),
                expected: expected[index].clone(),
            });
        }
    }

    // Remaining expected documents were never produced.
    for index in common..expected.len() {
        failures.push(Mismatch::Missing {
            doc_index: index,
            expected: expected[index].clone(),
        });
    }
    // Remaining actual documents were unexpected.
    for index in common..actual.len() {
        failures.push(Mismatch::Unexpected {
            doc_index: index,
            actual: actual[index].clone(),
        });
    }

    failures
}

/// True if `actual` equals, or is a superset of, `expected`:
/// - objects: every `expected` key is present in `actual` with a superset value
///   (extra `actual` keys are allowed);
/// - arrays: same length and element-wise superset (matching jsondiff, which
///   reports differing lengths as a mismatch);
/// - numbers: [`compare_numbers`] (scaled epsilon);
/// - other scalars: exact equality.
pub fn superset_match(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Object(a), Value::Object(e)) => e.iter().all(|(key, e_val)| {
            a.get(key)
                .map(|a_val| superset_match(a_val, e_val))
                .unwrap_or(false)
        }),
        (Value::Array(a), Value::Array(e)) => {
            a.len() == e.len() && a.iter().zip(e).all(|(a, e)| superset_match(a, e))
        }
        (Value::Number(a), Value::Number(e)) => compare_numbers(a, e),
        _ => actual == expected,
    }
}

/// Compare two JSON numbers with a scaled epsilon, ported from `compareNumbers`
/// in `driver.go`.
///
/// The string representations are compared first: this is a fast path for exact
/// matches and a meaningful fallback for values out of `f64` range. Otherwise
/// the epsilon is scaled by the larger magnitude, since `FLT_EPSILON` is smaller
/// than the gap between adjacent floats above 2.0 and can exceed values below
/// 1.0.
pub fn compare_numbers(a: &serde_json::Number, b: &serde_json::Number) -> bool {
    if a.to_string() == b.to_string() {
        return true;
    }
    let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) else {
        // Out of `f64` range and string reprs already differ.
        return false;
    };
    let scaled_epsilon = EPSILON * af.abs().max(bf.abs());
    (af - bf).abs() < scaled_epsilon
}

/// Replace the value at `uuid_ptr` (a JSON Pointer such as `/_meta/uuid`) with
/// [`UUID_MASK`], if present. Applied to actual documents before comparison so
/// their synthetic UUIDs don't defeat matching. A no-op when `uuid_ptr` is empty
/// or the location is absent.
pub fn mask_uuid(doc: &mut Value, uuid_ptr: &str) {
    if uuid_ptr.is_empty() {
        return;
    }
    if let Some(slot) = doc.pointer_mut(uuid_ptr) {
        *slot = Value::String(UUID_MASK.to_string());
    }
}

/// Render verification `failures` as a readable multi-document report, mirroring
/// V1's `FailedVerifies` output. Callers prepend the failing test / step context.
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
                let _ = writeln!(out, "  actual:   {}", compact(actual));
                let _ = writeln!(out, "  expected: {}", compact(expected));
            }
            Mismatch::Missing {
                doc_index,
                expected,
            } => {
                let _ = writeln!(out, "missing expected document at index {doc_index}:");
                let _ = writeln!(out, "  {}", compact(expected));
            }
            Mismatch::Unexpected { doc_index, actual } => {
                let _ = writeln!(out, "unexpected actual document at index {doc_index}:");
                let _ = writeln!(out, "  {}", compact(actual));
            }
        }
    }
    out
}

fn compact(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "<unserializable>".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn superset_allows_extra_actual_fields() {
        // Actual carries an extra field; still a superset of expected.
        assert!(superset_match(
            &json!({"a": 1, "b": 2, "_meta": {"uuid": "x"}}),
            &json!({"a": 1, "b": 2}),
        ));
        // Missing an expected field fails.
        assert!(!superset_match(&json!({"a": 1}), &json!({"a": 1, "b": 2})));
        // Nested objects recurse.
        assert!(superset_match(
            &json!({"o": {"a": 1, "extra": 9}}),
            &json!({"o": {"a": 1}}),
        ));
        // Arrays must match length and element-wise.
        assert!(superset_match(&json!([1, 2, 3]), &json!([1, 2, 3])));
        assert!(!superset_match(&json!([1, 2]), &json!([1, 2, 3])));
        assert!(superset_match(
            &json!([{"a": 1, "x": 0}]),
            &json!([{"a": 1}]),
        ));
    }

    #[test]
    fn scaled_epsilon_number_compare() {
        // Exact string match (integer vs integer).
        assert!(compare_numbers(&num("1"), &num("1")));
        // 1.0 vs 1 — different string reprs, equal as floats within epsilon.
        assert!(compare_numbers(&num("1.0"), &num("1")));
        // A one-ULP perturbation at magnitude 100 (~1.42e-14) is within the
        // scaled epsilon (epsilon*100 ≈ 2.22e-14) and is tolerated.
        let a = serde_json::Number::from_f64(100.0).unwrap();
        let one_ulp = f64::from_bits(100.0f64.to_bits() + 1);
        let b = serde_json::Number::from_f64(one_ulp).unwrap();
        assert_ne!(a.to_string(), b.to_string(), "distinct string reprs");
        assert!(compare_numbers(&a, &b));
        // Genuinely different numbers do not match.
        assert!(!compare_numbers(&num("1.0"), &num("2.0")));
        // Superset match uses the epsilon compare for numbers.
        assert!(superset_match(&json!({"v": 1.0}), &json!({"v": 1})));
    }

    fn num(s: &str) -> serde_json::Number {
        serde_json::from_str(s).unwrap()
    }

    #[test]
    fn mask_uuid_replaces_placeholder() {
        let mut doc = json!({"id": "a", "_meta": {"uuid": "0000-real-uuid"}});
        mask_uuid(&mut doc, "/_meta/uuid");
        assert_eq!(doc, json!({"id": "a", "_meta": {"uuid": "flow-uuid"}}));

        // Absent location and empty pointer are no-ops.
        let mut doc2 = json!({"id": "b"});
        mask_uuid(&mut doc2, "/_meta/uuid");
        assert_eq!(doc2, json!({"id": "b"}));
        mask_uuid(&mut doc2, "");
        assert_eq!(doc2, json!({"id": "b"}));
    }

    #[test]
    fn compare_documents_reports_mismatch_missing_unexpected() {
        let actual = vec![
            json!({"k": 1, "v": "a", "extra": true}),
            json!({"k": 2, "v": "WRONG"}),
        ];
        let expected = vec![
            json!({"k": 1, "v": "a"}), // superset match — pass
            json!({"k": 2, "v": "b"}), // mismatch
            json!({"k": 3, "v": "c"}), // missing
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
    }

    #[test]
    fn compare_documents_reports_unexpected_extra_actuals() {
        let actual = vec![json!({"k": 1}), json!({"k": 2})];
        let expected = vec![json!({"k": 1})];

        let failures = compare_documents(&actual, &expected);
        assert_eq!(failures.len(), 1);
        assert!(matches!(
            &failures[0],
            Mismatch::Unexpected { doc_index: 1, .. }
        ));
    }

    #[test]
    fn passing_verification_is_empty() {
        let actual = vec![json!({"k": 1, "_meta": {"uuid": "flow-uuid"}})];
        let expected = vec![json!({"k": 1})];
        assert!(compare_documents(&actual, &expected).is_empty());
    }
}
