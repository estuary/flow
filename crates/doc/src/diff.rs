use super::SerPolicy;
use itertools::{
    EitherOrBoth::{Both, Left, Right},
    Itertools,
};
use json::{AsNode, Field, Fields, Location, Node};

/// Diff an actual (observed) document against an expected document,
/// pushing all detected differences into a Vec. Object properties
/// which are in the actual document but not the expected document
/// are ignored, but all other locations must match.
pub fn diff<'a, 'e, A: AsNode, E: AsNode>(
    actual: Option<&'a A>,
    expect: Option<&'e E>,
) -> Vec<Diff<'a, 'e, A, E>> {
    let mut out = Vec::new();
    Diff::diff_inner(actual, expect, &Location::Root, &mut out);
    out
}

/// Diff is a detected difference within a document.
#[derive(Debug)]
pub struct Diff<'a, 'e, A: AsNode, E: AsNode> {
    /// JSON-Pointer location of the difference.
    pub location: String,
    /// Actual value at the document location.
    pub actual: Option<&'a A>,
    /// Expected value at the document location.
    pub expect: Option<&'e E>,
    pub note: Option<&'static str>,
}

impl<'a, 'e, A: AsNode, E: AsNode> serde::Serialize for Diff<'a, 'e, A, E> {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        let mut map = s.serialize_map(None)?;
        map.serialize_entry("location", &self.location)?;

        if let Some(v) = self.actual {
            map.serialize_entry("actual", &SerPolicy::debug().on(v))?;
        }
        if let Some(v) = self.expect {
            map.serialize_entry("expect", &SerPolicy::debug().on(v))?;
        }
        if let Some(note) = self.note {
            map.serialize_entry("note", note)?;
        }
        map.end()
    }
}

impl<'a, 'e, A: AsNode, E: AsNode> Diff<'a, 'e, A, E> {
    fn diff_inner(
        actual: Option<&'a A>,
        expect: Option<&'e E>,
        location: &Location,
        out: &mut Vec<Self>,
    ) {
        let actual_node = actual.map(AsNode::as_node);
        let expect_node = expect.map(AsNode::as_node);

        // A float compares to a number of any representation under a
        // magnitude-scaled epsilon: JSON Schema draws no distinction between the
        // integer `0` and the float `0.0`, and neither do we, while reductions
        // introduce floaty funny bitness that an exact compare would report as a
        // difference.
        if let Some((actual_f64, expect_f64)) = actual_node
            .as_ref()
            .zip(expect_node.as_ref())
            .and_then(|(actual, expect)| epsilon_pair(actual, expect))
        {
            if !f64_eq(actual_f64, expect_f64) {
                out.push(Diff {
                    location: location.pointer_str().to_string(),
                    actual,
                    expect,
                    note: None,
                });
            }
            return;
        }

        match (actual_node, expect_node) {
            (Some(Node::Object(actual)), Some(Node::Object(expect))) => {
                for eob in actual
                    .iter()
                    .merge_join_by(expect.iter(), |l, r| l.property().cmp(r.property()))
                {
                    match eob {
                        Left(_actual) => {
                            // Ignore properties of `actual` not in `expect`.
                        }
                        Right(expect) => {
                            Self::diff_inner(
                                None,
                                Some(expect.value()),
                                &location.push_prop(expect.property()),
                                out,
                            );
                        }
                        Both(actual, expect) => {
                            Self::diff_inner(
                                Some(actual.value()),
                                Some(expect.value()),
                                &location.push_prop(actual.property()),
                                out,
                            );
                        }
                    }
                }
            }
            (Some(Node::Array(actual)), Some(Node::Array(expect))) => {
                for (index, eob) in actual.iter().zip_longest(expect.iter()).enumerate() {
                    Self::diff_inner(
                        eob.as_ref().left().cloned(),
                        eob.as_ref().right().cloned(),
                        &location.push_item(index),
                        out,
                    );
                }
            }
            // For remaining scalar cases, or differing types, fall back to basic equality.
            (Some(_), Some(_)) if json::node::compare(actual.unwrap(), expect.unwrap()).is_eq() => {
            }
            // Technically allowed for someone to pass in None, None.
            (None, None) => {}

            _ => {
                out.push(Diff {
                    location: location.pointer_str().to_string(),
                    actual,
                    expect,
                    note: if actual.is_none() {
                        Some("missing in actual document")
                    } else if expect.is_none() {
                        Some("unexpected in actual document")
                    } else {
                        None
                    },
                });
            }
        }
    }
}

/// The `f64` values of `actual` and `expect` if the pair should compare under a
/// scaled epsilon: both are numbers, and at least one is a float.
fn epsilon_pair<A: AsNode, E: AsNode>(
    actual: &Node<'_, A>,
    expect: &Node<'_, E>,
) -> Option<(f64, f64)> {
    let pair = (as_f64(actual)?, as_f64(expect)?);

    (matches!(actual, Node::Float(_)) || matches!(expect, Node::Float(_))).then_some(pair)
}

/// The `f64` value of a numeric node, or None if it isn't a number.
fn as_f64<N: AsNode>(node: &Node<'_, N>) -> Option<f64> {
    match node {
        Node::Float(v) => Some(*v),
        Node::NegInt(v) => Some(*v as f64),
        Node::PosInt(v) => Some(*v as f64),
        _ => None,
    }
}

fn f64_eq(actual: f64, expected: f64) -> bool {
    // Start with the machine epsilon and scale it up based on the relative size of the numbers
    let epsilon = f64::EPSILON * (actual.abs().max(expected.abs())).max(1.0);
    let diff = (actual - expected).abs();
    diff <= epsilon
}

#[cfg(test)]
mod test {
    use super::{diff, f64_eq};
    use serde_json::json;

    // I'm paranoid about the compiler pre-computing the math on constants.
    // Using this ensures that the math will always be performed as normal.
    #[inline(never)]
    fn sub(a: f64, b: f64) -> f64 {
        a - b
    }

    #[test]
    fn test_f64_eq() {
        assert!(f64_eq(0.0, 0.0));
        assert!(f64_eq(0.0, sub(4.01, 4.01)));
        assert!(f64_eq(3.3E-12, sub(6.6E-12, 3.3E-12)));
        assert!(f64_eq(2.0, sub(2.0, f64::EPSILON)));
        // Even for tiny numbers, we never scale the machine epsilon down, so these cases document
        // that behavior. We might change this in the future if we end up with a motivating use
        // case, but for now I can't think of one.
        assert!(f64_eq(0.0, sub(0.0, f64::EPSILON)));
        assert!(f64_eq(1.0E-10, sub(1.0E-10, f64::EPSILON)));
        assert!(f64_eq(1.01E-16, 1.04E-16));
        // This was an actual failure during catalog tests
        assert!(f64_eq(1.3999999999999775, 1.3999999999999773));

        assert!(!f64_eq(1.0, 1.00001));
        assert!(!f64_eq(4.56E+10, 4.5603E+10));
    }

    #[test]
    fn test_numeric_epsilon_across_representations() {
        let case = |equal, actual: serde_json::Value, expect: serde_json::Value| {
            let out = diff(Some(&actual), Some(&expect));
            assert_eq!(equal, out.is_empty(), "{actual} vs {expect}: {out:?}");
        };

        // Reduction drift lands an integral expectation on a neighboring float.
        case(true, json!(8.000000000000002), json!(8));
        // A float and an integer of the same value are the same number.
        case(true, json!(0), json!(0.0));
        case(true, json!(-42.0), json!(-42));
        // The epsilon floors at machine epsilon and never scales down,
        // so values very near zero compare equal to zero.
        case(true, json!(1e-17), json!(0.0));

        case(false, json!(1.0), json!(1.00001));
        case(false, json!(41), json!(42));

        // Two integers compare exactly: no epsilon widens with their magnitude,
        // so distinct IDs and nanosecond timestamps stay distinct.
        case(true, json!(u64::MAX), json!(u64::MAX));
        case(
            false,
            json!(9007199254740993u64),
            json!(9007199254740992u64),
        );
        case(false, json!(u64::MAX), json!(u64::MAX - 3000));
        case(
            false,
            json!(1750000000000000000u64),
            json!(1750000000000000300u64),
        );
        case(false, json!(i64::MIN), json!(i64::MIN + 808));
    }

    #[test]
    fn test_output_detail() {
        let expect = json!({
            "longer": [
                true,
                {
                    "bool-eq": true,
                    "bool-ne": true,
                    "float-eq": 4.2,
                    "float-ne": 4.2,
                    "missing": null,
                    "null-eq": null,
                    "null-ne": null,
                    "signed-eq": -42,
                    "signed-ne": -42,
                    "unsigned-eq": 32,
                    "unsigned-ne": 32,
                },
                {"extra": 1},
            ],
            "shorter": [false],
        });
        let actual = json!({
            "longer": [
                true,
                {
                    "bool-eq": true,
                    "bool-ne": false,
                    "float-eq": 4.2,
                    "float-ne": 4.0,
                    "null-eq": null,
                    "null-ne": 1,
                    "signed-eq": -42,
                    "signed-ne": -40,
                    "unsigned-eq": 32,
                    "unsigned-ne": 30,
                },
                // missing extra
            ],
            "shorter": [
                false,
                true, // Extra.
            ],
        });

        let out = diff(Some(&actual), Some(&expect));

        insta::assert_json_snapshot!(&out, @r###"
        [
          {
            "location": "/longer/1/bool-ne",
            "actual": false,
            "expect": true
          },
          {
            "location": "/longer/1/float-ne",
            "actual": 4.0,
            "expect": 4.2
          },
          {
            "location": "/longer/1/missing",
            "expect": null,
            "note": "missing in actual document"
          },
          {
            "location": "/longer/1/null-ne",
            "actual": 1,
            "expect": null
          },
          {
            "location": "/longer/1/signed-ne",
            "actual": -40,
            "expect": -42
          },
          {
            "location": "/longer/1/unsigned-ne",
            "actual": 30,
            "expect": 32
          },
          {
            "location": "/longer/2",
            "expect": {
              "extra": 1
            },
            "note": "missing in actual document"
          },
          {
            "location": "/shorter/1",
            "actual": true,
            "note": "unexpected in actual document"
          }
        ]
        "###);
    }

    #[test]
    fn test_subset_cases() {
        let case = |result, actual, expect| {
            assert_eq!(result, diff(Some(&actual), Some(&expect)).is_empty());
        };

        case(true, json!({}), json!({}));
        case(false, json!({}), json!({"a": 42}));
        case(true, json!({"a": 42, "b": 1}), json!({"a": 42}));
        case(
            false,
            json!({"a": 42, "b": 1, "c": []}),
            json!({"a": 42, "c": {"d": 5}}),
        );
        case(
            false,
            json!({"a": 42, "b": 1, "c": {"d": 6}}),
            json!({"a": 42, "c": {"d": 5}}),
        );
        case(
            true,
            json!({"a": 42, "b": 1, "c": {"d": 5}}),
            json!({"a": 42, "c": {"d": 5}}),
        );
        case(
            false,
            json!({"a": "43", "b": 1, "c": {"d": 5}}),
            json!({"a": 42, "c": {"d": 5}}),
        );
    }
}
