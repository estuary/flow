use estuary_json::Location;
use itertools::{
    EitherOrBoth::{Both, Left, Right},
    Itertools,
};
use serde::Serialize;
use serde_json::Value;

/// Diff is a detected difference within a document.
#[derive(Serialize, Debug)]
pub struct Diff {
    /// JSON-Pointer location of the difference.
    pub location: String,
    /// Actual value at the document location.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<Value>,
    /// Expected value at the document location.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expect: Option<Value>,
}

impl Diff {
    /// Diff an actual (observed) document against an expected document,
    /// pushing all detected differences into a Vec. Object properties
    /// which are in the actual document but not the expected document
    /// are ignored, but all other locations must match.
    pub fn diff(
        actual: Option<&Value>,
        expect: Option<&Value>,
        location: &Location,
        out: &mut Vec<Diff>,
    ) {
        match (actual, expect) {
            (Some(Value::Object(actual)), Some(Value::Object(expect))) => {
                for eob in actual
                    .iter()
                    .merge_join_by(expect.into_iter(), |(l, _), (r, _)| l.cmp(r))
                {
                    match eob {
                        Left((_p, _actual)) => {
                            // Ignore properties of |actual| not in |expect|.
                        }
                        Right((p, expect)) => {
                            Self::diff(None, Some(expect), &location.push_prop(p), out);
                        }
                        Both((p, actual), (_, expect)) => {
                            Self::diff(Some(actual), Some(expect), &location.push_prop(p), out);
                        }
                    }
                }
            }
            (Some(Value::Array(actual)), Some(Value::Array(expect))) => {
                for (index, eob) in actual.iter().zip_longest(expect.iter()).enumerate() {
                    Self::diff(
                        eob.as_ref().left().cloned(),
                        eob.as_ref().right().cloned(),
                        &location.push_item(index),
                        out,
                    );
                }
            }
            _ if expect == actual => {}
            _ => {
                out.push(Diff {
                    location: format!("{}", location.pointer_str()),
                    expect: expect.cloned(),
                    actual: actual.cloned(),
                });
            }
        }
    }
}

#[cfg(test)]

mod test {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_diff_cases() {
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

        let root = Location::Root;
        let mut out = Vec::new();
        Diff::diff(Some(&actual), Some(&expect), &root, &mut out);

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
            "expect": null
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
            }
          },
          {
            "location": "/shorter/1",
            "actual": true
          }
        ]
        "###);
    }
}
