use super::{count_nodes, reduce_item, reduce_prop, Cursor, Error, Reducer, Result};
use estuary_json::{json_cmp, json_cmp_at};
use itertools::EitherOrBoth;
use serde_json::Value;
use std::cmp::Ordering;
use std::convert::TryFrom;

pub use crate::doc::reduce::{Maximize, Merge, Minimize, Strategy};

/*
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "strategy", deny_unknown_fields, rename_all = "camelCase")]
pub enum Strategy {
    /// FirstWriteWins keeps the LHS value.
    FirstWriteWins,
    /// LastWriteWins takes the RHS value.
    LastWriteWins,
    /// Maximize keeps the greater of the LHS & RHS.
    /// A provided key, if present, determines the relative ordering.
    /// If values are equal, they're deeply merged.
    Maximize(Maximize),
    /// Minimize keeps the smaller of the LHS & RHS.
    /// A provided key, if present, determines the relative ordering.
    /// If values are equal, they're deeply merged.
    Minimize(Minimize),
    /// Sum the LHS and RHS.
    /// If either LHS or RHS are not numbers, Sum behaves as LastWriteWins.
    Sum,
    /// Merge recursively reduces shared document locations.
    ///
    /// If LHS and RHS are both Objects then it perform a deep merge of each property.
    ///
    /// If LHS and RHS are both Arrays then items at corresponding indexes are deeply
    /// merged. The shorter of the two arrays is extended by taking items of the longer.
    ///
    /// In all other cases, Merge behaves as LastWriteWins.
    Merge(Merge),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Minimize {
    #[serde(default)]
    key: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Maximize {
    #[serde(default)]
    key: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Merge {
    #[serde(default)]
    key: Vec<String>,
}

impl std::convert::TryFrom<&Value> for Strategy {
    type Error = serde_json::Error;

    fn try_from(v: &Value) -> std::result::Result<Self, Self::Error> {
        Strategy::deserialize(v)
    }
}
*/

impl Reducer for Strategy {
    fn reduce(&self, cur: Cursor) -> Result<Value> {
        match self {
            Strategy::FirstWriteWins => Self::first_write_wins(cur),
            Strategy::LastWriteWins => Self::last_write_wins(cur),
            Strategy::Maximize(max) => Self::maximize(cur, max),
            Strategy::Minimize(min) => Self::minimize(cur, min),
            Strategy::Sum => Self::sum(cur),
            Strategy::Merge(merge) => Self::merge(cur, merge),
            Strategy::Subtract(_) | Strategy::Append => panic!("not implemented"),
        }
    }
}

impl Strategy {
    fn first_write_wins(cur: Cursor) -> Result<Value> {
        match cur {
            Cursor::Right { tape, rhs, .. } => {
                *tape = &tape[count_nodes(&rhs)..];
                Ok(rhs)
            }
            Cursor::Both { tape, lhs, rhs, .. } => {
                *tape = &tape[count_nodes(&rhs)..];
                Ok(lhs)
            }
        }
    }

    fn last_write_wins(cur: Cursor) -> Result<Value> {
        match cur {
            Cursor::Right { tape, rhs, .. } | Cursor::Both { tape, rhs, .. } => {
                *tape = &tape[count_nodes(&rhs)..];
                Ok(rhs)
            }
        }
    }

    fn min_max_helper(cur: Cursor, key: &[String], reverse: bool) -> Result<Value> {
        match cur {
            Cursor::Right { tape, rhs, .. } => {
                *tape = &tape[count_nodes(&rhs)..];
                Ok(rhs)
            }
            Cursor::Both {
                tape,
                loc,
                prune,
                lhs,
                rhs,
            } => {
                let ord = match (key.is_empty(), reverse) {
                    (false, false) => json_cmp_at(key, &lhs, &rhs),
                    (false, true) => json_cmp_at(key, &rhs, &lhs),
                    (true, false) => json_cmp(&lhs, &rhs),
                    (true, true) => json_cmp(&rhs, &lhs),
                };

                match ord {
                    Ordering::Less => {
                        *tape = &tape[count_nodes(&rhs)..];
                        Ok(lhs)
                    }
                    Ordering::Greater => {
                        *tape = &tape[count_nodes(&rhs)..];
                        Ok(rhs)
                    }
                    Ordering::Equal if !key.is_empty() => {
                        let cur = Cursor::Both {
                            tape,
                            loc,
                            prune,
                            lhs,
                            rhs,
                        };
                        Self::merge_with_key(key, cur)
                    }
                    Ordering::Equal => {
                        *tape = &tape[count_nodes(&rhs)..];
                        Ok(lhs)
                    }
                }
            }
        }
    }

    fn minimize(cur: Cursor, min: &Minimize) -> Result<Value> {
        Self::min_max_helper(cur, &min.key, false)
    }

    fn maximize(cur: Cursor, max: &Maximize) -> Result<Value> {
        Self::min_max_helper(cur, &max.key, true)
    }

    fn sum(cur: Cursor) -> Result<Value> {
        match cur {
            Cursor::Both {
                tape,
                loc,
                lhs: Value::Number(lhs),
                rhs: Value::Number(rhs),
                ..
            } => {
                *tape = &tape[1..];

                let sum = estuary_json::Number::checked_add((&lhs).into(), (&rhs).into());
                let sum = sum
                    .ok_or(Error::SumNumericOverflow)
                    .map_err(|err| Error::at(loc, err))?;

                Ok(Value::try_from(sum).unwrap())
            }
            Cursor::Right {
                tape,
                rhs: Value::Number(rhs),
                ..
            } => {
                *tape = &tape[1..];
                Ok(Value::Number(rhs))
            }
            cur => Err(Error::cursor(cur, Error::SumWrongType)),
        }
    }

    fn merge_with_key(key: &[String], cur: Cursor) -> Result<Value> {
        match cur {
            // Merge of Object <= Object.
            Cursor::Both {
                tape,
                loc,
                prune,
                lhs: Value::Object(lhs),
                rhs: Value::Object(rhs),
            } => {
                *tape = &tape[1..]; // Increment for self.

                let m = itertools::merge_join_by(lhs.into_iter(), rhs.into_iter(), |lhs, rhs| {
                    lhs.0.cmp(&rhs.0)
                })
                .map(|eob| reduce_prop(tape, loc, prune, eob))
                .collect::<Result<_>>()?;

                Ok(Value::Object(m))
            }
            // Merge of Undefined <= Object.
            Cursor::Right {
                tape,
                loc,
                prune,
                rhs: Value::Object(rhs),
            } => {
                *tape = &tape[1..];

                let m = rhs
                    .into_iter()
                    .map(|prop| reduce_prop(tape, loc, prune, EitherOrBoth::Right(prop)))
                    .collect::<Result<_>>()?;

                Ok(Value::Object(m))
            }
            // Merge of Array <=> Array.
            Cursor::Both {
                tape,
                loc,
                prune,
                lhs: Value::Array(lhs),
                rhs: Value::Array(rhs),
            } => {
                *tape = &tape[1..];

                let m = itertools::merge_join_by(
                    lhs.into_iter().enumerate(),
                    rhs.into_iter().enumerate(),
                    |(lhs_ind, lhs), (rhs_ind, rhs)| -> Ordering {
                        if key.is_empty() {
                            lhs_ind.cmp(rhs_ind)
                        } else {
                            json_cmp_at(key, lhs, rhs)
                        }
                    },
                )
                .map(|eob| reduce_item(tape, loc, prune, eob))
                .collect::<Result<_>>()?;

                Ok(Value::Array(m))
            }
            // Merge of Undefined <= Array.
            Cursor::Right {
                tape,
                loc,
                prune,
                rhs: Value::Array(rhs),
            } => {
                *tape = &tape[1..];

                let m = rhs
                    .into_iter()
                    .enumerate()
                    .map(|item| reduce_item(tape, loc, prune, EitherOrBoth::Right(item)))
                    .collect::<Result<_>>()?;

                Ok(Value::Array(m))
            }

            cur => Err(Error::cursor(cur, Error::MergeWrongType)),
        }
    }

    fn merge(cur: Cursor, merge: &Merge) -> Result<Value> {
        Self::merge_with_key(&merge.key, cur)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::doc::{
        extract_reduce_annotations, validate, FullContext, Schema, SchemaIndex, Validator,
    };
    use estuary_json::{schema::build::build_schema, Location};
    use serde_json::{json, Value};
    use std::error::Error as StdError;

    struct Case {
        rhs: Value,
        expect: Result<Value>,
    }

    fn run_reduce_cases(schema: Value, cases: Vec<Case>) {
        let curi = url::Url::parse("http://example/schema").unwrap();
        let schema: Schema = build_schema(curi.clone(), &schema).unwrap();

        let mut index = SchemaIndex::new();
        index.add(&schema).unwrap();
        index.verify_references().unwrap();
        let index = Box::leak(Box::new(index)); // Coerce 'static lifetime.

        let mut validator = Validator::<FullContext>::new(index);
        let mut lhs: Option<Value> = None;

        let prune = false;

        for case in cases {
            let Case { rhs, expect } = case;

            let span = validate(&mut validator, &curi, &rhs).unwrap();
            let tape = extract_reduce_annotations(span, validator.outcomes());
            let tape = &mut tape.as_slice();

            let cursor = match &lhs {
                Some(lhs) => Cursor::Both {
                    tape,
                    loc: Location::Root,
                    lhs: lhs.clone(),
                    rhs,
                    prune,
                },
                None => Cursor::Right {
                    tape,
                    loc: Location::Root,
                    rhs,
                    prune,
                },
            };

            let reduced = cursor.reduce();

            match expect {
                Ok(expect) => {
                    let reduced = reduced.unwrap();
                    assert!(tape.is_empty());
                    assert_eq!(&reduced, &expect);
                    lhs = Some(reduced);
                }
                Err(expect) => {
                    let reduced = reduced.unwrap_err();
                    let mut reduced: &dyn StdError = &reduced;

                    while let Some(r) = reduced.source() {
                        reduced = r;
                    }
                    assert_eq!(format!("{}", reduced), format!("{}", expect));
                }
            }
        }
    }

    #[test]
    fn test_last_write_wins() {
        run_reduce_cases(
            json!(true),
            vec![
                Case {
                    rhs: json!("foo"),
                    expect: Ok(json!("foo")),
                },
                Case {
                    rhs: json!({"n": 42}),
                    expect: Ok(json!({"n": 42})),
                },
                Case {
                    rhs: json!(null),
                    expect: Ok(json!(null)),
                },
            ],
        )
    }

    #[test]
    fn test_first_write_wins() {
        run_reduce_cases(
            json!({ "reduce": { "strategy": "firstWriteWins" } }),
            vec![
                Case {
                    rhs: json!("foo"),
                    expect: Ok(json!("foo")),
                },
                Case {
                    rhs: json!({"n": 42}),
                    expect: Ok(json!("foo")),
                },
                Case {
                    rhs: json!(null),
                    expect: Ok(json!("foo")),
                },
            ],
        )
    }

    #[test]
    fn test_minimize_simple() {
        run_reduce_cases(
            json!({ "reduce": { "strategy": "minimize" } }),
            vec![
                Case {
                    rhs: json!(3),
                    expect: Ok(json!(3)),
                },
                Case {
                    rhs: json!(4),
                    expect: Ok(json!(3)),
                },
                Case {
                    rhs: json!(3),
                    expect: Ok(json!(3)),
                },
                Case {
                    rhs: json!(2),
                    expect: Ok(json!(2)),
                },
            ],
        )
    }

    #[test]
    fn test_maximize_simple() {
        run_reduce_cases(
            json!({ "reduce": { "strategy": "maximize" } }),
            vec![
                Case {
                    rhs: json!(3),
                    expect: Ok(json!(3)),
                },
                Case {
                    rhs: json!(4),
                    expect: Ok(json!(4)),
                },
                Case {
                    rhs: json!(4),
                    expect: Ok(json!(4)),
                },
                Case {
                    rhs: json!(2),
                    expect: Ok(json!(4)),
                },
            ],
        )
    }

    #[test]
    fn test_minimize_with_deep_merge() {
        run_reduce_cases(
            json!({
                "properties": {
                    "n": {"reduce": {"strategy": "sum"}}
                },
                "reduce": {
                    "strategy": "minimize",
                    "key": ["/k"],
                },
            }),
            vec![
                Case {
                    rhs: json!({"k": 3, "n": 1}),
                    expect: Ok(json!({"k": 3, "n": 1})),
                },
                Case {
                    rhs: json!({"k": 4, "n": 1}),
                    expect: Ok(json!({"k": 3, "n": 1})),
                },
                Case {
                    rhs: json!({"k": 3, "n": 1, "!": true}),
                    expect: Ok(json!({"k": 3, "n": 2, "!": true})),
                },
                Case {
                    rhs: json!({"k": 4, "n": 1, "!": false}),
                    expect: Ok(json!({"k": 3, "n": 2, "!": true})),
                },
                Case {
                    rhs: json!({"k": 3, "n": 1}),
                    expect: Ok(json!({"k": 3, "n": 3, "!": true})),
                },
                Case {
                    rhs: json!({"k": 2, "n": 1}),
                    expect: Ok(json!({"k": 2, "n": 1})),
                },
                // Missing key orders as 'null'.
                Case {
                    rhs: json!({"n": 1, "whoops": true}),
                    expect: Ok(json!({"n": 1, "whoops": true})),
                },
                Case {
                    rhs: json!({"k": null, "n": 1}),
                    expect: Ok(json!({"k": null, "n": 2, "whoops": true})),
                },
                // Keys are technically equal, and it attempts to deep-merge.
                Case {
                    rhs: json!(42),
                    expect: Err(Error::MergeWrongType),
                },
            ],
        )
    }

    #[test]
    fn test_maximize_with_deep_merge() {
        run_reduce_cases(
            json!({
                "items": [
                    {"reduce": {"strategy": "sum"}},
                    {"type": "integer"},
                ],
                "reduce": {
                    "strategy": "maximize",
                    "key": ["/1"],
                },
            }),
            vec![
                Case {
                    rhs: json!([1, 3]),
                    expect: Ok(json!([1, 3])),
                },
                Case {
                    rhs: json!([1, 4]),
                    expect: Ok(json!([1, 4])),
                },
                Case {
                    rhs: json!([1, 3]),
                    expect: Ok(json!([1, 4])),
                },
                Case {
                    rhs: json!([1, 4, '.']),
                    expect: Ok(json!([2, 4, '.'])),
                },
                // It returns a delegated merge error on equal keys.
                Case {
                    rhs: json!({"1": 4}),
                    expect: Err(Error::MergeWrongType),
                },
                Case {
                    rhs: json!([1, 2, "!"]),
                    expect: Ok(json!([2, 4, '.'])),
                },
                Case {
                    rhs: json!([1, 4, ':']),
                    expect: Ok(json!([3, 4, ':'])),
                },
                // Missing key orders as 'null'.
                Case {
                    rhs: json!([]),
                    expect: Ok(json!([3, 4, ':'])),
                },
                Case {
                    rhs: json!(32),
                    expect: Ok(json!([3, 4, ':'])),
                },
            ],
        )
    }

    #[test]
    fn test_sum() {
        run_reduce_cases(
            json!({ "reduce": { "strategy": "sum" } }),
            vec![
                // Non-numeric RHS (without LHS) returns an error.
                Case {
                    rhs: json!("whoops"),
                    expect: Err(Error::SumWrongType),
                },
                // Takes initial value.
                Case {
                    rhs: json!(123),
                    expect: Ok(json!(123)),
                },
                // Add unsigned.
                Case {
                    rhs: json!(45),
                    expect: Ok(json!(168)),
                },
                // Sum results in overflow.
                Case {
                    rhs: json!(u64::MAX - 32),
                    expect: Err(Error::SumNumericOverflow),
                },
                // Add signed.
                Case {
                    rhs: json!(-70),
                    expect: Ok(json!(98)),
                },
                // Add float.
                Case {
                    rhs: json!(0.1),
                    expect: Ok(json!(98.1)),
                },
                // Back to f64 zero.
                Case {
                    rhs: json!(-98.1),
                    expect: Ok(json!(0.0)),
                },
                // Add maximum f64.
                Case {
                    rhs: json!(std::f64::MAX),
                    expect: Ok(json!(std::f64::MAX)),
                },
                // Number which overflows returns an error.
                Case {
                    rhs: json!(std::f64::MAX / 10.0),
                    expect: Err(Error::SumNumericOverflow),
                },
                // Sometimes changes are too small to represent.
                Case {
                    rhs: json!(-1.0),
                    expect: Ok(json!(std::f64::MAX)),
                },
                // Sometimes they aren't.
                Case {
                    rhs: json!(std::f64::MIN / 2.0),
                    expect: Ok(json!(std::f64::MAX / 2.)),
                },
                Case {
                    rhs: json!(std::f64::MIN / 2.0),
                    expect: Ok(json!(0.0)),
                },
                // Non-numeric type (now with LHS) returns an error.
                Case {
                    rhs: json!("whoops"),
                    expect: Err(Error::SumWrongType),
                },
            ],
        );
    }

    #[test]
    fn test_merge_array_in_place() {
        run_reduce_cases(
            json!({
                "items": {
                    "reduce": { "strategy": "maximize" },
                },
                "reduce": { "strategy": "merge" },
            }),
            vec![
                // Non-numeric RHS (without LHS) returns an error.
                Case {
                    rhs: json!("whoops"),
                    expect: Err(Error::MergeWrongType),
                },
                Case {
                    rhs: json!([0, 1, 0]),
                    expect: Ok(json!([0, 1, 0])),
                },
                Case {
                    rhs: json!([3, 0, 2]),
                    expect: Ok(json!([3, 1, 2])),
                },
                Case {
                    rhs: json!([-1, 0, 4, "a"]),
                    expect: Ok(json!([3, 1, 4, "a"])),
                },
                Case {
                    rhs: json!([0, 32.6, 0, "b"]),
                    expect: Ok(json!([3, 32.6, 4, "b"])),
                },
                Case {
                    rhs: json!({}),
                    expect: Err(Error::MergeWrongType),
                },
            ],
        )
    }

    #[test]
    fn test_merge_ordered_scalars() {
        run_reduce_cases(
            json!({
                "reduce": {
                    "strategy": "merge",
                    "key": [""],
                },
            }),
            vec![
                Case {
                    rhs: json!([5, 9]),
                    expect: Ok(json!([5, 9])),
                },
                Case {
                    rhs: json!([7]),
                    expect: Ok(json!([5, 7, 9])),
                },
                Case {
                    rhs: json!([2, 4, 5]),
                    expect: Ok(json!([2, 4, 5, 7, 9])),
                },
                Case {
                    rhs: json!([1, 2, 7, 10]),
                    expect: Ok(json!([1, 2, 4, 5, 7, 9, 10])),
                },
            ],
        )
    }

    #[test]
    fn test_deep_merge_ordered_objects() {
        run_reduce_cases(
            json!({
                "items": {
                    "properties": {
                        "k": {"type": "integer"},
                    },
                    "additionalProperties": {
                        "reduce": { "strategy": "sum" },
                    },
                    "reduce": { "strategy": "merge" },
                },
                "reduce": {
                    "strategy": "merge",
                    "key": ["/k"],
                },
            }),
            vec![
                Case {
                    rhs: json!([{"k": 5, "n": 1}, {"k": 9, "n": 1}]),
                    expect: Ok(json!([{"k": 5, "n": 1}, {"k": 9, "n": 1}])),
                },
                Case {
                    rhs: json!([{"k": 7, "m": 1}]),
                    expect: Ok(json!([{"k": 5, "n": 1}, {"k": 7, "m": 1}, {"k": 9, "n": 1}])),
                },
                Case {
                    rhs: json!([{"k": 5, "n": 3}, {"k": 7, "m": 1}]),
                    expect: Ok(json!([{"k": 5, "n": 4}, {"k": 7, "m": 2}, {"k": 9, "n": 1}])),
                },
                Case {
                    rhs: json!([{"k": 9, "n": -2}]),
                    expect: Ok(json!([{"k": 5, "n": 4}, {"k": 7, "m": 2}, {"k": 9, "n": -1}])),
                },
            ],
        )
    }

    #[test]
    fn test_merge_objects() {
        run_reduce_cases(
            json!({
                "reduce": { "strategy": "merge" },
            }),
            vec![
                Case {
                    rhs: json!({"5": 5, "9": 9}),
                    expect: Ok(json!({"5": 5, "9": 9})),
                },
                Case {
                    rhs: json!({"7": 7}),
                    expect: Ok(json!({"5": 5, "7": 7, "9": 9})),
                },
                Case {
                    rhs: json!({"2": 2, "4": 4, "5": 55}),
                    expect: Ok(json!({"2": 2, "4": 4, "5": 55, "7": 7, "9": 9})),
                },
                Case {
                    rhs: json!({"1": 1, "2": 22, "7": 77, "10": 10}),
                    expect: Ok(
                        json!({"1": 1, "2": 22, "4": 4, "5": 55, "7": 77, "9": 9, "10": 10}),
                    ),
                },
                Case {
                    rhs: json!([1, 2]),
                    expect: Err(Error::MergeWrongType),
                },
            ],
        )
    }

    #[test]
    fn test_deep_merge_objects() {
        run_reduce_cases(
            json!({
                "reduce": {
                    "strategy": "merge",
                    "key": ["/k"],
                },
                "additionalProperties": {
                    "if": { "type": ["object", "array"] },
                    "then": { "$ref": "#" },
                },
                "items": { "$ref": "#/additionalProperties" }
            }),
            vec![
                Case {
                    rhs: json!([{"k": "b", "v": [{"k": 5}]}]),
                    expect: Ok(json!([{"k": "b", "v": [{"k": 5}]}])),
                },
                Case {
                    rhs: json!([
                        {"k": "a", "v": [{"k": 2}]},
                        {"k": "b", "v": [{"k": 3}]}
                    ]),
                    expect: Ok(json!([
                        {"k": "a", "v": [{"k": 2}]},
                        {"k": "b", "v": [{"k": 3}, {"k": 5}]}
                    ])),
                },
                Case {
                    rhs: json!([
                        {"k": "b", "v": [{"k": 1}, {"k": 5, "d": true}]},
                        {"k": "c", "v": [{"k": 9}]}
                    ]),
                    expect: Ok(json!([
                        {"k": "a", "v": [{"k": 2}]},
                        {"k": "b", "v": [{"k": 1}, {"k": 3}, {"k": 5, "d": true}]},
                        {"k": "c", "v": [{"k": 9}]},
                    ])),
                },
            ],
        )
    }
}
