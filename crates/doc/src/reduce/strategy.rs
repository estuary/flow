use super::{count_nodes, reduce_item, reduce_prop, Cursor, Error, Reducer, Result};
use itertools::EitherOrBoth;
use json::{json_cmp, json_cmp_at, Number};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::convert::TryFrom;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "strategy", deny_unknown_fields, rename_all = "camelCase")]
pub enum Strategy {
    /// Append each item of RHS to the end of LHS. RHS must be an array.
    /// LHS must be an array, or may be null, in which case no append is
    /// done and the reduction is a no-op.
    Append,
    /// FirstWriteWins keeps the LHS value.
    FirstWriteWins,
    /// LastWriteWins takes the RHS value.
    LastWriteWins,
    /// Maximize keeps the greater of the LHS & RHS.
    /// A provided key, if present, determines the relative ordering.
    /// If values are equal, they're deeply merged.
    Maximize(Maximize),
    /// Merge the LHS and RHS by recursively reducing shared document locations.
    /// The LHS and RHS must either both be Objects, or both be Arrays.
    ///
    /// If LHS and RHS are Arrays and a merge key is provide, the Arrays *must* be
    /// pre-sorted and de-duplicated by that key. Merge then performs a deep sorted
    /// merge of their respective items, as ordered by the key.
    /// Note that a key of [""] can be applied to use natural item ordering.
    ///
    /// If LHS and RHS are both Arrays and a key is not provided, items of each index
    /// in LHS and RHS are merged together, extending the shorter of the two by taking
    /// items of the longer.
    ///
    /// If LHS and RHS are both Objects then it perform a deep merge of each property.
    Merge(Merge),
    /// Minimize keeps the smaller of the LHS & RHS.
    /// A provided key, if present, determines the relative ordering.
    /// If values are equal, they're deeply merged.
    Minimize(Minimize),
    /// Interpret this location as an update to a set.
    ///
    /// The location *must* be an object having (only) "add", "intersect",
    /// and "remove" properties. Any single property is always allowed.
    ///
    /// An instance with "intersect" and "add" is allowed, and is interpreted
    /// as applying the intersection to the base set, followed by a union of
    /// the additions.
    ///
    /// An instance with "remove" and "add" is also allowed, and is interpreted
    /// as applying the removals to the base set, followed by a union of
    /// the additions.
    ///
    /// "remove" and "intersect" within the same instance is prohibited.
    ///
    /// Set additions are deeply merged. This makes sets behave as associative
    /// maps, where the "value" of a set member can be updated by adding it to
    /// set with a reducible update.
    ///
    /// Set components may be objects, in which case the object property is the
    /// set key, or arrays which are ordered using the Set's key extractor.
    /// Use a key extractor of [""] to apply the natural ordering of scalar
    /// values stored in a sorted array.
    ///
    /// Whether arrays or objects are used, the selected type must always be
    /// consistent across the "add" / "intersect" / "remove" terms of both
    /// sides of the reduction.
    Set(Set),
    /// Sum the LHS and RHS, both of which must be numbers.
    /// Sum will fail if the operation would result in a numeric overflow
    /// (in other words, the numbers become too large to be represented).
    ///
    /// In the future, we may allow for arbitrary-sized integer and
    /// floating-point representations which use a string encoding scheme.
    Sum,
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

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Set {
    #[serde(default)]
    pub key: Vec<String>,
}

impl std::convert::TryFrom<&Value> for Strategy {
    type Error = serde_json::Error;

    fn try_from(v: &Value) -> std::result::Result<Self, Self::Error> {
        Strategy::deserialize(v)
    }
}

impl Reducer for Strategy {
    fn reduce(&self, cur: Cursor) -> Result<Value> {
        match self {
            Strategy::Append => Self::append(cur),
            Strategy::FirstWriteWins => Self::first_write_wins(cur),
            Strategy::LastWriteWins => Self::last_write_wins(cur),
            Strategy::Maximize(max) => Self::maximize(cur, max),
            Strategy::Merge(merge) => Self::merge(cur, merge),
            Strategy::Minimize(min) => Self::minimize(cur, min),
            Strategy::Set(set) => set.reduce(cur),
            Strategy::Sum => Self::sum(cur),
        }
    }
}

impl Strategy {
    fn append(cur: Cursor) -> Result<Value> {
        let (tape, loc, prune, lhs, rhs) = match cur {
            // Merge of Null <= Array (treated as no-op).
            Cursor::Both {
                tape,
                lhs: Value::Null,
                rhs: rhs @ Value::Array(..),
                ..
            } => {
                *tape = &tape[count_nodes(&rhs)..];
                return Ok(Value::Null);
            }
            // Merge of Array <= Array.
            Cursor::Both {
                tape,
                loc,
                prune,
                lhs: Value::Array(lhs),
                rhs: Value::Array(rhs),
            } => (tape, loc, prune, lhs, rhs),
            // Merge of Undefined <= Array.
            Cursor::Right {
                tape,
                loc,
                prune,
                rhs: Value::Array(rhs),
            } => (tape, loc, prune, Vec::new(), rhs),

            cur => return Err(Error::cursor(cur, Error::AppendWrongType)),
        };

        *tape = &tape[1..]; // Consume array container.

        let rhs = rhs
            .into_iter()
            .enumerate()
            .map(|item| reduce_item(tape, loc, prune, EitherOrBoth::Right(item)));

        Ok(Value::Array(
            lhs.into_iter()
                .map(Result::Ok)
                .chain(rhs)
                .collect::<Result<_>>()?,
        ))
    }

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

                let sum = Number::checked_add((&lhs).into(), (&rhs).into());
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
            // Merge of Null <= Object (treated as no-op).
            Cursor::Both {
                tape,
                lhs: Value::Null,
                rhs: rhs @ Value::Object(..),
                ..
            } => {
                *tape = &tape[count_nodes(&rhs)..];
                Ok(Value::Null)
            }

            // Merge of Array <= Array.
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
            // Merge of Null <= Array (treated as no-op).
            Cursor::Both {
                tape,
                lhs: Value::Null,
                rhs: rhs @ Value::Array(..),
                ..
            } => {
                *tape = &tape[count_nodes(&rhs)..];
                Ok(Value::Null)
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
    use super::super::test::*;
    use super::*;

    #[test]
    fn test_append_array() {
        run_reduce_cases(
            json!({
                "if": { "type": "null" },
                "then": { "reduce": { "strategy": "lastWriteWins" } },
                "else": { "reduce": { "strategy": "append" } },
            }),
            vec![
                // Non-array RHS (without LHS) returns an error.
                Partial {
                    rhs: json!("whoops"),
                    expect: Err(Error::AppendWrongType),
                },
                Partial {
                    rhs: json!([0, 1]),
                    expect: Ok(json!([0, 1])),
                },
                Partial {
                    rhs: json!([2, 3, 4]),
                    expect: Ok(json!([0, 1, 2, 3, 4])),
                },
                Partial {
                    rhs: json!([-1, "a"]),
                    expect: Ok(json!([0, 1, 2, 3, 4, -1, "a"])),
                },
                Partial {
                    rhs: json!({}),
                    expect: Err(Error::AppendWrongType),
                },
                Partial {
                    rhs: json!(null),
                    expect: Ok(json!(null)),
                },
                // Append with null LHS is a no-op.
                Partial {
                    rhs: json!([5, 6, 4]),
                    expect: Ok(json!(null)),
                },
            ],
        )
    }

    #[test]
    fn test_last_write_wins() {
        run_reduce_cases(
            json!(true),
            vec![
                Partial {
                    rhs: json!("foo"),
                    expect: Ok(json!("foo")),
                },
                Partial {
                    rhs: json!({"n": 42}),
                    expect: Ok(json!({"n": 42})),
                },
                Partial {
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
                Partial {
                    rhs: json!("foo"),
                    expect: Ok(json!("foo")),
                },
                Partial {
                    rhs: json!({"n": 42}),
                    expect: Ok(json!("foo")),
                },
                Partial {
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
                Partial {
                    rhs: json!(3),
                    expect: Ok(json!(3)),
                },
                Partial {
                    rhs: json!(4),
                    expect: Ok(json!(3)),
                },
                Partial {
                    rhs: json!(3),
                    expect: Ok(json!(3)),
                },
                Partial {
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
                Partial {
                    rhs: json!(3),
                    expect: Ok(json!(3)),
                },
                Partial {
                    rhs: json!(4),
                    expect: Ok(json!(4)),
                },
                Partial {
                    rhs: json!(4),
                    expect: Ok(json!(4)),
                },
                Partial {
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
                Partial {
                    rhs: json!({"k": 3, "n": 1}),
                    expect: Ok(json!({"k": 3, "n": 1})),
                },
                Partial {
                    rhs: json!({"k": 4, "n": 1}),
                    expect: Ok(json!({"k": 3, "n": 1})),
                },
                Partial {
                    rhs: json!({"k": 3, "n": 1, "!": true}),
                    expect: Ok(json!({"k": 3, "n": 2, "!": true})),
                },
                Partial {
                    rhs: json!({"k": 4, "n": 1, "!": false}),
                    expect: Ok(json!({"k": 3, "n": 2, "!": true})),
                },
                Partial {
                    rhs: json!({"k": 3, "n": 1}),
                    expect: Ok(json!({"k": 3, "n": 3, "!": true})),
                },
                Partial {
                    rhs: json!({"k": 2, "n": 1}),
                    expect: Ok(json!({"k": 2, "n": 1})),
                },
                // Missing key orders as 'null'.
                Partial {
                    rhs: json!({"n": 1, "whoops": true}),
                    expect: Ok(json!({"n": 1, "whoops": true})),
                },
                Partial {
                    rhs: json!({"k": null, "n": 1}),
                    expect: Ok(json!({"k": null, "n": 2, "whoops": true})),
                },
                // Keys are technically equal, and it attempts to deep-merge.
                Partial {
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
                Partial {
                    rhs: json!([1, 3]),
                    expect: Ok(json!([1, 3])),
                },
                Partial {
                    rhs: json!([1, 4]),
                    expect: Ok(json!([1, 4])),
                },
                Partial {
                    rhs: json!([1, 3]),
                    expect: Ok(json!([1, 4])),
                },
                Partial {
                    rhs: json!([1, 4, '.']),
                    expect: Ok(json!([2, 4, '.'])),
                },
                // It returns a delegated merge error on equal keys.
                Partial {
                    rhs: json!({"1": 4}),
                    expect: Err(Error::MergeWrongType),
                },
                Partial {
                    rhs: json!([1, 2, "!"]),
                    expect: Ok(json!([2, 4, '.'])),
                },
                Partial {
                    rhs: json!([1, 4, ':']),
                    expect: Ok(json!([3, 4, ':'])),
                },
                // Missing key orders as 'null'.
                Partial {
                    rhs: json!([]),
                    expect: Ok(json!([3, 4, ':'])),
                },
                Partial {
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
                Partial {
                    rhs: json!("whoops"),
                    expect: Err(Error::SumWrongType),
                },
                // Takes initial value.
                Partial {
                    rhs: json!(123),
                    expect: Ok(json!(123)),
                },
                // Add unsigned.
                Partial {
                    rhs: json!(45),
                    expect: Ok(json!(168)),
                },
                // Sum results in overflow.
                Partial {
                    rhs: json!(u64::MAX - 32),
                    expect: Err(Error::SumNumericOverflow),
                },
                // Add signed.
                Partial {
                    rhs: json!(-70),
                    expect: Ok(json!(98)),
                },
                // Add float.
                Partial {
                    rhs: json!(0.1),
                    expect: Ok(json!(98.1)),
                },
                // Back to f64 zero.
                Partial {
                    rhs: json!(-98.1),
                    expect: Ok(json!(0.0)),
                },
                // Add maximum f64.
                Partial {
                    rhs: json!(std::f64::MAX),
                    expect: Ok(json!(std::f64::MAX)),
                },
                // Number which overflows returns an error.
                Partial {
                    rhs: json!(std::f64::MAX / 10.0),
                    expect: Err(Error::SumNumericOverflow),
                },
                // Sometimes changes are too small to represent.
                Partial {
                    rhs: json!(-1.0),
                    expect: Ok(json!(std::f64::MAX)),
                },
                // Sometimes they aren't.
                Partial {
                    rhs: json!(std::f64::MIN / 2.0),
                    expect: Ok(json!(std::f64::MAX / 2.)),
                },
                Partial {
                    rhs: json!(std::f64::MIN / 2.0),
                    expect: Ok(json!(0.0)),
                },
                // Non-numeric type (now with LHS) returns an error.
                Partial {
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
                Partial {
                    rhs: json!("whoops"),
                    expect: Err(Error::MergeWrongType),
                },
                Partial {
                    rhs: json!([0, 1, 0]),
                    expect: Ok(json!([0, 1, 0])),
                },
                Partial {
                    rhs: json!([3, 0, 2]),
                    expect: Ok(json!([3, 1, 2])),
                },
                Partial {
                    rhs: json!([-1, 0, 4, "a"]),
                    expect: Ok(json!([3, 1, 4, "a"])),
                },
                Partial {
                    rhs: json!([0, 32.6, 0, "b"]),
                    expect: Ok(json!([3, 32.6, 4, "b"])),
                },
                Partial {
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
                "if": { "type": "null" },
                "then": { "reduce": { "strategy": "lastWriteWins" } },
                "else": {
                    "reduce": {
                        "strategy": "merge",
                        "key": [""],
                    },
                },
            }),
            vec![
                Partial {
                    rhs: json!([5, 9]),
                    expect: Ok(json!([5, 9])),
                },
                Partial {
                    rhs: json!([7]),
                    expect: Ok(json!([5, 7, 9])),
                },
                Partial {
                    rhs: json!([2, 4, 5]),
                    expect: Ok(json!([2, 4, 5, 7, 9])),
                },
                Partial {
                    rhs: json!([1, 2, 7, 10]),
                    expect: Ok(json!([1, 2, 4, 5, 7, 9, 10])),
                },
                // After reducing null LHS, future merges are no-ops.
                Partial {
                    rhs: json!(null),
                    expect: Ok(json!(null)),
                },
                Partial {
                    rhs: json!([1, 2]),
                    expect: Ok(json!(null)),
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
                Partial {
                    rhs: json!([{"k": 5, "n": 1}, {"k": 9, "n": 1}]),
                    expect: Ok(json!([{"k": 5, "n": 1}, {"k": 9, "n": 1}])),
                },
                Partial {
                    rhs: json!([{"k": 7, "m": 1}]),
                    expect: Ok(json!([{"k": 5, "n": 1}, {"k": 7, "m": 1}, {"k": 9, "n": 1}])),
                },
                Partial {
                    rhs: json!([{"k": 5, "n": 3}, {"k": 7, "m": 1}]),
                    expect: Ok(json!([{"k": 5, "n": 4}, {"k": 7, "m": 2}, {"k": 9, "n": 1}])),
                },
                Partial {
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
                "if": { "type": "null" },
                "then": { "reduce": { "strategy": "lastWriteWins" } },
                "else": { "reduce": { "strategy": "merge" } },
            }),
            vec![
                Partial {
                    rhs: json!({"5": 5, "9": 9}),
                    expect: Ok(json!({"5": 5, "9": 9})),
                },
                Partial {
                    rhs: json!({"7": 7}),
                    expect: Ok(json!({"5": 5, "7": 7, "9": 9})),
                },
                Partial {
                    rhs: json!({"2": 2, "4": 4, "5": 55}),
                    expect: Ok(json!({"2": 2, "4": 4, "5": 55, "7": 7, "9": 9})),
                },
                Partial {
                    rhs: json!({"1": 1, "2": 22, "7": 77, "10": 10}),
                    expect: Ok(
                        json!({"1": 1, "2": 22, "4": 4, "5": 55, "7": 77, "9": 9, "10": 10}),
                    ),
                },
                Partial {
                    rhs: json!([1, 2]),
                    expect: Err(Error::MergeWrongType),
                },
                // After reducing null LHS, future merges are no-ops.
                Partial {
                    rhs: json!(null),
                    expect: Ok(json!(null)),
                },
                Partial {
                    rhs: json!({"9": 9}),
                    expect: Ok(json!(null)),
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
                Partial {
                    rhs: json!([{"k": "b", "v": [{"k": 5}]}]),
                    expect: Ok(json!([{"k": "b", "v": [{"k": 5}]}])),
                },
                Partial {
                    rhs: json!([
                        {"k": "a", "v": [{"k": 2}]},
                        {"k": "b", "v": [{"k": 3}]}
                    ]),
                    expect: Ok(json!([
                        {"k": "a", "v": [{"k": 2}]},
                        {"k": "b", "v": [{"k": 3}, {"k": 5}]}
                    ])),
                },
                Partial {
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
