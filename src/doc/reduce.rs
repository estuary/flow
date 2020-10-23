use estuary_json as ej;
use itertools::EitherOrBoth;
use serde::{Deserialize, Serialize};
use serde_json as sj;
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::iter::Iterator;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "strategy", deny_unknown_fields, rename_all = "camelCase")]
pub enum Strategy {
    /// FirstWriteWins keeps the LHS value.
    FirstWriteWins,
    /// LastWriteWins takes the RHS value.
    LastWriteWins,
    /// Maximize keeps the greater of the LHS & RHS.
    /// A provided key, if present, determines the relative ordering.
    Maximize(Maximize),
    /// Minimize keeps the smaller of the LHS & RHS.
    /// A provided key, if present, determines the relative ordering.
    Minimize(Minimize),
    /// Sum the LHS and RHS.
    /// If either LHS or RHS are not numbers, Sum behaves as LastWriteWins.
    /// TODO(johnny): Sum doesn't properly deal with overflow conditions yet.
    Sum,

    /// Merge is a recursive set union operation.
    ///
    /// If LHS and RHS are both Objects, it perform a deep merge of properties by name.
    ///
    /// If LHS and RHS are both Arrays and a key is not provided, items of each index
    /// in LHS and RHS are merged together, extended the shorter of the two by taking
    /// items of the longer.
    ///
    /// If LHS and RHS are both Arrays and a key *is* provided, a deep sorted merge
    /// of their respective items is performed, as ordered by that key.
    /// Note that a key of [""] can be applied to use natural item ordering.
    ///
    /// When applied to Arrays with a key, this reduction always produces a sorted
    /// output and similarly requires that its inputs already be sorted by the key.
    ///
    /// In all other cases, Merge behaves as LastWriteWins.
    Merge(Merge),
    /// TODO(johnny): Planning to remove this, as it's superseded by Set.
    Subtract(Subtract),

    // TODO(johnny): Planning to remove this. It's not clear it has actual utility.
    /// If LHS and RHS are both arrays or are both strings, extend LHS with RHS.
    /// Otherwise, Append defaults to LastWriteWins behavior.
    Append,
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
}

impl std::convert::TryFrom<&sj::Value> for Strategy {
    type Error = serde_json::Error;

    fn try_from(v: &sj::Value) -> Result<Self, Self::Error> {
        Strategy::deserialize(v)
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Set {
    #[serde(default)]
    pub key: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Minimize {
    #[serde(default)]
    pub key: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Maximize {
    #[serde(default)]
    pub key: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Merge {
    #[serde(default)]
    pub key: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Subtract {
    #[serde(default)]
    key: Vec<String>,
}

pub struct Reducer<'r> {
    pub at: usize,
    pub val: sj::Value,
    pub into: &'r mut sj::Value,
    pub created: bool,
    pub idx: &'r [(&'r Strategy, u64)],
}

impl<'r> Reducer<'r> {
    pub fn reduce(self) -> usize {
        /*
        log::debug!(
            "evaluating at {} doc {} strategy {:?}",
            self.at,
            &self.val,
            self.idx.get(self.at)
        );
        */

        match self.idx.get(self.at) {
            Some((Strategy::Append, _)) => self.append(),
            Some((Strategy::FirstWriteWins, _)) => self.first_write_wins(),
            Some((Strategy::LastWriteWins, _)) | None => self.last_write_wins(),
            Some((Strategy::Maximize(s), _)) => self.maximize(s),
            Some((Strategy::Merge(s), _)) => self.merge(s),
            Some((Strategy::Minimize(s), _)) => self.minimize(s),
            Some((Strategy::Subtract(s), _)) => self.subtract(s),
            Some((Strategy::Sum, _)) => self.sum(),
            Some((Strategy::Set(_), _)) => panic!("not implemented"),
        }
    }

    fn first_write_wins(self) -> usize {
        if self.created {
            self.at + take_val(self.val, self.into)
        } else {
            self.at + count_nodes(&self.val)
        }
    }

    fn last_write_wins(self) -> usize {
        self.at + take_val(self.val, self.into)
    }

    fn maximize(self, strategy: &Maximize) -> usize {
        let ord = if self.created {
            Ordering::Greater
        } else if strategy.key.is_empty() {
            ej::json_cmp(&self.val, self.into)
        } else {
            ej::json_cmp_at(&strategy.key, &self.val, self.into)
        };

        if ord == Ordering::Greater {
            self.at + take_val(self.val, self.into)
        } else {
            self.at + count_nodes(&self.val)
        }
    }

    fn minimize(self, strategy: &Minimize) -> usize {
        let ord = if self.created {
            Ordering::Less
        } else if strategy.key.is_empty() {
            ej::json_cmp(&self.val, self.into)
        } else {
            ej::json_cmp_at(&strategy.key, &self.val, self.into)
        };

        if ord == Ordering::Less {
            self.at + take_val(self.val, self.into)
        } else {
            self.at + count_nodes(&self.val)
        }
    }

    fn merge(self, strategy: &Merge) -> usize {
        let mut at = self.at;
        let idx = self.idx;

        match (self.into, self.val) {
            // Merge of two arrays. If a merge key is provided, deeply merge
            // arrays in sorted order over the given key (which could be simply
            // "/" to order over scalar items). Otherwise, do a deep merge of
            // each paired index.
            (into @ sj::Value::Array(_), sj::Value::Array(val)) => {
                // TODO: work-around for "cannot bind by-move and by-ref in the same pattern".
                // https://github.com/rust-lang/rust/issues/68354
                let into = into.as_array_mut().unwrap();
                let into_prev = std::mem::replace(into, Vec::new());

                at += 1;

                into.extend(
                    itertools::merge_join_by(
                        into_prev.into_iter().enumerate(),
                        val.into_iter().enumerate(),
                        |(lhs_ind, lhs), (rhs_ind, rhs)| -> Ordering {
                            if strategy.key.is_empty() {
                                lhs_ind.cmp(rhs_ind)
                            } else {
                                ej::json_cmp_at(&strategy.key, lhs, rhs)
                            }
                        },
                    )
                    .map(|eob| match eob {
                        EitherOrBoth::Both((_, mut into), (_, val)) => {
                            at = Reducer {
                                at,
                                val,
                                into: &mut into,
                                created: false,
                                idx,
                            }
                            .reduce();
                            into
                        }
                        EitherOrBoth::Right((_, val)) => {
                            let mut into = sj::Value::Null;
                            at = Reducer {
                                at,
                                val,
                                into: &mut into,
                                created: true,
                                idx,
                            }
                            .reduce();
                            into
                        }
                        EitherOrBoth::Left((_, into)) => into,
                    }),
                );

                at
            }
            (into @ sj::Value::Object(_), sj::Value::Object(val)) => {
                // TODO: work-around for "cannot bind by-move and by-ref in the same pattern".
                // https://github.com/rust-lang/rust/issues/68354

                type Map = sj::Map<String, sj::Value>;
                let into = into.as_object_mut().unwrap();
                let into_prev = std::mem::replace(into, Map::new());

                at += 1;

                into.extend(
                    itertools::merge_join_by(into_prev.into_iter(), val.into_iter(), |lhs, rhs| {
                        lhs.0.cmp(&rhs.0)
                    })
                    .map(|eob| match eob {
                        EitherOrBoth::Both((prop, mut into), (_, val)) => {
                            at = Reducer {
                                at,
                                val,
                                into: &mut into,
                                created: false,
                                idx,
                            }
                            .reduce();
                            (prop, into)
                        }
                        EitherOrBoth::Right((prop, val)) => {
                            let mut into = sj::Value::Null;
                            at = Reducer {
                                at,
                                val,
                                into: &mut into,
                                created: true,
                                idx,
                            }
                            .reduce();
                            (prop, into)
                        }
                        EitherOrBoth::Left(into) => into,
                    }),
                );

                at
            }
            (into, val) => at + take_val(val, into), // Default to last-write-wins.
        }
    }

    fn subtract(self, strategy: &Subtract) -> usize {
        match (self.into, self.val) {
            // Subtract of two arrays. If a merge key is provided, use it to determine
            // relative item ordering. Otherwise, use natural item order.
            (into @ sj::Value::Array(_), sj::Value::Array(val)) => {
                // TODO: work-around for "cannot bind by-move and by-ref in the same pattern".
                // https://github.com/rust-lang/rust/issues/68354
                let into = into.as_array_mut().unwrap();
                let into_prev = std::mem::replace(into, Vec::new());

                let at = self.at + val.iter().fold(1, |c, vv| c + count_nodes(vv));
                into.extend(
                    itertools::merge_join_by(into_prev.into_iter(), val.into_iter(), |lhs, rhs| {
                        if strategy.key.is_empty() {
                            ej::json_cmp(lhs, rhs)
                        } else {
                            ej::json_cmp_at(&strategy.key, lhs, rhs)
                        }
                    })
                    .filter_map(|eob| match eob {
                        EitherOrBoth::Both(_, _) | EitherOrBoth::Right(_) => None,
                        EitherOrBoth::Left(into) => Some(into),
                    }),
                );
                at
            }
            (into @ sj::Value::Object(_), sj::Value::Object(val)) => {
                // TODO: work-around for "cannot bind by-move and by-ref in the same pattern".
                // https://github.com/rust-lang/rust/issues/68354
                let into = into.as_object_mut().unwrap();
                let into_prev = std::mem::replace(into, sj::Map::new());

                let at = self.at + val.iter().fold(1, |c, (_, vv)| c + count_nodes(vv));
                into.extend(
                    itertools::merge_join_by(into_prev.into_iter(), val.into_iter(), |lhs, rhs| {
                        lhs.0.cmp(&rhs.0)
                    })
                    .filter_map(|eob| match eob {
                        EitherOrBoth::Both(_, _) | EitherOrBoth::Right(_) => None,
                        EitherOrBoth::Left(into) => Some(into),
                    }),
                );
                at
            }
            (into, val) => self.at + take_val(val, into), // Default to last-write-wins.
        }
    }

    fn sum(self) -> usize {
        match (&*self.into, &self.val) {
            (sj::Value::Number(lhs), sj::Value::Number(rhs)) => {
                let sum = ej::Number::checked_add(lhs.into(), rhs.into());
                if let Ok(sum) = sum.ok_or(()).and_then(sj::Value::try_from) {
                    *self.into = sum;
                }
                self.at + 1
            }
            (sj::Value::Null, sj::Value::Number(_)) if !self.created => {
                self.at + 1 // Leave as null.
            }
            _ => self.at + take_val(self.val, self.into), // Default to last-write-wins.
        }
    }

    fn append(self) -> usize {
        match (self.into, self.val) {
            // Append of two arrays.
            (into @ sj::Value::Array(_), sj::Value::Array(val)) => {
                // TODO: work-around for "cannot bind by-move and by-ref in the same pattern".
                // https://github.com/rust-lang/rust/issues/68354
                let into = into.as_array_mut().unwrap();

                let at = self.at + val.iter().fold(1, |c, vv| c + count_nodes(vv));
                into.extend(val.into_iter());
                at
            }
            // Append of two strings.
            (sj::Value::String(into), sj::Value::String(ref val)) => {
                into.push_str(val);
                self.at + 1
            }
            (into, val) => self.at + take_val(val, into), // Default to last-write-wins.
        }
    }
}

pub fn count_nodes(v: &sj::Value) -> usize {
    match v {
        sj::Value::Bool(_) | sj::Value::Null | sj::Value::String(_) | sj::Value::Number(_) => 1,
        sj::Value::Array(v) => v.iter().fold(1, |c, vv| c + count_nodes(vv)),
        sj::Value::Object(v) => v.iter().fold(1, |c, (_prop, vv)| c + count_nodes(vv)),
    }
}

fn take_val(val: sj::Value, into: &mut sj::Value) -> usize {
    *into = val;
    count_nodes(into)
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn test_node_counting() {
        assert_eq!(count_nodes(&json!(true)), 1);
        assert_eq!(count_nodes(&json!("string")), 1);
        assert_eq!(count_nodes(&json!(1234)), 1);
        assert_eq!(count_nodes(&Value::Null), 1);

        assert_eq!(count_nodes(&json!([])), 1);
        assert_eq!(count_nodes(&json!([2, 3, 4])), 4);
        assert_eq!(count_nodes(&json!([2, [4, 5]])), 5);

        assert_eq!(count_nodes(&json!({})), 1);
        assert_eq!(count_nodes(&json!({"2": 2, "3": 3})), 3);
        assert_eq!(count_nodes(&json!({"2": 2, "3": {"4": 4, "5": 5}})), 5);

        let doc: sj::Value = json!({
            "two": [3, [5, 6], {"eight": 8}],
            "nine": "nine",
            "ten": sj::Value::Null,
            "eleven": true,
        });
        assert_eq!(count_nodes(&doc), 11);
    }

    struct Case {
        val: sj::Value,
        expect: sj::Value,
        nodes: usize,
    }

    fn run_reduce_cases(r: &Strategy, cases: Vec<Case>) {
        let mut into = sj::Value::Null;
        let mut created = true;

        for case in cases {
            println!("reduce {} => expect {}", &case.val, &case.expect);

            // Sanity check that count_nodes(), the test fixture, and the reducer
            // all agree on the number of JSON document nodes.
            assert_eq!(count_nodes(&case.val), case.nodes);
            let idx = std::iter::repeat((r, 0))
                .take(case.nodes)
                .collect::<Vec<_>>();

            assert_eq!(
                case.nodes,
                Reducer {
                    at: 0,
                    val: case.val,
                    into: &mut into,
                    created: created,
                    idx: &idx[..],
                }
                .reduce()
            );

            assert_eq!(&into, &case.expect);
            created = false;
        }
    }

    #[test]
    fn test_minimize() {
        let m = Strategy::Minimize(Minimize {
            key: vec!["/k".to_owned()],
        });
        run_reduce_cases(
            &m,
            vec![
                // Takes initial value.
                Case {
                    val: json!({"k": 3, "d": 1}),
                    nodes: 3,
                    expect: json!({"k": 3, "d": 1}),
                },
                // Ignores larger key.
                Case {
                    val: json!({"k": 4}),
                    nodes: 2,
                    expect: json!({"k": 3, "d": 1}),
                },
                // Updates with smaller key.
                Case {
                    val: json!({"k": 2, "d": 2}),
                    nodes: 3,
                    expect: json!({"k": 2, "d": 2}),
                },
            ],
        )
    }

    #[test]
    fn test_maximize() {
        let m = Strategy::Maximize(Maximize {
            key: vec!["/k".to_owned()],
        });
        run_reduce_cases(
            &m,
            vec![
                // Takes initial value.
                Case {
                    val: json!({"k": 3, "d": 1}),
                    nodes: 3,
                    expect: json!({"k": 3, "d": 1}),
                },
                // Ignores smaller key.
                Case {
                    val: json!({"k": 2}),
                    nodes: 2,
                    expect: json!({"k": 3, "d": 1}),
                },
                // Updates with larger key.
                Case {
                    val: json!({"k": 4, "d": 2}),
                    nodes: 3,
                    expect: json!({"k": 4, "d": 2}),
                },
            ],
        )
    }

    #[test]
    fn test_sum() {
        let m = Strategy::Sum;

        run_reduce_cases(
            &m,
            vec![
                // Takes initial value.
                Case {
                    val: json!(123),
                    nodes: 1,
                    expect: json!(123),
                },
                // Add unsigned.
                Case {
                    val: json!(45),
                    nodes: 1,
                    expect: json!(168),
                },
                // Add signed.
                Case {
                    val: json!(-70),
                    nodes: 1,
                    expect: json!(98),
                },
                // Add float.
                Case {
                    val: json!(0.1),
                    nodes: 1,
                    expect: json!(98.1),
                },
                // Back to f64 zero.
                Case {
                    val: json!(-98.1),
                    nodes: 1,
                    expect: json!(0.0),
                },
                // Add maximum f64.
                Case {
                    val: json!(std::f64::MAX),
                    nodes: 1,
                    expect: json!(std::f64::MAX),
                },
                // Number which cannot be represented leaves the prior value.
                Case {
                    val: json!(std::f64::MAX / 10.0),
                    nodes: 1,
                    expect: json!(std::f64::MAX),
                },
                // Sometimes changes are too small to represent.
                Case {
                    val: json!(-1.0),
                    nodes: 1,
                    expect: json!(std::f64::MAX),
                },
                // Sometimes they aren't.
                Case {
                    val: json!(std::f64::MIN / 2.0),
                    nodes: 1,
                    expect: json!(std::f64::MAX / 2.),
                },
                Case {
                    val: json!(std::f64::MIN / 2.0),
                    nodes: 1,
                    expect: json!(0.0),
                },
                // Non-numeric types default to last-write-wins.
                Case {
                    val: json!("foo"),
                    nodes: 1,
                    expect: json!("foo"),
                },
                Case {
                    val: json!(1),
                    nodes: 1,
                    expect: json!(1),
                },
            ],
        );

        // Unsigned overflow handling.
        // TODO(johnny): This panics, today.
        /*
        run_reduce_cases(
            &m,
            vec![
                Case {
                    val: json!(123),
                    nodes: 1,
                    expect: json!(123),
                },
                Case {
                    val: json!(std::u64::MAX - 2),
                    nodes: 1,
                    expect: json!(std::u64::MAX),
                },
            ],
        );
        */
    }

    #[test]
    fn test_merge_array_in_place() {
        let m = Strategy::Merge(Merge { key: Vec::new() });
        run_reduce_cases(
            &m,
            vec![
                Case {
                    val: json!([5, 9]),
                    nodes: 3,
                    expect: json!([5, 9]),
                },
                Case {
                    val: json!([7]),
                    nodes: 2,
                    expect: json!([7, 9]),
                },
                Case {
                    val: json!([{"a": 1}, 4, 5]),
                    nodes: 5,
                    expect: json!([{"a": 1}, 4, 5]),
                },
                Case {
                    val: json!([{"b": 2}, 10]),
                    nodes: 4,
                    expect: json!([{"a": 1, "b": 2}, 10, 5]),
                },
                // Default to last-write-wins on incompatible types.
                Case {
                    val: json!({"foo": "bar"}),
                    nodes: 2,
                    expect: json!({"foo": "bar"}),
                },
            ],
        )
    }

    #[test]
    fn test_merge_array_of_scalars() {
        let m = Strategy::Merge(Merge {
            key: vec!["".to_owned()],
        });
        run_reduce_cases(
            &m,
            vec![
                Case {
                    val: json!([5, 9]),
                    nodes: 3,
                    expect: json!([5, 9]),
                },
                Case {
                    val: json!([7]),
                    nodes: 2,
                    expect: json!([5, 7, 9]),
                },
                Case {
                    val: json!([2, 4, 5]),
                    nodes: 4,
                    expect: json!([2, 4, 5, 7, 9]),
                },
                Case {
                    val: json!([1, 2, 7, 10]),
                    nodes: 5,
                    expect: json!([1, 2, 4, 5, 7, 9, 10]),
                },
                Case {
                    val: json!({"foo": "bar"}),
                    nodes: 2,
                    expect: json!({"foo": "bar"}),
                },
            ],
        )
    }

    #[test]
    fn test_merge_object() {
        let m = Strategy::Merge(Merge { key: Vec::new() });
        run_reduce_cases(
            &m,
            vec![
                Case {
                    val: json!({"5": 5, "9": 9}),
                    nodes: 3,
                    expect: json!({"5": 5, "9": 9}),
                },
                Case {
                    val: json!({"7": 7}),
                    nodes: 2,
                    expect: json!({"5": 5, "7": 7, "9": 9}),
                },
                Case {
                    val: json!({"2": 2, "4": 4, "5": 55}),
                    nodes: 4,
                    expect: json!({"2": 2, "4": 4, "5": 55, "7": 7, "9": 9}),
                },
                Case {
                    val: json!({"1": 1, "2": 22, "7": 77, "10": 10}),
                    nodes: 5,
                    expect: json!({"1": 1, "2": 22, "4": 4, "5": 55, "7": 77, "9": 9, "10": 10}),
                },
                // Default to last-write-wins on incompatible types.
                Case {
                    val: json!([1, 2]),
                    nodes: 3,
                    expect: json!([1, 2]),
                },
            ],
        )
    }

    #[test]
    fn test_merge_deep() {
        let m = Strategy::Merge(Merge {
            key: vec!["/k".to_owned()],
        });
        run_reduce_cases(
            &m,
            vec![
                Case {
                    val: json!([{"k": "b", "v": [{"k": 5}]}]),
                    nodes: 6,
                    expect: json!([{"k": "b", "v": [{"k": 5}]}]),
                },
                Case {
                    val: json!([
                        {"k": "a", "v": [{"k": 2}]},
                        {"k": "b", "v": [{"k": 3}]}
                    ]),
                    nodes: 11,
                    expect: json!([
                        {"k": "a", "v": [{"k": 2}]},
                        {"k": "b", "v": [{"k": 3}, {"k": 5}]}
                    ]),
                },
                Case {
                    val: json!([
                        {"k": "b", "v": [{"k": 1}, {"k": 5, "d": true}]},
                        {"k": "c", "v": [{"k": 9}]}
                    ]),
                    nodes: 14,
                    expect: json!([
                        {"k": "a", "v": [{"k": 2}]},
                        {"k": "b", "v": [{"k": 1}, {"k": 3}, {"k": 5, "d": true}]},
                        {"k": "c", "v": [{"k": 9}]},
                    ]),
                },
            ],
        )
    }

    #[test]
    fn test_subtract_array_of_scalars() {
        let m = Strategy::Subtract(Subtract { key: Vec::new() });
        run_reduce_cases(
            &m,
            vec![
                Case {
                    val: json!([1, 2, 3, 4, 5, 6, 7]),
                    nodes: 8,
                    expect: json!([1, 2, 3, 4, 5, 6, 7]),
                },
                Case {
                    val: json!([2, 3, 6]),
                    nodes: 4,
                    expect: json!([1, 4, 5, 7]),
                },
                Case {
                    val: json!([2, 3, 4, 5]),
                    nodes: 5,
                    expect: json!([1, 7]),
                },
                Case {
                    val: json!([1, 2, 7, 10]),
                    nodes: 5,
                    expect: json!([]),
                },
                Case {
                    val: json!({"foo": "bar"}),
                    nodes: 2,
                    expect: json!({"foo": "bar"}),
                },
            ],
        )
    }

    #[test]
    fn test_subtract_array_by_key() {
        let m = Strategy::Subtract(Subtract {
            key: vec!["/k".to_owned()],
        });
        run_reduce_cases(
            &m,
            vec![
                Case {
                    val: json!([{"k":"a"}, {"k":"b"}, {"k":"c"}]),
                    nodes: 7,
                    expect: json!([{"k":"a"}, {"k":"b"}, {"k":"c"}]),
                },
                Case {
                    val: json!([{"a": "ignored", "k":"aa"}, {"k":"c", "extra": 1}]),
                    nodes: 7,
                    expect: json!([{"k":"a"}, {"k":"b"}]),
                },
                Case {
                    val: json!([{"k":"a"}, {"k":"aa"}, {"k":"bb"}]),
                    nodes: 7,
                    expect: json!([{"k":"b"}]),
                },
            ],
        )
    }

    #[test]
    fn test_subtract_object() {
        let m = Strategy::Subtract(Subtract { key: Vec::new() });
        run_reduce_cases(
            &m,
            vec![
                Case {
                    val: json!({"1": 11, "2": 22, "3": 33, "4": 44}),
                    nodes: 5,
                    expect: json!({"1": 11, "2": 22, "3": 33, "4": 44}),
                },
                Case {
                    val: json!({"0": 0, "2": true, "6": false}),
                    nodes: 4,
                    expect: json!({"1": 11, "3": 33, "4": 44}),
                },
                Case {
                    val: json!({"2": null, "3": null, "4": null}),
                    nodes: 4,
                    expect: json!({"1": 11}),
                },
                Case {
                    val: json!({"1": 32, "foo": 22}),
                    nodes: 3,
                    expect: json!({}),
                },
                Case {
                    val: json!([1, 2]),
                    nodes: 3,
                    expect: json!([1, 2]),
                },
            ],
        )
    }

    #[test]
    fn test_first_write_wins() {
        let m = Strategy::FirstWriteWins;
        run_reduce_cases(
            &m,
            vec![
                Case {
                    val: json!("abc"),
                    nodes: 1,
                    expect: json!("abc"),
                },
                Case {
                    val: json!("def"),
                    nodes: 1,
                    expect: json!("abc"),
                },
                Case {
                    val: json!(123),
                    nodes: 1,
                    expect: json!("abc"),
                },
            ],
        );
    }

    #[test]
    fn test_last_write_wins() {
        let m = Strategy::LastWriteWins;
        run_reduce_cases(
            &m,
            vec![
                Case {
                    val: json!("abc"),
                    nodes: 1,
                    expect: json!("abc"),
                },
                Case {
                    val: json!("def"),
                    nodes: 1,
                    expect: json!("def"),
                },
                Case {
                    val: json!(123),
                    nodes: 1,
                    expect: json!(123),
                },
            ],
        );
    }

    #[test]
    fn test_append() {
        let m = Strategy::Append;
        run_reduce_cases(
            &m,
            vec![
                // Takes initial value.
                Case {
                    val: json!([1, 2]),
                    nodes: 3,
                    expect: json!([1, 2]),
                },
                // Additional values are appended.
                Case {
                    val: json!(["three"]),
                    nodes: 2,
                    expect: json!([1, 2, "three"]),
                },
                Case {
                    val: json!([null, 5.5]),
                    nodes: 3,
                    expect: json!([1, 2, "three", null, 5.5]),
                },
                // Default to last-write-wins on incompatible types.
                Case {
                    val: json!("aaa"),
                    nodes: 1,
                    expect: json!("aaa"),
                },
                // Strings may also be appended.
                Case {
                    val: json!("bbb"),
                    nodes: 1,
                    expect: json!("aaabbb"),
                },
                Case {
                    val: json!(""),
                    nodes: 1,
                    expect: json!("aaabbb"),
                },
                // Last-write-wins on incompatible type.
                Case {
                    val: json!({"an": "obj"}),
                    nodes: 2,
                    expect: json!({"an": "obj"}),
                },
            ],
        )
    }

    #[test]
    fn test_reduce_at() {
        // Create a fixture of reduce strategies indexed at given document locations.
        let idx = vec![
            Strategy::Merge(Merge { key: Vec::new() }),
            Strategy::Minimize(Minimize { key: Vec::new() }),
            Strategy::Maximize(Maximize { key: Vec::new() }),
            Strategy::Merge(Merge { key: Vec::new() }),
            Strategy::Sum,
            Strategy::FirstWriteWins,
        ];
        let idx = idx.iter().map(|s| (s, 0)).collect::<Vec<_>>();

        // Run a set of cases having shapes matching our strategy index fixture.
        // Expect each document location was reduced using the expected strategy.
        let cases = vec![
            // Array merge cases.
            (
                json!([5, 5.0, [10, "first"]]),
                json!([5, 5.0, [10, "first"]]),
            ),
            (
                json!([10.0, 10, [15, "second"]]),
                json!([5, 10, [25, "first"]]),
            ),
            (
                json!([4.1, 4, [-0.5, "third"]]),
                json!([4.1, 10, [24.5, "first"]]),
            ),
            // Object merge cases. The first reduced object document is taken as |into|,
            // as they're incompatible types and in this case merge defaults to LWW.
            (
                json!({"a": 5, "b": 5.0, "c": {"d": 10, "e": "first"}}),
                json!({"a": 5, "b": 5.0, "c": {"d": 10, "e": "first"}}),
            ),
            (
                json!({"a": 10.0, "b": 10, "c": {"d": 15, "e": "second"}}),
                json!({"a": 5, "b": 10, "c": {"d": 25, "e": "first"}}),
            ),
            (
                json!({"a": 4.1, "b": 4, "c": {"d": -0.5, "e": "third"}}),
                json!({"a": 4.1, "b": 10, "c": {"d": 24.5, "e": "first"}}),
            ),
        ];

        let mut into = Value::Null;
        for (i, (doc, expect)) in cases.into_iter().enumerate() {
            assert_eq!(
                6,
                Reducer {
                    at: 0,
                    val: doc,
                    into: &mut into,
                    created: i == 0,
                    idx: &idx,
                }
                .reduce()
            );

            assert_eq!(&expect, &into);
        }
    }
}
