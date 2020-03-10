use estuary_json as ej;
use itertools::EitherOrBoth;
use serde::{Deserialize, Serialize};
use serde_json as sj;
use std::cmp::Ordering;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "strategy", deny_unknown_fields, rename_all = "camelCase")]
pub enum Strategy {
    FirstWriteWins,
    LastWriteWins,
    Maximize(Maximize),
    Merge(Merge),
    Minimize(Minimize),
    Sum,
}

impl std::convert::TryFrom<&sj::Value> for Strategy {
    type Error = serde_json::Error;

    fn try_from(v: &sj::Value) -> Result<Self, Self::Error> {
        Strategy::deserialize(v)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Minimize {
    #[serde(default)]
    key: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Maximize {
    #[serde(default)]
    key: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Merge {
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
        println!(
            "evaluting at {} doc {} strat {:?}",
            self.at,
            &self.val,
            self.idx.get(self.at)
        );

        match self.idx.get(self.at) {
            Some((Strategy::FirstWriteWins, _)) => self.first_write_wins(),
            Some((Strategy::LastWriteWins, _)) | None => self.last_write_wins(),
            Some((Strategy::Maximize(s), _)) => self.maximize(s),
            Some((Strategy::Merge(s), _)) => self.merge(s),
            Some((Strategy::Minimize(s), _)) => self.minimize(s),
            Some((Strategy::Sum, _)) => self.sum(),
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
                                at: at,
                                val: val,
                                into: &mut into,
                                created: false,
                                idx: idx,
                            }
                            .reduce();
                            into
                        }
                        EitherOrBoth::Right((_, val)) => {
                            let mut into = sj::Value::Null;
                            at = Reducer {
                                at: at,
                                val: val,
                                into: &mut into,
                                created: true,
                                idx: idx,
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
                                at: at,
                                val: val,
                                into: &mut into,
                                created: false,
                                idx: idx,
                            }
                            .reduce();
                            (prop, into)
                        }
                        EitherOrBoth::Right((prop, val)) => {
                            let mut into = sj::Value::Null;
                            at = Reducer {
                                at: at,
                                val: val,
                                into: &mut into,
                                created: true,
                                idx: idx,
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

    fn sum(self) -> usize {
        match (&*self.into, &self.val) {
            (sj::Value::Number(lhs), sj::Value::Number(rhs)) => {
                *self.into = (ej::Number::from(lhs) + ej::Number::from(rhs)).into();
                self.at + 1
            }
            (sj::Value::Null, sj::Value::Number(_)) if !self.created => {
                self.at + 1 // Leave as null.
            }
            _ => self.at + take_val(self.val, self.into), // Default to last-write-wins.
        }
    }
}

fn count_nodes(v: &sj::Value) -> usize {
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
                // Number which cannot be represented becomes null.
                Case {
                    val: json!(std::f64::MAX / 10.0),
                    nodes: 1,
                    expect: Value::Null,
                },
                // And stays null as further values are added.
                Case {
                    val: json!(-1.0),
                    nodes: 1,
                    expect: Value::Null,
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
        )
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
