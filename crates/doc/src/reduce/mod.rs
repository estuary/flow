pub use super::{extract_reduce_annotations, validate, FailedValidation, FullContext, Validator};
use itertools::EitherOrBoth;
pub use json::{schema::types, validator::Context, LocatedItem, LocatedProperty, Location};
use serde_json::Value;
use url::Url;

mod set;
mod strategy;

pub use strategy::Strategy;

type Index<'a> = &'a [(&'a Strategy, u64)];

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("document is invalid: {}", serde_json::to_string_pretty(.0).unwrap())]
    FailedValidation(FailedValidation),
    #[error("'append' strategy expects arrays")]
    AppendWrongType,
    #[error("`sum` resulted in numeric overflow")]
    SumNumericOverflow,
    #[error("'sum' strategy expects numbers")]
    SumWrongType,
    #[error("'merge' strategy expects objects or arrays")]
    MergeWrongType,
    #[error(
        "'set' strategy expects objects having only 'add', 'remove', and 'intersect' properties with consistent object or array types"
    )]
    SetWrongType,

    #[error("while reducing {:?}", .ptr)]
    WithLocation {
        ptr: String,
        #[source]
        detail: Box<Error>,
    },
    #[error("having types LHS: {:?}, RHS: {:?}", .lhs_type, .rhs_type)]
    WithTypes {
        lhs_type: types::Set,
        rhs_type: types::Set,
        #[source]
        detail: Box<Error>,
    },
}

/// Reduce a RHS document into a preceding LHS document. The RHS document must
/// validate against the provided Validator and schema URI, which will also
/// provide reduction annotations.
/// If |prune|, then LHS is the root-most (or left-most) document in the reduction
/// sequence. Depending on the reduction strategy, additional pruning can be done
/// in this case (i.e., removing tombstones) that isn't possible in a partial
/// non-root reduction.
pub fn reduce<C: Context>(
    mut validator: &mut Validator<C>,
    schema_curi: &Url,
    lhs: Option<Value>,
    rhs: Value,
    prune: bool,
) -> Result<Value> {
    let span = validate(&mut validator, &schema_curi, &rhs).map_err(Error::FailedValidation)?;
    let tape = extract_reduce_annotations(span, validator.outcomes());
    let tape = &mut tape.as_slice();

    let reduced = match lhs {
        Some(lhs) => Cursor::Both {
            tape,
            loc: Location::Root,
            prune,
            lhs,
            rhs,
        },
        None => Cursor::Right {
            tape,
            loc: Location::Root,
            prune,
            rhs,
        },
    }
    .reduce()?;

    assert!(tape.is_empty());
    Ok(reduced)
}

impl Error {
    fn cursor(cur: Cursor, detail: Error) -> Error {
        let (ptr, lhs_type, rhs_type) = match cur {
            Cursor::Both { loc, lhs, rhs, .. } => (
                loc.pointer_str().to_string(),
                types::Set::for_value(&lhs),
                types::Set::for_value(&rhs),
            ),
            Cursor::Right { loc, rhs, .. } => (
                loc.pointer_str().to_string(),
                types::INVALID,
                types::Set::for_value(&rhs),
            ),
        };

        Error::WithLocation {
            ptr,
            detail: Box::new(Error::WithTypes {
                lhs_type,
                rhs_type,
                detail: Box::new(detail),
            }),
        }
    }

    fn at(loc: Location, detail: Error) -> Error {
        Error::WithLocation {
            ptr: loc.pointer_str().to_string(),
            detail: Box::new(detail),
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

/// Cursor models a joint document location which is being reduced.
enum Cursor<'i, 'l, 'a> {
    Both {
        tape: &'i mut Index<'a>,
        loc: Location<'l>,
        prune: bool,
        lhs: Value,
        rhs: Value,
    },
    Right {
        tape: &'i mut Index<'a>,
        loc: Location<'l>,
        prune: bool,
        rhs: Value,
    },
}

trait Reducer {
    fn reduce(&self, cur: Cursor) -> Result<Value>;
}

impl Cursor<'_, '_, '_> {
    pub fn reduce(self) -> Result<Value> {
        let (strategy, _) = match &self {
            Cursor::Both { tape, .. } | Cursor::Right { tape, .. } => tape.first().unwrap(),
        };
        strategy.reduce(self)
    }
}

fn reduce_prop<'i, 'l, 'a>(
    tape: &'i mut Index<'a>,
    loc: Location<'l>,
    prune: bool,
    eob: EitherOrBoth<(String, Value), (String, Value)>,
) -> Result<(String, Value)> {
    match eob {
        EitherOrBoth::Left((prop, lhs)) => Ok((prop, lhs)),
        EitherOrBoth::Right((prop, rhs)) => {
            let v = Cursor::Right {
                tape,
                loc: loc.push_prop(&prop),
                prune,
                rhs,
            }
            .reduce()?;

            Ok((prop, v))
        }
        EitherOrBoth::Both((prop, lhs), (_, rhs)) => {
            let v = Cursor::Both {
                tape,
                loc: loc.push_prop(&prop),
                prune,
                lhs,
                rhs,
            }
            .reduce()?;

            Ok((prop, v))
        }
    }
}

fn reduce_item<'i, 'l, 'a>(
    tape: &'i mut Index<'a>,
    loc: Location<'l>,
    prune: bool,
    eob: EitherOrBoth<(usize, Value), (usize, Value)>,
) -> Result<Value> {
    match eob {
        EitherOrBoth::Left((_, lhs)) => Ok(lhs),
        EitherOrBoth::Right((index, rhs)) => Cursor::Right {
            tape,
            loc: loc.push_item(index),
            prune,
            rhs,
        }
        .reduce(),
        EitherOrBoth::Both((_, lhs), (index, rhs)) => Cursor::Both {
            tape,
            loc: loc.push_item(index),
            prune,
            lhs,
            rhs,
        }
        .reduce(),
    }
}

fn count_nodes(v: &Value) -> usize {
    match v {
        Value::Bool(_) | Value::Null | Value::String(_) | Value::Number(_) => 1,
        Value::Array(v) => v.iter().fold(1, |c, vv| c + count_nodes(vv)),
        Value::Object(v) => v.iter().fold(1, |c, (_prop, vv)| c + count_nodes(vv)),
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    use crate::{FullContext, Schema, SchemaIndex, Validator};
    use json::schema::build::build_schema;
    pub use serde_json::{json, Value};
    use std::error::Error as StdError;

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

        let doc = json!({
            "two": [3, [5, 6], {"eight": 8}],
            "nine": "nine",
            "ten": null,
            "eleven": true,
        });
        assert_eq!(count_nodes(&doc), 11);
    }

    pub enum Case {
        Partial { rhs: Value, expect: Result<Value> },
        Full { rhs: Value, expect: Result<Value> },
    }
    pub use Case::{Full, Partial};

    pub fn run_reduce_cases(schema: Value, cases: Vec<Case>) {
        let curi = url::Url::parse("http://example/schema").unwrap();
        let schema: Schema = build_schema(curi.clone(), &schema).unwrap();

        let mut index = SchemaIndex::new();
        index.add(&schema).unwrap();
        index.verify_references().unwrap();

        let mut validator = Validator::<FullContext>::new(&index);
        let mut lhs: Option<Value> = None;

        for case in cases {
            let (rhs, expect, prune) = match case {
                Partial { rhs, expect } => (rhs, expect, false),
                Full { rhs, expect } => (rhs, expect, true),
            };
            let reduced = reduce(&mut validator, &curi, lhs.clone(), rhs, prune);

            match expect {
                Ok(expect) => {
                    let reduced = reduced.unwrap();
                    assert_eq!(&reduced, &expect);
                    lhs = Some(reduced)
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
}
