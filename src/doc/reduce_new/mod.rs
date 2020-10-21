mod strategy;
pub use estuary_json::{schema::types, LocatedItem, LocatedProperty, Location};
use itertools::EitherOrBoth;
use serde_json::Value;
pub use strategy::Strategy;

type Index<'a> = &'a [(&'a Strategy, u64)];

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("`sum` resulted in numeric overflow")]
    SumNumericOverflow,
    #[error("'sum' strategy expects numbers")]
    SumWrongType,
    #[error("'merge' strategy expects objects or arrays")]
    MergeWrongType,

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
/// Document LHS always preceeds RHS in the application of reducible operations.
/// If |prune|, then LHS is the root-most (or left-most) document in the reduction
/// sequence. Depending on the reduction strategy, additional pruning can be done
/// in this case (i.e., removing tombstones) that isn't possible in a partial
/// non-root reduction.
pub enum Cursor<'i, 'l, 'a> {
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
    fn reduce(self) -> Result<Value> {
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
mod test {
    use super::*;
    use serde_json::json;

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
}
