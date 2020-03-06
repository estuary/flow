use estuary_json as ej;
use itertools::EitherOrBoth;
use serde::{Deserialize, Serialize};
use serde_json as sj;
use std::cmp::Ordering;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "strategy", deny_unknown_fields, rename_all = "camelCase")]
pub enum Strategy {
    Minimize(Minimize),
    Maximize(Maximize),
    Sum(Sum),
    Merge(Merge),
    FirstWriteWins(FirstWriteWins),
    LastWriteWins(LastWriteWins),
}

impl std::convert::TryFrom<&sj::Value> for Strategy {
    type Error = serde_json::Error;

    fn try_from(v: &sj::Value) -> Result<Self, Self::Error> {
        Strategy::deserialize(v)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Minimize {
    ptr: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Maximize {
    ptr: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Sum {}

#[derive(Serialize, Deserialize, Debug)]
pub struct Merge {
    ptr: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FirstWriteWins {}

#[derive(Serialize, Deserialize, Debug)]
pub struct LastWriteWins {}

pub trait Reducer {
    fn reduce<R>(
        &self,
        at: usize,
        val: sj::Value,
        into: &mut sj::Value,
        created: bool,
        sub: &R,
    ) -> usize
    where
        R: Reducer;
}

impl Reducer for Strategy {
    fn reduce<R>(
        &self,
        at: usize,
        val: sj::Value,
        into: &mut sj::Value,
        created: bool,
        sub: &R,
    ) -> usize
    where
        R: Reducer,
    {
        match self {
            Strategy::Minimize(m) => m.reduce(at, val, into, created, sub),
            Strategy::Maximize(m) => m.reduce(at, val, into, created, sub),
            Strategy::Sum(m) => m.reduce(at, val, into, created, sub),
            Strategy::Merge(m) => m.reduce(at, val, into, created, sub),
            Strategy::FirstWriteWins(m) => m.reduce(at, val, into, created, sub),
            Strategy::LastWriteWins(m) => m.reduce(at, val, into, created, sub),
        }
    }
}

impl Reducer for FirstWriteWins {
    fn reduce<R>(
        &self,
        at: usize,
        val: sj::Value,
        into: &mut sj::Value,
        created: bool,
        _sub: &R,
    ) -> usize
    where
        R: Reducer,
    {
        if created {
            *into = val;
            at + count_nodes(into)
        } else {
            at + count_nodes(&val)
        }
    }
}

impl Reducer for LastWriteWins {
    fn reduce<R>(
        &self,
        at: usize,
        val: sj::Value,
        into: &mut sj::Value,
        _created: bool,
        _sub: &R,
    ) -> usize
    where
        R: Reducer,
    {
        *into = val;
        at + count_nodes(into)
    }
}

impl Reducer for Minimize {
    fn reduce<R>(
        &self,
        at: usize,
        val: sj::Value,
        into: &mut sj::Value,
        _created: bool,
        _sub: &R,
    ) -> usize
    where
        R: Reducer,
    {
        match json_cmp_at(self.ptr.as_deref(), &val, into) {
            Some(Ordering::Greater) | Some(Ordering::Equal) => at + count_nodes(&val),
            Some(Ordering::Less) | None => {
                *into = val;
                at + count_nodes(into)
            }
        }
    }
}

impl Reducer for Maximize {
    fn reduce<R>(
        &self,
        at: usize,
        val: sj::Value,
        into: &mut sj::Value,
        _created: bool,
        _sub: &R,
    ) -> usize
    where
        R: Reducer,
    {
        match json_cmp_at(self.ptr.as_deref(), &val, into) {
            Some(Ordering::Less) | Some(Ordering::Equal) => at + count_nodes(&val),
            Some(Ordering::Greater) | None => {
                *into = val;
                at + count_nodes(into)
            }
        }
    }
}

impl Reducer for Sum {
    fn reduce<R>(
        &self,
        at: usize,
        val: sj::Value,
        into: &mut sj::Value,
        created: bool,
        _sub: &R,
    ) -> usize
    where
        R: Reducer,
    {
        match (&*into, &val) {
            (sj::Value::Number(lhs), sj::Value::Number(rhs)) => {
                *into = (ej::Number::from(lhs) + ej::Number::from(rhs)).into();
                at + 1
            }
            (sj::Value::Null, sj::Value::Number(_)) if !created => {
                // Leave as null.
                at + 1
            }
            _ => {
                *into = val;
                at + count_nodes(into)
            }
        }
    }
}

impl Reducer for Merge {
    fn reduce<R>(
        &self,
        mut at: usize,
        val: sj::Value,
        into: &mut sj::Value,
        _created: bool,
        sub: &R,
    ) -> usize
    where
        R: Reducer,
    {
        match (into, val) {
            (into @ sj::Value::Array(_), sj::Value::Array(val)) => {
                // TODO: work-around for "cannot bind by-move and by-ref in the same pattern".
                // https://github.com/rust-lang/rust/issues/68354
                let into = into.as_array_mut().unwrap();
                let prev = std::mem::replace(into, Vec::new());

                into.extend(
                    itertools::merge_join_by(
                        prev.into_iter(),
                        val.into_iter(),
                        |lhs, rhs| -> Ordering {
                            json_cmp_at(self.ptr.as_deref(), lhs, rhs).unwrap_or(Ordering::Equal)
                        },
                    )
                    .map(|eob| match eob {
                        EitherOrBoth::Both(mut into, val) => {
                            at = sub.reduce(at + 1, val, &mut into, false, sub);
                            into
                        }
                        EitherOrBoth::Right(val) => {
                            let mut into = sj::Value::Null;
                            at = sub.reduce(at + 1, val, &mut into, true, sub);
                            into
                        }
                        EitherOrBoth::Left(into) => into,
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

                into.extend(
                    itertools::merge_join_by(
                        into_prev.into_iter(),
                        val.into_iter(),
                        |lhs, rhs| -> Ordering { lhs.0.cmp(&rhs.0) },
                    )
                    .map(|eob| match eob {
                        EitherOrBoth::Both((prop, mut into), (_, val)) => {
                            at = sub.reduce(at + 1, val, &mut into, false, sub);
                            (prop, into)
                        }
                        EitherOrBoth::Right((prop, val)) => {
                            let mut into = sj::Value::Null;
                            at = sub.reduce(at + 1, val, &mut into, true, sub);
                            (prop, into)
                        }
                        EitherOrBoth::Left(into) => into,
                    }),
                );

                at
            }
            (into, val) => {
                *into = val;
                at + count_nodes(into)
            }
        }
    }
}

/*

fn do_reduce<'a>(
    index: usize,
    mut outcomes: &[Outcome<'a>],
    prev: &mut sj::Value,
    next: sj::Value,
) {
    let lww = Reducer::LastWriteWins;
    let mut reduce = &lww;

    // Consume an available "reduce" annotation at this location.
    while let Some((first, rest)) = outcomes.split_first() {
        if first.1.span_begin != index {
            break;
        }
        match first.0 {
            validator::Outcome::Annotation(Annotation::Reduce(r)) => {
                reduce = r;
            }
            _ => (),
        }
        outcomes = rest;
    }

    use Reducer::*;
    match reduce {
        FirstWriteWins => {
            // No-op.
        }
        LastWriteWins => {
            *prev = next;
        }
        Minimize => do_minimize(prev, next),
        _ => {}
    }
}

fn do_minimize(lhs: &mut sj::Value, mut rhs: sj::Value) {
    let mut keep_lhs = false;

    if !keep_lhs {
        std::mem::swap(lhs, &mut rhs);
    }
}
*/

fn json_cmp_at(ptr: Option<&str>, lhs: &sj::Value, rhs: &sj::Value) -> Option<Ordering> {
    if let Some(ptr) = ptr {
        ej::json_cmp(
            lhs.pointer(ptr).unwrap_or(&sj::Value::Null),
            rhs.pointer(ptr).unwrap_or(&sj::Value::Null),
        )
    } else {
        ej::json_cmp(lhs, rhs)
    }
}

fn count_nodes(v: &sj::Value) -> usize {
    match v {
        sj::Value::Bool(_) | sj::Value::Null | sj::Value::String(_) | sj::Value::Number(_) => 1,
        sj::Value::Array(v) => v.iter().fold(0, |c, vv| c + count_nodes(vv)) + v.len(),
        sj::Value::Object(v) => v.iter().fold(0, |c, (_prop, vv)| c + count_nodes(vv)) + v.len(),
    }
}
