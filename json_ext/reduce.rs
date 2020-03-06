use super::{Annotation};
use estuary_json::{self as ej, validator};
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
    FirstWriteWins,
    LastWriteWins,
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
}

impl std::convert::TryFrom<&sj::Value> for Strategy {
    type Error = serde_json::Error;

    fn try_from(v: &sj::Value) -> Result<Self, Self::Error> {
        Strategy::deserialize(v)
    }
}

pub trait Reducer {
    fn reduce<R>(
        &self,
        at: usize,
        val: sj::Value,
        into: &mut sj::Value,
        created: bool,
        sub: R,
    ) where R: Reducer;
}

impl Reducer for Strategy {
    fn reduce<R>(&self, at: usize, val: sj::Value, into: &mut sj::Value, created: bool, sub: R)
        where R: Reducer
    {
        match self {
            Strategy::Minimize(m) => m.reduce(at, val, into, created, sub),
            Strategy::Maximize(m) => m.reduce(at, val, into, created, sub),
            Strategy::Sum(m) => m.reduce(at, val, into, created, sub),
            Strategy::Merge(m) => m.reduce(at, val, into, created, sub),
            Strategy::FirstWriteWins => if created {
                *into = val;
            }
            Strategy::LastWriteWins => *into = val,
        }
    }
}

impl Reducer for Minimize {
    fn reduce<R>(&self, at: usize, val: sj::Value, into: &mut sj::Value, created: bool, sub: R)
        where R: Reducer
    {
        match json_cmp_at(self.ptr.as_deref(), &val, into) {
            Some(Ordering::Greater) | Some(Ordering::Equal) => (),
            None | Some(Ordering::Less) => *into = val,
        }
    }
}

impl Reducer for Maximize {
    fn reduce<R>(&self, at: usize, val: sj::Value, into: &mut sj::Value, created: bool, sub: R)
        where R: Reducer
    {
        match json_cmp_at(self.ptr.as_deref(), &val, into) {
            Some(Ordering::Less) | Some(Ordering::Equal) => (),
            None | Some(Ordering::Greater) => *into = val,
        }
    }
}

impl Reducer for Sum {
    fn reduce<R>(&self, at: usize, val: sj::Value, into: &mut sj::Value, created: bool, sub: R)
        where R: Reducer
    {

    }
}

impl Reducer for Merge {
    fn reduce<R>(&self, at: usize, val: sj::Value, into: &mut sj::Value, created: bool, sub: R)
        where R: Reducer
    {

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
