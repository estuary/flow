use super::{Annotation, Reducer};
use estuary_json::{self as ej, validator};
use serde_json as sj;

// use thiserror;

type Outcome<'a> = (validator::Outcome<'a, Annotation>, validator::FullContext);

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
