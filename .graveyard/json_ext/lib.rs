use estuary_json::{schema, validator};
use serde_json as sj;
use std::convert::TryFrom;

pub mod accumulate;
pub mod message;
pub mod ptr;
pub mod reduce;
mod varint;

/*
type Outcomes<'a> = ;

struct FooReducer<'a>(&'a Outcomes<'a>);

impl<'sm> reduce::Reducer for FooReducer<'sm> {
    fn reduce<R>(
        &self,
        at: usize,
        val: sj::Value,
        into: &mut sj::Value,
        created: bool,
        _sub: &R,
    ) -> usize
    where
        R: reduce::Reducer,
    {
        let pivot = self.0.iter().enumerate().find_map(
            |(idx, (outcome, ctx))| -> Option<(usize, Option<&reduce::Strategy>)> {
                if ctx.span.begin > at {
                    Some((idx, None))
                } else if ctx.span.begin < at {
                    None
                } else {
                    match outcome {
                        validator::Outcome::Annotation(Annotation::Reduce(strat)) => {
                            Some((idx, Some(strat)))
                        }
                        _ => None,
                    }
                }
            },
        );
        match pivot {
            Some((idx, Some(strat))) => {
                strat.reduce(at, val, into, created, &FooReducer(&self.0[idx + 1..]))
            }
            Some((idx, None)) => reduce::LastWriteWins {}.reduce(
                at,
                val,
                into,
                created,
                &FooReducer(&self.0[idx + 1..]),
            ),
            None => {
                reduce::LastWriteWins {}.reduce(at, val, into, created, &FooReducer(&self.0[..0]))
            }
        }
    }
}

*/
