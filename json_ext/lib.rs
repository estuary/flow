use estuary_json::{schema, validator};
use serde_json as sj;
use std::convert::TryFrom;

pub mod reduce;

#[derive(Debug)]
pub enum Annotation {
    Core(schema::CoreAnnotation),
    Reduce(reduce::Strategy),
}

impl schema::Annotation for Annotation {
    fn as_core(&self) -> Option<&schema::CoreAnnotation> {
        match self {
            Annotation::Core(annot) => Some(annot),
            _ => None,
        }
    }
}

impl schema::build::AnnotationBuilder for Annotation {
    fn uses_keyword(keyword: &str) -> bool {
        if keyword == "reduce" {
            true
        } else {
            schema::CoreAnnotation::uses_keyword(keyword)
        }
    }

    fn from_keyword(keyword: &str, value: &sj::Value) -> Result<Self, schema::build::Error> {
        use schema::BuildError::AnnotationErr;
        use schema::CoreAnnotation as Core;

        if keyword == "reduce" {
            match reduce::Strategy::try_from(value) {
                Err(e) => Err(AnnotationErr(Box::new(e))),
                Ok(r) => Ok(Annotation::Reduce(r)),
            }
        } else {
            Ok(Annotation::Core(Core::from_keyword(keyword, value)?))
        }
    }
}

pub fn extract_reduce_annotations<'a, C>(
    outcomes: &[(validator::Outcome<'a, Annotation>, C)],
) -> Vec<(&'a reduce::Strategy, u64)>
where
    C: validator::Context,
{
    let mut idx = Vec::<(&reduce::Strategy, u64)>::new();

    for (outcome, ctx) in outcomes.iter() {
        let span = ctx.span();

        if span.begin >= idx.len() {
            idx.extend(std::iter::repeat((DEFAULT_STRATEGY, 0)).take(1 + span.begin - idx.len()));
        }
        if let validator::Outcome::Annotation(Annotation::Reduce(strategy)) = outcome {
            idx[span.begin] = (strategy, span.hashed);
        }
    }
    idx
}

static DEFAULT_STRATEGY: &reduce::Strategy = &reduce::Strategy::LastWriteWins;

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
