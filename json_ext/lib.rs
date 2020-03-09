use estuary_json::{self as ej, schema, validator};
use serde_json as sj;
use std::convert::TryFrom;

mod reduce;

#[derive(Debug)]
pub enum Annotation {
    Core(schema::CoreAnnotation),
    Reduce(reduce::Strategy),
}

#[derive(Debug)]
pub struct SpanContext {
    pub span: ej::Span,
}

impl Default for SpanContext {
    fn default() -> Self {
        SpanContext {
            span: ej::Span {
                begin: 0,
                end: 0,
                hashed: 0,
            },
        }
    }
}

impl validator::Context for SpanContext {
    fn with_details<'sm, 'a, A>(
        _loc: &'a ej::Location<'a>,
        span: &'a ej::Span,
        _scope: &validator::Scope<'sm, A, Self>,
        _parents: &[validator::Scope<'sm, A, Self>],
    ) -> Self
    where
        A: schema::Annotation,
    {
        SpanContext {
            span: ej::Span {
                begin: span.begin,
                end: span.end,
                hashed: span.hashed,
            },
        }
    }
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

/*
type Outcomes<'a> = [(validator::Outcome<'a, Annotation>, SpanContext)];

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