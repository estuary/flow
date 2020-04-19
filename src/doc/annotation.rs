use estuary_json::{schema, validator};
use serde_json;
use std::convert::TryFrom;
use super::reduce;

/// Enumeration of JSON-Schema associated annotations understood by Estuary.
#[derive(Debug)]
pub enum Annotation {
    /// Delegate all annotations of the core JSON-Schema spec.
    Core(schema::CoreAnnotation),
    /// "reduce" annotation keyword.
    Reduce(reduce::Strategy),
}

impl schema::Annotation for Annotation {
    fn as_core(&self) -> Option<&schema::CoreAnnotation> {
        match self {
            Annotation::Core(a) => Some(a),
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

    fn from_keyword(keyword: &str, value: &serde_json::Value) -> Result<Self, schema::build::Error> {
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

/// Given outcomes of a successful validation, extract reduce annotations & hashes.
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

