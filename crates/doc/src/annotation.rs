use super::reduce;
use json::{schema, validator, validator::Context};
use std::convert::TryFrom;

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

    fn from_keyword(
        keyword: &str,
        value: &serde_json::Value,
    ) -> Result<Self, schema::build::Error> {
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

impl<'sm, 'v> super::Valid<'sm, 'v> {
    pub fn extract_reduce_annotations(&self) -> Vec<(&'sm reduce::Strategy, u64)> {
        let mut idx = std::iter::repeat((DEFAULT_STRATEGY, 0))
            .take(self.0.span.end)
            .collect::<Vec<_>>();

        for (outcome, ctx) in self.0.validator.outcomes() {
            let subspan = ctx.span();

            if let validator::Outcome::Annotation(Annotation::Reduce(strategy)) = outcome {
                idx[subspan.begin] = (strategy, subspan.hashed);
            }
        }
        idx
    }
}

static DEFAULT_STRATEGY: &reduce::Strategy = &reduce::Strategy::LastWriteWins;
