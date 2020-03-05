use estuary_json::{self, schema};
use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "strategy", deny_unknown_fields, rename_all = "camelCase")]
pub enum Reducer {
    Minimize,
    Maximize,
    Sum,
    Merge,
    FirstWriteWins,
    LastWriteWins,
}

#[derive(Debug)]
pub enum Annotation {
    Core(schema::CoreAnnotation),
    Reduce(Reducer),
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

    fn from_keyword(
        keyword: &str,
        value: &serde_json::Value,
    ) -> Result<Self, schema::build::Error> {
        use schema::BuildError::AnnotationErr;
        use schema::CoreAnnotation as Core;

        if keyword == "reduce" {
            match Reducer::deserialize(value) {
                Err(e) => Err(AnnotationErr(Box::new(e))),
                Ok(r) => Ok(Annotation::Reduce(r)),
            }
        } else {
            Ok(Annotation::Core(Core::from_keyword(keyword, value)?))
        }
    }
}

pub mod reduce;
