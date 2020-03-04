use super::{Annotation, Application, index, Keyword, Schema, types, Validation};

use thiserror;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("index error: {0}")]
    IndexErr(#[from] index::Error),

    #[error("incompatible '{what}' keywords at {path}: {a} vs {b}")]
    IncompatibleKeywords{
        what: String,
        path: Path,
        a: Option<String>,
        b: Option<String>,
    }
}
use Error::*;

#[derive(Debug, PartialEq)]
pub enum PathElem {
    Property(String),
    PropertyPattern(regex::Regex),
    ItemAt(usize),
    ItemAfter(usize),
}

#[derive(Debug, PartialEq)]
pub struct Path(Vec<PathElem>);

#[derive(Debug)]
pub struct Inference {
    path: Path,
    type_set: types::Set,
    content_type: Option<String>,
    content_encoding: Option<String>,
}

impl Inference {

    fn fold_disjunction(self, other: &Inference) -> Result<Inference, Error> {
        Ok(Inference {
            path: self.path,
            type_set: self.type_set | other.type_set,
            content_type: if self.content_type == other.content_type {
                self.content_type
            } else {
                None
            },
            content_encoding: if self.content_encoding == other.content_encoding {
                self.content_encoding
            } else {
                None
            },
        })
    }

    fn fold_conjunction(self, other: &Inference) -> Result<Inference, Error> {
        if self.content_type != other.content_type {
            return Err(IncompatibleKeywords {
                what: "contentType".toOwned(),
                path: self.path,
                a: self.content_type,
                b: other.content_type.clone(),
            })
        }
        if self.content_encoding != other.content_encoding {
            return Err(IncompatibleKeywords {
                what: "contentEncoding".toOwned(),
                path: self.path,
                a: self.content_encoding,
                b: other.content_encoding.clone(),
            })
        }
        if self.type_set & other.type_set == types::INVALID {
            return Err(IncompatibleKeywords {
                what: "type".toOwned(),
                path: self.path,
                a: Some(format!("{:?}", self.type_set)),
                b: Some(format!("{:?}", other.type_set)),
            })
        }
        Ok(Inference {
            path: self.path,
            type_set: self.type_set & other.type_set,
            content_type: self.content_type,
            content_encoding: self.content_encoding,
        })
    }

}

fn reduce_and(v: Vec<Inference>) -> Result<Vec<Inference>> {
    let mut out = Vec::new();

    for (i, inf) in v.iter().enumerate() {

        v[0..i].iter().fold()

    }


    Ok(out)
}



pub fn extract<'s, A>(schema: &'s Schema<A>, idx: &index::Index<'s, A>) -> Result<Vec<Inference>, Error>
    where A: Annotation
{
    let mut type_set = types::ANY;
    let mut content_type : Option<String> = None;
    let mut content_enc : Option<String> = None;

    let mut out = Vec::new();
    let mut anyOf = Vec::new();
    let mut oneOf = Vec::new();
    let mut thenElse = Vec::new();

    for kw in &schema.kw {
        use Keyword::*;
        use Application::*;
        use Validation::*;

        enum andOr {
            And,
            Or
        };
        use andOr::{And, Or};

        match kw {
            Application(app, sub) => {

                match app {
                    Def { .. } => continue,
                    RecursiveRef(..) => continue, // TODO
                    Not { .. } => continue,

                    Ref(uri) => (And, idx.must_fetch(uri)?),

                    AnyOf { .. } => (Or, sub),
                    AllOf { .. } => (And, sub),
                    OneOf { .. } => (Or, sub),







                }
            },
            Validation(val) => {

            },
            Annotation(annot) => {

            },
            RecursiveAnchor | Anchor{..} => {}, // No-ops.
        }

    }





    Ok(out)
}
