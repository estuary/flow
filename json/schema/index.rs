use crate::schema::{Annotation, Application, Keyword, Schema};
use fxhash::FxHashMap as HashMap;
use thiserror;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("duplicate canonical URI: '{0}'")]
    DuplicateCanonicalURI(url::Url),
    #[error("duplicate anchor URI: '{0}'")]
    DuplicateAnchorURI(url::Url),
    #[error("schema $ref '{ruri}', referenced by '{curi}', was not found")]
    InvalidReference { ruri: url::Url, curi: String },
    #[error("schema '{uri}' was not found")]
    NotFound { uri: url::Url },
}

pub struct Index<'s, A>(HashMap<&'s str, &'s Schema<A>>)
where
    A: Annotation;

impl<'s, A> Index<'s, A>
where
    A: Annotation,
{
    pub fn new() -> Index<'s, A> {
        Index(HashMap::default())
    }

    pub fn add(&mut self, schema: &'s Schema<A>) -> Result<(), Error> {
        use Error::*;

        // Index this schema's canonical URI.
        if let Some(_) = self.0.insert(schema.curi.as_str(), schema) {
            return Err(DuplicateCanonicalURI(schema.curi.clone()));
        }
        //println!("indexed {}", schema.curi.as_str());

        for kw in &schema.kw {
            match kw {
                // Recurse to index a subordinate schema application.
                Keyword::Application(_, child) => self.add(child)?,
                // Index an alternative, anchor-form canonical URI.
                Keyword::Anchor(auri) => {
                    if let Some(_) = self.0.insert(auri.as_str(), schema) {
                        return Err(DuplicateAnchorURI(schema.curi.clone()));
                    }
                    //println!("indexed anchor {}", schema.curi.as_str());
                }
                // No-ops.
                Keyword::RecursiveAnchor | Keyword::Validation(_) | Keyword::Annotation(_) => (),
            }
        }
        Ok(())
    }

    pub fn verify_references(&self) -> Result<(), Error> {
        for (&curi, &schema) in &self.0 {
            for kw in &schema.kw {
                if let Keyword::Application(Application::Ref(ruri), _) = kw {
                    if !self.0.contains_key(ruri.as_str()) {
                        return Err(Error::InvalidReference {
                            ruri: ruri.clone(),
                            curi: curi.to_owned(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    pub fn fetch(&self, uri: &url::Url) -> Option<&'s Schema<A>> {
        self.0.get(uri.as_str()).map(|s| *s)
    }

    pub fn must_fetch(&self, uri: &url::Url) -> Result<&'s Schema<A>, Error> {
        match self.fetch(uri) {
            None => Err(Error::NotFound { uri: uri.clone() }),
            Some(scm) => Ok(scm),
        }
    }
}
