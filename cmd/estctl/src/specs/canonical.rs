
/*
use super::*;
use thiserror;

#[derive(thiserror::Error, Debug)]
#[error("joining '{s}' with base URL '{base}': {detail}")]
pub struct Error {
    s: String,
    base: url::Url,
    detail: url::ParseError,
}

pub trait Canonicalized
where
    Self: Sized,
{
    fn into_canonical(self, base: &url::Url) -> Result<Self, Error>;
    fn into_relative(self, root: &str) -> Self;
}

impl Canonicalized for Node {
    fn into_canonical(mut self, base: &Url) -> Result<Self, Error> {
        self.import = self
            .import
            .into_iter()
            .map(|s| join_str(base, &s))
            .collect::<Result<Vec<String>, Error>>()?;
        self.collections = self
            .collections
            .into_iter()
            .map(|c| c.into_canonical(base))
            .collect::<Result<Vec<Collection>, Error>>()?;
        self.materializations = self
            .materializations
            .into_iter()
            .map(|m| m.into_canonical(base))
            .collect::<Result<Vec<Materialization>, Error>>()?;
        Ok(self)
    }

    fn into_relative(mut self, root: &str) -> Self {
        self.collections = self
            .collections
            .into_iter()
            .map(|c| c.into_relative(root))
            .collect();
        self.materializations = self
            .materializations
            .into_iter()
            .map(|m| m.into_relative(root))
            .collect();
        self
    }
}

impl Canonicalized for Collection {
    fn into_canonical(mut self, base: &url::Url) -> Result<Self, Error> {
        self.name = join_str(base, &self.name)?;
        self.schema = join_str(base, &self.schema)?;

        if !self.examples.is_empty() {
            self.examples = join_str(base, &self.examples)?;
        }

        self.derivation = match self.derivation {
            None => None,
            Some(d) => Some(d.into_canonical(base)?),
        };
        Ok(self)
    }

    fn into_relative(mut self, root: &str) -> Self {
        self.name = remove_prefix(self.name, root);
        self.schema = remove_prefix(self.schema, root);
        self.examples = remove_prefix(self.examples, root);
        self.derivation = match self.derivation {
            None => None,
            Some(d) => Some(d.into_relative(root)),
        };
        self
    }
}

impl Canonicalized for Materialization {
    fn into_canonical(mut self, base: &Url) -> Result<Self, Error> {
        self.collection = join_str(base, &self.collection)?;
        Ok(self)
    }

    fn into_relative(mut self, root: &str) -> Self {
        self.collection = remove_prefix(self.collection, root);
        self
    }
}

impl Canonicalized for Derivation {
    fn into_canonical(self, base: &url::Url) -> Result<Self, Error> {
        use Derivation::*;
        match self {
            Jq(mut d) => {
                d.transforms = d
                    .transforms
                    .into_iter()
                    .map(|mut t| {
                        t.source = join_str(base, &t.source)?;
                        t.path = join_str(base, &t.path)?;
                        Ok(t)
                    })
                    .collect::<Result<Vec<JQTransform>, Error>>()?;
                Ok(Jq(d))
            }
        }
    }
    fn into_relative(self, root: &str) -> Self {
        use Derivation::*;
        match self {
            Jq(mut d) => {
                d.transforms = d
                    .transforms
                    .into_iter()
                    .map(|mut t| {
                        t.source = remove_prefix(t.source, root);
                        t.path = remove_prefix(t.path, root);
                        t
                    })
                    .collect::<Vec<JQTransform>>();
                Jq(d)
            }
        }
    }
}

fn join_str(base: &url::Url, s: &str) -> Result<String, Error> {
    join(base, s).map(|u| u.into_string())
}

pub fn join(base: &url::Url, s: &str) -> Result<url::Url, Error> {
    match base.join(&s) {
        Ok(url) => Ok(url),
        Err(e) => Err(Error {
            s: s.to_owned(),
            base: base.clone(),
            detail: e,
        }),
    }
}


fn remove_prefix(mut s: String, prefix: &str) -> String {
    if s.starts_with(prefix) {
        s.drain(0..prefix.len());
    }
    s
}
*/
