use crate::schema::{Annotation, Application, Keyword, Schema};
use error_chain::bail;
use fxhash::FxHashMap as HashMap;

error_chain::error_chain! {
    errors { NotFound }
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

    pub fn add(&mut self, schema: &'s Schema<A>) -> Result<()> {
        // Index this schema's canonical URI.
        if let Some(_) = self.0.insert(schema.curi.as_str(), schema) {
            bail!("duplicate canonical URI: '{}'", schema.curi);
        }
        //println!("indexed {}", schema.curi.as_str());

        for kw in &schema.kw {
            match kw {
                // Recurse to index a subordinate schema application.
                Keyword::Application(_, child) => self.add(child)?,
                // Index an alternative, anchor-form canonical URI.
                Keyword::Anchor(auri) => {
                    if let Some(_) = self.0.insert(auri.as_str(), schema) {
                        bail!("duplicate anchor URI: '{}'", schema.curi);
                    }
                    //println!("indexed anchor {}", schema.curi.as_str());
                }
                // No-ops.
                Keyword::RecursiveAnchor | Keyword::Validation(_) | Keyword::Annotation(_) => (),
            }
        }
        Ok(())
    }

    pub fn verify_references(&self) -> Result<()> {
        for (&curi, &schema) in &self.0 {
            for kw in &schema.kw {
                if let Keyword::Application(Application::Ref(ruri), _) = kw {
                    if !self.0.contains_key(ruri.as_str()) {
                        bail!(
                            "schema $ref '{}', referenced by '{}', is not indexed",
                            ruri,
                            curi
                        );
                    }
                }
            }
        }
        Ok(())
    }

    pub fn fetch(&self, uri: &url::Url) -> Option<&'s Schema<A>> {
        self.0.get(uri.as_str()).map(|s| *s)
    }
}
