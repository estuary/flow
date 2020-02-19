use super::*;
use crate::specs::canonical;
use crate::specs::canonical::Canonicalized;
use std::io;

type Error = Box<dyn std::error::Error>;

pub trait FileSystem {
    fn open(&self, url: &url::Url) -> Result<Box<dyn io::Read>, Error>;
}

pub struct Loader {
    fs: Box<dyn FileSystem>,
    //collections: Vec<specs::Collection>,
}

impl Loader {
    pub fn new(fs: Box<dyn FileSystem>) -> Loader {
        Loader { fs }
    }

    pub fn load_node(&mut self, base: url::Url) -> Result<specs::Node, Error> {
        let rdr = self.fs.open(&base)?;
        let br = io::BufReader::new(rdr);
        let spec: specs::Node = serde_yaml::from_reader(br)?;
        let spec = spec.into_canonical(&base)?;
        Ok(spec)
    }

    /*
    fn process_root(&mut self, base: url::Url, spec: specs::Project) -> Result<(), Error> {
        for mut c in spec.collections {
            c.name = base.join(&c.name)?.to_string();
            c.schema = base.join(&c.schema)?.to_string();

            if !c.examples.is_empty() {
                c.examples = base.join(&c.examples)?.to_string();
            }
            if let Some(d) = &mut c.derivation {
                self.process_derivation(&base, d)
            }
        }
        Ok(())
    }

    fn process_derivation(&mut self, base: &url::Url, spec: &mut specs::Derivation) -> Result<(), Error> {
        use specs::Derivation;

        match &mut d {
            Derivation::Jq(d) =>
        }
    }

    fn process_path(&mut self, base: &url::Url, path: &mut String) -> Result<(), Error> {
        Ok(*path = base.join(path)?.to_string())
    }

    */
}
