use super::{Collection, Error, Resource, Result};
use crate::specs::build as specs;
use rusqlite::Connection as DB;
use url::Url;

/// Source represents a top-level catalog build input.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct Source {
    pub resource: Resource,
}

impl Source {
    /// Register an Estuary Source specification with the catalog.
    pub fn register(db: &DB, uri: Url) -> Result<Source> {
        let source = Source {
            resource: Resource::register(db, uri)?,
        };
        if !source.resource.added {
            return Ok(source);
        }

        let spec = source.resource.fetch_to_string(db)?;
        let spec: specs::Source = serde_yaml::from_str(&spec)?;

        for uri in &spec.import {
            let uri = source.resource.join(db, uri)?;
            let import = Self::register(db, uri.clone()).map_err(|err| Error::At {
                loc: format!("import {}", uri),
                detail: Box::new(err),
            })?;
            Resource::register_import(db, source.resource, import.resource)?;
        }
        for spec in &spec.collections {
            Collection::register(db, source, spec).map_err(|err| Error::At {
                loc: format!("collection {}", spec.name),
                detail: Box::new(err),
            })?;
        }
        Ok(source)
    }
}
