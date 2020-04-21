use super::{Resource, Schema, Result, Source, Derivation, Error};
use crate::specs::build as specs;
use crate::doc::Pointer;
use rusqlite::{params as sql_params, Connection as DB};
use std::convert::TryFrom;

/// Collection represents a catalog Collection.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct Collection {
    pub id: i64,
    pub resource: Resource,
}

impl Collection {
    /// Registers a Collection of the Source with the catalog.
    pub fn register(db: &DB, source: Source, spec: &specs::Collection) -> Result<Collection> {
        // Canonicalize and register the JSON-Schema URI.
        let schema = source.resource.join(db, &spec.schema)?;
        let schema = Schema::register(db, schema)?;
        Resource::register_import(db, source.resource, schema.resource)?;

        // Marshal key extractor to JSON.
        for key in spec.key.iter() {
            Pointer::try_from(key)?;
        }
        let key_json = serde_json::to_string(&spec.key)?;

        // Do the insert.
        db.prepare_cached(
            "INSERT INTO collections (name, schema_uri, key_json, resource_id) VALUES (?, ?, ?, ?)",
        )?
        .execute(sql_params![
            spec.name,
            schema.resource.uri(db)?,
            key_json,
            source.resource.id,
        ])?;
        let col = Collection {
            id: db.last_insert_rowid(),
            resource: source.resource,
        };

        for spec in &spec.fixtures {
            col.register_fixture(db, spec).map_err(|err| Error::At {
                msg: format!("registering fixture {}", spec),
                detail: Box::new(err),
            })?;
        }
        for spec in &spec.projections {
            col.register_projection(db, spec)?;
        }
        if let Some(spec) = &spec.derivation {
            Derivation::register(db, col, spec)?;
        }

        Ok(col)
    }

    pub fn query(db: &DB, name: &str, from: Resource) -> Result<Collection> {
        let query = db
            .prepare_cached("SELECT collection_id, resource_id FROM collections WHERE name = ?;")?
            .query_row(&[name], |row| Ok((row.get(0)?, row.get(1)?)));

        let col = match query {
            Ok((id, rid)) => Collection {
                id,
                resource: Resource {
                    id: rid,
                    added: false,
                },
            },
            Err(err) => {
                return Err(Error::At {
                    msg: format!("querying for collection {}", name),
                    detail: Box::new(err.into()),
                })
            }
        };
        Resource::verify_import(db, col.resource, from)?;

        Ok(col)
    }

    /// Fetch the key of this Collection.
    pub fn key(&self, db: &DB) -> Result<Vec<String>> {
        let mut s = db.prepare_cached("SELECT key_json FROM collections WHERE id = ?;")?;
        let key: String = s.query_row(&[self.id], |row| row.get(0))?;
        let key: Vec<String> = serde_json::from_str(&key)?;
        Ok(key)
    }

    fn register_fixture(&self, db: &DB, uri: &str) -> Result<()> {
        let import = self.resource.join(db, uri)?;
        let import = Resource::register(db, import)?;
        Resource::register_import(db, self.resource, import)?;

        let content = import.fetch_to_string(db)?;
        let fixtures: Vec<specs::Fixture> = serde_yaml::from_str(&content)?;

        for fx in fixtures.into_iter() {
            db.prepare_cached(
                "INSERT INTO fixtures (
                        collection_id,
                        document_json,
                        key_json,
                        projections_json,
                        resource_id
                    ) VALUES (?, ?, ?, ?, ?)",
            )?
            .execute(sql_params![
                self.id,
                fx.document,
                serde_json::Value::Array(fx.key),
                serde_json::Value::Object(fx.projections),
                import.id,
            ])?;
        }
        Ok(())
    }

    fn register_projection(&self, db: &DB, spec: &specs::Projection) -> Result<()> {
        Pointer::try_from(&spec.location)?;

        db.prepare_cached(
            "INSERT INTO projections (
                    collection_id,
                    field,
                    location_ptr,
                    is_logical_partition
                ) VALUES (?, ?, ?, ?)",
        )?
        .execute(sql_params![
            self.id,
            spec.field,
            spec.location,
            spec.partition,
        ])?;

        Ok(())
    }
}

