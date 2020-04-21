use super::{Derivation, Error, Resource, Result, Schema, Source};
use crate::doc::Pointer;
use crate::specs::build as specs;
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
        // Canonicalize and register the Collection JSON-Schema.
        let schema_uri = source.resource.join(db, &spec.schema)?;
        let schema = Schema::register(db, schema_uri.clone()).map_err(|err| Error::At {
            loc: format!("schema {:?}", schema_uri),
            detail: Box::new(err),
        })?;
        Resource::register_import(db, source.resource, schema.resource)?;

        // Marshal key extractor to JSON.
        for key in spec.key.iter() {
            Pointer::try_from(key)?;
        }
        let key_json = serde_json::to_string(&spec.key)?;

        // Do the insert.
        db.prepare_cached(
            "INSERT INTO collections (
                    name,
                    schema_uri,
                    key_json,
                    resource_id
                ) VALUES (?, ?, ?, ?)",
        )?
        .execute(sql_params![
            spec.name,
            schema_uri,
            key_json,
            source.resource.id,
        ])?;
        let collection = Collection {
            id: db.last_insert_rowid(),
            resource: source.resource,
        };

        for uri in &spec.fixtures {
            collection
                .register_fixture(db, uri)
                .map_err(|err| Error::At {
                    loc: format!("fixture {:?}", uri),
                    detail: Box::new(err),
                })?;
        }
        for spec in &spec.projections {
            collection.register_projection(db, spec)?;
        }
        if let Some(spec) = &spec.derivation {
            Derivation::register(db, collection, spec)?;
        }

        log::info!("added collection {}", spec.name);
        Ok(collection)
    }

    /// Query a collection of the given name, verifying there's an accessible import
    /// path from the Resource |from|.
    pub fn query(db: &DB, name: &str, from: Resource) -> Result<Collection> {
        let query = db
            .prepare_cached("SELECT id, resource_id FROM collections WHERE name = ?;")?
            .query_row(&[name], |row| Ok((row.get(0)?, row.get(1)?)));

        let collection = match query {
            Ok((id, rid)) => Collection {
                id,
                resource: Resource {
                    id: rid,
                    added: false,
                },
            },
            Err(err) => {
                return Err(Error::At {
                    loc: format!("querying collection {:?}", name),
                    detail: Box::new(err.into()),
                })
            }
        };
        Resource::verify_import(db, from, collection.resource)?;

        Ok(collection)
    }

    /// Fetch the key of this Collection.
    pub fn _key(&self, db: &DB) -> Result<Vec<String>> {
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

#[cfg(test)]
mod test {
    use super::{super::db, *};
    use serde_json::json;
    use tempfile;
    use url::Url;

    #[test]
    fn test_register_and_query() -> Result<()> {
        // Install fixtures in a temp directory.
        let dir = tempfile::tempdir().unwrap();
        let fixtures = [
            (
                Url::from_file_path(dir.path().join("schema.json")).unwrap(),
                json!({ "$defs": {"a-def": true} }),
            ),
            (
                Url::from_file_path(dir.path().join("fixtures.json")).unwrap(),
                json!([
                    {
                        "document": {"key": ["foo", "bar"], "other": "value"},
                        "key": ["bar", "foo"],
                        "projections": {"field-name": "value"}
                    },
                ]),
            ),
        ];
        for (uri, val) in fixtures.iter() {
            std::fs::write(uri.to_file_path().unwrap(), val.to_string())?;
        }

        let db = DB::open_in_memory()?;
        db::init(&db)?;

        let source = fixtures[0].0.join("root")?;
        let source = Source {
            resource: Resource::register(&db, source)?,
        };

        let spec: specs::Collection = serde_json::from_value(json!({
            "name": "test/collection",
            "schema": "schema.json#/$defs/a-def",
            "key": ["/key/1", "/key/0"],
            "fixtures": ["fixtures.json"],
            "projections": [
                {"field": "field-name", "location": "/other", "partition": true},
            ],
        }))?;
        Collection::register(&db, source, &spec)?;

        // Expect the collection records the absolute schema URI, with fragment component.
        let full_schema_uri = source
            .resource
            .uri(&db)?
            .join("schema.json#/$defs/a-def")?
            .to_string();

        let dump = db::dump_tables(&db, &["collections", "schemas", "projections", "fixtures"])?;

        assert_eq!(
            dump,
            json!({
                "collections": [
                    [1, "test/collection", full_schema_uri, ["/key/1","/key/0"], 1],
                ],
                "fixtures": [
                    [1, {"key":["foo","bar"], "other":"value"}, ["bar", "foo"], {"field-name":"value"}, 3],
                ],
                "projections": [
                    [1, "field-name", "/other", true],
                ],
                "schemas":[
                    [{"$defs": {"a-def": true}}, 2],
                ],
            }),
        );

        // Expect we're able to query the collection in the context of |source|.
        assert_eq!(
            1,
            Collection::query(&db, "test/collection", source.resource)?.id
        );

        // But not from an unrelated resource.
        let other = Resource::register(&db, Url::parse("http://other")?)?;
        assert_eq!(
            format!(
                "{:?} references {:?} without directly or indirectly importing it",
                other.uri(&db)?,
                source.resource.uri(&db)?
            ),
            format!(
                "{}",
                Collection::query(&db, "test/collection", other).unwrap_err()
            ),
        );

        // Expect a reasonable error if the collection doesn't exist.
        assert_eq!(
            "querying collection \"not/found\": catalog database error: Query returned no rows",
            format!(
                "{}",
                Collection::query(&db, "not/found", source.resource).unwrap_err()
            ),
        );

        Ok(())
    }
}
