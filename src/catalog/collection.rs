use super::{sql_params, ContentType, Derivation, Error, Resource, Result, Schema, Source, DB};
use crate::doc::Pointer;
use crate::specs::build as specs;
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
        // Register and import the schema document.
        let schema_url = source.resource.join(db, &spec.schema)?;
        let schema = Schema::register(db, &schema_url).map_err(|err| Error::At {
            loc: format!("schema {:?}", schema_url),
            detail: Box::new(err),
        })?;
        Resource::register_import(db, source.resource, schema.resource)?;

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
            schema_url,
            serde_json::to_string(&spec.key)?,
            source.resource.id,
        ])?;
        let collection = Collection {
            id: db.last_insert_rowid(),
            resource: source.resource,
        };

        for url in &spec.fixtures {
            collection
                .register_fixture(db, url)
                .map_err(|err| Error::At {
                    loc: format!("fixture {:?}", url),
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

    fn register_fixture(&self, db: &DB, url: &str) -> Result<()> {
        let url = self.resource.join(db, url)?;
        let fixtures = Resource::register(db, ContentType::CatalogFixtures, &url)?;
        Resource::register_import(db, self.resource, fixtures)?;

        if !fixtures.is_processed(db)? {
            // Just verify fixtures parse correctly.
            serde_yaml::from_slice::<Vec<specs::Fixture>>(&fixtures.content(db)?)?;
            fixtures.mark_as_processed(db)?;
        }
        db.prepare_cached("INSERT INTO fixtures (collection_id, resource_id) VALUES (?, ?)")?
            .execute(sql_params![self.id, fixtures.id])?;

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
    use super::{
        super::{dump_tables, init_db_schema, open},
        *,
    };
    use rusqlite::params as sql_params;
    use serde_json::json;

    #[test]
    fn test_register() -> Result<()> {
        let db = open(":memory:")?;
        init_db_schema(&db)?;

        let schema = json!({"$anchor": "foobar"});
        let fixtures = json!([
            {
                "document": {"key": ["foo", "bar"], "other": "value"},
                "key": ["bar", "foo"],
                "projections": {"field-name": "value"}
            },
        ]);
        db.execute(
            "INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                    (1, 'application/vnd.estuary.dev-catalog-spec+yaml', X'1234', FALSE),
                    (10, 'application/schema+yaml', CAST(? AS BLOB), FALSE),
                    (20, 'application/vnd.estuary.dev-catalog-fixtures+yaml', CAST(? AS BLOB), FALSE);",
            sql_params![schema, fixtures],
        )?;
        db.execute(
            "INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                    (1, 'test://example/spec', TRUE),
                    (10, 'test://example/schema.json', TRUE),
                    (20, 'test://example/fixtures.json', TRUE);",
            sql_params![],
        )?;

        let spec: specs::Collection = serde_json::from_value(json!({
            "name": "test/collection",
            "schema": "schema.json#foobar",
            "key": ["/key/1", "/key/0"],
            "fixtures": ["fixtures.json"],
            "projections": [
                {"field": "field_a", "location": "/a/a", "partition": true},
                {"field": "field_b", "location": "/b/b", "partition": false},
            ],
        }))?;

        let source = Source {
            resource: Resource { id: 1 },
        };
        Collection::register(&db, source, &spec)?;

        // Expect that the schema and fixtures were processed.
        assert!(Resource { id: 10 }.is_processed(&db)?);
        assert!(Resource { id: 20 }.is_processed(&db)?);

        // Expect the collection records the absolute schema URI, with fragment component.
        let dump = dump_tables(
            &db,
            &["resource_imports", "collections", "projections", "fixtures"],
        )?;

        assert_eq!(
            dump,
            json!({
                "resource_imports": [[1, 10], [1, 20]],
                "collections": [
                    [
                        1,
                        "test/collection",
                        "test://example/schema.json#foobar",
                        ["/key/1","/key/0"],
                        1,
                    ],
                ],
                "fixtures": [[1, 20]],
                "projections": [
                    [1, "field_a", "/a/a", true],
                    [1, "field_b", "/b/b", false],
                ],
            }),
        );

        Ok(())
    }
}
