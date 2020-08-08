use super::{
    sql_params, BuildContext, Catalog, ContentType, Derivation, Resource, Result, Schema, DB,
};
use crate::specs::build as specs;

/// Collection represents a catalog Collection.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct Collection {
    pub id: i64,
    pub resource: Resource,
}

impl Collection {
    /// Registers a Collection of the Source with the catalog.
    pub fn register(
        context: &BuildContext,
        source: Catalog,
        spec: &specs::Collection,
    ) -> Result<Collection> {
        // Register and import the schema document.
        let schema = context.process_child_field("schema", &spec.schema, Schema::register)?;
        Resource::register_import(context.db, source.resource, schema.resource)?;

        context
            .db
            .prepare_cached(
                "INSERT INTO collections (
                    name,
                    schema_uri,
                    key_json,
                    resource_id
                ) VALUES (?, ?, ?, ?)",
            )?
            .execute(sql_params![
                spec.name,
                schema.primary_url_with_fragment(context.db)?,
                serde_json::to_string(&spec.key)?,
                source.resource.id,
            ])?;
        let collection = Collection {
            id: context.db.last_insert_rowid(),
            resource: source.resource,
        };

        context.process_child_array("fixtures", spec.fixtures.iter(), |context, fixture| {
            collection.register_fixture(context.db, fixture)
        })?;

        context.process_child_array(
            "projections",
            spec.projections.iter(),
            |context, projection| collection.register_projection(context.db, projection),
        )?;

        if let Some(spec) = &spec.derivation {
            context.process_child_field("derivation", spec, |context, spec| {
                Derivation::register(context, collection, spec)
            })?;
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
        db.prepare_cached(
            "INSERT INTO projections (collection_id, field, location_ptr)
                    VALUES (?, ?, ?)",
        )?
        .execute(sql_params![self.id, spec.field, spec.location])?;

        if spec.partition {
            db.prepare_cached(
                "INSERT INTO partitions (collection_id, field)
                        VALUES (?, ?)",
            )?
            .execute(sql_params![self.id, spec.field])?;
        }

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
    use url::Url;

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

        let source = Catalog {
            resource: Resource { id: 1 },
        };
        let url = Url::parse("test://example/spec").unwrap();
        let context = BuildContext::new_from_root(&db, &url);
        Collection::register(&context, source, &spec)?;

        // Expect that the schema and fixtures were processed.
        assert!(Resource { id: 10 }.is_processed(&db)?);
        assert!(Resource { id: 20 }.is_processed(&db)?);

        // Expect the collection records the absolute schema URI, with fragment component.
        let dump = dump_tables(
            &db,
            &[
                "resource_imports",
                "collections",
                "projections",
                "partitions",
                "fixtures",
            ],
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
                    [1, "field_a", "/a/a"],
                    [1, "field_b", "/b/b"],
                ],
                "partitions": [[1, "field_a"]],
            }),
        );

        Ok(())
    }
}
