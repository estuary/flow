use super::{
    projections, sql_params, ContentType, Derivation, Materialization, Resource, Result, Schema,
    Scope,
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
    pub fn register(scope: Scope, spec: &specs::Collection) -> Result<Collection> {
        // Register and import the schema document.
        let schema = scope.push_prop("schema").then(|scope| {
            let schema = Schema::register(scope, &spec.schema)?;
            Resource::register_import(scope, schema.resource)?;
            Ok(schema)
        })?;
        let defaults_spec = spec.projections.defaults.get_config();
        let default_projections_max_depth = if defaults_spec.enabled {
            log::debug!(
                "generating default projections with default depth for collection: {}",
                spec.name
            );
            defaults_spec.max_depth.unwrap_or(4u8)
        } else {
            log::debug!(
                "default projections are disabled for collection: {}",
                spec.name
            );
            0u8
        };

        scope
            .db
            .prepare_cached(
                "INSERT INTO collections (
                    collection_name,
                    schema_uri,
                    key_json,
                    resource_id,
                    default_projections_max_depth
                ) VALUES (?, ?, ?, ?, ?)",
            )?
            .execute(sql_params![
                spec.name,
                schema.primary_url_with_fragment(scope.db)?,
                serde_json::to_string(&spec.key)?,
                scope.resource().id,
                default_projections_max_depth,
            ])?;

        let collection = Collection {
            id: scope.db.last_insert_rowid(),
            resource: scope.resource(),
        };

        for (index, fixture) in spec.fixtures.iter().enumerate() {
            scope
                .push_prop("fixtures")
                .push_item(index)
                .then(|scope| collection.register_fixture(scope, fixture))?;
        }

        scope
            .push_prop("projections")
            .push_prop("fields)")
            .then(|scope| {
                for projection in spec.projections.iter() {
                    scope.push_prop(projection.field).then(|scope| {
                        projections::register_user_provided_projection(
                            &scope,
                            collection,
                            &projection,
                        )
                    })?;
                }
                Ok(())
            })?;

        for (name, materialization) in spec.materializations.iter() {
            scope
                .push_prop("materializations")
                .push_prop(name)
                .then(|scope| {
                    Materialization::register(&scope, collection, name, materialization)
                })?;
        }

        if let Some(spec) = &spec.derivation {
            scope
                .push_prop("derivation")
                .then(|scope| Derivation::register(scope, collection, spec))?;
        }

        log::info!("added collection {}", spec.name);
        Ok(collection)
    }

    fn register_fixture(&self, scope: Scope, url: &str) -> Result<()> {
        let url = self.resource.join(scope.db, url)?;
        let fixtures = Resource::register(scope.db, ContentType::CatalogFixtures, &url)?;
        Resource::register_import(scope, fixtures)?;

        if !fixtures.is_processed(scope.db)? {
            scope.push_resource(fixtures).then(|scope| {
                // Just verify fixtures parse correctly.
                serde_yaml::from_slice::<Vec<specs::Fixture>>(&fixtures.content(scope.db)?)?;
                fixtures.mark_as_processed(scope.db)
            })?;
        }
        scope
            .db
            .prepare_cached("INSERT INTO fixtures (collection_id, resource_id) VALUES (?, ?)")?
            .execute(sql_params![self.id, fixtures.id])?;

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
            "projections": {
                "fields": {
                    "field_a": {"location": "/a/a", "partition": true},
                    "field_b": {"location": "/b/b", "partition": false},
                }
            }
        }))?;

        Scope::empty(&db)
            .push_resource(Resource { id: 1 })
            .then(|scope| Collection::register(scope, &spec))
            .expect("failed to register collection");

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
                        4,
                    ],
                ],
                "fixtures": [[1, 20]],
                "projections": [
                    [1, "field_a", "/a/a", true],
                    [1, "field_b", "/b/b", true],
                ],
                "partitions": [[1, "field_a"]],
            }),
        );

        Ok(())
    }
}
