use super::{projections, sql_params, Derivation, Resource, Result, Schema, Scope};
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

        scope
            .db
            .prepare_cached(
                "INSERT INTO collections (
                    collection_name,
                    schema_uri,
                    key_json,
                    resource_id
                ) VALUES (?, ?, ?, ?)",
            )?
            .execute(sql_params![
                spec.name,
                schema.primary_url_with_fragment(scope.db)?,
                serde_json::to_string(&spec.key)?,
                scope.resource().id,
            ])?;

        let collection = Collection {
            id: scope.db.last_insert_rowid(),
            resource: scope.resource(),
        };

        scope.push_prop("projections").then(|scope| {
            projections::register_projections(&scope, collection, &spec.projections)
        })?;

        if let Some(spec) = &spec.derivation {
            scope
                .push_prop("derivation")
                .then(|scope| Derivation::register(scope, collection, spec))?;
        }

        log::info!("added collection {}", spec.name);
        Ok(collection)
    }

    /// Returns the collection with the given name, or an error if it doesn't exist
    pub fn get_by_name(scope: Scope, name: &str) -> Result<Collection> {
        let (collection_id, resource_id) = scope
            .db
            .prepare_cached(
                "SELECT collection_id, resource_id FROM collections WHERE collection_name = ?",
            )?
            .query_row(&[name], |r| Ok((r.get(0)?, r.get(1)?)))?;

        let collection = Collection {
            id: collection_id,
            resource: Resource { id: resource_id },
        };

        // Verify that the catalog spec of the collection is imported by the current scope.
        Resource::verify_import(scope, collection.resource)?;
        Ok(collection)
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

        let schema = json!({
            "$anchor": "foobar",
            "type": "object",
            "properties": {
                "a": {
                    "type": "object",
                    "properties": {
                        "a": {
                            "type": "string"
                        }
                    }
                },
                "b": {
                    "type": "object",
                    "properties": {
                        "b": {
                            "type": "string"
                        }
                    }
                },
                "key": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "minItems": 2
                }
            }
        });
        db.execute(
            "INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                    (1, 'application/vnd.estuary.dev-catalog-spec+yaml', X'1234', FALSE),
                    (10, 'application/schema+yaml', CAST(? AS BLOB), FALSE);",
            sql_params![schema],
        )?;
        db.execute(
            "INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                    (1, 'test://example/spec', TRUE),
                    (10, 'test://example/schema.json', TRUE)",
            sql_params![],
        )?;

        let spec: specs::Collection = serde_json::from_value(json!({
            "name": "test/collection",
            "schema": "schema.json#foobar",
            "key": ["/key/1", "/key/0"],
            "projections": {
                "field_a": {"location": "/a/a", "partition": true},
                "field_b": {"location": "/b/b", "partition": false},
            }
        }))?;

        Scope::empty(&db)
            .push_resource(Resource { id: 1 })
            .then(|scope| Collection::register(scope, &spec))
            .expect("failed to register collection");

        // Expect that the schema was processed.
        assert!(Resource { id: 10 }.is_processed(&db)?);

        // Expect the collection records the absolute schema URI, with fragment component.
        let dump = dump_tables(
            &db,
            &[
                "resource_imports",
                "collections",
                "projections",
                "partitions",
            ],
        )?;

        assert_eq!(
            dump,
            json!({
                "resource_imports": [[1, 10]],
                "collections": [
                    [
                        1,
                        "test/collection",
                        "test://example/schema.json#foobar",
                        ["/key/1","/key/0"],
                        1,
                    ],
                ],
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
