use crate::{inference, specs, Collection, Error, Result, Schema, Scope, DB};
use doc::inference::Shape;
use rusqlite::params as sql_params;

pub fn register_projections(
    scope: &Scope,
    collection: Collection,
    projections: &specs::Projections,
) -> Result<()> {
    let schema_uri = collection.schema_uri(scope.db)?;
    let shape = Schema::shape_for(scope.db, collection.resource.id, &schema_uri)?;

    for projection in projections.iter() {
        scope
            .push_prop("fields")
            .push_prop(projection.field)
            .then(|scope| {
                register_user_provided_projection(
                    &scope,
                    collection,
                    &projection,
                    &shape,
                    schema_uri.as_str(),
                )
            })?;
    }

    register_canonical_projections_for_shape(
        scope.db,
        collection.id,
        schema_uri.as_str(),
        &shape,
        projections,
    )
}

pub fn register_user_provided_projection(
    scope: &Scope,
    collection: Collection,
    spec: &specs::ProjectionSpec,
    schema_shape: &Shape,
    schema_uri: &str,
) -> Result<()> {
    scope
        .db
        .prepare_cached(
            "INSERT INTO projections (collection_id, field, location_ptr, user_provided)
                VALUES (?, ?, ?, TRUE);",
        )?
        .execute(sql_params![collection.id, spec.field, spec.location])?;

    if spec.partition {
        scope
            .db
            .prepare_cached(
                "INSERT INTO partitions (collection_id, field)
                    VALUES (?, ?);",
            )?
            .execute(sql_params![collection.id, spec.field])?;
    }

    if let Some(location) = inference::Inference::locate_within(spec.location, schema_shape) {
        inference::register_one(scope.db, schema_uri, &location)
    } else {
        Err(Error::InvalidProjection(NoSuchLocationError {
            field: spec.field.to_string(),
            location_ptr: spec.location.to_string(),
        }))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("The location pointer: '{location_ptr}' of projection: '{field}' does not exist according to the collection schema")]
pub struct NoSuchLocationError {
    field: String,
    location_ptr: String,
}

fn register_canonical_projections_for_shape(
    db: &DB,
    collection_id: i64,
    schema_uri: &str,
    shape: &Shape,
    spec: &specs::Projections,
) -> Result<()> {
    let inferences = inference::get_inferences(shape);
    for inference in inferences {
        inference::register_one(db, schema_uri, &inference)?;

        if inference.shape.type_.is_single_scalar_type() {
            let field = inference.location_ptr.trim_start_matches('/');

            if inference.location_ptr.ends_with("/-") {
                continue; // Don't project inferred variable items of arrays.
            } else if contains_location(spec, inference.location_ptr.as_str(), field) {
                continue; // Already specified as a user-provided projection.
            }

            let mut proj_stmt = db.prepare_cached(
                "INSERT INTO projections (collection_id, field, location_ptr, user_provided)
                    VALUES (?, ?, ?, FALSE)",
            )?;
            proj_stmt.execute(sql_params![
                collection_id,
                field,
                inference.location_ptr.as_str()
            ])?;
        }
    }

    Ok(())
}

/// Checks whether the set of user-provided projections already contains a projection that matches
/// the auto-generated one with the given location and field. This checks only for an exact match
/// of both the location and the field. This is important because we're walking a narrow path to
/// avoid conflicts between user-provided projections and those that are generated automatically.
/// Given an auto-generated projection of `foo => /foo`, we must allow users to provide projections
/// like `other_name => /foo`, but we must disallow projections like `foo => /other_pointer` (these
/// will be caught by the uniqueness constraint on field names).
fn contains_location(spec: &specs::Projections, location: &str, field: &str) -> bool {
    spec.iter()
        .any(|p| p.location == location && p.field == field)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{create, dump_tables, Resource};
    use rusqlite::params as sql_params;
    use serde_json::json;

    #[test]
    fn default_projections_and_inferences_are_registered() {
        let db = create(":memory:").unwrap();

        let schema = json!({
            "$defs": {
                "foo": {
                    "type": "object",
                    "properties": {
                        "fooObj": {
                            "type": "object",
                            "properties": {
                                "a": { "type": "string", "format": "email" },
                                "b": { "type": "boolean" },
                                "c": { "type": "integer" },
                                "d": { "type": "number" },
                                "x": { "type": "array" },
                                "y": { "type": "object" }
                            },
                            "required": ["a"]
                        },
                        "fooArray": {
                            "type": "array",
                            "items": [
                                { "type": "number" },
                                { "type": "number" }
                            ],
                            "minItems": 1
                        }
                    },
                    "required": ["fooObj", "fooArray"]
                }
            },
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "oneFoo": { "$ref": "#/$defs/foo" },
                "twoFoo": {
                    "allOf": [
                        {"$ref": "#/$defs/foo"},
                        {
                            "type": "object",
                            "properties": {
                                "extra": {"type": "integer"}
                            }
                        }
                    ]
                },
                "redFoo": {
                    "type": "object",
                    "properties": {
                        "nested": {
                            "type": "object",
                            "properties": {
                                "reallyNested": { "$ref": "#/$defs/foo" }
                            }
                        }
                    }
                },
                "blueFoo": {
                    "oneOf": [
                        {"$ref": "#/$defs/foo"},
                        {
                            "type": "object",
                            "properties": {
                                "fooObj": {
                                    "type": "object",
                                    "properties": {
                                        "a": { "type": "integer" },
                                        "b": { "type": "string" },
                                        // this property should get projected because
                                        // it has the same type and path as the one from
                                        // '$defs/foo'
                                        "c": {"type": "integer"},
                                    }
                                }
                            }
                        }
                    ]
                }
            },
            // make oneFoo required so that there will be an entire chain of required properties
            // to `/oneFoo/fooObj/a`, so that we can validate that the inference knows it's not
            // nullable.
            "required": ["id", "oneFoo"]
        });
        db.execute(
            "INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                    (1, 'application/vnd.estuary.dev-catalog-spec+yaml', X'1234', FALSE),
                    (10, 'application/schema+yaml', CAST(? AS BLOB), FALSE);",
            sql_params![schema],
        )
        .unwrap();
        db.execute(
            "insert into resource_imports (resource_id, import_id) values (1, 10);",
            rusqlite::NO_PARAMS,
        )
        .unwrap();
        db.execute(
            "INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                    (1, 'test://example/spec', TRUE),
                    (10, 'test://example/schema.json', TRUE);",
            sql_params![],
        )
        .unwrap();
        db.execute(
            "INSERT INTO collections
            (collection_id, collection_name, schema_uri, key_json, resource_id)
            VALUES
            (1, 'testCollection', 'test://example/schema.json', '[\"/id\"]', 1);",
            sql_params![],
        )
        .unwrap();

        let collection = Collection {
            id: 1,
            resource: Resource { id: 1 },
        };
        let projections = serde_json::from_str::<specs::Projections>(
            r##"{
                "field_a": "/oneFoo/fooObj/a",
                "field_b": {
                    "location": "/oneFoo/fooObj/c",
                    "partition": true
                },
                "red_foo": "/redFoo"
            }"##,
        )
        .unwrap();
        let root_scope = Scope::empty(&db);
        let scope = root_scope.push_resource(Resource { id: 1 });
        let scope = scope.push_prop("projections");
        register_projections(&scope, collection, &projections)
            .expect("failed to register projections");

        let actual = dump_tables(&db, &["projections", "partitions", "inferences"]).unwrap();

        insta::assert_json_snapshot!(actual);
    }
}
