use crate::catalog::{Collection, Result, Schema, Scope, DB};
use crate::doc::inference::Shape;
use crate::doc::{Pointer, SchemaIndex};
use crate::specs::build as specs;
use estuary_json::schema::types;
use estuary_json::Location;
use rusqlite::params as sql_params;
use url::Url;

pub fn register_projections(
    scope: &Scope,
    collection: Collection,
    projections: &specs::Projections,
) -> Result<()> {
    let compiled_schemas = Schema::compile_for(scope.db, collection.resource.id)?;
    let mut index = SchemaIndex::new();
    for schema in compiled_schemas.iter() {
        index.add(schema)?;
    }
    let schema_uri = get_schema_uri(scope.db, collection)?;
    let collection_schema = index.must_fetch(&schema_uri)?;
    let shape = Shape::infer(collection_schema, &index);

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
        Location::Root,
        collection.id,
        schema_uri.as_str(),
        &shape,
        true,
        projections,
    )
}

fn get_schema_uri(db: &DB, collection: Collection) -> Result<Url> {
    let url_string: String = db.query_row(
        "SELECT schema_uri FROM collections WHERE collection_id = ?;",
        rusqlite::params![collection.id],
        |row| row.get(0),
    )?;
    Url::parse(url_string.as_str()).map_err(Into::into)
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

    let pointer = Pointer::from(spec.location);
    let (field_shape, must_exist) =
        schema_shape
            .locate(&pointer)
            .ok_or_else(|| NoSuchLocationError {
                field: spec.field.to_string(),
                location_ptr: spec.location.to_string(),
            })?;

    let mut stmt = scope.db.prepare_cached(
        "INSERT OR IGNORE INTO inferences (
            schema_uri,
            location_ptr,
            types_json,
            must_exist,
            string_content_type,
            string_format,
            string_content_encoding_is_base64,
            string_max_length
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
    )?;
    let params = rusqlite::params![
        schema_uri,
        spec.location,
        field_shape.type_.to_json_array(),
        must_exist,
        field_shape.string.content_type.as_deref(),
        field_shape.string.format.as_deref(),
        field_shape.string.is_base64,
        field_shape.string.max_length.map(usize_to_i64),
    ];
    stmt.execute(params)?;

    Ok(())
}

fn usize_to_i64(unsigned: usize) -> i64 {
    unsigned.min(usize::MAX - 1) as i64
}

#[derive(Debug, thiserror::Error)]
#[error("The location pointer: '{location_ptr}' of projection: '{field}' does not exist according to the collection schema")]
pub struct NoSuchLocationError {
    field: String,
    location_ptr: String,
}

fn register_canonical_projections_for_shape(
    db: &DB,
    location: Location,
    collection_id: i64,
    schema_uri: &str,
    shape: &Shape,
    must_exist: bool,
    spec: &specs::Projections,
) -> Result<()> {
    let pointer = location.pointer_str().to_string();
    if contains_location(spec, pointer.as_str()) {
        return Ok(());
    }

    // Temporarily remove null and match on the remainder of the possible types. We're only looking
    // at fields with a single possible type (apart from null). Any fields with multiple possible
    // types (e.g. can be either a string or an object) are ignored.
    let non_nullable_type = (!types::NULL) & shape.type_;
    match non_nullable_type {
        types::STRING | types::INTEGER | types::NUMBER | types::BOOLEAN => {
            let field = field_name_from_location(location);

            let mut proj_stmt = db.prepare_cached(
                "INSERT INTO projections (collection_id, field, location_ptr, user_provided)
                    VALUES (?, ?, ?, FALSE)",
            )?;
            proj_stmt.execute(sql_params![collection_id, field, pointer])?;

            let types_json = shape.type_.to_json_array();
            let params = sql_params![
                schema_uri,
                pointer,
                types_json,
                must_exist,
                shape.string.content_type.as_ref(),
                shape.string.format.as_ref(),
                shape.string.is_base64,
                shape.string.max_length.map(usize_to_i64)
            ];
            db.prepare_cached(
                "INSERT OR IGNORE INTO inferences (
                    schema_uri,
                    location_ptr,
                    types_json,
                    must_exist,
                    string_content_type,
                    string_format,
                    string_content_encoding_is_base64,
                    string_max_length
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
            )?
            .execute(params)?;
        }
        types::ARRAY => {
            for (index, shape) in shape.array.tuple.iter().enumerate() {
                let location = location.push_item(index);
                register_canonical_projections_for_shape(
                    db,
                    location,
                    collection_id,
                    schema_uri,
                    shape,
                    must_exist && !shape.type_.overlaps(types::NULL),
                    spec,
                )?;
            }
        }
        types::OBJECT => {
            for property in shape.object.properties.iter() {
                let location = location.push_prop(property.name.as_str());
                register_canonical_projections_for_shape(
                    db,
                    location,
                    collection_id,
                    schema_uri,
                    &property.shape,
                    must_exist && !shape.type_.overlaps(types::NULL),
                    spec,
                )?;
            }
        }
        _ => { /* no-op */ }
    }
    Ok(())
}

fn contains_location(spec: &specs::Projections, location: &str) -> bool {
    spec.iter().any(|p| p.location == location)
}

fn field_name_from_location(location: Location) -> String {
    use std::fmt::{Display, Write};
    fn push_segment(name: &mut String, segment: impl Display) {
        if !name.is_empty() {
            name.push('/');
        }
        write!(name, "{}", segment).unwrap();
    }
    location.fold(String::with_capacity(32), |loc, mut name| {
        match loc {
            Location::Root => {}
            Location::Property(prop) => {
                push_segment(&mut name, prop.name);
            }
            Location::Item(item) => {
                push_segment(&mut name, item.index);
            }
        }
        name
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::catalog::{create, Resource};
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
                            ]
                        }
                    },
                    "required": ["fooObj"]
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
            "required": ["oneFoo"]
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

        let actual =
            crate::catalog::dump_tables(&db, &["projections", "partitions", "inferences"]).unwrap();

        insta::assert_json_snapshot!(actual);
    }

    #[test]
    fn property_name_is_generated_from_location() {
        let root = Location::Root;
        let a = root.push_prop("a");
        let a_5 = a.push_item(5);
        let b = a_5.push_prop("b");

        let name = field_name_from_location(b);
        assert_eq!("a/5/b", name.as_str());

        let name = field_name_from_location(root);
        assert!(name.is_empty());
    }
}
