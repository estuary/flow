use crate::catalog::{
    Collection as CollectionResource, Result, Schema as SchemaResource, Scope, DB,
};
use crate::doc::inference::Shape;
use crate::doc::{Pointer, SchemaIndex};
use crate::specs::build::ProjectionSpec;
use estuary_json::schema::types;
use estuary_json::Location;
use rusqlite::params as sql_params;
use rusqlite::types::{ToSqlOutput, ValueRef};
use std::collections::HashSet;
use url::Url;

pub fn register_user_provided_projection(
    scope: &Scope,
    collection: CollectionResource,
    spec: &ProjectionSpec,
) -> Result<()> {
    scope
        .db
        .prepare_cached(
            "INSERT INTO projections (collection_id, field, location_ptr, user_provided)
                        VALUES (?, ?, ?, TRUE)",
        )?
        .execute(sql_params![collection.id, spec.field, spec.location])?;

    if spec.partition {
        scope
            .db
            .prepare_cached(
                "INSERT INTO partitions (collection_id, field)
                            VALUES (?, ?)",
            )?
            .execute(sql_params![collection.id, spec.field])?;
    }

    Ok(())
}

pub fn register_default_projections_and_inferences(
    scope: &Scope,
    collections: &[CollectionResource],
) -> Result<()> {
    let compiled_schemas = SchemaResource::compile_all(scope.db)?;

    let mut index = SchemaIndex::new();
    for schema in compiled_schemas.iter() {
        index.add(schema)?;
    }

    for collection in collections {
        let (name, schema_url, default_projection_max_depth): (String, String, i32) = scope.db.query_row(
            "SELECT collection_name, schema_uri, default_projections_max_depth FROM collections where collection_id = ?",
            rusqlite::params![collection.id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let scope = &scope.push_prop(name.as_str());

        if default_projection_max_depth == 0 {
            continue;
        }

        let user_provided_pointers = get_user_provided_projection_locations(scope.db, collection)?;

        let schema_url = Url::parse(&schema_url)?;
        let schema = index.must_fetch(&schema_url)?;
        let shape = Shape::infer(&schema, &index);
        register_projections_for_shape(
            scope,
            Location::Root,
            &collection,
            default_projection_max_depth as u8,
            &shape,
            &user_provided_pointers,
        )?;

        register_inferences(scope.db, collection, &shape)?;
    }
    Ok(())
}

fn register_inferences(
    db: &DB,
    collection: &CollectionResource,
    collection_schema_shape: &Shape,
) -> Result<()> {
    // we'll look at all the projections here, both user-provided and auto-generated
    let mut stmt =
        db.prepare_cached("select field, location_ptr from projections where collection_id = ?")?;
    let mut rows = stmt.query(sql_params![collection.id])?;

    let mut type_buffer = Vec::with_capacity(4);
    let mut type_json_buffer = Vec::with_capacity(64);
    while let Some(row) = rows.next()? {
        let field: String = row.get(0)?;
        let pointer_str: String = row.get(1)?;
        let pointer = Pointer::from(pointer_str.as_str());

        if let Some((shape, must_exist)) = collection_schema_shape.locate(&pointer) {
            type_buffer.clear();
            shape.type_.fill_types(&mut type_buffer);
            if !must_exist && !type_buffer.contains(&"null") {
                type_buffer.push("null");
            }
            type_json_buffer.clear();
            let mut cursor = std::io::Cursor::new(&mut type_json_buffer);
            serde_json::to_writer(&mut cursor, &type_buffer)?;

            let mut stmt = db.prepare_cached(
                "INSERT INTO inferences
                 (collection_id, field, types_json, string_content_type, 
                  string_content_encoding_is_base64, string_max_length)
                 VALUES (?, ?, ?, ?, ?, ?);",
            )?;
            // this weird cast is just so we don't end up with -1 in case someone uses the max
            // value of a usize for the maxLength in their schema.
            let str_max_len = shape
                .string
                .max_length
                .map(|l| l.min(usize::MAX - 1) as i64);
            let params = sql_params![
                collection.id,
                field,
                ToSqlOutput::Borrowed(ValueRef::Text(type_json_buffer.as_slice())),
                shape.string.content_type,
                shape.string.is_base64,
                str_max_len,
            ];
            stmt.execute(params)?;
        } else {
            // TODO: error since there's a projection that we know nothing about
            panic!("need to handle this error condition");
        }
    }
    Ok(())
}

fn get_user_provided_projection_locations(
    db: &DB,
    collection: &CollectionResource,
) -> Result<HashSet<String>> {
    let mut stmt =
        db.prepare_cached("SELECT location_ptr FROM projections WHERE collection_id = ?;")?;
    let set = stmt
        .query_map(sql_params![collection.id], |row| row.get(0))?
        .collect::<rusqlite::Result<HashSet<String>>>()?;
    Ok(set)
}

fn register_projections_for_shape(
    scope: &Scope,
    location: Location,
    collection: &CollectionResource,
    max_depth: u8,
    shape: &Shape,
    user_provided_pointers: &HashSet<String>,
) -> Result<()> {
    let pointer = location.pointer_str().to_string();
    if user_provided_pointers.contains(&pointer) {
        return Ok(());
    }

    let non_nullable_type = (!types::NULL) & shape.type_;
    match non_nullable_type {
        types::STRING | types::INTEGER | types::NUMBER | types::BOOLEAN => {
            let field = field_name_from_location(location);
            scope.db.prepare_cached(
                "insert into projections (collection_id, field, location_ptr, user_provided) values (?, ?, ?, FALSE);"
                    )?.execute(sql_params![collection.id, field, pointer])?;
        }
        types::ARRAY if location.depth() < max_depth as u32 => {
            for (index, shape) in shape.array.tuple.iter().enumerate() {
                let location = location.push_item(index);
                register_projections_for_shape(
                    scope,
                    location,
                    collection,
                    max_depth,
                    shape,
                    user_provided_pointers,
                )?;
            }
        }
        types::OBJECT if location.depth() < max_depth as u32 => {
            for property in shape.object.properties.iter() {
                let location = location.push_prop(property.name.as_str());
                register_projections_for_shape(
                    scope,
                    location,
                    collection,
                    max_depth,
                    &property.shape,
                    user_provided_pointers,
                )?;
            }
        }
        _ => { /* no-op */ }
    }
    Ok(())
}

fn field_name_from_location(location: Location) -> String {
    use std::fmt::{Display, Write};
    fn push_segment(name: &mut String, segment: impl Display) {
        if !name.is_empty() && !name.ends_with('_') {
            name.push('_')
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
    use crate::catalog::{self, Resource};
    use rusqlite::params as sql_params;
    use serde_json::json;

    #[test]
    fn default_projections_and_inferences_are_registered() {
        let db = catalog::open(":memory:").unwrap();
        catalog::init_db_schema(&db).unwrap();

        let fixtures = json!([{
            "id": 333,
            "oneFoo": {
                "fooObj": {"a": "someString"},
                "fooArray": [ 5, 6 ]
            },
            "twoFoo": {
                "fooObj": {"a": "someString"},
                "fooArray": [ 7, 8 ],
                "extra": 9,
            },
            "redFoo": {
                "nested": {
                    "fooObj": {"a": "someString"},
                    "fooArray": [ 7, 8 ]
                }
            },
            "blueFoo": [ {
                    "fooObj": {"a": "someString"},
                    "fooArray": [ 7, 8 ]
            } ],
        }]);
        let schema = json!({
            "$defs": {
                "foo": {
                    "type": "object",
                    "properties": {
                        "fooObj": {
                            "type": "object",
                            "properties": {
                                "a": { "type": "string" },
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
                // too deeply nested
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
                    (10, 'application/schema+yaml', CAST(? AS BLOB), FALSE),
                    (20, 'application/vnd.estuary.dev-catalog-fixtures+yaml', CAST(? AS BLOB), FALSE);",
            sql_params![schema, fixtures],
        ).unwrap();
        db.execute(
            "INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                    (1, 'test://example/spec', TRUE),
                    (10, 'test://example/schema.json', TRUE),
                    (20, 'test://example/fixtures.json', TRUE);",
            sql_params![],
        )
        .unwrap();
        db.execute(
            "INSERT INTO collections 
            (collection_id, collection_name, schema_uri, key_json, resource_id, default_projections_max_depth)
            VALUES
            (1, 'testCollection', 'test://example/schema.json', '[\"/id\"]', 1, 4);",
            sql_params![],
        )
        .unwrap();
        // Simulate the user having manually specified a projection, so we can assert that
        // we don't also register a default projection with the same location_ptr.
        db.execute(
            "INSERT INTO projections 
            (collection_id, field, location_ptr, user_provided)
            VALUES
            (1, 'user_provided_field', '/oneFoo/fooObj/a', true);",
            sql_params![],
        )
        .unwrap();

        let coll = CollectionResource {
            id: 1,
            resource: Resource { id: 1 },
        };
        let inputs = &[coll];

        let scope = Scope::empty(&db);
        register_default_projections_and_inferences(&scope.push_resource(coll.resource), inputs)
            .expect("failed to register defaults");

        let actual: Vec<(String, String, String)> = db
            .prepare(
                "SELECT field, location_ptr, types_json FROM projections NATURAL JOIN inferences ORDER BY field ASC",
            )
            .unwrap()
            .query_map(rusqlite::NO_PARAMS, |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .expect("failed to query projections")
            .collect::<rusqlite::Result<Vec<(String, String, String)>>>()
            .expect("failed to read query results");

        insta::assert_json_snapshot!(actual);
    }

    #[test]
    fn property_name_is_generated_from_location() {
        let root = Location::Root;
        let a = root.push_prop("a");
        let a_5 = a.push_item(5);
        let b = a_5.push_prop("b");

        let name = field_name_from_location(b);
        assert_eq!("a_5_b", name.as_str());

        let name = field_name_from_location(root);
        assert!(name.is_empty());
    }
}
