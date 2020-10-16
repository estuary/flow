use crate::catalog::{self, Scope, DB};
use crate::doc::inference::Shape;
use crate::doc::Pointer;
use estuary_json::schema::index::Index;
use estuary_json::schema::types;
use estuary_json::Location;
use url::Url;

/// Type information that's been inferred for a specific location within a schema.
#[derive(Debug)]
pub struct Inference<'a> {
    pub location_ptr: String,
    pub shape: &'a Shape,
    pub must_exist: bool,
}

impl<'a> Inference<'a> {
    pub fn locate_within(location_ptr: &str, shape: &'a Shape) -> Option<Inference<'a>> {
        if let Some((located, must_exist)) = shape.locate(&Pointer::from(location_ptr)) {
            Some(Inference {
                location_ptr: location_ptr.to_owned(),
                shape: located,
                must_exist,
            })
        } else {
            None
        }
    }
}

/// Traverses the shape and persists inferences for all of the "projectable" locations within it.
pub fn register_all(scope: Scope, schema_uri: &Url) -> catalog::Result<()> {
    let schemas = catalog::Schema::compile_for(scope.db, scope.resource().id)?;
    let mut index = Index::new();
    for schema in schemas.iter() {
        index.add(schema)?;
    }
    let schema = index.must_fetch(schema_uri)?;

    let shape = Shape::infer(schema, &index);
    let inferences = get_inferences(&shape);
    for inference in inferences {
        register_one(scope.db, schema_uri.as_str(), &inference)?;
    }
    Ok(())
}

/// Persists the given inference.
pub fn register_one(db: &DB, schema_uri: &str, inference: &Inference) -> catalog::Result<()> {
    let types_json = inference.shape.type_.to_json_array();
    let params = rusqlite::params![
        schema_uri,
        inference.location_ptr.as_str(),
        types_json,
        inference.must_exist,
        inference.shape.string.content_type.as_ref(),
        inference.shape.string.format.as_ref(),
        inference.shape.string.is_base64,
        inference.shape.string.max_length.map(usize_to_i64)
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
    Ok(())
}

/// Traverses all of the "projectable" locations in the shape and returns a vec of `Inference`s.
pub fn get_inferences<'a, 'b>(shape: &'a Shape) -> Vec<Inference<'a>> {
    let mut fields = Vec::with_capacity(32);
    fill_inferences(Location::Root, shape, true, &mut fields);
    fields
}

fn fill_inferences<'a, 'b>(
    location: Location<'b>,
    shape: &'a Shape,
    must_exist: bool,
    inferences: &mut Vec<Inference<'a>>,
) {
    // Temporarily remove null and match on the remainder of the possible types. We're only looking
    // at fields with a single possible type (apart from null). Any fields with multiple possible
    // types (e.g. can be either a string or an object) are ignored.
    let non_nullable_type = (!types::NULL) & shape.type_;
    let is_nullable = shape.type_.overlaps(types::NULL);

    // We'll always add the location to the output, even if it has mixed types. We won't recurse
    // into fields with multiple types, but we will yield them here. This allows us to populate
    // inferences for many of the fields that have multiple types, which in turn allows us to
    // produce better error messages when a user attempts to use these locations as keys.
    inferences.push(Inference {
        location_ptr: location.pointer_str().to_string(),
        shape,
        must_exist,
    });

    // We only test for strict equality here (not `overlaps`). This is so we only traverse into
    // nested fields that have a single possible type.
    if non_nullable_type == types::OBJECT {
        for property in shape.object.properties.iter() {
            let new_location = location.push_prop(property.name.as_str());
            // All parents (including the current shape) must be required and not nullable,
            // in addition to this property being required. Note that this can still be true
            // even if the value may be null.
            let location_must_exist = must_exist && property.is_required && !is_nullable;
            fill_inferences(
                new_location,
                &property.shape,
                location_must_exist,
                inferences,
            );
        }
    }
    if non_nullable_type == types::ARRAY {
        for (index, item_shape) in shape.array.tuple.iter().enumerate() {
            let location = location.push_item(index);
            // For array items, must_exist should be false unless the index of the current
            // tuple item is less than the minimum number of required items. Note that this can
            // still be true even if the value may be null.
            let location_must_exist =
                must_exist && !is_nullable && index < shape.array.min.unwrap_or_default();

            fill_inferences(location, item_shape, location_must_exist, inferences);
        }
    }
}

fn usize_to_i64(unsigned: usize) -> i64 {
    unsigned.min(usize::MAX - 1) as i64
}

#[cfg(test)]
mod test {
    use super::*;
    use estuary_json::schema::{build::build_schema, index::Index};
    use itertools::Itertools;

    #[test]
    fn inferences_are_registered_by_schema_uri() {
        let db = catalog::create(":memory:").unwrap();

        let schema = r##"
            type: object
            properties:
              a:
                type: object
                properties:
                  a: {type: string}
                  b: {type: boolean}
                  c: {type: integer}
                  d: {type: number}
                required: [a, b]
            required: [a]
            "##;
        db.execute(
            "INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                    (1, 'application/vnd.estuary.dev-catalog-spec+yaml', X'1234', FALSE),
                    (22, 'application/schema+yaml', CAST(? AS BLOB), FALSE);",
            rusqlite::params![schema],
        )
        .unwrap();
        db.execute_batch(
            r##"
            INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                    (1, 'test://test/flow.yaml', TRUE),
                    (22, 'test://schema.yaml', TRUE);
            INSERT INTO resource_imports (resource_id, import_id) VALUES (1, 22);
                         "##,
        )
        .unwrap();

        let uri = Url::parse("test://schema.yaml").unwrap();
        let scope = Scope::for_test(&db, 1);
        register_all(scope, &uri).expect("failed to register inferences");

        let actual = catalog::dump_table(&db, "inferences").unwrap();
        insta::assert_json_snapshot!(actual);
    }

    #[test]
    fn inferences_are_returned_for_shape() {
        let schema = r##"
            type: object
            properties:
              a:
                type: object
                properties:
                  a: {type: string}
                  b: {type: boolean}
                  c: {type: integer}
                  d: {type: number}
                required: [a, b]
              b:
                oneOf:
                  - type: object
                    properties:
                      a: {type: string}
                      b: {type: boolean}
                      c: {type: integer}
                      d: {type: number}
                  - type: string

              c:
                type: array
                items:
                  - type: string
                  - type: integer
                minItems: 1
              d: {type: integer}
            required: [a, b, c]
            "##;
        let dom: serde_json::Value =
            serde_yaml::from_str(schema).expect("failed to compile schema");
        let schema_uri = url::Url::parse("test://test.test/test.json").unwrap();
        let compiled_schema =
            build_schema(schema_uri.clone(), &dom).expect("failed to compile schema");
        let mut index = Index::new();
        index.add(&compiled_schema).unwrap();
        let shape = Shape::infer(&compiled_schema, &index);

        let result = get_inferences(&shape);
        // Format the list of inferences into a string representation that will be easy
        // to view and compare as an insta snapshot.
        let repr = result
            .into_iter()
            .map(|inference| {
                let types = inference.shape.type_.to_json_array();
                format!(
                    "location: {:?}, types: {}, must_exist: {}",
                    inference.location_ptr, types, inference.must_exist
                )
            })
            .join("\n");
        insta::assert_snapshot!(repr);
    }
}
