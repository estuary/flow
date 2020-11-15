use crate::catalog::{self, Schema, Scope, DB};
use crate::doc::inference::Shape;
use crate::doc::Pointer;
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
    let shape = Schema::shape_for(scope.db, scope.resource().id, schema_uri)?;

    for inference in get_inferences(&shape) {
        register_one(scope.db, schema_uri.as_str(), &inference)?;
    }
    Ok(())
}

/// Persists the given inference. This function is safe to call multiple times for the same
/// inference. This behavior was chosen because inferences may be generated from both user-provided
/// and automatically generated projections.
pub fn register_one(db: &DB, schema_uri: &str, inference: &Inference) -> catalog::Result<()> {
    let types_json = inference.shape.type_.to_json_array();
    let params = rusqlite::params![
        schema_uri,
        inference.location_ptr.as_str(),
        types_json,
        inference.must_exist,
        inference.shape.title,
        inference.shape.description,
        inference.shape.string.content_type.as_ref(),
        inference.shape.string.format.as_ref(),
        inference.shape.string.is_base64,
        inference.shape.string.max_length.map(usize_to_i64)
    ];
    // Using OR IGNORE here so that we don't return an error if the inference already exists.
    db.prepare_cached(
        "INSERT OR IGNORE INTO inferences (
                schema_uri,
                location_ptr,
                types_json,
                must_exist,
                title,
                description,
                string_content_type,
                string_format,
                string_content_encoding_is_base64,
                string_max_length
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
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
    inferences.push(Inference {
        location_ptr: location.pointer_str().to_string(),
        shape,
        must_exist,
    });

    // Traverse sub-locations of this location when it takes an object
    // or array type. As a rule, children must exist only if their parent
    // does, the parent can *only* take the applicable type, and it has
    // validations which require that the child exist.

    for property in &shape.object.properties {
        fill_inferences(
            location.push_prop(&property.name),
            &property.shape,
            must_exist && shape.type_ == types::OBJECT && property.is_required,
            inferences,
        );
    }

    for (index, item_shape) in shape.array.tuple.iter().enumerate() {
        fill_inferences(
            location.push_item(index),
            item_shape,
            must_exist
                && shape.type_ == types::ARRAY
                && index < shape.array.min.unwrap_or_default(),
            inferences,
        );
    }

    // As an aide to documentation of repeated items, produce an inference
    // using '-' ("after last item" within json-pointer spec).
    if let Some(item_shape) = &shape.array.additional {
        fill_inferences(location.push_prop("-"), item_shape, false, inferences);
    };
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
                  c:
                    type: integer
                    title: the C title
                    description: the C description
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
                      b: {type: [boolean, "null"]}
                      c: {type: integer}
                      d:
                        type: array
                        allOf:
                            - items: [{type: integer}]
                              additionalItems: {type: [number, boolean]}
                            - items: {type: [number, string]}
                    required: [a, d]
                  - type: string

              c:
                type: array
                oneOf:
                    - items:
                        - type: string
                        - type: integer
                    - items: {type: "null"}
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
