use itertools::Itertools;
use proto_flow::flow;

/// Nests the provided shape under a JSON pointer path by creating the necessary object hierarchy.
/// For example, given pointer "/a/b/c" and a field shape, creates an object structure:
/// { "a": { "b": { "c": field_shape } } }
pub fn build_shape_at_pointer(ptr: &doc::Pointer, shape: doc::Shape) -> doc::Shape {
    match ptr.0.first() {
        Some(token) => match token {
            doc::ptr::Token::Property(name) => {
                let mut obj = doc::Shape::nothing();
                obj.type_ = json::schema::types::OBJECT;

                obj.object.properties.push(doc::shape::ObjProperty {
                    name: Box::from(name.as_str()),
                    is_required: true,
                    shape: build_shape_at_pointer(
                        &doc::Pointer::from_iter(ptr.iter().cloned().skip(1)),
                        shape,
                    ),
                });
                return obj;
            }
            doc::ptr::Token::Index(_) => {
                // Create an array shape with the next level nested inside
                let mut array = doc::Shape::nothing();
                array.type_ = json::schema::types::ARRAY;
                array.array.additional_items = Some(Box::new(build_shape_at_pointer(
                    &doc::Pointer::from_iter(ptr.iter().cloned().skip(1)),
                    shape,
                )));

                return array;
            }
            _ => unreachable!("NextIndex/NextProperty shouldn't appear in concrete pointers"),
        },
        None => {
            // Final case
            return shape;
        }
    }
}

pub fn build_field_selection_shape(
    source_shape: doc::Shape,
    fields: Vec<String>,
    projections: Vec<flow::Projection>,
) -> anyhow::Result<(doc::Shape, Vec<flow::Projection>)> {
    let selected_projections = fields
        .iter()
        .filter(|f| f.len() > 0)
        .map(|field| {
            let projection = projections.iter().find(|proj| proj.field == *field);
            if let Some(projection) = projection {
                Some(projection.clone())
            } else {
                tracing::warn!(
                    ?field,
                    "Missing projection for field on materialization built spec"
                );
                None
            }
        })
        .flatten(); // transform from Option<T> to T by filtering out Nones

    let mut starting_shape = doc::Shape::nothing();
    starting_shape.type_ = json::schema::types::OBJECT;

    let mapped_shape =
        selected_projections
            .clone()
            .fold(starting_shape, |value_shape, projection| {
                let source_ptr = doc::Pointer::from_str(&projection.ptr);
                let (source_shape, exists) = source_shape.locate(&source_ptr);
                if exists.cannot() {
                    tracing::warn!(
                        projection = ?source_ptr,
                        "Projection field not found in schema"
                    );
                    value_shape
                } else {
                    let nested_shape = build_shape_at_pointer(
                        &doc::Pointer::from_str(&format!("/{}", projection.field)),
                        source_shape.clone(),
                    );
                    doc::Shape::intersect(value_shape, nested_shape)
                }
            });

    Ok((mapped_shape, selected_projections.collect_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::assert_json_snapshot;

    #[test]
    fn test_empty_pointer() {
        let ptr = doc::Pointer::empty();
        let mut shape = doc::Shape::nothing();
        shape.type_ = json::schema::types::STRING;

        let result = build_shape_at_pointer(&ptr, shape);
        let schema = doc::shape::schema::to_schema(result);
        assert_json_snapshot!(schema, @r###"
        {
          "$schema": "https://json-schema.org/draft/2019-09/schema",
          "type": "string"
        }
        "###);
    }

    #[test]
    fn test_single_property() {
        let ptr = doc::Pointer::from("/name");
        let mut shape = doc::Shape::nothing();
        shape.type_ = json::schema::types::STRING;

        let result = build_shape_at_pointer(&ptr, shape);
        let schema = doc::shape::schema::to_schema(result);
        assert_json_snapshot!(schema, @r###"
        {
          "$schema": "https://json-schema.org/draft/2019-09/schema",
          "type": "object",
          "required": [
            "name"
          ],
          "properties": {
            "name": {
              "type": "string"
            }
          }
        }
        "###);
    }

    #[test]
    fn test_nested_properties() {
        let ptr = doc::Pointer::from("/user/address/street");
        let mut shape = doc::Shape::nothing();
        shape.type_ = json::schema::types::STRING;

        let result = build_shape_at_pointer(&ptr, shape);
        let schema = doc::shape::schema::to_schema(result);
        assert_json_snapshot!(schema, @r###"
        {
          "$schema": "https://json-schema.org/draft/2019-09/schema",
          "type": "object",
          "required": [
            "user"
          ],
          "properties": {
            "user": {
              "type": "object",
              "required": [
                "address"
              ],
              "properties": {
                "address": {
                  "type": "object",
                  "required": [
                    "street"
                  ],
                  "properties": {
                    "street": {
                      "type": "string"
                    }
                  }
                }
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn test_array() {
        let ptr = doc::Pointer::from("/am_an_obj/0");
        let mut shape = doc::Shape::nothing();
        shape.type_ = json::schema::types::STRING;

        let result = build_shape_at_pointer(&ptr, shape);
        let schema = doc::shape::schema::to_schema(result);
        assert_json_snapshot!(schema, @r###"
        {
          "$schema": "https://json-schema.org/draft/2019-09/schema",
          "type": "object",
          "required": [
            "am_an_obj"
          ],
          "properties": {
            "am_an_obj": {
              "type": "array",
              "items": {
                "type": "string"
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn test_mixed_path() {
        let ptr = doc::Pointer::from("/users/0/addresses/1/street");
        let mut shape = doc::Shape::nothing();
        shape.type_ = json::schema::types::STRING;

        let result = build_shape_at_pointer(&ptr, shape);
        let schema = doc::shape::schema::to_schema(result);
        assert_json_snapshot!(schema, @r###"
        {
          "$schema": "https://json-schema.org/draft/2019-09/schema",
          "type": "object",
          "required": [
            "users"
          ],
          "properties": {
            "users": {
              "type": "array",
              "items": {
                "type": "object",
                "required": [
                  "addresses"
                ],
                "properties": {
                  "addresses": {
                    "type": "array",
                    "items": {
                      "type": "object",
                      "required": [
                        "street"
                      ],
                      "properties": {
                        "street": {
                          "type": "string"
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        "###);
    }
}
