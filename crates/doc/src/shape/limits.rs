// This module defines limits which are used to simplify complex,
// typically inferred schema Shapes.
use super::*;
use json::Location;

// Prune any locations in this shape that have more than the allowed fields,
// squashing those fields into the `additionalProperties` subschema for that location.
pub fn enforce_field_count_limits(slf: &mut Shape, loc: Location) {
    // TODO: If we implement inference/widening of array tuple shapes
    // then we'll need to also check that those aren't excessively large.
    if slf.type_.overlaps(types::ARRAY) {
        if let Some(array_shape) = slf.array.additional.as_mut() {
            enforce_field_count_limits(array_shape, loc.push_item(0));
        }
    }

    if !slf.type_.overlaps(types::OBJECT) {
        return;
    }

    let limit = match loc {
        Location::Root => MAX_ROOT_FIELDS,
        _ => MAX_NESTED_FIELDS,
    };

    if slf.object.properties.len() > limit {
        // Take all of the properties' shapes and
        // union them into additionalProperties

        let existing_additional_properties = slf
            .object
            .additional
            // `Shape::union` takes owned Shapes which is why we
            // have to take ownership here.
            .take()
            .map(|boxed| *boxed)
            .unwrap_or(Shape::nothing());

        let merged_additional_properties = slf
            .object
            .properties
            // As part of squashing all known property shapes together into
            // additionalProperties, we need to also remove those explicit properties.
            .drain(..)
            .fold(existing_additional_properties, |accum, mut prop| {
                // Recur here to avoid excessively large `additionalProperties` shapes
                enforce_field_count_limits(&mut prop.shape, loc.push_prop(&prop.name));
                Shape::union(accum, prop.shape)
            });

        slf.object.additional = Some(Box::new(merged_additional_properties));
    } else {
        for prop in slf.object.properties.iter_mut() {
            enforce_field_count_limits(&mut prop.shape, loc.push_prop(&prop.name))
        }
    }
}

pub const MAX_ROOT_FIELDS: usize = 750;
pub const MAX_NESTED_FIELDS: usize = 200;

#[cfg(test)]
mod test {
    use super::*;
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    fn widening_snapshot_helper(
        initial_schema: Option<&str>,
        expected_schema: &str,
        docs: &[serde_json::Value],
        enforce_limits: bool,
    ) -> Shape {
        let mut schema = match initial_schema {
            Some(initial) => shape_from(initial),
            None => Shape::nothing(),
        };

        for doc in docs {
            schema.widen(doc, Location::Root);
        }

        let expected = shape_from(expected_schema);

        if enforce_limits {
            enforce_field_count_limits(&mut schema, Location::Root);
        }

        assert_eq!(expected, schema);

        schema
    }

    #[test]
    fn test_field_count_limits() {
        let dynamic_keys = (0..800)
            .map(|id| {
                json!({
                    "known_key": id,
                    format!("key-{id}"): id*5
                })
            })
            .collect_vec();

        widening_snapshot_helper(
            None,
            r#"
            type: object
            additionalProperties:
                type: integer
            "#,
            dynamic_keys.as_slice(),
            true,
        );
    }

    #[test]
    fn test_field_count_nested() {
        // Create an object like
        // {
        //    "big_key": {
        //        ...751 properties...
        //    },
        //    ...750 more properties...
        // }
        let mut root = BTreeMap::new();
        for id in 0..800 {
            root.insert(format!("key-{id}"), json!(id * 5));
        }

        root.insert("big_key".to_string(), json!(root.clone()));

        widening_snapshot_helper(
            None,
            r#"
            type: object
            additionalProperties:
                anyOf:
                    - type: integer
                    - type: object
                      additionalProperties:
                        type: integer
            "#,
            &[json!(root)],
            true,
        );
    }

    #[test]
    fn test_field_count_limits_nested() {
        let mut nested = BTreeMap::default();
        for id in 0..1 {
            nested.insert(format!("key-{id}"), json!(id * 5));
        }

        widening_snapshot_helper(
            None,
            r#"
            type: object
            additionalProperties: false
            required: [container]
            properties:
                container:
                    type: object
                    additionalProperties: false
                    required: [key-0]
                    properties:
                        key-0:
                            type: integer
            "#,
            &[json!({ "container": nested })],
            true,
        );

        for id in 0..300 {
            nested.insert(format!("key-{id}"), json!(id * 5));
        }

        widening_snapshot_helper(
            None,
            r#"
            type: object
            additionalProperties: false
            required: [container]
            properties:
                container:
                    type: object
                    additionalProperties:
                        type: [integer]
            "#,
            &[json!({ "container": nested })],
            true,
        );
    }
    #[test]
    fn test_field_count_limits_inside_array() {
        widening_snapshot_helper(
            None,
            r#"
            type: array
            minItems: 1
            maxItems: 1
            items:
                type: object
                additionalProperties: false
                required: [key]
                properties:
                    key:
                        type: string
                        minLength: 4
                        maxLength: 4
            "#,
            &[json!([{"key": "test"}])],
            true,
        );
        let dynamic_array_objects = (0..800)
            .map(|id| {
                json!([{
                    format!("key-{id}"): "test"
                }])
            })
            .collect_vec();

        widening_snapshot_helper(
            Some(
                r#"
                type: array
                minItems: 1
                maxItems: 1
                items:
                    type: object
                    additionalProperties: false
                    required: [key]
                    properties:
                        key:
                            type: string
                            minLength: 4
                            maxLength: 4
                "#,
            ),
            r#"
                type: array
                minItems: 1
                maxItems: 1
                items:
                    type: object
                    additionalProperties:
                        type: string
                        minLength: 4
                        maxLength: 4
                "#,
            &dynamic_array_objects,
            true,
        );
    }

    #[test]
    fn test_field_count_limits_noop() {
        let dynamic_keys = (0..1)
            .map(|id| {
                json!({
                    "known_key": id,
                    format!("key-{id}"): id*5
                })
            })
            .collect_vec();

        widening_snapshot_helper(
            None,
            r#"
            type: object
            additionalProperties: false
            required: [known_key, key-0]
            properties:
                known_key:
                    type: integer
                key-0:
                    type: integer
            "#,
            dynamic_keys.as_slice(),
            true,
        );
    }
}
