// This module defines limits which are used to simplify complex,
// typically inferred schema Shapes.
use super::*;
use crate::{ptr::Token, Pointer};
use itertools::Itertools;
use std::cmp::Ordering;

fn resolve_shape_mut(shape: &mut Shape, field: Token) -> Option<&mut Shape> {
    match field {
        Token::Index(idx) => shape.array.tuple.get_mut(idx),
        Token::Property(prop_name) if prop_name == "*" => {
            shape.object.additional_properties.as_deref_mut()
        }
        Token::Property(prop_name) => shape
            .object
            .properties
            .iter_mut()
            .find(|prop| *prop.name == *prop_name)
            .map(|inner| &mut inner.shape),
        Token::NextIndex => shape.array.additional_items.as_deref_mut(),
    }
}

fn squash_location_inner(shape: &mut Shape, name: Token) {
    match name {
        Token::Index(idx) => {
            // Remove location from parent properties
            let shape_to_squash = shape.array.tuple.remove(idx);

            if let Some(addl_items) = &shape.array.additional_items {
                shape.array.additional_items =
                    Some(Box::new(Shape::union(*addl_items.clone(), shape_to_squash)));
            } else {
                shape.array.additional_items = Some(Box::new(shape_to_squash));
            }
        }
        Token::Property(prop_name) => {
            let prop_position = shape
                .object
                .properties
                .iter()
                .position(|prop| *prop.name == *prop_name);

            if let Some(prop_position) = prop_position {
                // Remove location from parent properties
                let ObjProperty {
                    shape: shape_to_squash,
                    ..
                } = shape.object.properties.remove(prop_position);

                // First check to see if it matches a pattern
                // and if so squash into that pattern's shape
                if let Some(pattern) = shape
                    .object
                    .pattern_properties
                    .iter_mut()
                    .find(|pattern| regex_matches(&pattern.re, &prop_name))
                {
                    pattern.shape = Shape::union(pattern.shape.clone(), shape_to_squash)
                } else if let Some(addl_properties) = &shape.object.additional_properties {
                    shape.object.additional_properties = Some(Box::new(Shape::union(
                        *addl_properties.clone(),
                        shape_to_squash,
                    )));
                } else {
                    shape.object.additional_properties = Some(Box::new(shape_to_squash))
                }
            }
        }
        Token::NextIndex => {}
    }
}

fn squash_location(shape: &mut Shape, location: Pointer) {
    match &location.0.as_slice() {
        [] => unreachable!(),
        [first] => squash_location_inner(shape, first.to_owned()),
        [first, last] => {
            if let Some(parent) = resolve_shape_mut(shape, first.to_owned()) {
                match last {
                    // These represent the locations for array `additionalItems`, and object `additionalProperties`.
                    // The only way I can figure out to reduce the complexity of these locations
                    // is to simply "widen" their shapes to accept anything, thereby removing schema complexity.
                    Token::NextIndex => {
                        parent.array.additional_items = Some(Box::new(Shape::anything()))
                    }
                    Token::Property(prop_name) if prop_name == "*" => {
                        parent.object.additional_properties = Some(Box::new(Shape::anything()))
                    }
                    _ => squash_location_inner(parent, last.to_owned()),
                }
            }
        }
        [first, more @ ..] => {
            if let Some(inner) = resolve_shape_mut(shape, first.to_owned()) {
                squash_location(inner, Pointer(more.to_vec()))
            }
        }
    }
}

fn is_additionalx_field(token: &Token) -> bool {
    match token {
        Token::NextIndex => true,
        Token::Property(prop_name) if prop_name == "*" => true,
        _ => false,
    }
}

/// Reduce the size/complexity of a shape while making sure that all
/// objects that used to pass validation still do.
pub fn enforce_shape_complexity_limit(shape: &mut Shape, limit: usize) {
    let mut locations = shape
        .locations()
        .into_iter()
        .filter_map(|(ptr, _, shape, _)| {
            if ptr.0.len() > 0 {
                Some((ptr, shape.clone()))
            } else {
                None
            }
        })
        .collect_vec();

    if locations.len() < limit {
        return;
    }

    locations.sort_by(|(a_ptr, _), (b_ptr, _)| {
        // make sure that all `additional*` fields are last
        // AND that they are sorted by depth within their group
        // then order by depth, then lexicographically
        match (a_ptr.0.as_slice(), b_ptr.0.as_slice()) {
            // Order additional* fields by depth within their group
            ([.., a_token], [.., b_token])
                if is_additionalx_field(a_token) && is_additionalx_field(b_token) =>
            {
                a_ptr.0.len().cmp(&b_ptr.0.len())
            }
            // Order additional* fields after all other fields
            ([.., a_token], _) if is_additionalx_field(a_token) => Ordering::Less,
            (_, [.., b_token]) if is_additionalx_field(b_token) => Ordering::Greater,
            // Neither are additional*, now order by depth
            _ => match a_ptr.0.len().cmp(&b_ptr.0.len()) {
                // Same depth, stably sort lexicographically
                Ordering::Equal => a_ptr.to_string().cmp(&b_ptr.to_string()),
                depth => depth,
            },
        }
    });

    while locations.len() > limit {
        let (location_ptr, _) = locations
            .pop()
            .expect("locations vec was just checked to be non-empty");

        squash_location(shape, location_ptr);
    }
}

pub const DEFAULT_SCHEMA_COMPLEXITY_LIMIT: usize = 2000;

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
        enforce_limits: Option<usize>,
    ) -> Shape {
        let mut schema = match initial_schema {
            Some(initial) => shape_from(initial),
            None => Shape::nothing(),
        };

        for doc in docs {
            schema.widen(doc);
        }

        let expected = shape_from(expected_schema);

        if let Some(limit) = enforce_limits {
            enforce_shape_complexity_limit(&mut schema, limit);
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
                minimum: 0
                maximum: 10000
            "#,
            dynamic_keys.as_slice(),
            Some(0),
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
                      minimum: 0
                      maximum: 10000
                    - type: object
                      additionalProperties:
                        type: integer
                        minimum: 0
                        maximum: 10000
            "#,
            &[json!(root)],
            Some(2),
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
                            minimum: 0
                            maximum: 0
            "#,
            &[json!({ "container": nested })],
            Some(4),
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
                        type: integer
                        minimum: 0
                        maximum: 10000
            "#,
            &[json!({ "container": nested })],
            Some(3),
        );
    }

    #[test]
    fn test_field_count_limits_inside_array() {
        widening_snapshot_helper(
            None,
            r#"
            type: array
            minItems: 0
            maxItems: 1
            items:
                type: object
                additionalProperties: false
                required: [key]
                properties:
                    key:
                        type: string
                        minLength: 2
                        maxLength: 4
            "#,
            &[json!([{"key": "test"}])],
            Some(3),
        );
        let dynamic_array_objects = (0..8)
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
                        minLength: 2
                        maxLength: 4
                "#,
            &dynamic_array_objects,
            Some(2),
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
                    minimum: 0
                    maximum: 0
                key-0:
                    type: integer
                    minimum: 0
                    maximum: 0
            "#,
            dynamic_keys.as_slice(),
            Some(20),
        );
    }

    #[test]
    fn test_quickcheck_regression() {
        widening_snapshot_helper(
            None,
            r#"
            type: array
            maxItems: 1
            items:
                type: object
                additionalProperties: true
            "#,
            &[json!([{}])],
            Some(0),
        );
    }

    #[test]
    fn test_quickcheck_regression_2() {
        widening_snapshot_helper(
            None,
            r#"
            type: object
            additionalProperties:
                type: array
                items: false
                maxItems: 0
            "#,
            &[json!({"foo":[]})],
            Some(0),
        );
    }
}
