// This module defines limits which are used to simplify complex,
// typically inferred schema Shapes.
use super::*;
use crate::ptr::Token;
use itertools::Itertools;
use std::cmp::Ordering;

// Potential future improvement: currently this squashes any non-INVALID `additional*`
// shape to accept anything, the equivalent to the `true` schema. But really, we just
// want to remove recursive shapes if we overlap with `OBJECT` or `ARRAY`, and could
// happily leave other non-recursive/atomic types alone, retaining e.g integer or string bounds.
fn squash_addl(props: Option<Box<Shape>>) -> Option<Box<Shape>> {
    match props {
        Some(inner) if inner.type_.eq(&types::INVALID) => Some(Box::new(Shape::nothing())),
        Some(_) | None => None,
    }
}

// Squashing a shape inside an array tuple is special because the location
// of shapes inside the tuple is _itself_ the key into that container.
// This means that if we do anything to shift the keys of still-existing shapes,
// they won't be valid any longer. With that in mind, there's also no good reason
// to squash one object field over any other, so let's just treat
// Token::Index and Token::Property as signals to squash _an_ index or property,
// leaving it up to the implementation to determine which one.
fn squash_location_inner(shape: &mut Shape, name: &Token) {
    match name {
        // Squashing of `additional*` fields is not possible here because we don't
        // have access to the parent shape
        Token::NextIndex => unreachable!(),
        Token::AdditionalProperties => unreachable!(),

        Token::Index(_) => {
            // Pop the last element from the array tuple shape to avoid
            // shifting the indexes of any other tuple shapes
            let mut shape_to_squash = shape
                .array
                .tuple
                .pop()
                .expect("No array tuple property to squash");

            shape_to_squash.array.additional_items =
                squash_addl(shape_to_squash.array.additional_items);
            shape_to_squash.object.additional_properties =
                squash_addl(shape_to_squash.object.additional_properties);

            if let Some(addl_items) = shape.array.additional_items.take() {
                shape.array.additional_items =
                    Some(Box::new(Shape::union(*addl_items, shape_to_squash)));
            } else {
                shape.array.additional_items = Some(Box::new(shape_to_squash));
            }
        }
        Token::Property(_) => {
            // Remove location from parent properties
            let ObjProperty {
                shape: mut shape_to_squash,
                name: prop_name,
                ..
            } = shape
                .object
                .properties
                .pop()
                .expect("No object property to squash");

            shape_to_squash.array.additional_items =
                squash_addl(shape_to_squash.array.additional_items);
            shape_to_squash.object.additional_properties =
                squash_addl(shape_to_squash.object.additional_properties);

            // First check to see if it matches a pattern
            // and if so squash into that pattern's shape
            if let Some(pattern) = shape
                .object
                .pattern_properties
                .iter_mut()
                .find(|pattern| regex_matches(&pattern.re, &prop_name))
            {
                pattern.shape = Shape::union(
                    // Ideally we'd use a function like `replace_with` to allow replacing
                    // pattern.shape with a value mapped from its previous value, but
                    // that function doesn't exist yet. See https://github.com/rust-lang/rfcs/pull/1736
                    // Instead, we must replace it with something temporarily while
                    // Shape::union runs. Once it finishes, this `Shape::nothing()` is discarded.
                    std::mem::replace(&mut pattern.shape, Shape::nothing()),
                    shape_to_squash,
                )
            } else if let Some(addl_properties) = shape.object.additional_properties.take() {
                shape.object.additional_properties =
                    Some(Box::new(Shape::union(*addl_properties, shape_to_squash)));
            } else {
                shape.object.additional_properties = Some(Box::new(shape_to_squash))
            }
        }
    }
}

fn squash_location(shape: &mut Shape, location: &[Token]) {
    match location {
        [] => unreachable!(),
        [Token::NextIndex] => unreachable!(),
        [Token::AdditionalProperties] => unreachable!(),

        [first] => squash_location_inner(shape, first),
        [first, more @ ..] => {
            let inner = match first {
                Token::NextIndex => shape.array.additional_items.as_deref_mut(),
                Token::AdditionalProperties => shape.object.additional_properties.as_deref_mut(),
                Token::Index(idx) => shape.array.tuple.get_mut(*idx),
                Token::Property(prop_name) => shape
                    .object
                    .properties
                    .binary_search_by(|prop| prop.name.as_ref().cmp(&prop_name))
                    .ok()
                    .and_then(|idx| shape.object.properties.get_mut(idx))
                    .map(|inner| &mut inner.shape),
            }
            .expect(&format!(
                "Attempted to find property {first} that does not exist (more: {more:?})"
            ));
            squash_location(inner, more)
        }
    }
}

/// Reduce the size/complexity of a shape while making sure that all
/// objects that used to pass validation still do.
pub fn enforce_shape_complexity_limit(shape: &mut Shape, limit: usize) {
    let mut pointers = shape
        .locations()
        .into_iter()
        .filter_map(|(ptr, _, _, _)| match ptr.0.as_slice() {
            // We need to include `/*/foo` in order to squash inside `additional*` subschemas,
            // but we don't want to include those locations that are leaf nodes, since
            // leaf node recursion is squashed every time we squash a concrete property.
            [.., Token::NextIndex] => None,
            [.., Token::AdditionalProperties] => None,
            [] => None,
            _ => Some(ptr),
        })
        .collect_vec();

    if pointers.len() < limit {
        return;
    }

    pointers.sort_by(|a_ptr, b_ptr| {
        // order by depth, then by pointer location
        match a_ptr.0.len().cmp(&b_ptr.0.len()) {
            // Same depth, stably sort by pointer location
            Ordering::Equal => a_ptr.cmp(&b_ptr),
            depth => depth,
        }
    });

    while pointers.len() > limit {
        let location_ptr = pointers
            .pop()
            .expect("locations vec was just checked to be non-empty");

        squash_location(shape, location_ptr.0.as_slice());
    }
}

pub const DEFAULT_SCHEMA_COMPLEXITY_LIMIT: usize = 1_000;

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
        //      "key-0": 5,
        //        ...750 more properties...
        //    },
        //    "key-0": 5,
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
            "#,
            &[json!(root)],
            Some(0),
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
            Some(1),
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
            Some(0),
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
    fn test_deep_nesting() {
        let mut doc = json!({});
        for idx in 0..10 {
            doc = json!({format!("foo{idx}"): doc, format!("bar{idx}"): doc});
        }

        widening_snapshot_helper(
            None,
            r#"
            type: object
            additionalProperties:
                type: object
            "#,
            &[doc],
            Some(0),
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
                additionalProperties: false
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
                maxItems: 0
                additionalItems: false
            "#,
            &[json!({"foo":[]})],
            Some(0),
        );
    }

    #[test]
    fn test_quickcheck_regression_3() {
        widening_snapshot_helper(
            Some(
                r#"
                type: object
                required:
                  - ""
                  - "*"
                properties:
                  "":
                    type: "null"
                  "*":
                    type: string
                    maxLength: 0
                additionalProperties: false
            "#,
            ),
            r#"
            type: object
            additionalProperties:
                type: ["null", "string"]
                maxLength: 0
            "#,
            &[],
            Some(0),
        );
    }
}
