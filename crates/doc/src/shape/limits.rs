// This module defines limits which are used to simplify complex,
// typically inferred schema Shapes.
use super::*;
use crate::ptr::Token;
use itertools::Itertools;
use std::cmp::Ordering;

// This logic somewhat overlaps with [`Shape::locate_token`], but
// here we don't care about recursion, we don't care about how the location
// may or may not exist, and most importantly we need an &mut Shape.
fn resolve_shape_mut(shape: &mut Shape, field: Token) -> Option<&mut Shape> {
    match field {
        Token::Index(idx) => shape.array.tuple.get_mut(idx),
        Token::Property(prop_name) if prop_name == "*" => {
            shape.object.additional_properties.as_deref_mut()
        }
        Token::Property(prop_name) => shape
            .object
            .properties
            .binary_search_by(|prop| prop.name.as_ref().cmp(&prop_name))
            .ok()
            .and_then(|idx| shape.object.properties.get_mut(idx))
            .map(|inner| &mut inner.shape),
        Token::NextIndex => shape.array.additional_items.as_deref_mut(),
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
        Token::Property(prop) if prop == "*" => unreachable!(),

        Token::Index(_) => {
            // Remove the last location from the array tuple shape
            let shape_to_squash = shape
                .array
                .tuple
                .pop()
                .expect("No array tuple property to squash");

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
                shape: shape_to_squash,
                name: prop_name,
                ..
            } = shape
                .object
                .properties
                .pop()
                .expect("No object property to squash");

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

fn squash_subschema(shape: &mut Shape, location: &Token) {
    match location {
        Token::NextIndex => shape.array.additional_items = None,
        Token::Property(prop_name) if prop_name == "*" => shape.object.additional_properties = None,
        _ => unreachable!(),
    }
}

fn squash_location(shape: &mut Shape, location: &[Token]) {
    match location {
        [] => unreachable!(),
        [first] => match first {
            // These represent the root `/*` and `/~` locations. Because they
            // have no parent, we instead remove the whole shape's subschema.
            token if is_additionalx_field(token) => squash_subschema(shape, token),
            _ => squash_location_inner(shape, first),
        },
        [first, last] => {
            if let Some(parent) = resolve_shape_mut(shape, first.to_owned()) {
                match last {
                    // These represent the locations for array `additionalItems`, and object `additionalProperties`.
                    // Once we've already squashed every other explicit field inside a particular container,
                    // we then must widen that container's `additionalItems`/`additionalProperties` into the
                    // "true" shape, otherwise schema inference on excessively nested documents could result in
                    // extremely deep Shapes, even after fully squashing them.
                    token if is_additionalx_field(token) => squash_subschema(parent, token),
                    _ => squash_location_inner(parent, last),
                }
            }
        }
        [first, more @ ..] => {
            let inner = resolve_shape_mut(shape, first.to_owned()).expect(&format!(
                "Attempted to find property {first} that does not exist (more: {more:?})"
            ));
            squash_location(inner, more)
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
    let mut pointers = shape
        .locations()
        .into_iter()
        .filter_map(|(mut ptr, _, _, _)| {
            // We transform these `additional*` locations from this:
            // /foo/bar/baz/*
            // into this:
            // /*/*/*/*
            // Because by the time we get to squashing these locations, the
            // specific field names have already been squashed
            // and so won't exist, but we still want to squash
            // `additionalProperties`/`additionalItems` themselves.
            let ptr_len = ptr.0.len();
            if ptr_len > 0 {
                if is_additionalx_field(ptr.0.last().unwrap()) {
                    ptr.0.iter_mut().for_each(|ancestor| match ancestor {
                        Token::Index(_) => *ancestor = Token::NextIndex,
                        Token::Property(_) => *ancestor = Token::Property("*".to_string()),
                        _ => {}
                    })
                }
                Some(ptr)
            } else {
                None
            }
        })
        .collect_vec();

    if pointers.len() < limit {
        return;
    }

    pointers.sort_by(|a_ptr, b_ptr| {
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
            Some(1),
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
            Some(1),
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
            "#,
            &[json!([{}])],
            Some(1),
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
            "#,
            &[json!({"foo":[]})],
            Some(1),
        );
    }
}
