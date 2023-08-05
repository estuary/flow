// This module defines a "widen" operation which widens the constraints
// of a Shape as needed, to allow a given document to properly validate.
// It's used as a base operation for schema inference.
use super::*;
use crate::AsNode;
use itertools::EitherOrBoth;
use json::Location;

impl ObjShape {
    /// See [`Shape::widen()`] for details on the order of widening.
    fn widen<'n, N>(&mut self, fields: &'n N::Fields, loc: Location, is_first_time: bool) -> bool
    where
        N: AsNode,
    {
        use crate::{Field, Fields};

        // `additionalProperties` is a full Schema. According to JSON schema,
        // a blank schema matches all documents. If we didn't initialize to
        // `additionalProperties: false`, every field would fall into `additionalProperties`
        //  and we wouldn't get any useful schemas.
        let mut additional_properties = if let Some(addl) = self.additional.take() {
            *addl
        } else {
            Shape::nothing()
        };

        let mut hint = false;

        let new_fields: Vec<_> =
            itertools::merge_join_by(self.properties.iter_mut(), fields.iter(), |prop, field| {
                prop.name.cmp(&field.property().to_string())
            })
            .filter_map(|eob| match eob {
                // Both the shape and node have this field, recursion time
                EitherOrBoth::Both(lhs, rhs) => {
                    hint |= lhs.shape.widen(rhs.value(), loc.push_prop(rhs.property()));
                    None
                }
                // Shape has a field that the node is missing, so let's make sure it's not marked as required
                EitherOrBoth::Left(mut lhs) => {
                    lhs.is_required = false;
                    None
                }
                // The Node has a field that the shape doesn't, let's add it
                EitherOrBoth::Right(rhs) => {
                    let mut prop = ObjProperty {
                        name: rhs.property().to_owned(),
                        // A field can only be required if every single document we've seen
                        // has that field present. This means that ONLY fields that exist
                        // on the very first object we encounter for a particular location should
                        // get marked as required, as any subsequent "new" fields
                        // by definition did not exist on previous objects (and so cannot be required)
                        // otherwise they would already be in the Shape
                        // (and we would be in the `EoB::Both` branch).
                        is_required: is_first_time,
                        // Leave shape blank here, we're going to recur and expand it right below
                        // Note: Shape starts out totally unconstrained (types::ANY) by default,
                        // whereas we want it maximally constrained initially
                        shape: Shape::nothing(),
                    };

                    hint |= prop.shape.widen(rhs.value(), loc.push_prop(rhs.property()));

                    Some(prop)
                }
            })
            // Our iterator now contains a fully widened entry for unmatched field.
            // First, let's widen these into any matching `patternProperties`,
            // then remove those fields from consideration.
            .filter_map(|new_field| {
                if let Some(matching_pattern) = self
                    .patterns
                    .iter_mut()
                    .find(|pattern| regex_matches(&pattern.re, &new_field.name))
                {
                    matching_pattern.shape =
                        Shape::union(matching_pattern.shape.clone(), new_field.shape);
                    None
                } else {
                    Some(new_field)
                }
            })
            .collect();

        // We're now left with `new_fields` containing all new fields that neither have
        // an explicit match in `properties`, nor match any defined pattern.
        // If `additionalProperties: false`, we need to add those fields explicitly to `properties`.
        // Otherwise, we need to merge their shapes into `additionalProperties`.
        if !new_fields.is_empty() {
            // additionalProperties: false
            if additional_properties.type_.eq(&types::INVALID) {
                // These new shapes can not conflict with existing properties by definition
                // because they were produced by the right-hand-side of the `merge_join_by`.
                // That is, these fields explicitly do not yet exist on this shape.
                self.properties.extend(new_fields.into_iter());
                self.properties.sort_by(|a, b| a.name.cmp(&b.name))
            } else {
                for field in new_fields {
                    additional_properties =
                        Shape::union(additional_properties.clone(), field.shape);
                }
            }
        }

        self.additional = Some(Box::new(additional_properties));

        match (hint, loc) {
            (true, _) => true,
            (false, Location::Root) => self.properties.len() > limits::MAX_ROOT_FIELDS,
            (false, _) => self.properties.len() > limits::MAX_NESTED_FIELDS,
        }
    }
}

impl Shape {
    /// Minimally widen the shape so the provided document will successfully validate.
    /// Returns a hint if some locations might exceed their maximum allowable size.
    /// In order to build useful object schemas, we need to widen in order of explicitness:
    /// * Fields matching explicitly named `properties` will always be handled by widening
    ///   those properties to accept the shape of the field.
    /// * Any remaining fields whose names match a pattern in `patternProperties` will always
    ///   be handled by widening that patternProperty's shape to accept the field.
    ///
    /// Any remaining fields will be handled differently depending on `additionalProperties`:
    /// * If this schema has `additionalProperties: false`, that means that that
    ///    unmatched fields are forbidden when validating. In this case, we create new
    ///    explicitly-named `properties` for each leftover field.
    /// * If this schema has `additionalProperties` _other_ than `false`, we use that as a
    ///    signal to indicate that we should not add any more explicit `properties`. Instead,
    ///    we simply widen the shape of `additionalProperties` to accept all unmatched fields.
    ///
    /// Arrays are widened by expanding their `items` to fit the provided document.
    /// Scalar values are widened along whatever dimensions exist: string formats and lengths, number ranges, etc.
    pub fn widen<'n, N>(&mut self, node: &'n N, loc: Location) -> bool
    where
        N: AsNode,
    {
        match node.as_node() {
            crate::Node::Object(fields) => {
                // See comment in `ObjShape::widen` about when to set `is_required`
                // on newly encountered fields. Detects whether this is the
                // very first time this location has seen an OBJECT shape.
                let is_first_time = !self.type_.overlaps(types::OBJECT);
                self.type_ = self.type_ | types::OBJECT;

                self.object.widen::<N>(fields, loc, is_first_time)
            }

            crate::Node::Array(arr) => {
                let mut shape = self
                    .array
                    .additional
                    .take()
                    .unwrap_or(Box::new(Shape::nothing()));

                // Look at each element in the observed array and widen the shape to accept it
                let hint = arr.iter().enumerate().fold(false, |accum, (idx, node)| {
                    accum || shape.widen(node, loc.push_item(idx))
                });

                self.array.additional = Some(shape);

                self.array.min = match self.array.min {
                    Some(prev_min) => Some(prev_min.min(arr.len())),
                    None => Some(arr.len()),
                };
                self.array.max = match self.array.max {
                    Some(prev_max) => Some(prev_max.max(arr.len())),
                    None => Some(arr.len()),
                };

                self.type_ = self.type_ | types::ARRAY;

                hint
            }
            crate::Node::Bool(_) => {
                self.type_ = self.type_ | types::BOOLEAN;
                false
            }
            crate::Node::Bytes(_) => {
                self.type_ = self.type_ | types::STRING;

                let partial_stringshape = StringShape {
                    content_encoding: Some("base64".to_string()),
                    ..StringShape::new()
                };

                self.string = StringShape::union(self.string.clone(), partial_stringshape);
                false
            }
            crate::Node::Null => {
                self.type_ = self.type_ | types::NULL;
                false
            }
            crate::Node::Number(num) => {
                self.type_ = self.type_ | types::Set::for_number(&num);
                false
            }
            crate::Node::String(s) => {
                let is_first_time = !self.type_.overlaps(types::STRING);

                self.type_ = self.type_ | types::STRING;

                // Similar to the nuance around `is_required`, string format
                // should only "become detected" the very first time. We still
                // need to run `detect_format` on subsequent strings because
                // `StringShape::union()` can sometimes widen a string format,
                // e.g from `integer` -> `number`
                let format = if is_first_time || self.string.format.is_some() {
                    Format::detect(s)
                } else {
                    None
                };

                let partial_stringshape = StringShape {
                    format,
                    max_length: Some(s.len()),
                    min_length: s.len(),
                    ..StringShape::new()
                };

                if is_first_time {
                    self.string = partial_stringshape
                } else {
                    self.string = StringShape::union(self.string.clone(), partial_stringshape);
                }

                false
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    fn widening_snapshot_helper(
        initial_schema: Option<&str>,
        expected_schema: &str,
        docs: &[serde_json::Value],
    ) -> Shape {
        let mut schema = match initial_schema {
            Some(initial) => shape_from(initial),
            None => Shape::nothing(),
        };

        for doc in docs {
            schema.widen(doc, Location::Root);
        }

        let expected = shape_from(expected_schema);

        assert_eq!(expected, schema);

        schema
    }

    #[test]
    fn test_widening_explicit_fields() {
        // since additionalProperties:false, we need to recursively widen
        // each of the input fields adding new ones as required.
        widening_snapshot_helper(
            Some(
                r#"
            type: object
            additionalProperties: false
            properties:
                known:
                    type: string
            "#,
            ),
            r#"
            type: object
            additionalProperties: false
            properties:
                known:
                    type: string
                unknown:
                    type: string
                    minLength: 4
                    maxLength: 4
            "#,
            &[json!({"unknown": "test"})],
        );

        // we need to find and widen any `properties` explicitly matching input fields,
        // and otherwise widen `additionalProperties` where not matched.
        widening_snapshot_helper(
            Some(
                r#"
            type: object
            additionalProperties:
                type: string
                minLength: 1
                maxLength: 2
            properties:
                known:
                    type: string
            "#,
            ),
            r#"
            type: object
            additionalProperties:
                type: [string, integer]
                minLength: 1
                maxLength: 5
            properties:
                known:
                    type: [string, integer]
            "#,
            &[json!({"known": 5, "unknown": "pizza"}), json!({"foo": 5})],
        );
    }

    #[test]
    fn test_widening_pattern_properties() {
        // First widen explicit properties
        // Then widen matching pattern properties
        // only then widen additional properties
        widening_snapshot_helper(
            Some(
                r#"
            type: object
            additionalProperties:
                type: string
                minLength: 0
                maxLength: 0
            patternProperties:
                '^S_':
                    type: string
                    minLength: 0
                    maxLength: 0
                '^I_':
                    type: integer
                    minimum: 0
                    maximum: 0
            properties:
                known:
                    type: string
            "#,
            ),
            r#"
            type: object
            additionalProperties:
                type: string
                minLength: 0
                maxLength: 5
            patternProperties:
                '^S_':
                    type: string
                    minLength: 0
                    maxLength: 4
                '^I_':
                    type: integer
                    minimum: 0
                    maximum: 2
            properties:
                known:
                    type: [string, integer]
            "#,
            &[json!({"known": 5, "S_str_pattern": "test", "I_int_pattern": 2, "unknown": "pizza"})],
        );
    }

    #[test]
    fn test_widening_string_format() {
        // Should detect format the first time
        widening_snapshot_helper(
            None,
            r#"
            type: string
            format: integer
            maxLength: 1
            minLength: 1
            "#,
            &[json!("5")],
        );

        // Should widen from integer to number
        widening_snapshot_helper(
            Some(
                r#"
            type: string
            format: integer
            maxLength: 1
            minLength: 1
            "#,
            ),
            r#"
                    type: string
                    format: number
                    maxLength: 3
                    minLength: 1
                    "#,
            &[json!("5.4")],
        );

        // Once we encounter a string that doesn't match the format, throw it away
        widening_snapshot_helper(
            Some(
                r#"
            type: string
            format: integer
            maxLength: 1
            minLength: 1
            "#,
            ),
            r#"
            type: string
            maxLength: 5
            minLength: 1
            "#,
            &[json!("pizza")],
        );

        // And don't re-infer it ever again
        widening_snapshot_helper(
            Some(
                r#"
            type: string
            maxLength: 5
            minLength: 1
            "#,
            ),
            r#"
            type: string
            maxLength: 5
            minLength: 1
            "#,
            &[json!("5")],
        );
    }

    #[test]
    fn test_widening_from_scratch() {
        widening_snapshot_helper(
            None,
            r#"
            type: object
            additionalProperties: false
            required: [test_key]
            properties:
                test_key:
                    type: object
                    additionalProperties: false
                    required: [test_nested]
                    properties:
                        test_nested:
                            type: string
                            minLength: 5
                            maxLength: 5
            "#,
            &[json!({"test_key": {"test_nested": "pizza"}})],
        );
    }

    #[test]
    fn test_widening_required_properties() {
        // Fields introduced from scratch should be required
        widening_snapshot_helper(
            None,
            r#"
            type: object
            additionalProperties: false
            required: [first_key]
            properties:
                first_key:
                    type: string
                    minLength: 5
                    maxLength: 5
            "#,
            &[json!({"first_key": "hello"})],
        );
        // Fields encountered after the first should not be required
        // AND required fields should stay required, so long as they
        // are always encountered
        widening_snapshot_helper(
            Some(
                r#"
            type: object
            additionalProperties: false
            required: [first_key]
            properties:
                first_key:
                    type: string
            "#,
            ),
            r#"
            type: object
            additionalProperties: false
            required: [first_key]
            properties:
                first_key:
                    type: string
                second_key:
                    type: string
                    minLength: 7
                    maxLength: 7
            "#,
            &[json!({"first_key": "hello", "second_key": "goodbye"})],
        );
        // Required fields get demoted once we encounter a document
        // where they are not present
        widening_snapshot_helper(
            Some(
                r#"
            type: object
            additionalProperties: false
            required: [first_key]
            properties:
                first_key:
                    type: string
                second_key:
                    type: string
            "#,
            ),
            r#"
            type: object
            additionalProperties: false
            properties:
                first_key:
                    type: string
                second_key:
                    type: string
            "#,
            &[json!({"second_key": "goodbye"})],
        );
    }

    // Widening with an object that already fully matches should have no effect
    #[test]
    fn test_widening_noop() {
        let schema = r#"
            type: object
            additionalProperties: false
            required: [test_key]
            properties:
                test_key:
                    type: object
                    additionalProperties: false
                    required: [test_nested]
                    properties:
                        test_nested:
                            type: string
            "#;
        widening_snapshot_helper(
            Some(schema),
            schema,
            &[json!({"test_key": {"test_nested": "pizza"}})],
        );
    }

    // Widening with an object that doesn't match should widen
    #[test]
    fn test_widening_nested_expansion() {
        let schema = r#"
                type: object
                additionalProperties: false
                required: [test_key]
                properties:
                    test_key:
                        type: object
                        additionalProperties: false
                        required: [test_nested]
                        properties:
                            test_nested:
                                type: string
                "#;
        widening_snapshot_helper(
            Some(schema),
            r#"
                type: object
                additionalProperties: false
                required: [test_key]
                properties:
                    test_key:
                        type: object
                        additionalProperties: false
                        properties:
                            test_nested:
                                type: string
                            nested_second:
                                type: integer
                "#,
            &[json!({"test_key": {"nested_second": 68}})],
        );
    }

    // Widening a shape that has additionalProperties set should widen `additionalProperties` instead
    #[test]
    fn test_widening_additional_properties_noop() {
        let schema = r#"
                type: object
                additionalProperties:
                    type: string
                "#;
        widening_snapshot_helper(
            Some(schema),
            schema,
            &[
                json!({"test_key": "a_string"}),
                json!({"toast_key": "another_string"}),
            ],
        );
    }

    #[test]
    fn test_widening_additional_properties_type() {
        let schema = r#"
                type: object
                additionalProperties:
                    type: string
                "#;
        widening_snapshot_helper(
            Some(schema),
            r#"
            type: object
            additionalProperties:
                type: [string, integer]
            "#,
            &[json!({"test_key": "a_string"}), json!({"toast_key": 5})],
        );
    }

    #[test]
    fn test_widening_type_expansion() {
        let schema = r#"
                type: object
                additionalProperties: false
                properties:
                    test_key:
                        type: object
                        additionalProperties: false
                        properties:
                            test_nested:
                                type: string
                "#;
        widening_snapshot_helper(
            Some(schema),
            r#"
                type: object
                additionalProperties: false
                properties:
                    test_key:
                        type: object
                        additionalProperties: false
                        properties:
                            test_nested:
                                type: [string, integer]
                "#,
            &[json!({"test_key": {"test_nested": 68}})],
        );
    }

    #[test]
    fn test_widening_arrays() {
        widening_snapshot_helper(
            None,
            r#"
                type: array
                minItems: 2
                maxItems: 2
                items:
                    type: string
                    minLength: 4
                    maxLength: 5
                "#,
            &[json!(["test", "toast"])],
        );

        widening_snapshot_helper(
            None,
            r#"
                type: array
                minItems: 2
                maxItems: 2
                items:
                    anyOf:
                        - type: string
                          minLength: 4
                          maxLength: 4
                        - type: integer
                "#,
            &[json!(["test", 5])],
        );

        widening_snapshot_helper(
            Some(
                r#"
            type: array
            minItems: 0
            maxItems: 1
            items:
                type: string
                minLength: 4
                maxLength: 5
            "#,
            ),
            r#"
                type: array
                minItems: 0
                maxItems: 2
                items:
                    type: string
                    minLength: 4
                    maxLength: 5
                "#,
            &[json!(["test", "toast"])],
        );
    }

    #[test]
    fn test_widening_arrays_into_object() {
        widening_snapshot_helper(
            None,
            r#"
                anyOf:
                    - type: array
                      minItems: 2
                      maxItems: 2
                      items:
                          anyOf:
                            - type: integer
                            - type: string
                              minLength: 4
                              maxLength: 4
                    - type: object
                      additionalProperties: false
                      required: [test_key]
                      properties:
                        test_key:
                            type: integer
                "#,
            &[json!(["test", 5]), json!({"test_key": 5})],
        );

        widening_snapshot_helper(
            None,
            r#"
                anyOf:
                    - type: array
                      minItems: 2
                      maxItems: 3
                      items:
                          anyOf:
                            - type: fractional
                            - type: string
                              minLength: 4
                              maxLength: 4
                    - type: object
                      additionalProperties: false
                      properties:
                        test_key:
                            type: integer
                        toast_key:
                            type: integer
                "#,
            &[
                json!(["test", 5.2]),
                json!(["test", 5.2, 3.2]),
                json!({"test_key": 5}),
                json!({"toast_key": 5}),
            ],
        );
    }
}
