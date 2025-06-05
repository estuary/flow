// This module defines a "widen" operation which widens the constraints
// of a Shape as needed, to allow a given document to properly validate.
// It's used as a base operation for schema inference.
use super::*;
use crate::{AsNode, Node, OwnedNode};
use std::cmp::Ordering;

impl StringShape {
    fn widen(&mut self, val: &str, is_first: bool) -> bool {
        let chars_count = val.chars().count();
        if is_first {
            let (min, max) = length_bounds(chars_count);
            self.format = Format::detect(val);
            self.max_length = Some(max);
            self.min_length = min;
            // TODO(johnny): detect base64?
            return true;
        }

        let mut changed = false;

        match &self.format {
            None => {}
            Some(lhs) if lhs.validate(val).is_ok() => {}

            Some(Format::Integer) if Format::Number.validate(val).is_ok() => {
                self.format = Some(Format::Number); // Widen integer => number.
                changed = true;
            }
            _ => {
                self.format = None;
                changed = true;
            }
        }

        if self.min_length as usize > chars_count {
            self.min_length = length_bounds(chars_count).0;
            changed = true;
        }

        if let Some(max) = &mut self.max_length {
            if (*max as usize) < chars_count {
                *max = length_bounds(chars_count).1;
                changed = true;
            }
        }

        changed
    }
}

impl ObjShape {
    /// See [`Shape::widen()`] for details on the order of widening.
    fn widen<'n, N>(&mut self, fields: &'n N::Fields, is_first: bool) -> bool
    where
        N: AsNode + 'n,
    {
        use crate::{Field, Fields};

        // on_unknown_property closes over `new_fields` to enqueue
        // properties which will be added to this ObjShape.
        let mut new_fields = Vec::new();

        let mut on_unknown_property =
            |rhs: &<<N as AsNode>::Fields as Fields<N>>::Field<'n>| -> bool {
                // Is there a pattern-property that covers `rhs`? If so, widen it.
                // TODO(johnny): Technically this should iterate over _all_ matching
                // patterns, and then return whether _any_ of them were updated.
                if let Some(pattern) = self
                    .pattern_properties
                    .iter_mut()
                    .find(|pattern| pattern.re.is_match(rhs.property()))
                {
                    return pattern.shape.widen(rhs.value());
                }

                match &mut self.additional_properties {
                    // If `additionalProperties` is unset, its default is the "true" schema
                    // which accepts any JSON document. No need to widen further.
                    None => return false,
                    // If `additionalProperties` is an explicit schema _other_ than "false", widen it.
                    Some(additional) if additional.type_ != types::INVALID => {
                        return additional.widen(rhs.value());
                    }
                    // `additionalProperties` is "false". Fall through to add a new property.
                    _ => (),
                }

                let mut shape = Shape::nothing();
                _ = shape.widen(rhs.value());

                new_fields.push(ObjProperty {
                    name: rhs.property().into(),
                    // A field can only be required if every single document we've seen
                    // has that field present. This means that ONLY fields that exist
                    // on the very first object we encounter for a particular location should
                    // get marked as required.
                    is_required: is_first,
                    shape,
                });

                true
            };

        let mut changed = is_first;
        let mut lhs_it = self.properties.iter_mut();
        let mut rhs_it = fields.iter();
        let mut maybe_lhs = lhs_it.next();
        let mut maybe_rhs = rhs_it.next();

        // Perform an ordered merge over `lhs_it` and `rhs_it`.
        // This loop structure is much faster than Itertools::merge_join_by.
        loop {
            match (&mut maybe_lhs, &maybe_rhs) {
                (Some(lhs), Some(rhs)) => match lhs.name.as_ref().cmp(rhs.property()) {
                    Ordering::Equal => {
                        // Both the Shape and `fields` have this property.
                        changed |= lhs.shape.widen(rhs.value());
                        maybe_lhs = lhs_it.next();
                        maybe_rhs = rhs_it.next();
                    }
                    Ordering::Less => {
                        // Shape has a property that the node is missing.
                        if lhs.is_required {
                            lhs.is_required = false;
                            changed = true;
                        }
                        maybe_lhs = lhs_it.next();
                    }
                    Ordering::Greater => {
                        changed |= on_unknown_property(rhs);
                        maybe_rhs = rhs_it.next();
                    }
                },
                (Some(lhs), None) => {
                    // Shape has a property that the node is missing.
                    if lhs.is_required {
                        lhs.is_required = false;
                        changed = true;
                    }
                    maybe_lhs = lhs_it.next();
                }
                (None, Some(rhs)) => {
                    changed |= on_unknown_property(rhs);
                    maybe_rhs = rhs_it.next();
                }
                (None, None) => break,
            }
        }

        // Add any `new_fields` to properties, maintaining the sort-by-property invariant.
        // By construction, properties of `new_fields` don't already exist in `self.properties`.
        if !new_fields.is_empty() {
            self.properties.extend(new_fields.into_iter());
            self.properties.sort_by(|a, b| a.name.cmp(&b.name))
        }

        changed
    }
}

impl ArrayShape {
    fn widen<'n, N>(&mut self, items: &'n [N], is_first: bool) -> bool
    where
        N: AsNode,
    {
        let mut changed = false;

        // First widen any tuple item shapes.
        for (ind, shape) in self.tuple.iter_mut().enumerate() {
            changed |= shape.widen(&items[ind]);
        }
        // Then widen an additional item shape.
        if let Some(additional) = &mut self.additional_items {
            for rhs in items.iter().skip(self.tuple.len()) {
                changed |= additional.widen(rhs);
            }
        }

        if is_first {
            let (min, max) = length_bounds(items.len());
            self.max_items = Some(max);
            self.min_items = min;
            changed = true;
        } else {
            if self.min_items as usize > items.len() {
                self.min_items = length_bounds(items.len()).0;
                changed = true;
            }
            if let Some(max) = &mut self.max_items {
                if (*max as usize) < items.len() {
                    *max = length_bounds(items.len()).1;
                    changed = true;
                }
            }
        }

        changed
    }
}

impl NumericShape {
    fn widen(&mut self, num: json::Number, is_first: bool) -> bool {
        let mut changed = is_first;

        // We confirm minimum and maximum are None because INTEGER and FRACTIONAL
        // are separate types and will result in is_first being true,
        // even though they're min / maxed using the same ring.
        if is_first && self.minimum.is_none() && self.maximum.is_none() {
            let (min, max) = number_bounds(num);
            self.minimum = Some(min);
            self.maximum = Some(max);
            changed = true;
        } else {
            if let Some(min) = &mut self.minimum {
                if *min > num {
                    *min = number_bounds(num).0;
                    changed = true;
                }
            }
            if let Some(max) = &mut self.maximum {
                if *max < num {
                    *max = number_bounds(num).1;
                    changed = true;
                }
            }
        }

        changed
    }
}

impl Shape {
    /// Minimally widen the Shape so the provided document will successfully validate.
    /// Returns an indication of whether this or a sub-Shape was modified to fit this document.
    ///
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
    /// If an ArrayShape already defines a tuple, its indexed elements are widened,
    /// with any additional items widen its `additionalItems` schema.
    ///
    /// Scalar values are widened along whatever dimensions exist: string formats and lengths,
    /// number ranges, etc.
    ///
    /// The approximate lengths of arrays and strings are attached to widened Shapes at
    /// power-of-two boundaries. Numeric ranges are also attached, at order-of-magnitude
    /// (10x) boundaries.
    pub fn widen<'n, N>(&mut self, node: &'n N) -> bool
    where
        N: AsNode,
    {
        use json::Number;

        if let Some(_) = &self.enum_ {
            return self.widen_enum(node);
        }

        // is_first doesn’t mean “the first time i’ve seen this location”,
        // it’s “the first time i’ve seen this location with this type”
        let mut apply_type = |type_| -> bool {
            if self.type_ & type_ != type_ {
                self.type_ = self.type_ | type_;
                true
            } else {
                false
            }
        };

        match node.as_node() {
            Node::Array(items) => {
                let is_first = apply_type(types::ARRAY);
                if is_first {
                    self.array.additional_items = Some(Box::new(Shape::nothing()));
                }
                self.array.widen(items, is_first)
            }
            Node::Bool(_) => apply_type(types::BOOLEAN),
            Node::Bytes(_) => panic!("not implemented"),
            Node::Null => apply_type(types::NULL),
            Node::Float(f) => self.numeric.widen(
                Number::Float(f),
                apply_type(
                    if f.fract() != 0.0 || f > u64::MAX as f64 || f < i64::MIN as f64 {
                        types::INT_OR_FRAC // Equivalent to "type: number".
                    } else {
                        types::INTEGER
                    },
                ),
            ),
            Node::PosInt(f) => self
                .numeric
                .widen(Number::Unsigned(f), apply_type(types::INTEGER)),
            Node::NegInt(f) => self
                .numeric
                .widen(Number::Signed(f), apply_type(types::INTEGER)),
            Node::Object(fields) => {
                let is_first = apply_type(types::OBJECT);
                if is_first {
                    self.object.additional_properties = Some(Box::new(Shape::nothing()));
                }
                self.object.widen::<N>(fields, is_first)
            }
            Node::String(s) => self.string.widen(s, apply_type(types::STRING)),
        }
    }

    #[inline]
    pub fn widen_owned(&mut self, node: &OwnedNode) -> bool {
        match node {
            OwnedNode::Heap(n) => self.widen(n.get()),
            OwnedNode::Archived(n) => self.widen(n.get()),
        }
    }

    #[cold]
    #[inline(never)]
    fn widen_enum<'n, N>(&mut self, node: &'n N) -> bool
    where
        N: AsNode,
    {
        let Some(enums) = self.enum_.as_mut() else {
            unreachable!("enum must be Some")
        };

        return match (
            enums.binary_search_by(|lhs| crate::compare(lhs, node)),
            node.as_node(),
        ) {
            // Exact match.
            (Ok(_index), _) => false,

            // Insert new string enum value.
            // TODO(johnny): Support other scalars?
            (Err(index), Node::String(str)) => {
                enums.insert(index, serde_json::json!(str));
                true
            }
            // Remove `enums` and fold into Shape.
            (Err(_), _) => {
                let enums = self.enum_.take().unwrap();

                for enum_ in enums {
                    self.widen(&enum_);
                }
                self.widen(node);

                true
            }
        };
    }
}

// Compute a lower and upper power-of-two bound for a given length.
#[cold]
fn length_bounds(l: usize) -> (u32, u32) {
    let l = u32::try_from(l).unwrap_or(u32::MAX);

    if l == 0 {
        (0, 0)
    } else if l.is_power_of_two() {
        (l >> 1, l)
    } else if let Some(b) = l.checked_next_power_of_two() {
        (b >> 1, b)
    } else {
        (1 << 31, u32::MAX)
    }
}

// Compute a lower and upper order-of-magnitude bound for the given number.
#[cold]
fn number_bounds(num: json::Number) -> (json::Number, json::Number) {
    use json::Number;

    match num {
        // Positive integers.
        Number::Unsigned(n) if n > 0 => {
            let e = n.ilog10();
            (
                Number::Unsigned(10u64.pow(e)),
                Number::Unsigned(10u64.checked_pow(e + 1).unwrap_or(u64::MAX)),
            )
        }
        Number::Signed(n) if n >= 0 => unreachable!("invalid Number::Signed (should be negative)"),
        // Floats >= 1.0.
        Number::Float(f) if f >= 1.0 => {
            let e = f.log10() as i32;
            let u = 10f64.powi(e + 1);
            (
                Number::Float(10f64.powi(e)),
                Number::Float(if u.is_finite() { u } else { f64::MAX }),
            )
        }
        // Floats between (0.0, 1.0).
        Number::Float(f) if f > 0.0 => (Number::Float(0.0), Number::Float(1.0)),
        // Floats between (-1.0, 0.0).
        Number::Float(f) if f > -1.0 && f < 0.0 => (Number::Float(-1.0), Number::Float(0.0)),
        // Floats <= -1.0.
        Number::Float(f) if f <= -1.0 => {
            let e = (-f).log10() as i32;
            let l = -(10f64.powi(e + 1));
            (
                Number::Float(if l.is_finite() { l } else { f64::MIN }),
                Number::Float(-(10f64.powi(e))),
            )
        }
        // Negative integers.
        Number::Signed(n) => {
            let e = n.saturating_neg().ilog10();
            (
                Number::Signed(
                    10i64
                        .checked_pow(e + 1)
                        .map(i64::saturating_neg)
                        .unwrap_or(i64::MIN),
                ),
                Number::Signed(-(10i64.pow(e))),
            )
        }
        // Zero.
        Number::Unsigned(_) => (Number::Unsigned(0), Number::Unsigned(0)),
        Number::Float(_) => (Number::Float(0.0), Number::Float(0.0)),
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
        docs: &[(bool, serde_json::Value)],
    ) -> Shape {
        let mut schema = match initial_schema {
            Some(initial) => shape_from(initial),
            None => Shape::nothing(),
        };

        for (expect_changed, doc) in docs {
            assert_eq!(*expect_changed, schema.widen(doc));
        }
        let expected = shape_from(expected_schema);

        assert_eq!(expected, schema);

        schema
    }

    // Cases detected by quickcheck:
    #[test]
    fn test_unicode_multibyte_widening() {
        widening_snapshot_helper(
            None,
            r#"
            type: string
            minLength: 0
            maxLength: 1"#,
            &[(true, json!("ࠀ"))],
        );
    }
    #[test]
    fn test_widening_floats() {
        widening_snapshot_helper(
            None,
            r#"
            type: array
            minItems: 1
            maxItems: 2
            items:
                type: number
                minimum: 0
                maximum: 100000000
            "#,
            &[(true, json!([0.0, 71113157.14749053]))],
        );
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
                    minLength: 2
                    maxLength: 4
            "#,
            &[(true, json!({"unknown": "test"}))],
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
                maxLength: 8
                minimum: 1
                maximum: 10
            properties:
                known:
                    type: [string, integer]
                    minimum: 1
                    maximum: 10
            "#,
            &[
                (true, json!({"known": 5, "unknown": "pizza"})),
                (false, json!({"foo": "pie"})),
                (true, json!({"bar": 9})),
            ],
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
                maxLength: 8
            patternProperties:
                '^S_':
                    type: string
                    minLength: 0
                    maxLength: 4
                '^I_':
                    type: integer
                    minimum: 0
                    maximum: 10
            properties:
                known:
                    type: [string, integer]
                    minimum: 1
                    maximum: 10
            "#,
            &[(
                true,
                json!({"known": 5, "S_str_pattern": "test", "I_int_pattern": 7, "unknown": "pizza"}),
            )],
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
            minLength: 0
            "#,
            &[(true, json!("5"))],
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
            maxLength: 4
            minLength: 1
            "#,
            &[(true, json!("5.4"))],
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
            maxLength: 8
            minLength: 1
            "#,
            &[(true, json!("pizza"))],
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
            &[(false, json!("5"))],
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
                            minLength: 4
                            maxLength: 8
            "#,
            &[(true, json!({"test_key": {"test_nested": "pizza"}}))],
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
                    minLength: 4
                    maxLength: 8
            "#,
            &[(true, json!({"first_key": "hello"}))],
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
                    minLength: 4
                    maxLength: 8
            "#,
            &[(true, json!({"first_key": "hello", "second_key": "goodbye"}))],
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
            &[(true, json!({"second_key": "goodbye"}))],
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
            &[(false, json!({"test_key": {"test_nested": "pizza"}}))],
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
                                minimum: 10
                                maximum: 100
                "#,
            &[(true, json!({"test_key": {"nested_second": 68}}))],
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
                (false, json!({"test_key": "a_string"})),
                (false, json!({"toast_key": "another_string"})),
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
                minimum: 1
                maximum: 10
            "#,
            &[
                (false, json!({"test_key": "a_string"})),
                (true, json!({"toast_key": 5})),
            ],
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
                            minimum: 10
                            maximum: 100
            "#,
            &[(true, json!({"test_key": {"test_nested": 68}}))],
        );
    }

    #[test]
    fn test_widening_arrays() {
        widening_snapshot_helper(
            None,
            r#"
                type: array
                minItems: 1
                maxItems: 2
                items:
                    type: string
                    minLength: 2
                    maxLength: 8
                "#,
            &[(true, json!(["test", "toast"]))],
        );

        widening_snapshot_helper(
            None,
            r#"
                type: array
                minItems: 1
                maxItems: 2
                items:
                    anyOf:
                        - type: string
                          minLength: 2
                          maxLength: 4
                        - type: integer
                          minimum: 1
                          maximum: 10
                "#,
            &[(true, json!(["test", 5]))],
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
                maxItems: 4
                items:
                    type: string
                    minLength: 2
                    maxLength: 16
                "#,
            &[(true, json!(["test", "toast", "tin", "todotodo!"]))],
        );
    }

    #[test]
    fn test_widening_arrays_into_object() {
        widening_snapshot_helper(
            None,
            r#"
                anyOf:
                    - type: array
                      minItems: 1
                      maxItems: 2
                      items:
                          anyOf:
                            - type: integer
                              minimum: 1
                              maximum: 10
                            - type: string
                              minLength: 2
                              maxLength: 4
                    - type: object
                      additionalProperties: false
                      required: [test_key]
                      properties:
                        test_key:
                            type: integer
                            minimum: 1
                            maximum: 10
                "#,
            &[(true, json!(["test", 5])), (true, json!({"test_key": 5}))],
        );

        widening_snapshot_helper(
            None,
            r#"
                anyOf:
                    - type: array
                      minItems: 1
                      maxItems: 4
                      items:
                          anyOf:
                            - type: number
                              minimum: 1.0
                              maximum: 10.0
                            - type: string
                              minLength: 2
                              maxLength: 4
                    - type: object
                      additionalProperties: false
                      properties:
                        test_key:
                            type: integer
                            minimum: 1
                            maximum: 10
                        toast_key:
                            type: integer
                            minimum: 1
                            maximum: 10
                "#,
            &[
                (true, json!(["test", 5.2])),
                (true, json!(["test", 5.2, 3.2])),
                (true, json!({"test_key": 5})),
                (true, json!({"toast_key": 5})),
            ],
        );
    }

    #[test]
    fn test_widening_enums() {
        let schema = r#"
            enum: [one, three, 32, {a: 1}]
        "#;
        // We preserve an enum if it's only widened with strings and exact matches.
        widening_snapshot_helper(
            Some(schema),
            r#"
            enum: [one, two, three, four, 32, {a: 1}]
            "#,
            &[
                (false, json!("one")),
                (true, json!("two")),
                (false, json!("three")),
                (true, json!("four")),
                (false, json!("two")),
                (false, json!(32)),
                (false, json!({"a": 1})),
            ],
        );

        // We collapse the enum if it's widened with a non-string.
        widening_snapshot_helper(
            Some(schema),
            r#"
            type: [string, integer, object]
            "#,
            &[
                (true, json!("hello")),
                (false, json!("one")),
                (true, json!({"a": 5})),
            ],
        );
    }

    #[test]
    fn test_widening_tuples() {
        let schema = r#"
            type: array
            items:
                - false
                - false
            minItems: 1
            maxItems: 2
            additionalItems: false
        "#;
        widening_snapshot_helper(
            Some(schema),
            r#"
            type: array
            items:
                - type: string
                  minLength: 2
                  maxLength: 16
                - type: integer
                  minimum: 1
                  maximum: 1000
            minItems: 1
            maxItems: 4
            additionalItems:
                type: boolean
            "#,
            &[
                (true, json!(["one", 1])),
                (true, json!(["one hundred", 100])),
                (false, json!(["thirty two", 32])),
                (true, json!(["extra", 7, true])), // Updates additionalItems.
            ],
        );
    }

    #[test]
    fn test_length_bounds() {
        assert_eq!(length_bounds(0), (0, 0));
        assert_eq!(length_bounds(1), (0, 1));
        assert_eq!(length_bounds(2), (1, 2));
        assert_eq!(length_bounds(3), (2, 4));
        assert_eq!(length_bounds(4), (2, 4));
        assert_eq!(length_bounds(5), (4, 8));
        assert_eq!(length_bounds(7), (4, 8));
        assert_eq!(length_bounds(8), (4, 8));
        assert_eq!(length_bounds(9), (8, 16));
        assert_eq!(length_bounds((1 << 30) - 1), (1 << 29, 1 << 30));
        assert_eq!(length_bounds((1 << 30) + 0), (1 << 29, 1 << 30));
        assert_eq!(length_bounds((1 << 30) + 1), (1 << 30, 1 << 31));
        assert_eq!(length_bounds((1 << 31) + 0), (1 << 30, 1 << 31));
        assert_eq!(length_bounds((1 << 31) + 1), (1 << 31, u32::MAX));
        assert_eq!(length_bounds((1 << 33) + 1), (1 << 31, u32::MAX)); // Saturates as u32::MAX.
    }

    #[test]
    fn test_widening_numeric() {
        // Integers and non-fractional floats are `type: integer`.
        widening_snapshot_helper(
            None,
            r#"
            type: integer
            minimum: 1
            maximum: 100
            "#,
            &[
                (true, json!(3)),
                (false, json!(4)),
                (false, json!(5.0)),
                (true, json!(30)),
                (false, json!(99.0)),
            ],
        );

        // A fractional float widens to `type: number`.
        widening_snapshot_helper(
            None,
            r#"
            type: number
            minimum: -10
            maximum: -1
            "#,
            &[
                (true, json!(-3)),
                (false, json!(-4)),
                (true, json!(-4.5)),
                (false, json!(-7.1)),
                (false, json!(-8)),
            ],
        );

        // Non-fractional floats which are within the bounds of u64/i64 continue to be integers.
        widening_snapshot_helper(
            None,
            r#"
            type: integer
            minimum: -1e19
            maximum: 1e20
            "#,
            &[
                (true, json!(1)),
                (true, json!(u64::MAX as f64)),
                (true, json!(i64::MIN as f64)),
            ],
        );

        // However, they widen to number if they exceed what a native integer can represent.
        widening_snapshot_helper(
            None,
            r#"
            type: number
            minimum: 1
            maximum: 1e20
            "#,
            &[(true, json!(1)), (true, json!(u64::MAX as f64 + 1e10))],
        );
        widening_snapshot_helper(
            None,
            r#"
            type: number
            minimum: -1e19
            maximum: 0
            "#,
            &[(true, json!(0)), (true, json!(i64::MIN as f64 - 1e10))],
        );
    }

    #[test]
    fn test_number_bounds() {
        use json::Number;

        let cases: Vec<(serde_json::Number, serde_json::Number, serde_json::Number)> =
            serde_json::from_value(json!([
                // Zero cases.
                [0, 0, 0],
                [0.0, 0.0, 0.0],
                // Positive cases.
                [0.001, 0.0, 1.0],
                [0.999, 0.0, 1.0],
                [1, 1, 10],
                [1.001, 1.0, 10.0],
                [5, 1, 10],
                [5.5, 1.0, 10.0],
                [9, 1, 10],
                [9.9, 1.0, 10.0],
                [10, 10, 100],
                [10.1, 10.0, 100.0],
                [101, 100, 1_000],
                [101.1, 100.0, 1_000.0],
                [8_675_309, 1_000_000, 10_000_000],
                [8_675_309.5, 1_000_000.0, 10_000_000.0],
                [u64::MAX - 100, 10000000000000000000u64, u64::MAX],
                [u64::MAX as f64 + 1.0, 1e19, 1e20],
                [5e31, 1e31, 1e32],
                // Negative cases.
                [-5e31, -1e32, -1e31],
                [i64::MIN as f64 - 1.0, -1e19, -1e18],
                [i64::MIN + 1, i64::MIN, -1000000000000000000i64],
                [-8_675_309.5, -10_000_000.0, -1_000_000.0],
                [-8_675_309, -10_000_000, -1_000_000],
                [-101.1, -1_000.0, -100.0],
                [-101, -1_000, -100],
                [-10, -100, -10],
                [-10.1, -100.0, -10.0],
                [-9.9, -10.0, -1.0],
                [-9, -10, -1],
                [-5.5, -10.0, -1.0],
                [-1.001, -10.0, -1.0],
                [-1.00, -10, -1],
                [-0.999, -1.0, 0.0],
                [-0.001, -1.0, 0.0],
            ]))
            .unwrap();

        for (given, expect_min, expect_max) in cases.iter() {
            let given: Number = given.into();
            let expect: (Number, Number) = (expect_min.into(), expect_max.into());

            assert_eq!(number_bounds(given), expect, "number bounds of: {}", given);
        }
    }
}
